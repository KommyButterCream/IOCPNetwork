#include "ClientSession.h"

#include "SessionJobQueue.h"
#include "../Buffer/RecvPacketBuffer.h"
#include "../Buffer/SendPacketQueue.h"
#include "../Diagnostics/EngineAssert.h"
#include "../Protocol/PacketHeader.h"
#include "../../Core/Util/Logger.h"
#include <utility> // for std::move

using namespace Core::Util;

namespace
{
	// 상대가 연결을 끊어서 발생한 I/O 실패인지 판정한다.
	// 이건 정상적인 종료 경로이므로 ERROR 로 올리면 진짜 문제가 묻힌다.
	bool IsPeerClosedError(int errorCode)
	{
		switch (errorCode)
		{
		case WSAECONNRESET:         // 상대가 강제 종료 (RST)
		case WSAECONNABORTED:       // 연결 중단
		case WSAENOTCONN:           // 이미 끊김
		case WSAESHUTDOWN:          // 송수신 불가
		case ERROR_NETNAME_DELETED: // 네트워크 이름 삭제 = 연결 끊김
		case ERROR_OPERATION_ABORTED: // CancelIoEx 로 인한 정상 취소
			return true;
		default:
			return false;
		}
	}
}

RecvPacketBuffer& ClientSession::GetReceiveBuffer()
{
	return *m_recvPacketBuffer;
}

// 백프레셔의 전체 그림
//
//   생산자 : 수신 완료 핸들러. 패킷을 파싱해 잡 큐에 넣고, 마지막에
//            다음 수신을 건다. 그 자리가 여기(PostReceiveOrPause)다.
//   소비자 : 잡 워커. 큐를 드레인하고 마지막에 ResumeReceiveIfDrained 를 부른다.
//
// 생산자는 자기 자신을 멈출 수 있다. 수신을 걸지 않으면 커널 수신 버퍼가
// 차고 TCP 수신 윈도가 0 으로 닫혀서 보내는 쪽이 막힌다. 우리가 아무것도
// 하지 않는 것이 곧 제어 신호다.
//
// 유실 깨움
//   이 구조의 고전적인 실패는 "생산자가 멈추기로 정한 직후, 표시하기 전에
//   소비자가 큐를 다 비우는" 것이다. 소비자는 아직 0 인 플래그를 보고 깨울
//   일이 없다고 판단하고, 그 뒤에 생산자가 1 을 쓴다. 그 세션은 영원히
//   조용해진다. m_sending / m_processing 이 같은 부류의 문제를 이미 겪었다.
//
//   그래서 순서를 뒤집는다 — 먼저 표시하고, 그 다음에 다시 확인한다.
//   표시 이후에 드레인이 끝나면 소비자가 플래그를 보고 깨워 준다.
//   표시 이전에 이미 끝났다면 아래의 재확인이 잡는다. 둘 중 하나는 반드시
//   성립하고, 둘 다 성립해도 CAS 가 하나만 고른다.
bool ClientSession::PostReceiveOrPause()
{
	const uint32_t pauseLevel = m_bufferConfig.recvPauseJobDepth;

	// 0 은 백프레셔를 쓰지 않는다는 뜻이다. 이 경로가 없던 때와 완전히 같다.
	if (pauseLevel == 0 || GetJobQueueDepth() < pauseLevel)
	{
		if (PostReceive())
			return true;

		// 취소가 이미 걸려 있으면 부르는 쪽이 할 일이 없다.
		//
		// 아래 멈춤 경로는 이 구분을 처음부터 하고 있었다(BeginIO 가 false 면
		// true 를 돌려준다). 이 경로만 빠져 있어서, 정리가 이미 진행 중인
		// 세션마다 "failed to post the next recv" 를 ERROR 로 남기고 반납을
		// 한 번 더 요청했다. 그 요청은 poolState CAS 에 걸려 "release skipped"
		// 로 끝나므로 동작은 옳았지만, 실측 bench 1회에 252줄이 그 오탐이었다.
		// 진짜 실패(링이 가득 참 / 접속이 끊김)가 그 안에 묻힌다.
		//
		// 취소가 아닌 실패는 그대로 false 다. 그때는 정말로 아무도 이 세션을
		// 정리하고 있지 않으므로 부르는 쪽이 반납해야 한다.
		return IsIOCancelRequested();
	}

	// --- 여기부터 멈춤 ---
	//
	// 카운트를 하나 잡고 들어간다. 멈춘 세션은 미완료 I/O 가 하나도 없으므로,
	// 이 카운트가 없으면 예약된 반납이 곧바로 마무리되어 세션이 풀로 돌아간다.
	// 그 상태에서 워커가 재개하러 오면 남의 접속이 된 세션에 수신을 건다.
	//
	// 카운트를 들고 있으면 반납은 예약 상태로 머물고, 재개하는 쪽이 카운트를
	// 내려놓는 순간 정상적으로 마무리된다.
	if (!BeginIO())
	{
		// 취소가 이미 걸렸다. 수신을 걸 일도, 멈출 일도 없다.
		// 부르는 쪽(HandleRecv)이 자기 몫의 카운트를 아직 들고 있으므로
		// 세션이 발밑에서 사라지지는 않는다.
		return true;
	}

	::InterlockedExchange(&m_recvPaused, 1);
	m_diagnostics.recvPauseCount.fetch_add(1, std::memory_order_relaxed);

	// 표시 이후의 깊이를 한 번만 읽어서 로그와 판단에 같이 쓴다.
	//
	// 이 읽기는 반드시 표시 뒤여야 한다. 앞에서 읽은 값을 재사용하면
	// 재확인이 성립하지 않는다 — 표시와 드레인 완료의 순서를 가리는 것이
	// 이 읽기의 목적이기 때문이다.
	//
	// 읽기 한 번에 큐 락을 한 번 잡으므로 두 번 읽지 않는다. 수위가 얕게
	// 잡힌 설정에서는 이 경로가 초당 천 번대로 돈다.
	const uint32_t depthAfterMark = GetJobQueueDepth();

	LOGI("session %u pausing recv : job queue depth %u reached the pause level %u",
		GetSessionID(), depthAfterMark, pauseLevel);

	// 표시 이전에 워커가 큐를 다 비웠을 수 있다. 그러면 깨워 줄 사람이
	// 없으므로 여기서 직접 재개한다. (위 "유실 깨움" 주석)
	if (depthAfterMark > m_bufferConfig.recvResumeJobDepth)
	{
		// 멈춘 채로 둔다. 카운트는 재개하는 쪽이 소비한다.
		return true;
	}

	ResumeReceive();

	// 여기서 true 를 돌려주는 것은 "부르는 쪽은 세션을 반납하지 말라" 는 뜻이다.
	//
	// ResumeReceive 안의 PostReceive 가 실패하면 그쪽이 이미 NotifyDisconnect 로
	// 반납을 요청했다. 게다가 그 뒤의 DecrementIO 이후로는 세션을 만질 수 없어서
	// 실패 여부를 여기까지 들고 올라올 수도 없다. 그래서 판단을 그쪽에 맡긴다.
	return true;
}

void ClientSession::ResumeReceiveIfDrained()
{
	// 대부분의 호출이 여기서 끝난다. 멈춰 있지 않으면 원자 읽기 하나가 전부다.
	if (::ReadAcquire(&m_recvPaused) == 0)
		return;

	if (GetJobQueueDepth() > m_bufferConfig.recvResumeJobDepth)
		return;

	ResumeReceive();
}

bool ClientSession::IsReceivePaused() const
{
	return ::ReadAcquire(&m_recvPaused) == 1;
}

uint32_t ClientSession::GetRecvPauseCount() const
{
	return m_diagnostics.recvPauseCount.load(std::memory_order_relaxed);
}

bool ClientSession::EnqueueSendPacket(PacketHandle&& packetHandle)
{
	SendPacketQueue* queue = m_sendPacketQueue.get();

	if (!queue || !packetHandle)
		return false;

	if (packetHandle.Size() < sizeof(PACKET_HEADER) ||
		packetHandle.Size() > PACKET_SIZE_LIMIT)
	{
		LOGE("session %u invalid packet size", GetSessionID());
		return false;
	}

	// 위에서 범위를 확인했으므로 안전하게 변환할 수 있다.
	const uint32_t packetSize = static_cast<uint32_t>(packetHandle.Size());

	const PACKET_HEADER* header = reinterpret_cast<const PACKET_HEADER*>(packetHandle.View().data());

	if (header->packetSize != packetSize)
	{
		LOGE("session %u packet size mismatch (header=%u, arg=%u)",
			GetSessionID(), header->packetSize, packetSize);
		return false;
	}

	if (packetSize > m_bufferConfig.maxSendPacketSize)
	{
		LOGE("session %u packet too large to send (%u bytes, max %u)",
			GetSessionID(),
			packetSize,
			m_bufferConfig.maxSendPacketSize);
		return false;
	}

	// 소유권 이전 후에는 패킷 메모리에 접근하지 않도록
	// 필요한 값은 미리 보관한다.
	const auto packetId = header->packetId;

	if (!CanSendPacket(packetId))
	{
		uint32_t suppressed = 0;

		if (m_diagnostics.ShouldLogSendReject(SendRejectReason::NotEstablished, suppressed))
		{
			LOGW("session %u packet send blocked before session established "
				"(PacketID : %u, %u more suppressed in the last second)",
				GetSessionID(), packetId, suppressed);
		}

		return false;
	}

	if (!queue->Enqueue(std::move(packetHandle)))
	{
		uint32_t suppressed = 0;

		if (m_diagnostics.ShouldLogSendReject(SendRejectReason::SendQueueFull, suppressed))
		{
			LOGW("session %u send queue admission failed "
				"(PacketID : %u, Size : %u, "
				"%u more suppressed in the last second)",
				GetSessionID(), packetId, packetSize, suppressed);
		}

		return false;
	}

	// 큐 등록 후에는 packet과 header로 데이터에 접근하지 않는다.
	TrySendNext();

	return true;
}

bool ClientSession::EnqueueSharedSendPacket(SharedPacketHandle packetHandle)
{
	if (!packetHandle || !m_sendPacketQueue)
	{
		return false;
	}

	const auto bytes = packetHandle.View();

	if (bytes.size() < sizeof(PACKET_HEADER) || bytes.size() > PACKET_SIZE_LIMIT)
	{
		LOGE("session %u invalid shared packet size",
			GetSessionID());

		return false;
	}

	const uint32_t packetSize = static_cast<uint32_t>(bytes.size());

	const auto* packetHeader = reinterpret_cast<const PACKET_HEADER*>(bytes.data());

	if (packetHeader == nullptr || packetSize < sizeof(PACKET_HEADER))
	{
		LOGE("session %u invalid packet header", GetSessionID());
		return false;
	}

	if (packetHeader->packetSize != packetSize)
	{
		LOGE("session %u packet size mismatch (header=%u, arg=%u)", GetSessionID(), packetHeader->packetSize, packetSize);
		return false;
	}

	// 받는 쪽은 자기 maxRecvPacketSize 를 넘는 헤더를 프로토콜 위반으로 보고
	// 연결을 끊는다. 여기서 막지 않으면 보낸 쪽은 원인 모를 피어 끊김만
	// 보게 되므로, 같은 상한을 송신 시점에 적용해 호출부에서 잡아준다.
	if (packetSize > m_bufferConfig.maxSendPacketSize)
	{
		LOGE("session %u packet too large to send (%u bytes, max %u). the peer would drop the connection",
			GetSessionID(), packetSize, m_bufferConfig.maxSendPacketSize);
		return false;
	}

	if (!CanSendPacket(packetHeader->packetId))
	{
		uint32_t suppressed = 0;
		if (m_diagnostics.ShouldLogSendReject(SendRejectReason::NotEstablished, suppressed))
		{
			LOGW("session %u packet send blocked before session established "
				"(PacketID : %u, %u more suppressed in the last second)",
				GetSessionID(), packetHeader->packetId, suppressed);
		}
		return false;
	}

	if (!m_sendPacketQueue->EnqueueShared(std::move(packetHandle)))
	{
		uint32_t suppressed = 0;
		if (m_diagnostics.ShouldLogSendReject(SendRejectReason::SendQueueFull, suppressed))
		{
			LOGW("session %u failed to enqueue a shared send packet "
				"(%u more suppressed in the last second)", GetSessionID(), suppressed);
		}
		return false;
	}

	TrySendNext();

	return true;
}

bool ClientSession::OnSendCompleted(const DWORD bytesTransferred)
{
	// 송신 완료 1건당 호출되는 핫패스. 릴리스에서는 컴파일 제거된다.
	LOGT("session %u send completed %lu bytes", GetSessionID(), bytesTransferred);

	if (!m_currentSendPacket)
	{
		// 다른 정리 경로에서 현재 패킷을 이미 반환한 상태입니다.
		// 다른 송신 담당자가 보유할 수 있는 토큰은 변경하지 않습니다.
		return false;
	}

	// 부분 전송 처리
	m_sendOffset += bytesTransferred;

	// 아직 남은 데이터가 있으면 같은 패킷을 유지하면서 이어 보냅니다.
	if (m_sendOffset < m_currentSendPacket.Size())
	{
		return PostCurrentSend();
	}

	// 전체 송신이 끝났으므로 패킷 소유권을 반환합니다.
	m_currentSendPacket.Reset();
	m_sendOffset = 0;

	::InterlockedExchange(&m_sending, 0);

	// 다음 패킷 있으면 이어서 송신
	TrySendNext();

	return true;
}

bool ClientSession::HasPendingSend() const
{
	// 걸려 있는 WSASend 를 먼저 본다. 큐가 비었어도 마지막 한 건이
	// 나가는 중일 수 있고, 그 한 건이 바로 "상대가 안 읽고 있다" 는
	// 상태일 수 있다.
	if (::ReadAcquire(&m_sending) != 0)
		return true;

	// 초기화 전/정리 후에도 안전해야 한다. 이 함수는 세션 배열을 훑는
	// 주기 점검에서 불리므로 사용 중이 아닌 슬롯도 들어온다.
	if (m_sendPacketQueue == nullptr)
		return false;

	return !m_sendPacketQueue->IsEmpty();
}

void ClientSession::HandleSocketError(int errorCode, IO_OPERATION ioOperation)
{
	if (IsPeerClosedError(errorCode))
		LOGI("session %u io %d ended : the peer closed the connection (error %d)", GetSessionID(), (int)ioOperation, errorCode);
	else
		LOGE("session %u io %d socket error %d", GetSessionID(), (int)ioOperation, errorCode);

	// DecrementIO 는 이 함수 끝에서 한다.
	//
	// 여기서 복구하려는 카운트는 방금 실패한 WSARecv / WSASend 몫이다.
	// 예전에는 그걸 함수 맨 앞에서 내렸는데, 아래 정리와 NotifyDisconnect 가
	// 전부 그 뒤에 있었다. 지연 반납이 들어오면서 이 순서가 위험해졌다 —
	// 여기서 카운트가 0 이 되면 그 자리에서 세션이 반납되고 프리 리스트에
	// 올라가, 아래 코드가 이미 회수된(그리고 다른 접속에 재배포됐을 수도
	// 있는) 세션을 계속 만진다.
	//
	// 정리를 먼저 끝내고 마지막에 내려놓는다. NotifyDisconnect 는 이제
	// 기다리지 않고 예약만 하므로 이 순서가 성립한다.

	// IO Operation Type 에 따른 우선 처리 되어야 하는 예외 처리
	if (ioOperation == IO_OPERATION::RECV)
	{

	}
	else if (ioOperation == IO_OPERATION::SEND)
	{
		// SEND 인 경우
		// 현재 처리되어야 하는 패킷를 큐에 반환하고
		// nullptr 초기화 한다.
		m_currentSendPacket.Reset();
		m_sendOffset = 0;

		::InterlockedExchange(&m_sending, 0);
	}
	switch (errorCode)
	{
	case WSAECONNRESET:       // 연결이 비정상 종료됨 (상대방 강제 종료)
	case WSAECONNABORTED:     // 연결 중단됨
	case WSAENOTCONN:         // 연결이 이미 끊김
	case WSAESHUTDOWN:        // 소켓 송수신 불가
	case ERROR_NETNAME_DELETED: // 네트워크 이름 삭제됨 (연결 끊김)
		//Log::Info("[Session %u] Connection closed (op=%d, err=%d)", m_sessionId, opType, err);
		//OnDisconnected();
		NoteDisconnectReason(DisconnectReason::SocketError);
		NotifyDisconnect();
		break;

	case WSAENOBUFS:
	case WSAEMFILE:
		// 예전에는 여기서 ::Sleep(10) 을 했다. 이 함수는 IOCP 완료 핸들러
		// 안에서 불리므로 그 대기가 워커 스레드를 통째로 막는다. 게다가
		// 잠든 뒤 재시도하는 코드가 없어서 잠들기만 했다.
		//
		// 자원 부족은 이 세션 하나의 문제가 아니라 프로세스 전체의 문제다.
		// 세션 단위로 할 수 있는 일이 없으므로 크게 남기고 넘어간다.
		LOGE("session %u hit a system resource shortage (error %d, op %d)",
			GetSessionID(), errorCode, static_cast<int>(ioOperation));
		break;

	case ERROR_OPERATION_ABORTED:
		// CancelIoEx 또는 서버 종료 중 발생한 정상적인 취소
		//Log::Debug("[Session %u] Operation canceled (op=%d)", m_sessionId, opType);
		break;

	default:
		// 분류되지 않은 소켓 오류다. 연결이 죽은 것으로 다루므로
		// 사유도 같게 남긴다.
		NoteDisconnectReason(DisconnectReason::SocketError);
		NotifyDisconnect();
		//Log::Error("[Session %u] Unknown socket error %d (op=%d)", m_sessionId, err, opType);
		break;
	}

	// 실패한 I/O 몫을 여기서 내려놓는다. 이 함수의 마지막 줄이어야 한다.
	// 위에서 NotifyDisconnect 가 반납을 예약했다면, 이 호출이 카운트를
	// 0 으로 내리는 순간 반납이 마무리된다. 그 뒤로 세션을 만지면 안 된다.
	DecrementIO();
}

bool ClientSession::PostReceive()
{
	// 수신 1건당 호출되는 핫패스. 릴리스에서는 컴파일 제거된다.
	LOGT("session %u post recv", GetSessionID());

	if (!IsTransportConnected())
	{
		LOGE("session %u cannot post recv : the session is not connected", GetSessionID());

		return false;
	}

	DWORD flags = 0;
	DWORD bytesReceived = 0;

	RecvPacketBuffer& recvBuf = GetReceiveBuffer();

	// 링 끝에 붙어 있으면 남은 조각을 앞으로 당겨 연속 공간을 만든다.
	// 이게 없으면 쓰기 위치가 끝에 가까워질수록 WSARecv 길이가 줄어들어,
	// 같은 양을 받는 데 필요한 완료 횟수가 계속 늘어난다.
	recvBuf.PrepareForReceive();

	const auto writable = recvBuf.GetWritableSpan();

	m_recvOverlapped.ResetForNextIO(GetSessionID(), writable.data(), static_cast<ULONG>(writable.size()));

	if (m_recvOverlapped.wsaBuffer.len == 0)
	{
		// 수신 링에 빈 자리가 없다. 여기서 false 를 돌려주면 호출부
		// (HandleRecv)가 세션을 반납한다. 그게 이 상황의 처리다.
		//
		// 예전에는 이 자리에서 시각을 m_lastRecvBufferFullTime 에 적어 두고
		// "별도의 타이머 스레드가 타임아웃 처리를 한다" 고 주석이 말했다.
		// 그런 스레드는 없었고, 그 멤버를 읽는 코드도 없었다. 쓰기만 세 군데
		// 있는 죽은 변수였으므로 멤버째 지웠다.
		//
		// ENGINE_BREAK_IF_DEBUGGER 도 뺐다. 이 조건은 피어가 만들 수 있다 —
		// 링보다 빨리 밀어 넣으면 된다. 디버거를 붙이고 부하를 거는 동안
		// 남이 멈출 수 있는 자리를 둘 이유가 없다. 불변식 위반이 아니라
		// 정상적으로 일어날 수 있는 혼잡 상태다.
		LOGE("session %u recv ring is full, cannot post recv (stored %zu / capacity %zu). releasing the session",
			GetSessionID(), recvBuf.GetBufferedSize(), recvBuf.GetCapacity());

		return false;
	}

	// 취소가 걸린 뒤에는 새 수신을 걸지 않는다. (사정은 ClientSession::BeginIO 주석)
	//
	// 아이디를 미리 읽어 둔다. BeginIO 가 false 를 돌려주면 그 안의 되돌림이
	// 마지막 카운트였을 수 있고, 그러면 이 세션은 이미 풀로 돌아가 있다.
	const uint32_t sessionIdForLog = GetSessionID();

	if (!BeginIO())
	{
		LOGI("session %u declining to post recv : IO cancel is in progress", sessionIdForLog);
		return false;
	}

	int result = ::WSARecv(
		GetClientSocket(),
		&m_recvOverlapped.wsaBuffer,
		1,
		&bytesReceived,
		&flags,
		(LPWSAOVERLAPPED)&m_recvOverlapped,
		nullptr);

	if (result == SOCKET_ERROR)
	{
		int errorCode = ::WSAGetLastError();
		if (errorCode != WSA_IO_PENDING)
		{
			if (IsPeerClosedError(errorCode))
				LOGI("session %u recv ended : the peer closed the connection (error %d)", GetSessionID(), errorCode);
			else
				LOGE("session %u WSARecv failed (error %d)", GetSessionID(), errorCode);

			// 더이상 해당 세션에 I/O 를 걸 수 없는 상태이므로 중요한 예외 처리 부분이다!
			HandleSocketError(errorCode, IO_OPERATION::RECV);

			return false;
		}
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] WSARecv async success : %d\n", __FUNCTION__, clientSocket_);

	return true;
}

uint32_t ClientSession::GetJobQueueDepth() const
{
	// GetJobQueue 와 달리 널을 정상 입력으로 다룬다.
	// 이 둘은 풀 전체를 훑는 쪽에서 불리므로, 아직 임대되지 않았거나
	// 이미 정리된 세션도 지나간다.
	return m_jobQueue ? m_jobQueue->GetCount() : 0;
}

void ClientSession::ResumeReceive()
{
	// 이 전이를 이긴 스레드 하나만 재개하고, 일시정지의 IO 카운트도 그 스레드가 갖는다.
	if (::InterlockedCompareExchange(&m_recvPaused, 0, 1) != 1)
		return;

	const uint32_t sessionIdForLog = GetSessionID();

	if (!PostReceive())
	{
		// 수신을 다시 걸지 못했다. 이 세션은 아무것도 받을 수 없으므로
		// 그대로 두면 하트비트 타임아웃까지 슬롯만 차지하는 좀비가 된다.
		//
		// 순서는 엔진의 규칙 그대로다 — 반납을 먼저 요청하고, 카운트는
		// 마지막에 내려놓는다. 반대로 하면 카운트가 0 이 되는 자리에서
		// 반납이 마무리되어, 그 뒤의 요청은 이미 남의 것이 된 세션을 건드린다.
		//
		// 이미 반납 중인 세션이 여기 오는 것은 정상이다 (취소가 걸리면
		// PostReceive 가 false 를 돌려준다). 그때는 poolState 의 CAS 가
		// 중복 반납을 걸러내고 "release skipped" 만 남는다.
		LOGI("session %u could not resume recv, requesting release", sessionIdForLog);

		// 이미 다른 경로가 사유를 남겼으면 그게 이긴다. 취소가 걸려서
		// PostReceive 가 실패한 경우가 그렇고, 그때 진짜 사유는 취소를
		// 건 쪽이 알고 있다. 여기 값은 순수하게 자원 문제로 다시 걸지
		// 못한 경우의 바닥이다.
		NoteDisconnectReason(DisconnectReason::ResourceExhausted);
		NotifyDisconnect();
	}
	else
	{
		LOGI("session %u resumed recv", sessionIdForLog);
	}

	// 일시정지가 들고 있던 몫. 이 줄 이후로 세션을 만지면 안 된다.
	DecrementIO();
}

void ClientSession::ReleaseUnpostedIO()
{
	// 일시정지가 들고 있는 카운트를 놓는다.
	//
	// 정상 재개(ResumeReceive)와 같은 전이를 쓴다. 둘이 겹쳐도 CAS 를 이긴
	// 쪽 하나만 카운트를 내리므로 두 번 내려가지 않는다. 다른 점은 수신을
	// 다시 걸지 않는다는 것뿐이다 — 취소가 걸린 뒤라 BeginIO 가 어차피
	// 거절한다.
	if (::InterlockedCompareExchange(&m_recvPaused, 0, 1) != 1)
		return;

	LOGI("session %u dropping the recv pause : IO cancel is in progress, there is nothing to resume",
		GetSessionID());

	// 이 줄 이후로 세션을 만지면 안 된다. (ResumeReceive 와 같은 규칙)
	DecrementIO();
}

bool ClientSession::TrySendNext()
{
	if (!m_sendPacketQueue)
		return false;

	for (;;)
	{
		// 이미 전송 중이라면 WSASend 호출 하지 않고 빠져나간다.
		// 전송을 담당 중인 스레드가 완료 시점에 다음 패킷을 이어서 보낸다.
		if (::InterlockedCompareExchange(&m_sending, 1, 0) != 0)
		{
			return false;
		}

		if (!m_currentSendPacket)
		{
			m_currentSendPacket = m_sendPacketQueue->Dequeue();

			if (!m_currentSendPacket)
			{
				// 보낼 패킷이 없는 경우
				// Idle 상태로 플래그를 다시 변경 해준다.
				::InterlockedExchange(&m_sending, 0);

				// 토큰을 반납하기 직전에 다른 스레드가 Enqueue 했을 수 있다.
				// 그 스레드는 m_sending 이 1 이어서 "전송 담당자가 이어서 보내줄 것"으로
				// 판단하고 물러났으므로, 여기서 재확인하지 않으면
				// 그 패킷은 큐에 갇힌 채 아무도 보내지 않는 상태가 된다.
				if (m_sendPacketQueue->IsEmpty())
				{
					return false;
				}

				// 그 사이에 들어온 패킷이 있으므로 토큰 재획득을 시도한다.
				continue;
			}

			m_sendOffset = 0;
		}

		// PostCurrentSend 는 실패 시 내부에서(또는 HandleSocketError 에서)
		// 현재 패킷과 m_sending 토큰을 이미 정리한다.
		// 여기서 토큰을 한 번 더 반납하면, 그 사이 정당하게 토큰을 획득한
		// 다른 스레드의 소유권을 뺏어 WSASend 가 동시에 두 번 발행될 수 있다.
		// m_sendOverlapped 는 세션당 하나뿐이므로 진행 중인 I/O 구조체가 손상된다.
		return PostCurrentSend();
	}
}

bool ClientSession::PostCurrentSend()
{
	if (!m_currentSendPacket)
	{
		return false;
	}

	const DWORD packetSize = static_cast<DWORD>(m_currentSendPacket.Size());

	const DWORD remainingBytes = packetSize - m_sendOffset;

	if (remainingBytes == 0)
	{
		m_currentSendPacket.Reset();
		m_sendOffset = 0;

		::InterlockedExchange(&m_sending, 0);

		return false;
	}

	const auto bytes = m_currentSendPacket.View();

	m_sendOverlapped.ResetForNextIO(GetSessionID(), const_cast<char*>(reinterpret_cast<const char*>(bytes.data() + m_sendOffset)), remainingBytes);

	DWORD flags = 0;
	DWORD bytesSent = 0;

	// 취소가 걸린 뒤에는 새 송신을 걸지 않는다. (사정은 ClientSession::BeginIO 주석)
	//
	// 정리를 BeginIO 앞에서 끝내는 것이 중요하다. 카운트를 들고 있는 동안은
	// 세션이 반납되지 않으므로 여기서 패킷를 되돌리는 것이 안전하고,
	// BeginIO 가 false 를 돌려준 뒤에는 세션을 만질 수 없다.
	//
	// 패킷를 되돌리지 않으면 패킷과 패킷가 그대로 새고, m_sending
	// 토큰이 잡힌 채로 남아 이 세션은 이후 어떤 송신도 발행하지 못한다.
	const uint32_t sessionIdForLog = GetSessionID();

	if (IsIOCancelRequested())
	{
		LOGI("session %u declining to post send : IO cancel is in progress", sessionIdForLog);

		m_currentSendPacket.Reset();
		m_sendOffset = 0;

		::InterlockedExchange(&m_sending, 0);

		return false;
	}

	if (!BeginIO())
	{
		// 위 검사와 여기 사이에 취소가 걸렸다. 정리는 카운트를 들고 있는
		// BeginIO 안에서 할 수 없으므로, 발행만 포기하고 패킷는 그대로 둔다.
		// 세션 반납 경로(ResetSession / Finalize)가 m_currentSendPacket 을
		// 회수한다.
		LOGI("session %u declining to post send : IO cancel raced the post", sessionIdForLog);
		return false;
	}

	int result = ::WSASend(
		GetClientSocket(),
		&m_sendOverlapped.wsaBuffer,
		1,
		&bytesSent,
		flags,
		(LPWSAOVERLAPPED)&m_sendOverlapped,
		nullptr);

	if (result == SOCKET_ERROR)
	{
		int errorCode = WSAGetLastError();
		if (errorCode != WSA_IO_PENDING)
		{
			if (IsPeerClosedError(errorCode))
				LOGI("session %u send ended : the peer closed the connection (error %d)", GetSessionID(), errorCode);
			else
				LOGE("session %u WSASend failed (error %d)", GetSessionID(), errorCode);

			// 더이상 해당 세션에 I/O 를 걸 수 없는 상태이므로 중요한 예외 처리 부분이다!
			HandleSocketError(errorCode, IO_OPERATION::SEND);

			return false;
		}
	}

	return true;
}

bool ClientSession::CanSendPacket(PACKET_ID_TYPE packetId) const
{
	if (IsSystemPacketId(packetId))
	{
		return IsTransportConnected();
	}

	if (IsServicePacketId(packetId))
	{
		return IsEstablished();
	}

	return false;
}

// 이유별로 1초에 한 줄만 통과시킨다.
//
// 세지 못하고 놓치는 경우가 있어도 괜찮다. 이건 회계가 아니라 "얼마나 자주
// 거부되고 있는가" 를 읽는 사람에게 알려 주는 수치라, 창 경계에서 한두 건이
// 어긋나는 것보다 로그 양에 상한이 있는 것이 중요하다.
bool ClientSession::Diagnostics::ShouldLogSendReject(SendRejectReason reason, uint32_t& outSuppressed)
{
	outSuppressed = 0;

	const uint32_t index = static_cast<uint32_t>(reason);
	if (index >= static_cast<uint32_t>(SendRejectReason::Count))
		return true;

	constexpr uint64_t logIntervalMs = 1000;
	auto& window = sendRejects[index];
	uint64_t last = window.lastLogTick.load(std::memory_order_relaxed);
	const uint64_t now = ::GetTickCount64();

	// 같은 사유의 로그는 한 스레드만 출력하고 나머지는 억제 횟수에 더한다.
	if ((last != 0 && now - last < logIntervalMs) ||
		!window.lastLogTick.compare_exchange_strong(last, now, std::memory_order_relaxed))
	{
		window.suppressed.fetch_add(1, std::memory_order_relaxed);
		return false;
	}

	outSuppressed = window.suppressed.exchange(0, std::memory_order_relaxed);
	return true;
}

void ClientSession::Diagnostics::Reset()
{
	recvPauseCount.store(0, std::memory_order_relaxed);
	for (auto& window : sendRejects)
	{
		window.lastLogTick.store(0, std::memory_order_relaxed);
		window.suppressed.store(0, std::memory_order_relaxed);
	}
}
