#include "ClientSession.h"
#include "SessionJobQueue.h"
#include "../Network/UniqueSocket.h"
#include "../Buffer/RecvPacketBuffer.h"
#include "../Diagnostics/EngineAssert.h"
#include "../Job/Job.h"
#include "../Memory/EngineMemoryPoolHelper.h"
#include "../Protocol/PacketHeader.h"
#include "../Protocol/PacketID.h"
#include "../Protocol/SystemPacket.h"
#include "../../Core/Sync/SRWLockGuard.h"
#include "../../Core/Util/Logger.h"

#include <cstring> // for std::strncmp, std::memcpy
#include <memory> // for std::construct_at, std::make_unique
#include <utility> // for std::exchange, std::move

#pragma comment(lib, "ws2_32.lib")
using namespace Core::Sync;
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
			return true;
		default:
			return false;
		}
	}
}

// 발행과 취소 사이에 틈이 없도록 소켓 호출까지 같은 락에서 실행한다.
// 동기 성공과 pending은 완료 통지를 기다리고 즉시 실패는 집계하지 않는다.
template<typename StartOperation>
int ClientSession::PostIO(StartOperation start)
{
	Core::Sync::SRWWriteLockGuard lock(m_lifetimeLock);
	if (m_cancelled)
		return ERROR_OPERATION_ABORTED;

	const int error = start() ? ERROR_SUCCESS : ::WSAGetLastError();
	if (error != ERROR_SUCCESS && error != WSA_IO_PENDING)
		return error;

	++m_ioCount;
	return ERROR_SUCCESS;
}

// 생성 및 연결 수명 주기

ClientSession::ClientSession()
{
	m_connectOverlapped.operation = IO_OPERATION::CONNECT;
	m_recvOverlapped.operation = IO_OPERATION::RECV;
	m_sendOverlapped.operation = IO_OPERATION::SEND;
}

ClientSession::~ClientSession()
{
	CheckIdle();
	if (!IsSocketInvalid())
		ENGINE_VIOLATION("session %u destroyed with a live socket", GetSessionID());

	SetSessionContext(nullptr);
}

bool ClientSession::Initialize(SESSION_ROLE sessionType, uint32_t sessionId)
{
	if (sessionType == SESSION_ROLE::NONE || sessionId == INVALID_SESSION_ID)
		return false;

	m_sessionRole = sessionType;
	m_sessionId = sessionId;
	return true;
}

bool ClientSession::InitializeMemoryPool(TlsMemoryPool* jobMemoryPool, TlsMemoryPool* packetMemoryPool, const SessionBufferConfig& bufferConfig)
{
	if (!jobMemoryPool)
		return false;

	if (!bufferConfig.IsValid())
	{
		ENGINE_VIOLATION("session %u received an invalid buffer config (recv %u/ring %u, send %u, queue %u)",
			GetSessionID(), bufferConfig.maxRecvPacketSize, bufferConfig.recvRingSize,
			bufferConfig.maxSendPacketSize, bufferConfig.sendQueueDepth);
		return false;
	}

	// 세 자원을 모두 준비한 뒤 세션에 등록한다.
	auto recvPacketBuffer = std::make_unique<RecvPacketBuffer>();
	if (!recvPacketBuffer->Initialize(packetMemoryPool, bufferConfig.recvRingSize, bufferConfig.maxRecvPacketSize))
		return false;

	auto jobQueue = std::make_unique<SessionJobQueue>(GetSessionRole(), *jobMemoryPool);

	if (!m_sendQueue.Initialize(bufferConfig.sendQueueDepth))
		return false;

	m_bufferConfig = bufferConfig;
	m_packetMemoryPool = packetMemoryPool;
	m_recvPacketBuffer = std::move(recvPacketBuffer);
	m_jobQueue = std::move(jobQueue);
	return true;
}

bool ClientSession::AttachSocket(UniqueSocket socket, const char* remoteIP, uint16_t port)
{
	SOCKET expected = INVALID_SOCKET;
	if (socket.Get() == INVALID_SOCKET || !IsReadyToConnect() ||
		!m_clientSocket.compare_exchange_strong(expected, socket.Get()))
	{
		ENGINE_VIOLATION("session %u could not take ownership of a connection socket", GetSessionID());
		return false;
	}
	(void)socket.Release();
	if (remoteIP)
	{
		SRWWriteLockGuard lock(m_remoteAddressLock);
		::strncpy_s(m_clientIPAddress, sizeof(m_clientIPAddress), remoteIP, _TRUNCATE);
		m_clientPort = port;
	}
	return true;
}

bool ClientSession::PostConnect(LPFN_CONNECTEX connectEx, const SOCKADDR_IN& address)
{
	m_connectOverlapped.ResetForNextIO(GetSessionID());
	const int error = PostIO([&] {
		DWORD bytesSent = 0;
		m_state.store(State::Connecting);
		return connectEx(GetClientSocket(), reinterpret_cast<const sockaddr*>(&address), sizeof(address),
			nullptr, 0, &bytesSent, &m_connectOverlapped.wsaOverlapped) != FALSE;
	});
	if (error == ERROR_SUCCESS)
		return true;

	if (error != ERROR_OPERATION_ABORTED)
		RequestDisconnect(DisconnectReason::ConnectFailed);
	return false;
}

bool ClientSession::OnConnect()
{
	m_state.store(State::Connected);

	char ipAddress[sizeof(m_clientIPAddress)] = {};
	uint16_t port = 0;

	{
		Core::Sync::SRWReadLockGuard lock(m_remoteAddressLock);
		std::memcpy(ipAddress, m_clientIPAddress, sizeof(ipAddress));
		port = m_clientPort;
	}

	if (ipAddress[0] != '\0')
	{
		LOGI("session %u connected from %s:%u (role %d)",
			GetSessionID(), ipAddress, port, static_cast<int>(GetSessionRole()));
	}
	else
	{
		LOGI("session %u connected (role %d)", GetSessionID(), static_cast<int>(GetSessionRole()));
	}

	UpdateLastRecvTick();
	UpdateLastHeartbeatTick();

	if (!PostReceive())
	{
		// 첫 수신을 걸지 못하면 이 세션은 아무것도 받을 수 없다.
		LOGE("session %u failed to post the first recv right after connect", GetSessionID());
		return false;
	}

	// 첫 수신을 걸고 접속 통지를 호출할 수 있는 연결만 종료 통지의 짝을 갖는다.
	::InterlockedExchange(&m_serviceConnectNotified, 1);
	return true;
}

void ClientSession::RequestDisconnect(DisconnectReason reason)
{
	RecordDisconnectReason(reason);
	if (m_ownerCallbacks.requestDisconnect)
	{
		m_ownerCallbacks.requestDisconnect(m_ownerCallbacks.owner, this);
	}
}

void ClientSession::RecordDisconnectReason(DisconnectReason reason)
{
	::InterlockedCompareExchange(&m_disconnectReason, static_cast<LONG>(reason),
		static_cast<LONG>(DisconnectReason::Unknown));
}

void ClientSession::FinishDisconnect(CloseSocketFunc closeSocket, SessionDisconnectNotifyFunc notify, void* context)
{
	const SOCKET socket = m_clientSocket.exchange(INVALID_SOCKET);
	if (socket != INVALID_SOCKET)
	{
		if (closeSocket)
			closeSocket(socket);
		else if (::closesocket(socket) == SOCKET_ERROR)
			LOGE("failed to close client socket (error %d)", ::WSAGetLastError());
	}

	// 서비스가 원격 주소와 컨텍스트를 마지막으로 조회할 수 있게 락 밖에서 먼저 알린다.
	if (::InterlockedExchange(&m_serviceConnectNotified, 0) && notify)
		notify(context, this, static_cast<DisconnectReason>(::ReadAcquire(&m_disconnectReason)));

	ClearRemoteAddress();
	m_state.store(State::Disconnected);
	LOGI("session %u disconnected (role %d)", GetSessionID(), static_cast<int>(m_sessionRole));
}

void ClientSession::ResetSession()
{
	CheckIdle();
	{
		SRWWriteLockGuard lock(m_lifetimeLock);
		m_cancelled = false;
	}
	m_state.store(State::Ready);
	::InterlockedExchange(&m_disconnectReason, static_cast<LONG>(DisconnectReason::Unknown));
	::InterlockedExchange(&m_processing, 0);
	m_recvPauseCount.store(0, std::memory_order_relaxed);

	// 풀 재대여는 새 연결이므로 통지 기록도 초기화한다.
	::InterlockedExchange(&m_serviceConnectNotified, 0);

	SetSessionContext(nullptr);

	m_sendQueue.Reset();
	m_currentSendPacket.Reset();
	m_sendOffset = 0;
	m_sending.store(false);
	m_sendOverlapped.Clear();

	// 이전 연결의 미완성 패킷이 다음 연결의 헤더에 섞이지 않도록 비운다.
	if (m_recvPacketBuffer)
	{
		m_recvPacketBuffer->Reset();
	}

	if (m_jobQueue)
	{
		m_jobQueue->Reset();
	}

	::InterlockedExchange64(&m_lastRecvTick, 0);
	::InterlockedExchange64(&m_lastHeartbeatTick, 0);

	if (GetSessionRole() == SESSION_ROLE::CLIENT)
	{
		m_connectOverlapped.Clear();
	}
	else if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		m_recvOverlapped.Clear();
	}
}

// 연결 상태와 인증

bool ClientSession::IsReadyToConnect() const
{
	return m_state.load() == State::Ready;
}

bool ClientSession::IsTransportConnected() const
{
	const State state = m_state.load();
	return state == State::Connected || state == State::Authenticating ||
		state == State::Established || state == State::TimedOut;
}

bool ClientSession::IsEstablished() const
{
	return m_state.load() == State::Established;
}

bool ClientSession::BeginAuthentication()
{
	State expected = State::Connected;
	return m_state.compare_exchange_strong(expected, State::Authenticating);
}

bool ClientSession::Establish()
{
	State expected = State::Authenticating;
	return m_state.compare_exchange_strong(expected, State::Established);
}

void ClientSession::RejectAuthentication(DisconnectReason reason, PacketHandle response)
{
	// 거절 응답의 즉시 송신 실패가 최초 사유를 덮어쓰지 않게 한다.
	RecordDisconnectReason(reason);
	if (response)
		(void)EnqueueSendPacket(std::move(response));
	RequestDisconnect(reason);
}

// 수명 참조와 비동기 I/O 완료·취소

ClientSession::Reference ClientSession::AcquireReference()
{
	SRWWriteLockGuard lock(m_lifetimeLock);
	++m_referenceCount;
	return Reference(this);
}

void ClientSession::ReturnReference::operator()(ClientSession* session) const noexcept
{
	session->ReleaseReference();
}

void ClientSession::ReleaseReference()
{
	{
		SRWWriteLockGuard lock(m_lifetimeLock);
		ENGINE_CHECK_RETVOID(m_referenceCount > 0, "session %u released an unowned reference", GetSessionID());
		--m_referenceCount;
		if (!ClaimReleaseLocked())
			return;
	}
	NotifyReleaseReady();
}

ClientSession::IOCompletion ClientSession::TakeIOCompletion()
{
	return IOCompletion(this);
}

void ClientSession::FinishIO::operator()(ClientSession* session) const noexcept
{
	session->CompleteIO();
}

void ClientSession::CompleteIO()
{
	{
		SRWWriteLockGuard lock(m_lifetimeLock);
		ENGINE_CHECK_RETVOID(m_ioCount > 0, "session %u completed an unposted IO", GetSessionID());
		if (--m_ioCount == 0)
			::WakeAllConditionVariable(&m_ioCompleted);
		if (!ClaimReleaseLocked())
			return;
	}
	NotifyReleaseReady();
}

void ClientSession::CloseWhenIdle()
{
	if (!CancelPendingIO())
		ENGINE_VIOLATION("session %u could not cancel IO during disconnect", GetSessionID());
	{
		SRWWriteLockGuard lock(m_lifetimeLock);
		m_releasePending = true;
		if (!ClaimReleaseLocked())
			return;
	}
	NotifyReleaseReady();
}

bool ClientSession::ClaimReleaseLocked()
{
	if (m_ioCount != 0 || m_referenceCount != 0)
		return false;
	return std::exchange(m_releasePending, false);
}

void ClientSession::NotifyReleaseReady()
{
	if (m_ownerCallbacks.releaseReady)
		m_ownerCallbacks.releaseReady(m_ownerCallbacks.owner, this);
}

bool ClientSession::CancelPendingIO()
{
	ClientSession::Reference pause;
	DWORD error;
	{
		SRWWriteLockGuard lock(m_lifetimeLock);
		m_cancelled = true;
		pause = TakeReceivePauseLocked();
		error = CancelSocketIOLocked();
	}
	if (error != ERROR_SUCCESS)
		LOGE("session %u CancelIoEx failed (error %lu)", GetSessionID(), error);
	// 일시정지 참조의 반환은 소켓 취소 결과와 무관하며 락 밖에서 끝낸다.
	return error == ERROR_SUCCESS;
}

DWORD ClientSession::CancelSocketIOLocked() const
{
	const SOCKET socket = GetClientSocket();
	if (m_ioCount == 0 || socket == INVALID_SOCKET || ::CancelIoEx(reinterpret_cast<HANDLE>(socket), nullptr))
		return ERROR_SUCCESS;

	const DWORD error = ::GetLastError();
	// 이미 완료 큐에 있거나 핸들러가 처리 중인 요청도 카운트에 남는다.
	return error == ERROR_NOT_FOUND ? ERROR_SUCCESS : error;
}

IOWaitResult ClientSession::WaitForIOCancelComplete(uint32_t timeoutMs)
{
	const ULONGLONG deadline = ::GetTickCount64() + timeoutMs;
	SRWWriteLockGuard lock(m_lifetimeLock);
	while (m_ioCount != 0)
	{
		const ULONGLONG now = ::GetTickCount64();
		if (now >= deadline)
			return { IOWaitStatus::TimedOut, GetSessionID() };

		if (!::SleepConditionVariableSRW(&m_ioCompleted, &m_lifetimeLock, static_cast<DWORD>(deadline - now), 0))
		{
			const DWORD error = ::GetLastError();
			if (error != ERROR_TIMEOUT)
				return { IOWaitStatus::Failed, GetSessionID(), error };
			// 이전 소켓 취소가 실패했으면 다음 진단 주기에도 취소할 기회를 준다.
			if (m_cancelled && m_ioCount != 0)
				CancelSocketIOLocked();
		}
	}
	return { IOWaitStatus::Completed };
}

LONG ClientSession::GetOutstandingIOCount() const
{
	SRWReadLockGuard lock(m_lifetimeLock);
	return m_ioCount;
}

void ClientSession::CheckIdle() const
{
	SRWReadLockGuard lock(m_lifetimeLock);
	if (m_ioCount || m_referenceCount || m_releasePending || m_recvPaused)
		ENGINE_VIOLATION("session %u cleaned up while still in use (io %ld, references %ld)",
			GetSessionID(), m_ioCount, m_referenceCount);
}

void ClientSession::HandlePostError(int error, IO_OPERATION operation)
{
	if (error == ERROR_OPERATION_ABORTED)
		return;

	if (IsPeerClosedError(error))
		LOGI("session %u peer closed while posting io %d (error %d)", GetSessionID(), static_cast<int>(operation), error);
	else
		LOGE("session %u could not post io %d (error %d)", GetSessionID(), static_cast<int>(operation), error);

	const bool exhausted = error == WSAENOBUFS || error == WSAEMFILE;
	// 일시적인 송신 자원 부족은 이번 패킷만 포기한다. 수신 실패는 후속 수신이 없어 종료한다.
	if (exhausted && operation == IO_OPERATION::SEND)
		return;
	RequestDisconnect(exhausted ? DisconnectReason::ResourceExhausted : DisconnectReason::SocketError);
}

// 수신 완료와 Job 큐 수위에 따른 재발행

bool ClientSession::OnReceiveCompleted(DWORD bytesTransferred)
{
	if (bytesTransferred == 0)
	{
		RequestDisconnect(DisconnectReason::PeerClosed);
		return false;
	}
	UpdateLastRecvTick();
	auto& buffer = *m_recvPacketBuffer;
	if (!buffer.CommitWrite(bytesTransferred))
	{
		ENGINE_VIOLATION("session %u recv ring overflow: %lu bytes (stored %zu / capacity %zu)",
			GetSessionID(), bytesTransferred, buffer.GetBufferedSize(), buffer.GetCapacity());
		RequestDisconnect(DisconnectReason::ProtocolError);
		return false;
	}
	return true;
}

ClientSession::ReceiveResult ClientSession::ReadReceivedPacket()
{
	// 서비스 콜백이 종료를 요청하거나 다른 스레드가 수신을 취소한 경우 인출을 멈춘다.
	{
		SRWReadLockGuard lock(m_lifetimeLock);
		if (m_cancelled || ::ReadAcquire(&m_disconnectReason) != static_cast<LONG>(DisconnectReason::Unknown))
			return { ReceiveStatus::Stopped };
	}
	auto [status, packet, id] = m_recvPacketBuffer->ReadPacket();
	if (status == PacketReadStatus::NeedMoreData)
		return { ReceiveStatus::NeedMoreData };
	if (status != PacketReadStatus::Ok)
	{
		LOGE("session %u failed to read packet: %s", GetSessionID(),
			status == PacketReadStatus::Invalid ? "invalid packet size" : "packet pool exhausted");
		RequestDisconnect(status == PacketReadStatus::Invalid ?
			DisconnectReason::ProtocolError : DisconnectReason::ResourceExhausted);
		return { ReceiveStatus::Stopped };
	}
	if (!IsValidPacketId(id))
	{
		LOGE("session %u received invalid packet id %u", GetSessionID(), id);
		RequestDisconnect(DisconnectReason::ProtocolError);
		return { ReceiveStatus::Stopped };
	}
	if (IsServicePacketId(id) && !IsEstablished())
	{
		LOGW("session %u received service packet %u before authentication", GetSessionID(), id);
		RequestDisconnect(DisconnectReason::UnexpectedPacket);
		return { ReceiveStatus::Stopped };
	}
	return { ReceiveStatus::Packet, std::move(packet), id };
}

bool ClientSession::PostReceive()
{
	if (!IsTransportConnected())
		return false;

	auto& buffer = *m_recvPacketBuffer;
	buffer.PrepareForReceive();
	const auto writable = buffer.GetWritableSpan();
	if (writable.empty())
	{
		LOGE("session %u recv ring is full (%zu bytes)", GetSessionID(), buffer.GetBufferedSize());
		RequestDisconnect(DisconnectReason::ResourceExhausted);
		return false;
	}

	m_recvOverlapped.ResetForNextIO(GetSessionID(), writable.data(), static_cast<ULONG>(writable.size()));
	const int error = PostIO([&] {
		DWORD flags = 0;
		return ::WSARecv(GetClientSocket(), &m_recvOverlapped.wsaBuffer, 1, nullptr, &flags,
			&m_recvOverlapped.wsaOverlapped, nullptr) != SOCKET_ERROR;
	});
	if (error == ERROR_SUCCESS)
		return true;

	HandlePostError(error, IO_OPERATION::RECV);
	return false;
}

void ClientSession::ContinueReceive()
{
	{
		SRWReadLockGuard lock(m_lifetimeLock);
		if (m_cancelled || ::ReadAcquire(&m_disconnectReason) != static_cast<LONG>(DisconnectReason::Unknown))
			return;
	}
	// 검사 이후의 취소 경합도 PostIO와 TryPauseReceive의 락에서 다시 확인한다.
	const uint32_t pauseLevel = m_bufferConfig.recvPauseJobDepth;
	if (pauseLevel == 0 || GetJobQueueDepth() < pauseLevel)
	{
		PostReceive();
		return;
	}

	// 취소와 같은 락에서 표시하고 수명 참조를 받는다. 실제 소켓 I/O는 없다.
	if (!TryPauseReceive())
		return;

	m_recvPauseCount.fetch_add(1, std::memory_order_relaxed);
	// 워커가 표시 직전에 큐를 비웠더라도 재개 기회를 잃지 않는다.
	ResumeReceiveIfDrained();
}

bool ClientSession::TryPauseReceive()
{
	SRWWriteLockGuard lock(m_lifetimeLock);
	if (m_cancelled || m_recvPaused)
		return false;

	m_recvPaused = true;
	++m_referenceCount;
	return true;
}

void ClientSession::ResumeReceiveIfDrained()
{
	if (!IsReceivePaused())
		return;

	if (GetJobQueueDepth() > m_bufferConfig.recvResumeJobDepth)
		return;

	Reference pause;
	{
		SRWWriteLockGuard lock(m_lifetimeLock);
		pause = TakeReceivePauseLocked();
	}
	if (pause)
		PostReceive();
}

ClientSession::Reference ClientSession::TakeReceivePauseLocked()
{
	if (!std::exchange(m_recvPaused, false))
		return {};
	return ClientSession::Reference(this);
}

bool ClientSession::IsReceivePaused() const
{
	SRWReadLockGuard lock(m_lifetimeLock);
	return m_recvPaused;
}

uint32_t ClientSession::GetRecvPauseCount() const
{
	return m_recvPauseCount.load(std::memory_order_relaxed);
}

// 송신 큐와 부분 송신

bool ClientSession::EnqueueSendPacket(PacketHandle&& packet)
{
	if (!packet || !CanEnqueueSend(packet.View()))
		return false;
	return StartQueuedSend(m_sendQueue.Enqueue(std::move(packet)));
}

bool ClientSession::EnqueueSharedSendPacket(SharedPacketHandle packet)
{
	if (!packet || !CanEnqueueSend(packet.View()))
		return false;
	return StartQueuedSend(m_sendQueue.EnqueueShared(std::move(packet)));
}

bool ClientSession::CanEnqueueSend(std::span<const std::byte> bytes) const
{
	if (bytes.size() < sizeof(PACKET_HEADER) || bytes.size() > PACKET_SIZE_LIMIT || bytes.size() > m_bufferConfig.maxSendPacketSize)
	{
		LOGE("session %u invalid send size %zu (limit %u)", GetSessionID(), bytes.size(), m_bufferConfig.maxSendPacketSize);
		return false;
	}
	const auto* header = reinterpret_cast<const PACKET_HEADER*>(bytes.data());
	if (header->packetSize != bytes.size())
	{
		LOGE("session %u send size mismatch (header %u, buffer %zu)", GetSessionID(), header->packetSize, bytes.size());
		return false;
	}

	return (IsSystemPacketId(header->packetId) && IsTransportConnected()) ||
		(IsServicePacketId(header->packetId) && IsEstablished());
}

bool ClientSession::StartQueuedSend(bool enqueued)
{
	if (!enqueued)
	{
		LOGW("session %u send queue admission failed", GetSessionID());
		return false;
	}
	TrySendNext();
	return true;
}

void ClientSession::TrySendNext()
{
	for (;;)
	{
		if (m_sending.exchange(true))
			return;

		m_currentSendPacket = m_sendQueue.Dequeue();
		if (m_currentSendPacket)
		{
			m_sendOffset = 0;
			PostCurrentSend();
			return;
		}

		// 실행권을 놓기 직전에 들어온 패킷을 큐에 남겨 두지 않는다.
		m_sending.store(false);
		if (m_sendQueue.IsEmpty())
			return;
	}
}

void ClientSession::PostCurrentSend()
{
	const auto bytes = m_currentSendPacket.View().subspan(m_sendOffset);
	m_sendOverlapped.ResetForNextIO(GetSessionID(),
		const_cast<char*>(reinterpret_cast<const char*>(bytes.data())), static_cast<ULONG>(bytes.size()));
	const int error = PostIO([&] {
		return ::WSASend(GetClientSocket(), &m_sendOverlapped.wsaBuffer, 1, nullptr, 0,
			&m_sendOverlapped.wsaOverlapped, nullptr) != SOCKET_ERROR;
	});
	if (error == ERROR_SUCCESS)
		return;

	m_currentSendPacket.Reset();
	m_sendOffset = 0;
	m_sending.store(false);
	HandlePostError(error, IO_OPERATION::SEND);
}

void ClientSession::OnSendCompleted(DWORD bytesTransferred)
{
	if (!m_currentSendPacket)
		return;

	m_sendOffset += bytesTransferred;
	if (m_sendOffset < m_currentSendPacket.Size())
	{
		PostCurrentSend();
		return;
	}

	m_currentSendPacket.Reset();
	m_sendOffset = 0;
	m_sending.store(false);
	TrySendNext();
}

bool ClientSession::HasPendingSend() const
{
	return m_sending.load() || !m_sendQueue.IsEmpty();
}

// Job 등록과 실행권

bool ClientSession::EnqueueJob(JobHandle&& jobHandle)
{
	if (!m_jobQueue)
	{
		ENGINE_VIOLATION("session %u has no job queue", GetSessionID());
		return false;
	}
	return m_jobQueue->Enqueue(std::move(jobHandle));
}

SessionJobQueue& ClientSession::GetJobQueue() const
{
	return *m_jobQueue;
}

uint32_t ClientSession::GetJobQueueDepth() const
{
	return m_jobQueue ? m_jobQueue->GetCount() : 0;
}

uint32_t ClientSession::GetJobQueuePeakDepth() const
{
	return m_jobQueue ? m_jobQueue->GetPeakCount() : 0;
}

bool ClientSession::TryClaimProcessing()
{
	// 0에서 1로 바꾼 호출자만 세션을 준비 큐에 등록한다.
	return (::InterlockedCompareExchange(&m_processing, 1, 0) == 0);
}

void ClientSession::ReleaseProcessing()
{
	::InterlockedExchange(&m_processing, 0);
}

void ClientSession::SetCurrentJob(Job* job)
{
	// 클라이언트 핸들러가 실행되는 동안만 현재 Job을 빌린다.
	m_currentJob = job;
}

PacketHandle ClientSession::TakeCurrentPacket()
{
	// 현재 Job을 연결하는 스케줄러는 클라이언트 쪽뿐이다.
	if (GetSessionRole() != SESSION_ROLE::CLIENT)
	{
		ENGINE_VIOLATION(
			"session %u TakeCurrentPacket is client-only",
			GetSessionID());

		return {};
	}

	// 잡 사이에는 널이다. 스케줄러가 반납 직전에 끊어 둔다.
	if (!m_currentJob)
		return {};

	return m_currentJob->TakePacket();
}

// 서비스 컨텍스트와 원격 주소

void ClientSession::SetSessionContext(SessionContext* sessionContext)
{
	if (m_sessionContext != nullptr && m_sessionContext != sessionContext)
	{
		delete m_sessionContext;
	}

	m_sessionContext = sessionContext;
}

SessionContext* ClientSession::GetSessionContext() const
{
	return m_sessionContext;
}

bool ClientSession::HasRemoteAddress(const char* ipAddress) const
{
	if (!ipAddress || ipAddress[0] == '\0')
	{
		return false;
	}

	Core::Sync::SRWReadLockGuard lock(m_remoteAddressLock);
	return std::strncmp(m_clientIPAddress, ipAddress, sizeof(m_clientIPAddress)) == 0;
}

void ClientSession::ClearRemoteAddress()
{
	Core::Sync::SRWWriteLockGuard lock(m_remoteAddressLock);
	memset(m_clientIPAddress, 0, sizeof(m_clientIPAddress));
	m_clientPort = 0;
}

// 활동 시각과 하트비트

void ClientSession::UpdateLastRecvTick()
{
	::InterlockedExchange64(&m_lastRecvTick, static_cast<LONGLONG>(::GetTickCount64()));
}

uint64_t ClientSession::GetLastRecvTick() const
{
	return static_cast<uint64_t>(::ReadAcquire64(&m_lastRecvTick));
}

void ClientSession::UpdateLastHeartbeatTick()
{
	::InterlockedExchange64(&m_lastHeartbeatTick, static_cast<LONGLONG>(::GetTickCount64()));
}

uint64_t ClientSession::GetLastActiveTick() const
{
	const uint64_t lastRecvTick = GetLastRecvTick();
	const auto lastHeartbeatTick = static_cast<uint64_t>(::ReadAcquire64(&m_lastHeartbeatTick));
	return (lastRecvTick > lastHeartbeatTick) ? lastRecvTick : lastHeartbeatTick;
}

bool ClientSession::IsHeartbeatTimedOut(uint64_t nowTick, uint64_t timeout_ms) const
{
	if (timeout_ms == 0)
	{
		return false;
	}

	const uint64_t lastActiveTick = GetLastActiveTick();
	if (lastActiveTick == 0 || nowTick < lastActiveTick)
	{
		return false;
	}

	return ((nowTick - lastActiveTick) >= timeout_ms);
}

void ClientSession::MarkHeartbeatTimeout()
{
	if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		m_state.store(State::TimedOut);
	}
}

bool ClientSession::SendSystemHeartbeatRequest()
{
	if (GetSessionRole() != SESSION_ROLE::SERVER || !IsEstablished() || !m_packetMemoryPool)
	{
		return false;
	}

	auto packetHandle = MEMORY_POOL::CreatePacketHandle(*m_packetMemoryPool, sizeof(SC_SYSTEM_HEARTBEAT_REQUEST_PACKET));

	if (!packetHandle)
	{
		return false;
	}

	auto* request = std::construct_at(reinterpret_cast<SC_SYSTEM_HEARTBEAT_REQUEST_PACKET*>(packetHandle.MutableView().data()));

	request->tick = ::GetTickCount64();

	return EnqueueSendPacket(std::move(packetHandle));
}

bool ClientSession::SendSystemHeartbeatResponse(uint64_t requestTick)
{
	if (GetSessionRole() != SESSION_ROLE::CLIENT || !IsEstablished())
	{
		return false;
	}

	auto packetHandle = MEMORY_POOL::CreatePacketHandle(*m_packetMemoryPool, sizeof(CS_SYSTEM_HEARTBEAT_RESPONSE_PACKET));

	if (!packetHandle)
	{
		return false;
	}

	auto* response = std::construct_at(reinterpret_cast<CS_SYSTEM_HEARTBEAT_RESPONSE_PACKET*>(packetHandle.MutableView().data()));

	response->tick = requestTick;

	return EnqueueSendPacket(std::move(packetHandle));
}
