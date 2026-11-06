#pragma once

#include <stdint.h>
#include <memory> // for std::unique_ptr

#include "ISession.h"
#include "SessionDefs.h"
#include "DisconnectReason.h"

#include "../Network/OverlappedEx.h"
#include "../Protocol/PacketID.h"
#include "../Buffer/SendPacketQueue.h"
#include "../Buffer/SessionBufferConfig.h"
#include "../Job/JobHandle.h"
class Job;

class SessionJobQueue;
class RecvPacketBuffer;
class TlsMemoryPool;
class PacketHandle;
class SharedPacketHandle;

#define INET_ADDRSTRLEN  22

class ClientSession final : public ISession
{
public:
	ClientSession();
	~ClientSession() override;

	// 마지막 수명 참조를 반환한 스레드가 소유자에게 반납 완료 처리를 맡긴다.
	struct OwnerCallbacks
	{
		void* owner;
		void (*requestDisconnect)(void*, ClientSession*);
		void (*releaseReady)(void*, ClientSession*);
	};

private:
	bool m_destroyFlag = false;

	// 역할. Initialize 에서 한 번 정해지고 그 뒤로 바뀌지 않는다.
	// 세션이 어떤 I/O 에도 등록되기 전에 쓰이므로 원자 접근이 필요 없다.
	SESSION_ROLE m_sessionRole = SESSION_ROLE::NONE;

	// 세션 상태.
	//
	// 예전에는 평범한 enum 멤버였다. 그런데 쓰는 쪽과 읽는 쪽이 다른 스레드다.
	//   쓰기 : HeartbeatThread    -> MarkHeartbeatTimeout (HEARTBEAT_TIMEOUT)
	//          IOCP 워커          -> 접속/인증 시퀀스 (CONNECTED, AUTH_PENDING, ESTABLISHED)
	//          반납 경로          -> ResetSession / Finalize (CONNECT_READY)
	//   읽기 : IOCP 워커          -> HandleRecv 안의 IsEstablished() 로
	//                                서비스 패킷 디스패치 여부를 가른다
	//          앱 스레드          -> StopServer -> DisconnectAllSessions
	//
	// 지금까지 드러나지 않은 것은 두 가지 우연 덕분이었다. x64 MSVC 에서
	// 정렬된 4바이트 접근이 찢어지지 않는다는 것, 그리고 접근자가 다른 번역
	// 단위에 있어 인라인되지 않으므로 컴파일러가 값을 레지스터에 눌러
	// 담아두지 못한다는 것. 둘 다 설정 하나로(LTCG/WPO) 사라지는 전제다.
	//
	// AcceptSession::m_sessionOwned 가 같은 부류의 문제를 먼저 맞았다 —
	// 상태가 원자적이지 않아 두 스레드가 같은 슬롯을 비었다고 읽었다.
	// 그쪽은 CAS 플래그를 따로 뒀지만 나머지 상태는 그대로 남아 있었다.
	//
	// 저장은 volatile LONG 이고 접근은 반드시 Interlocked 로만 한다.
	// 읽기 방식은 GetOutstandingIOCount와 같다.
	volatile LONG m_clientSessionState = static_cast<LONG>(ClientSessionState::NONE);
	volatile LONG m_serverSessionState = static_cast<LONG>(ServerSessionState::NONE);

	// 이 세션이 왜 끝났는가. 서비스의 종료 훅으로 그대로 올라간다.
	// 먼저 쓴 값이 이긴다 (NoteDisconnectReason 주석).
	volatile LONG m_disconnectReason = static_cast<LONG>(DisconnectReason::Unknown);

	SOCKET m_clientSocket = INVALID_SOCKET;

	uint32_t m_sessionId = INVALID_SESSION_ID;

	volatile LONG m_closing = 0;
	volatile LONG m_ioCount = 0;
	// I/O와 준비 큐·워커의 참조를 함께 셉니다. 모두 반환되기 전에는 세션을 재사용하지 않습니다.
	mutable SRWLOCK m_lifetimeLock = SRWLOCK_INIT;
	LONG m_lifetimeReferences = 0;
	volatile LONG m_cancelIo = 0;

	// 반납 예약. 1 이면 "I/O와 작업 수명 참조가 모두 끝나는 대로 이 세션을 반납하라".
	//
	// 이 플래그는 예약이면서 동시에 소유권 토큰이다. 1 -> 0 전이에
	// 성공한 스레드 하나만 마무리를 수행한다. RequestRelease 를 부른
	// 스레드와 마지막 ReleaseLifetimeReference를 하는 스레드가 동시에 "내가 마지막"
	// 이라고 판단할 수 있어, 둘 중 하나만 고르는 장치가 필요하다.
	bool m_releasePending = false;

	OwnerCallbacks m_ownerCallbacks{};

	HANDLE m_ioCancelCompleteEvent = nullptr;

	TlsMemoryPool* m_jobMemoryPool = nullptr;
	TlsMemoryPool* m_packetMemoryPool = nullptr;

	// Client Role
	OverlappedEx m_connectOverlapped{};

	// Client / Server Role
	OverlappedEx m_recvOverlapped{};
	std::unique_ptr<RecvPacketBuffer> m_recvPacketBuffer;

	// 이 세션의 버퍼 크기 정책. InitializeMemoryPool 에서 받아 보관한다.
	// 송신 상한 검사가 여기를 본다.
	SessionBufferConfig m_bufferConfig;

	OverlappedEx m_sendOverlapped{};

	// 송신 중인 패킷을 소유합니다.
	// 부분 송신이면 유지하고, 최종 완료 또는 안전한 종료 정리 시 반환합니다.
	SendPacket m_currentSendPacket;
	uint32_t m_sendOffset = 0;

	Job* m_currentJob = nullptr;

	std::unique_ptr<SendPacketQueue> m_sendPacketQueue;

	std::unique_ptr<SessionJobQueue> m_jobQueue;

	volatile LONG m_sending = 0; // sending flag: 0 = not sending, 1 = sending (Interlocked)
	volatile LONG m_processing = 0; // processing flag: 0 = idle, 1 = being processed by a worker

	// 수신 백프레셔 상태. 1 이면 잡 큐가 밀려서 다음 WSARecv 를 걸지 않았다.
	//
	// 단순한 상태 표시가 아니라 소유권 토큰이다. m_releasePending 과 같은 방식으로
	// 쓴다 — 1 -> 0 전이에 성공한 스레드 하나만 재개를 수행한다. 멈추는 쪽
	// (수신 완료 핸들러)과 재개하는 쪽(잡 워커)이 동시에 "내가 할 차례" 라고
	// 판단할 수 있어서, 둘 중 하나만 고르는 장치가 필요하다.
	//
	// 이 플래그가 1 인 동안 세션은 IO 카운트를 하나 들고 있다. 그게 이
	// 설계의 핵심이다 — 멈춰 있는 세션이 회수되어 다른 접속에 재배포되는
	// 것을 그 카운트가 막는다. 카운트의 소유권은 1 -> 0 을 이긴 스레드가
	// 가져가고, 그 스레드가 반드시 DecrementIO 로 내려놓는다.
	volatile LONG m_recvPaused = 0;

	// 여태 백프레셔가 걸린 횟수. 진단 전용이라 정확한 순서는 필요 없다.
	volatile LONG m_recvPauseCount = 0;

	// 서비스에 접속을 알렸는가. 종료 통지의 짝을 맞추는 래치다.
	// (MarkServiceConnectNotified / ConsumeServiceConnectNotified 주석 참고)
	volatile LONG m_serviceConnectNotified = 0;


	// 서비스 로직별 세션 컨텍스트
	SessionContext* m_sessionContext = nullptr;

	// IP Address & Port
	mutable SRWLOCK m_remoteAddressLock = SRWLOCK_INIT;
	char m_clientIPAddress[INET_ADDRSTRLEN] = { 0, };
	uint16_t m_clientPort = 0;
	volatile LONGLONG m_lastRecvTick = 0;
	volatile LONGLONG m_lastHeartbeatTick = 0;

public:
	// --- 기본 동작 ---
	bool Initialize(SESSION_ROLE sessionType, uint32_t sessionId);
	void ResetSession();
	void Finalize();
	bool OnConnect();
	bool OnDisconnect();

	// --- 소켓 제어 ---
	//
	// AttachSocket 과 DetachSocket 은 짝이다. 붙일 때는 이미 들고 있지
	// 않은지 검사하므로(사용 중인 세션이 재배포되면 그 경로로 온다)
	// 본문이 .cpp 에 있다.
	void AttachSocket(SOCKET socket);
	SOCKET DetachSocket();

	void SetClientSocket(SOCKET socket) { m_clientSocket = socket; }
	SOCKET GetClientSocket() const { return m_clientSocket; }
	bool IsSocketInvalid() const { return (m_clientSocket == INVALID_SOCKET); }

	// --- 상태 관리 ---
	//
	// 연결 세션은 역할에 따라 Client 상태 또는 Server 상태를 사용한다.
	//
	// 헤더에 남겨 인라인시킨다. Interlocked 는 내장 함수라 DLL 호출이 생기지
	// 않으므로, 접근자를 .cpp 로 내리면 호출 비용만 늘고 얻는 것이 없다.
	//
	// 주의: 값 하나로 여러 번 비교해야 하면 getter 를 여러 번 부르지 말고
	// 지역 변수에 한 번 받아서 쓴다. 호출마다 다시 읽으므로 비교 도중에
	// 값이 바뀌면 어느 분기에도 걸리지 않는 결과가 나온다.
	// (IsTransportConnected 가 그 예다)
	void SetClientSessionState(ClientSessionState sessionState)
	{
		::InterlockedExchange(&m_clientSessionState, static_cast<LONG>(sessionState));
	}
	ClientSessionState GetClientSessionState() const
	{
		return static_cast<ClientSessionState>(::ReadAcquire(&m_clientSessionState));
	}

	void SetServerSessionState(ServerSessionState sessionState)
	{
		::InterlockedExchange(&m_serverSessionState, static_cast<LONG>(sessionState));
	}
	ServerSessionState GetServerSessionState() const
	{
		return static_cast<ServerSessionState>(::ReadAcquire(&m_serverSessionState));
	}

	// --- 타입 및 식별자 ---
	SESSION_ROLE GetSessionRole() const { return m_sessionRole; }
	void SetSessionID(uint32_t sessionId) { m_sessionId = sessionId; }

	// 서비스에 제공하는 세션 식별자의 공통 구현입니다.
	uint32_t GetSessionID() const override { return m_sessionId; }

	// --- 종료 사유 ---
	//
	// 끊기로 결정한 자리에서 부른다. 실제로 종료가 일어나는 자리가 아니라
	// 이유를 아는 자리다. 그 둘은 대개 다르다.
	//
	// 먼저 쓴 값이 이긴다. 종료는 연쇄로 일어나기 때문이다 — 프로토콜
	// 위반으로 끊기로 하면 곧 소켓이 닫히고 걸려 있던 I/O 가 10054 로
	// 실패한다. 나중 것이 이기면 근본 원인이 매번 SocketError 로 덮인다.
	void NoteDisconnectReason(DisconnectReason reason)
	{
		::InterlockedCompareExchange(&m_disconnectReason,
			static_cast<LONG>(reason), static_cast<LONG>(DisconnectReason::Unknown));
	}

	DisconnectReason GetDisconnectReason() const
	{
		return static_cast<DisconnectReason>(::ReadAcquire(&m_disconnectReason));
	}

	// 세션을 재사용하기 전에 되돌린다. 남아 있으면 다음 접속의 종료가
	// 지난 접속의 사유를 물려받는다.
	void ClearDisconnectReason()
	{
		::InterlockedExchange(&m_disconnectReason, static_cast<LONG>(DisconnectReason::Unknown));
	}

	// --- I/O 수명 ---
	//
	// 엔진 내부 전용. ISession 에는 없다 (이유는 그쪽 주석).
	// 이미 유효한 세션 참조를 가진 호출자만 추가할 수 있습니다.
	void AddLifetimeReference();
	// 마지막 참조라면 세션이 반환될 수 있으므로 호출 후 세션에 접근하지 않습니다.
	void ReleaseLifetimeReference();
	void IncrementIO();
	void DecrementIO();

	bool CancelPendingIO();
	IOWaitResult WaitForIOCancelComplete(const uint32_t timeout_ms);

	// I/O 발행을 시도한다. 발행 지점(PostReceive / PostCurrentSend)이
	// IncrementIO 대신 부른다.
	//
	//   true  : 카운트를 하나 들고 나왔다. 부르는 쪽은 반드시 WSARecv/WSASend 를
	//           걸어야 하고, 그 완료가 DecrementIO 를 한다.
	//   false : 취소가 이미 걸렸다. 카운트는 이 함수가 되돌렸으므로 부르는
	//           쪽이 따로 할 일은 없다.
	//
	// 주의: false 를 받은 뒤에는 세션을 만지면 안 된다. 되돌린 그 감소가
	// 마지막이었다면 예약된 반납이 그 자리에서 끝나고 세션은 풀로 돌아간다.
	// 정리할 것이 있으면 이 함수를 부르기 전에 끝내야 한다.
	//
	// 왜 필요한가
	//   CancelIoEx 는 부르는 그 순간 걸려 있던 것만 취소하는데, 완료 핸들러는
	//   마지막에 늘 다음 I/O 를 건다. 막지 않으면 카운트가 0 으로 내려오지
	//   않아 WaitForIOCancelComplete 가 10초를 태우고, 미완료 I/O 를 안은
	//   세션이 리셋된다. (실측: bench Phase 4 반복 8회 전부)
	//
	// 왜 검사보다 증가가 먼저인가
	//   반대로 하면 검사를 통과한 채 멈춘 스레드가 있는 동안 카운트가 0 에
	//   도달할 수 있다. 그러면 대기가 통과해 세션이 리셋되고, 그 뒤에 그
	//   스레드가 I/O 를 걸어 죽은 세션에 미완료 I/O 를 남긴다.
	//   (실측: 검사를 먼저 두었을 때 Phase 4 가 4회 중 1회 위반 2건으로 실패)
	//   먼저 올려 두면 그 구간 내내 카운트가 0 이 아니므로 대기가 통과하지
	//   못하고, 실제로 걸린 I/O 는 재취소가 걷어 간다.
	bool BeginIO();

	// 취소가 걸렸는가. 값싼 사전 확인용이다.
	//
	// 이것만으로는 발행을 막을 수 없다 (검사와 발행 사이에 취소가 걸린다).
	// 발행 여부의 판단은 반드시 BeginIO 가 한다. 이 함수는 "카운트를 들고
	// 있는 동안 미리 정리해 둬야 하는" 경우에만 쓴다 —
	// PostCurrentSend 가 송신 엔트리를 되돌리는 자리가 그렇다.
	bool IsIOCancelRequested() const;

	// --- 지연 반납 ---
	//
	// 완료 핸들러 안에서 반납이 필요해지는 경로가 여럿이다. 그때 그 자리에서
	// 카운트가 0 이 되기를 기다리면, 핸들러 자신이 들고 있는 몫 때문에
	// 자기를 기다리게 된다(실측: bench 실행당 200회 이상의 10초 타임아웃).
	//
	// 그래서 기다리지 않는다. 반납을 예약해 두고, 마지막 완료가 카운트를
	// 0 으로 내리는 그 자리에서 마무리한다.
	void SetOwnerCallbacks(OwnerCallbacks callbacks);

	// 반납을 예약한다.
	//   true  : 남은 수명 참조가 없고 마무리 권한을 이 스레드가 잡았다.
	//           부르는 쪽이 그 자리에서 마무리해야 한다.
	//   false : 아직 I/O 또는 작업 참조가 남아 있거나, 다른 스레드가 이미
	//           권한을 잡았다. 부르는 쪽은 즉시 돌아간다.
	bool RequestRelease();

	// 반납이 예약되어 있는가. 핸들러가 더 진행할지 판단할 때 쓴다.
	bool IsReleasePending() const;

	// --- 진단 ---
	//
	// 미완료 I/O 수입니다. 0이어도 준비 큐·Job 워커가 세션을 사용할 수 있습니다.
	// 실제 세션 반환은 전체 수명 참조가 0일 때만 허용합니다.
	// 하네스가 시나리오마다 회계를 검사하려면 밖에서 읽을 수 있어야 한다.
	LONG GetOutstandingIOCount() const;

	bool InitializeMemoryPool(TlsMemoryPool* jobMemoryPool, TlsMemoryPool* packetMemoryPool, const SessionBufferConfig& bufferConfig);

	bool IsReady() const;
	// 두 단계다. IsTransportConnected 는 소켓이 붙었는가,
	// IsEstablished 는 인증까지 끝났는가. (설명은 .cpp 주석)
	bool IsTransportConnected() const;
	bool IsEstablished() const override;

	// --- 서비스 접속/종료 통지의 짝 맞추기 ---
	//
	// OnClientConnect 를 받은 세션만 OnClientDisconnect 를 받아야 하고,
	// 정확히 한 번만 받아야 한다. 그 조건을 세션에 래치로 들고 있는다.
	//
	// 상태(ServerSessionState 등)로 대신할 수 없다. OnConnect 가 성공해
	// CONNECTED 로 가더라도 그 직후 통지 호출까지 갔는지는 상태에 남지
	// 않는다. 접속 시퀀스가 중간에 실패하면 통지 없이 끝난다.
	void MarkServiceConnectNotified();

	// 종료를 알려야 하는가. true 는 한 스레드에게만 돌아간다.
	//
	// 반납 경로가 여럿이고(소켓 오류 / 좀비 정리 / 서버 종료) 서로 겹칠
	// 수 있으므로, "알릴 차례인가" 와 "표시 지우기" 가 한 번에 일어나야
	// 두 번 알리지 않는다.
	bool ConsumeServiceConnectNotified();

	OverlappedEx& GetConnectOverlapped();

	const RecvPacketBuffer& GetReceiveBuffer() const;
	RecvPacketBuffer& GetReceiveBuffer();

	const SendPacketQueue* GetSendPacketQueue() const;
	SendPacketQueue* GetSendPacketQueue();

	// 아직 상대에게 다 건네지 못한 송신이 남아 있는가.
	//
	// 큐에 밀려 있거나 WSASend 가 걸려 있으면 참이다. 이건 단순한 수위
	// 조회가 아니라 "상대의 수신 윈도가 닫혀 있다" 는 관측이다 — 윈도를
	// 닫아 두려면 상대의 커널이 살아서 ACK 를 하고 있어야 하므로, 침묵한
	// 세션이 죽은 것인지 느린 것인지 가르는 유일한 단서가 된다.
	// (쓰는 곳은 ClientSessionPool::DisconnectZombieSessions)
	bool HasPendingSend() const;

	SessionJobQueue& GetJobQueue() const;

	// 잡 큐 수위. 큐 객체가 없는 상태(초기화 전/정리 후)에서도 안전하다.
	uint32_t GetJobQueueDepth() const;
	uint32_t GetJobQueuePeakDepth() const;

	bool PostReceive();

	// --- 수신 백프레셔 ---
	//
	// 수신 완료 핸들러가 "다음 수신을 건다" 자리에서 PostReceive 대신 이것을
	// 부른다. 잡 큐가 고수위를 넘었으면 수신을 걸지 않고 멈춘다. 커널 수신
	// 버퍼가 차면 TCP 수신 윈도가 닫히고, 보내는 쪽이 스스로 막힌다.
	//
	//   true  : 다음 수신이 걸렸거나, 백프레셔로 의도적으로 멈췄다. 둘 다 정상.
	//   false : 수신을 걸 수 없다. 부르는 쪽이 세션을 반납해야 한다.
	//           (PostReceive 가 false 를 돌려주던 것과 같은 계약)
	//
	// 수위는 SessionBufferConfig 가 정한다. recvPauseJobDepth 가 0 이면
	// 그냥 PostReceive 를 부르는 것과 같다.
	//
	// 하트비트와의 관계
	//   멈춰 있는 동안은 수신이 없으므로 m_lastRecvTick 이 갱신되지 않는다.
	//   멈춤이 하트비트 타임아웃(기본 15초)보다 오래 이어지면 좀비 정리가
	//   그 세션을 걷어 간다. 그게 옳다 — 15초를 드레인하지 못하는 소비자를
	//   기다려 주는 것은 백프레셔가 아니라 그냥 멈춘 서버다. 정상 부하에서
	//   한 번의 멈춤은 (고수위 x 잡 1건 처리시간) 이므로 밀리초 단위다.
	bool PostReceiveOrPause();

	// 잡 워커가 드레인을 끝낸 자리에서 부른다. 멈춰 있고 큐가 저수위 아래로
	// 내려왔으면 수신을 다시 건다. 그 외에는 아무것도 하지 않는다.
	//
	// 주의: 이 함수가 돌아온 뒤에는 세션을 만지면 안 된다. 재개가 일시정지의
	// IO 카운트를 내려놓고, 그게 마지막 카운트였다면 예약된 반납이 그 자리에서
	// 마무리되어 세션이 풀로 돌아간다. 워커 루프의 마지막 줄이어야 한다.
	void ResumeReceiveIfDrained();

	// 백프레셔가 실제로 걸렸는지 밖에서 확인하는 관측점.
	// 이 값이 0 이면 부하를 걸었어도 수위가 고수위에 닿지 않은 것이다.
	uint32_t GetRecvPauseCount() const;
	bool IsReceivePaused() const;

	bool TrySendNext();
	bool PostCurrentSend();
	bool OnSendCompleted(const DWORD bytesTransferred);

	// 성공하면 핸들이 비워지고 큐가 소유한다.
	// 실패하면 호출자가 소유권을 유지한다.
	[[nodiscard]]
	bool SubmitJob(JobHandle&& jobHandle);

	// 성공하면 packet은 비워지고 송신 큐가 소유한다.
	// 실패하면 packet은 소유권을 유지한다.
	[[nodiscard]]
	bool EnqueueSendPacket(PacketHandle&& packetHandle) override;

	// Consumes this reference on both success and failure.
	[[nodiscard]]
	bool EnqueueSharedSendPacket(SharedPacketHandle packetHandle) override;

	void HandleSocketError(int errorCode, IO_OPERATION ioOperation);

	void UpdateProcessingFlag(LONG value);
	bool IsProcessingReady();

	void SetSessionContext(SessionContext* sessionContext) override;
	SessionContext* GetSessionContext() const override;

	void SetCurrentJob(Job* job);
	// Called only by the currently executing client packet handler.
	[[nodiscard]]
	PacketHandle TakeCurrentPacket() override;

	void SetRemoteAddress(const char* ipAddress, uint16_t port);

	// 접속 폭주 방어에서 같은 주소의 접속 수를 셀 때 쓴다.
	// 세션이 비어 있으면 빈 문자열이다.
	[[nodiscard]] bool HasRemoteAddress(const char* ipAddress) const;
	void ClearRemoteAddress();
	void UpdateLastRecvTick();
	void UpdateLastHeartbeatTick();
	uint64_t GetLastRecvTick() const;
	uint64_t GetLastHeartbeatTick() const;
	uint64_t GetLastActiveTick() const;
	bool IsHeartbeatTimedOut(uint64_t nowTick, uint64_t timeout_ms) const;
	void MarkHeartbeatTimeout();
	bool SendSystemHeartbeatRequest();
	bool SendSystemHeartbeatResponse(uint64_t requestTick);

private:
	bool CanSendPacket(PACKET_ID_TYPE packetId) const;

	// --- 송신 거부 로그의 발생 억제 ---
	//
	// EnqueueSendPacket 의 거부 두 가지(인증 전 송신, 송신 큐 포화)는
	// 호출부가 얼마나 자주 부르는지에 따라 빈도가 정해진다. 그런데 거부를
	// 만난 호출부가 재시도 루프를 도는 것은 지극히 자연스러운 형태다 —
	// 그러면 거부 1건이 로그 수억 줄이 된다.
	//
	// 실측: 세션이 established 에 도달하지 못한 실행에서 하네스가 재시도를
	// 돌자 337초 만에 로그가 41GB 까지 자랐다. 디스크가 찰 때까지 멈추지
	// 않는다. 엔진이 호출부의 루프를 디스크 고갈로 바꾼 셈이다.
	//
	// 그래서 이유별로 1초에 한 줄만 남기고, 그 사이 억제된 횟수를 다음 줄에
	// 실어 보낸다. 정보는 유지되고 양은 상한이 생긴다.
	// (같은 판단이 EngineMemoryPoolHelper.h 에도 적혀 있다 — 패킷당 호출되는
	//  경로에 로그를 남기면 그것만으로 처리량이 한 자리 수로 떨어진다)
	enum class SendRejectReason : uint32_t
	{
		NotEstablished = 0,
		SendQueueFull = 1,
		Count
	};

	// true 면 지금 한 줄 남겨도 된다. outSuppressed 는 직전 창에서 삼킨 횟수다.
	bool ShouldLogSendReject(SendRejectReason reason, uint32_t& outSuppressed);

	volatile LONGLONG m_sendRejectLogTick[static_cast<uint32_t>(SendRejectReason::Count)] = {};
	volatile LONG     m_sendRejectSuppressed[static_cast<uint32_t>(SendRejectReason::Count)] = {};

	// 멈춰 뒀던 수신을 실제로 다시 건다.
	//
	// m_recvPaused 의 1 -> 0 전이를 이긴 스레드 하나만 본문을 수행하고,
	// 그 스레드가 일시정지의 IO 카운트도 소비한다. 진 스레드는 즉시 돌아간다.
	// 돌아온 뒤 세션 접근 금지는 ResumeReceiveIfDrained 와 같다.
	void ResumeReceive();

	// 취소가 걸린 뒤 남아 있는 일시정지의 IO 카운트를 놓는다.
	void ReleaseUnpostedIO();
	[[nodiscard]] bool BeginIOCancellation();
	void ReissueCancelIo();
	void ResetLifetimeState();
	void SetConnectReadyState();

	// 종료 시 송신 큐, Job 큐, 수신 버퍼 순서로 해제한다.
	void ReleaseMemoryResources();

public:
	// Callback Func
	void NotifyDisconnect();

};
