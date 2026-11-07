#pragma once

#include <stdint.h> // for uint16_t, uint32_t, uint64_t
#include <atomic> // for std::atomic, std::memory_order_relaxed
#include <memory> // for std::unique_ptr

#include "ISession.h"
#include "SessionDefs.h"
#include "DisconnectReason.h"

#include "../Network/OverlappedEx.h"
#include <mswsock.h> // for LPFN_CONNECTEX
#include "../Buffer/SessionBufferConfig.h"
#include "../Buffer/SendPacketQueue.h"
#include <span> // for std::span
#include "../Job/JobHandle.h"

class Job;
class SessionJobQueue;
class UniqueSocket;
class RecvPacketBuffer;
class TlsMemoryPool;

#define INET_ADDRSTRLEN  22

class ClientSession final : public ISession
{
	struct ReturnReference { void operator()(ClientSession* session) const noexcept; };
	struct FinishIO { void operator()(ClientSession* session) const noexcept; };

public:
	using Reference = std::unique_ptr<ClientSession, ReturnReference>;
	using IOCompletion = std::unique_ptr<ClientSession, FinishIO>;

	// 종료 요청과 마지막 참조 반환을 같은 소유자에게 전달한다.
	struct OwnerCallbacks
	{
		void* owner;
		void (*requestDisconnect)(void*, ClientSession*);
		void (*releaseReady)(void*, ClientSession*);
	};

	ClientSession();
	~ClientSession() override;

	// 엔진이 세션을 초기화하고 연결 상태를 전환한다.
	bool Initialize(SESSION_ROLE sessionType, uint32_t sessionId);
	bool InitializeMemoryPool(TlsMemoryPool* jobMemoryPool, TlsMemoryPool* packetMemoryPool, const SessionBufferConfig& bufferConfig);
	bool OnConnect();
	// 소켓을 닫고 접속을 알렸던 서비스에 종료 사유를 전달한 뒤 연결 상태를 정리한다.
	void FinishDisconnect(CloseSocketFunc closeSocket, SessionDisconnectNotifyFunc notify, void* context);
	void ResetSession();

	// 소켓 소유권을 인수하고 엔진에는 IOCP 등록에 쓸 핸들만 빌려준다.
	[[nodiscard]] bool AttachSocket(UniqueSocket socket, const char* remoteIP = nullptr, uint16_t port = 0);
	SOCKET GetClientSocket() const { return m_clientSocket.load(); }
	bool IsSocketInvalid() const { return GetClientSocket() == INVALID_SOCKET; }
	bool PostConnect(LPFN_CONNECTEX connectEx, const SOCKADDR_IN& address);

	// 역할에 맞는 연결 상태를 읽거나 변경한다.
	uint32_t GetSessionID() const override { return m_sessionId; }
	SESSION_ROLE GetSessionRole() const { return m_sessionRole; }
	bool IsReadyToConnect() const;
	bool BeginAuthentication();
	bool Establish();
	void RejectAuthentication(DisconnectReason reason, PacketHandle response);
	bool IsTransportConnected() const;
	bool IsEstablished() const override;

	// 최초 종료 사유를 보존하고 접속을 알린 세션에만 종료를 한 번 통지한다.
	void RequestDisconnect(DisconnectReason reason);

	// 참조를 보유한 동안만 세션을 사용할 수 있다. 마지막 참조는 예약된 반환을 실행할 수 있다.
	void SetOwnerCallbacks(OwnerCallbacks callbacks) { m_ownerCallbacks = callbacks; }
	[[nodiscard]] Reference AcquireReference();
	// 종료를 선점한 소유자가 호출한다. 새 I/O를 차단하고 마지막 사용자에게 반환을 맡긴다.
	void CloseWhenIdle();

	// 완료 분배자가 발행 시 확보한 한 건을 인수하고 핸들러가 끝날 때 반환한다.
	[[nodiscard]] IOCompletion TakeIOCompletion();
	LONG GetOutstandingIOCount() const;
	bool CancelPendingIO();
	IOWaitResult WaitForIOCancelComplete(const uint32_t timeout_ms);

	enum class ReceiveStatus { Packet, NeedMoreData, Stopped };
	struct [[nodiscard]] ReceiveResult
	{
		ReceiveStatus status;
		PacketHandle packet;
		uint16_t packetId = 0;
	};

	// 수신 바이트를 반영한다. 종료 또는 오류이면 false를 반환한다.
	[[nodiscard]] bool OnReceiveCompleted(DWORD bytesTransferred);
	// 완성된 패킷 하나를 이동 반환하며, 종료 요청 이후에는 인출을 중단한다.
	[[nodiscard]] ReceiveResult ReadReceivedPacket();
	// 큐 수위와 취소 상태에 따라 다음 수신을 발행하거나 일시정지한다.
	void ContinueReceive();
	void ResumeReceiveIfDrained();
	bool IsReceivePaused() const;
	uint32_t GetRecvPauseCount() const;

	// 단독 송신은 성공할 때만 핸들을 인수하고 공유 송신은 전달된 참조를 소비한다.
	[[nodiscard]] bool EnqueueSendPacket(PacketHandle&& packetHandle) override;
	[[nodiscard]] bool EnqueueSharedSendPacket(SharedPacketHandle packetHandle) override;
	void OnSendCompleted(const DWORD bytesTransferred);
	bool HasPendingSend() const;

	// Job을 등록하고 워커가 실행할 세션을 한 번만 선점한다.
	[[nodiscard]] bool EnqueueJob(JobHandle&& jobHandle);
	SessionJobQueue& GetJobQueue() const;
	uint32_t GetJobQueuePeakDepth() const;
	void SetCurrentJob(Job* job);
	[[nodiscard]] PacketHandle TakeCurrentPacket() override;

	// 서비스 컨텍스트를 소유하고 연결의 원격 주소를 관리한다.
	void SetSessionContext(SessionContext* sessionContext) override;
	SessionContext* GetSessionContext() const override;
	[[nodiscard]] bool HasRemoteAddress(const char* ipAddress) const;

	// 최근 통신 시각으로 타임아웃을 판단하고 시스템 하트비트를 송수신한다.
	uint64_t GetLastRecvTick() const;
	void UpdateLastHeartbeatTick();
	bool IsHeartbeatTimedOut(uint64_t nowTick, uint64_t timeout_ms) const;
	void MarkHeartbeatTimeout();
	bool SendSystemHeartbeatRequest();
	bool SendSystemHeartbeatResponse(uint64_t requestTick);

private:
	friend class ServerReadySessionQueue;
	friend class ServerJobScheduler;

	bool TryClaimProcessing();
	void ReleaseProcessing();
	void NotifyReleaseReady();
	void RecordDisconnectReason(DisconnectReason reason);

	enum class State { Ready, Connecting, Connected, Authenticating, Established, TimedOut, Disconnected };
	// 재사용 및 소멸 전제와 역할별 초기 상태를 관리한다.
	void ClearRemoteAddress();

	// 발행과 취소를 같은 락으로 직렬화하고 마지막 완료 또는 참조 반환에서 소유자를 깨운다.
	template<typename StartOperation> int PostIO(StartOperation start);
	void CompleteIO();
	void ReleaseReference();
	bool ClaimReleaseLocked();
	DWORD CancelSocketIOLocked() const;
	void CheckIdle() const;
	bool TryPauseReceive();
	Reference TakeReceivePauseLocked();

	bool CanEnqueueSend(std::span<const std::byte> bytes) const;
	bool StartQueuedSend(bool enqueued);
	void TrySendNext();
	void PostCurrentSend();

	void HandlePostError(int errorCode, IO_OPERATION operation);

	// 재개와 취소 중 한 경로만 일시정지 참조를 반환한다.
	void UpdateLastRecvTick();
	bool PostReceive();
	uint32_t GetJobQueueDepth() const;

	// 수신과 하트비트 중 더 최근의 시각을 조회한다.
	uint64_t GetLastActiveTick() const;

	// 세션 식별자와 연결 상태
	SESSION_ROLE m_sessionRole = SESSION_ROLE::NONE;
	uint32_t m_sessionId = INVALID_SESSION_ID;
	std::atomic<State> m_state{State::Ready};
	std::atomic<SOCKET> m_clientSocket{INVALID_SOCKET};
	volatile LONG m_disconnectReason = static_cast<LONG>(DisconnectReason::Unknown);
	volatile LONG m_serviceConnectNotified = 0;

	// 수명·취소 상태는 풀의 슬롯마다 한 번 만들고 재대여할 때 재사용한다.
	OwnerCallbacks m_ownerCallbacks{};
	mutable SRWLOCK m_lifetimeLock = SRWLOCK_INIT;
	CONDITION_VARIABLE m_ioCompleted = CONDITION_VARIABLE_INIT;
	LONG m_ioCount = 0;
	LONG m_referenceCount = 0;
	bool m_releasePending = false;
	bool m_cancelled = false;
	bool m_recvPaused = false;

	// 서비스 데이터와 원격 주소
	SessionContext* m_sessionContext = nullptr;
	mutable SRWLOCK m_remoteAddressLock = SRWLOCK_INIT;
	char m_clientIPAddress[INET_ADDRSTRLEN] = { 0, };
	uint16_t m_clientPort = 0;

	// 연결 및 수신 I/O
	SessionBufferConfig m_bufferConfig;
	OverlappedEx m_connectOverlapped{};
	OverlappedEx m_recvOverlapped{};
	std::unique_ptr<RecvPacketBuffer> m_recvPacketBuffer;

	// 송신 실행권을 가진 한 작업만 현재 패킷과 부분 송신 위치를 변경한다.
	SendPacketQueue m_sendQueue;
	OverlappedEx m_sendOverlapped{};
	SendPacket m_currentSendPacket;
	uint32_t m_sendOffset = 0;
	std::atomic<bool> m_sending{false};

	// Job 실행권과 핸들러가 빌려 쓰는 현재 Job
	std::unique_ptr<SessionJobQueue> m_jobQueue;
	volatile LONG m_processing = 0;
	Job* m_currentJob = nullptr;

	// 하트비트 송수신과 타임아웃 판정
	TlsMemoryPool* m_packetMemoryPool = nullptr;
	volatile LONGLONG m_lastRecvTick = 0;
	volatile LONGLONG m_lastHeartbeatTick = 0;

	// 진단용 누계. 수신 재개 판단에는 사용하지 않는다.
	std::atomic<uint32_t> m_recvPauseCount{0};
};
