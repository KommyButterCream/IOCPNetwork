#pragma once

#include <stdint.h> // for uint16_t, uint32_t, uint64_t
#include <atomic> // for std::atomic, std::memory_order_relaxed
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

#define INET_ADDRSTRLEN  22

class ClientSession final : public ISession
{
public:
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
	void OnDisconnect();
	void ResetSession();

	// 소켓의 부착과 분리는 연결 소유자가 담당한다.
	void AttachSocket(SOCKET socket);
	SOCKET DetachSocket();
	SOCKET GetClientSocket() const { return m_clientSocket; }
	bool IsSocketInvalid() const { return (m_clientSocket == INVALID_SOCKET); }
	OverlappedEx& GetConnectOverlapped();

	// 역할에 맞는 연결 상태를 읽거나 변경한다.
	uint32_t GetSessionID() const override { return m_sessionId; }
	SESSION_ROLE GetSessionRole() const { return m_sessionRole; }
	ClientSessionState GetClientSessionState() const
	{
		return static_cast<ClientSessionState>(::ReadAcquire(&m_clientSessionState));
	}
	void SetClientSessionState(ClientSessionState sessionState)
	{
		::InterlockedExchange(&m_clientSessionState, static_cast<LONG>(sessionState));
	}
	ServerSessionState GetServerSessionState() const
	{
		return static_cast<ServerSessionState>(::ReadAcquire(&m_serverSessionState));
	}
	void SetServerSessionState(ServerSessionState sessionState)
	{
		::InterlockedExchange(&m_serverSessionState, static_cast<LONG>(sessionState));
	}
	bool IsTransportConnected() const;
	bool IsEstablished() const override;

	// 최초 종료 사유를 보존하고 접속을 알린 세션에만 종료를 한 번 통지한다.
	void NoteDisconnectReason(DisconnectReason reason)
	{
		::InterlockedCompareExchange(&m_disconnectReason,
			static_cast<LONG>(reason), static_cast<LONG>(DisconnectReason::Unknown));
	}
	DisconnectReason GetDisconnectReason() const
	{
		return static_cast<DisconnectReason>(::ReadAcquire(&m_disconnectReason));
	}
	void MarkServiceConnectNotified();
	bool ConsumeServiceConnectNotified();
	void NotifyDisconnect();

	// 참조를 보유한 동안만 세션을 사용할 수 있다. 마지막 참조는 예약된 반환을 실행할 수 있다.
	void SetOwnerCallbacks(OwnerCallbacks callbacks);
	void AddLifetimeReference();
	void ReleaseLifetimeReference();
	// 즉시 반환 가능하면 true, 참조가 남으면 마지막 반환자에게 마무리를 맡긴다.
	bool RequestRelease();
	bool IsReleasePending() const;

	// 미완료 I/O를 추적한다. I/O 완료와 세션의 마지막 수명 참조 반환은 별개다.
	void IncrementIO();
	void DecrementIO();
	LONG GetOutstandingIOCount() const;
	bool CancelPendingIO();
	IOWaitResult WaitForIOCancelComplete(const uint32_t timeout_ms);

	// 수신 완료 처리와 Job 큐 수위에 따른 일시정지·재개를 지원한다.
	RecvPacketBuffer& GetReceiveBuffer();
	bool PostReceiveOrPause();
	void ResumeReceiveIfDrained();
	bool IsReceivePaused() const;
	uint32_t GetRecvPauseCount() const;

	// 단독 송신은 성공할 때만 핸들을 인수하고 공유 송신은 전달된 참조를 소비한다.
	[[nodiscard]] bool EnqueueSendPacket(PacketHandle&& packetHandle) override;
	[[nodiscard]] bool EnqueueSharedSendPacket(SharedPacketHandle packetHandle) override;
	bool OnSendCompleted(const DWORD bytesTransferred);
	bool HasPendingSend() const;

	// Job을 등록하고 워커가 실행할 세션을 한 번만 선점한다.
	[[nodiscard]] bool SubmitJob(JobHandle&& jobHandle);
	SessionJobQueue& GetJobQueue() const;
	uint32_t GetJobQueuePeakDepth() const;
	// 이미 실행권을 가진 워커가 있으면 false를 반환한다.
	[[nodiscard]] bool TryClaimProcessing();
	void ReleaseProcessing();
	void SetCurrentJob(Job* job);
	[[nodiscard]] PacketHandle TakeCurrentPacket() override;

	// 서비스 컨텍스트를 소유하고 연결의 원격 주소를 관리한다.
	void SetSessionContext(SessionContext* sessionContext) override;
	SessionContext* GetSessionContext() const override;
	void SetRemoteAddress(const char* ipAddress, uint16_t port);
	[[nodiscard]] bool HasRemoteAddress(const char* ipAddress) const;

	// 최근 통신 시각으로 타임아웃을 판단하고 시스템 하트비트를 송수신한다.
	void UpdateLastRecvTick();
	uint64_t GetLastRecvTick() const;
	void UpdateLastHeartbeatTick();
	bool IsHeartbeatTimedOut(uint64_t nowTick, uint64_t timeout_ms) const;
	void MarkHeartbeatTimeout();
	bool SendSystemHeartbeatRequest();
	bool SendSystemHeartbeatResponse(uint64_t requestTick);

private:
	enum class SendRejectReason : uint32_t
	{
		NotEstablished = 0,
		SendQueueFull = 1,
		Count
	};

	// 관측값은 연결별로 집계하며 송수신이나 수명 전이를 결정하지 않는다.
	struct Diagnostics
	{
		struct SendRejectWindow
		{
			std::atomic<uint64_t> lastLogTick{0};
			std::atomic<uint32_t> suppressed{0};
		};

		std::atomic<uint32_t> recvPauseCount{0};
		SendRejectWindow sendRejects[static_cast<uint32_t>(SendRejectReason::Count)]{};

		bool ShouldLogSendReject(SendRejectReason reason, uint32_t& outSuppressed);
		void Reset();
	};

	// 재사용 및 소멸 전제와 역할별 초기 상태를 관리한다.
	void SetConnectReadyState();
	void CheckCleanupPreconditions() const;
	void ClearRemoteAddress();
	void ReleaseMemoryResources();

	// 발행 전에 참조를 확보하고 취소 이후의 추가 I/O를 차단한다.
	bool BeginIO();
	bool IsIOCancelRequested() const;
	[[nodiscard]] bool BeginIOCancellation();
	void ReissueCancelIo();
	void HandleSocketError(int errorCode, IO_OPERATION ioOperation);

	// 재개와 취소 중 한 경로만 일시정지 참조를 반환한다.
	bool PostReceive();
	uint32_t GetJobQueueDepth() const;
	void ResumeReceive();
	void ReleaseUnpostedIO();

	// 송신 권한을 확보하고 부분 송신의 다음 구간을 발행한다.
	bool TrySendNext();
	bool PostCurrentSend();
	bool CanSendPacket(PACKET_ID_TYPE packetId) const;

	// 수신과 하트비트 중 더 최근의 시각을 조회한다.
	uint64_t GetLastHeartbeatTick() const;
	uint64_t GetLastActiveTick() const;

	// 세션 식별자와 연결 상태
	SESSION_ROLE m_sessionRole = SESSION_ROLE::NONE;
	uint32_t m_sessionId = INVALID_SESSION_ID;
	volatile LONG m_clientSessionState = static_cast<LONG>(ClientSessionState::NONE);
	volatile LONG m_serverSessionState = static_cast<LONG>(ServerSessionState::NONE);
	SOCKET m_clientSocket = INVALID_SOCKET;
	volatile LONG m_disconnectReason = static_cast<LONG>(DisconnectReason::Unknown);
	volatile LONG m_serviceConnectNotified = 0;

	// 소유자와 수명 참조. 반환 예약과 참조 수는 같은 락으로 보호한다.
	OwnerCallbacks m_ownerCallbacks{};
	mutable SRWLOCK m_lifetimeLock = SRWLOCK_INIT;
	LONG m_lifetimeReferences = 0;
	bool m_releasePending = false;
	volatile LONG m_ioCount = 0;
	volatile LONG m_cancelIo = 0;
	HANDLE m_ioCancelCompleteEvent = nullptr;

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
	volatile LONG m_recvPaused = 0;

	// 송신 완료 전까지 보관하는 패킷과 오프셋
	OverlappedEx m_sendOverlapped{};
	std::unique_ptr<SendPacketQueue> m_sendPacketQueue;
	SendPacket m_currentSendPacket;
	uint32_t m_sendOffset = 0;
	volatile LONG m_sending = 0;

	// Job 실행권과 핸들러가 빌려 쓰는 현재 Job
	std::unique_ptr<SessionJobQueue> m_jobQueue;
	volatile LONG m_processing = 0;
	Job* m_currentJob = nullptr;

	// 하트비트 송수신과 타임아웃 판정
	TlsMemoryPool* m_packetMemoryPool = nullptr;
	volatile LONGLONG m_lastRecvTick = 0;
	volatile LONGLONG m_lastHeartbeatTick = 0;

	Diagnostics m_diagnostics;
};
