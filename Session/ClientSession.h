#pragma once

#include <stdint.h>


#include "ISession.h"
#include "BaseSession.h"
#include "SessionContext.h"

#include "../Network/OverlappedEx.h"
#include "../Protocol/PacketID.h"
#include "../Buffer/SendPacketEntry.h"
#include "../Buffer/SessionBufferConfig.h"

struct Job;

class SessionJobQueue;
class RecvPacketBuffer;
class SendPacketQueue;
class TlsMemoryPool;
class ISessionEvent;
class PacketHandle;

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

#define INET_ADDRSTRLEN  22

class IOCP_ENGINE_API ClientSession final : public BaseSession
{
public:
	ClientSession();
	~ClientSession() override;

	// ClientSession 전용 멤버 변수
private:
	TlsMemoryPool* m_jobMemoryPool = nullptr;
	TlsMemoryPool* m_packetMemoryPool = nullptr;

	// Client Role
	OverlappedEx m_connectOverlapped{};

	// Client / Server Role
	OverlappedEx m_recvOverlapped{};
	RecvPacketBuffer* m_recvPacketBuffer = nullptr;

	// 이 세션의 버퍼 크기 정책. InitializeMemoryPool 에서 받아 보관한다.
	// 송신 상한 검사가 여기를 본다.
	SessionBufferConfig m_bufferConfig;

	OverlappedEx m_sendOverlapped{};
	SendPacketQueue* m_sendPacketQueue = nullptr;

	// 큐에서 꺼내 전송 중인 엔트리. 큐 밖에 있는 유일한 엔트리이고,
	// 다 쓰면 m_sendPacketQueue->ReleaseEntry 로 되돌린다.
	//
	// 예전에는 전용 서술자 풀 포인터(m_sendPacketPool)를 따로 들고 있었다.
	// 패킷 반납과 서술자 반납을 세션이 각각 불러야 했기 때문이다. 반납을
	// 큐 한 곳으로 모으면서 그 멤버가 필요 없어졌다.
	SendPacketEntry* m_currentSendPacket = nullptr;
	Job* m_currentJob = nullptr;
	uint32_t m_sendOffset = 0;
	SessionJobQueue* m_jobQueue = nullptr;

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

	ISessionEvent* m_eventHandler = nullptr;

	// 서비스 로직별 세션 컨텍스트
	SessionContext* m_sessionContext = nullptr;

	// IP Address & Port
	char m_clientIPAddress[INET_ADDRSTRLEN] = { 0, };
	uint16_t m_clientPort = 0;
	volatile LONGLONG m_lastRecvTick = 0;
	volatile LONGLONG m_lastHeartbeatTick = 0;


	// Override
public:
	// --- 기본 동작 ---
	bool Initialize(SESSION_ROLE sessionType, uint32_t sessionId) override;
	void ResetSession() override;
	void Finalize() override;

	bool OnAccept() override;
	bool OnConnect() override;
	bool OnDisconnect() override;

	// ClientSession 전용 메서드
public:
	bool InitializeMemoryPool(TlsMemoryPool* sendQueueMemoryPool, TlsMemoryPool* jobMemoryPool, TlsMemoryPool* packetMemoryPool, const SessionBufferConfig& bufferConfig);

	bool IsReady() const;
	// 두 단계다. IsTransportConnected 는 소켓이 붙었는가,
	// IsEstablished 는 인증까지 끝났는가. (설명은 .cpp 주석)
	bool IsTransportConnected() const;
	bool IsEstablished() const;

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

	bool EnqueueJob(Job* job, bool& wasEmpty);
	bool SubmitJob(Job* job);
	bool EnqueueSendPacket(void** packetData, uint32_t packetSize);
	// 성공하면 packet은 비워지고 송신 큐가 소유한다.
	// 실패하면 packet은 소유권을 유지한다.
	[[nodiscard]]
	bool EnqueueSendPacket(PacketHandle&& packetHandle);

	bool EnqueueSharedSendPacket(const void* packetData, uint32_t packetSize, SendPacketReleaseFunc releaseFunc, void* releaseContext);

	void HandleSocketError(int errorCode, IO_OPERATION ioOperation);

	void UpdateProcessingFlag(LONG value);
	bool IsProcessingReady();

	void SetSessionContext(SessionContext* sessionContext);
	SessionContext* GetSessionContext() const;

	void SetCurrentJob(Job* job);
	void ClearCurrentJobData();

	void SetRemoteAddress(const char* ipAddress, uint16_t port);

	// 접속 폭주 방어에서 같은 주소의 접속 수를 셀 때 쓴다.
	// 세션이 비어 있으면 빈 문자열이다.
	const char* GetClientIPAddress() const { return m_clientIPAddress; }
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
	// (왜 필요한지는 BaseSession::ReleaseUnpostedIO 주석)
	void ReleaseUnpostedIO() override;

	// InitializeMemoryPool 이 잡은 자원만 되돌린다.
	// 그쪽의 실패 정리와 Finalize 가 함께 쓴다. 부분 생성 상태에서도 안전하다.
	void ReleaseMemoryResources();

public:
	// Callback Func
	void SetEventHandler(ISessionEvent* handler);
	void NotifyDisconnect();

	// 여기에 서비스 훅(OnClientConnect / OnClientDisconnect / OnReceive /
	// OnSend)이 protected 가상으로 네 개 더 있었다. 전부 죽은 선언이었다.
	//
	//   이 클래스는 final 이라 재정의가 문법적으로 불가능하고,
	//   protected 라 밖에서 부를 수도 없고,
	//   ClientSession 자신도 한 번도 부르지 않았고,
	//   ISession / BaseSession 에 같은 이름이 없어 무언가를 재정의하던
	//   것도 아니었다.
	//
	// 실제 훅은 IOCPServer / IOCPClient 쪽 동명 가상이고, 서비스는 그것을
	// 재정의한다. 세션에 달린 사본은 같은 이름이 두 계층에 있다는 사실만으로
	// "세션을 상속해서 받는 길도 있나" 를 찾게 만드는 함정이었다.
	//
	// ISession 을 순수 가상 14개에서 1개로 줄일 때의 판단과 같은 정리다 —
	// virtual 은 재정의될 수 있다는 선언인데, 그게 참이 아니면 읽는 사람의
	// 시간만 쓴다.
};

