#pragma once

#include "IOCPCore.h"
#include "ClientLivenessConfig.h"
#include "../Session/ISessionEvent.h"
#include "../Session/DisconnectReason.h"
#include "../Job/JobDefs.h"
#include "../Buffer/SessionBufferConfig.h"
#include "../Memory/EnginePoolConfig.h"

#include <stdint.h>


enum class IO_OPERATION;

struct OverlappedEx;

class ISession;
class BaseSession;
class ClientSession;
class TlsMemoryPool;
class ClientSessionScheduler;
class ClientWatchdogThread;
class PacketHandlerTable;
class SessionContext;

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

#define INET_ADDRSTRLEN  22

// ISessionEvent 를 직접 상속하는 이유. (서버는 그렇지 않다)
//
// 세션의 종료 요청(OnDisconnectRequest)을 받는 주체가 서버와 클라이언트에서
// 다르다. 서버는 그 신호로 세션을 풀에 반납해야 하므로 풀을 가진
// SessionManager 가 받는다. 클라이언트는 세션이 m_session 하나뿐이고 반납할
// 풀이 없으니 자기가 직접 받는다.
//
// 그래서 IOCPServer 는 IOCPCore 만 상속하고, 이쪽만 다중 상속이다. 같은
// 이벤트가 두 가지로 배선된 것처럼 보이지만 받아서 할 일이 다르다.
// (어댑터 멤버를 끼워 단일 상속으로 맞출 수도 있으나, 클래스와 간접 호출을
//  하나씩 늘려 가독성 흠 하나를 바꾸는 거래라 하지 않는다)
class IOCP_ENGINE_API IOCPClient : public IOCPCore, public ISessionEvent
{
public:
	IOCPClient();
	virtual ~IOCPClient();

public:
	// iocpThreadCount 는 GQCS 워커 수다. 0 이면 기본값 2 를 쓴다.
	//
	// 왜 서버처럼 논리 프로세서 수가 아닌가.
	//   클라의 세션은 하나이고, 그 세션에 걸린 WSARecv 도 하나뿐이다
	//   (m_recvOverlapped 가 세션당 하나). 그래서 수신 완료는 스레드를
	//   아무리 늘려도 직렬로 처리된다. 늘려서 얻는 것은 병렬 파싱이 아니라
	//   "하나가 붙들려도 다른 하나가 큐를 계속 비운다" 뿐이다.
	//
	// 그 하나가 필요한 이유는 서비스 콜백(OnReceive 등)이 워커 스레드에서
	// 실행되기 때문이다. 그래서 최소 2 다.
	//
	// 조절할 수 있게 연 이유는 그 콜백이 무거운 서비스가 있기 때문이다.
	// 다만 늘린다고 처리량이 비례해 늘지는 않는다 — 클라의 진짜 상한은
	// 잡을 실행하는 ClientSessionScheduler 스레드 하나다.
	//
	// poolConfig 의 사정은 IOCPServer::StartServer 의 같은 인자 주석과 같다.
	//
	// livenessConfig 는 "서버가 죽었다" 를 이쪽이 스스로 판정하는 방법이다.
	// 예전에는 그 수단이 아예 없어서, 랜선이 빠지거나 서버 프로세스가
	// 행되면 클라가 영원히 기다렸다. 기본값으로 켜져 있다.
	// (자세한 사정은 Core/ClientLivenessConfig.h)
	bool StartClient(const char* serverIp, const uint16_t port,
		const SessionBufferConfig& bufferConfig = SessionBufferPreset::Client(),
		uint32_t iocpThreadCount = 0,
		const EnginePoolConfig& poolConfig = EnginePoolPreset::Client(),
		const ClientLivenessConfig& livenessConfig = ClientLivenessConfig());
	void StopClient();

private:
	// 메모리 풀 객체를 실제로 지운다.
	//
	// StopClient 는 Finalize 까지만 한다. 풀 포인터가 서비스 핸들러
	// (HandlerContext)와 GetPacketMemoryPool() 로 나가 있어서, 늦게 도착하는
	// 반납이 해제된 객체를 만질 수 있기 때문이다.
	// (사정은 ENGINE_POOL::FinalizePools 주석)
	void DestroyMemoryPools();

	// Destory Flag
	LONG m_destroyFlag = 0;

	// 종료 절차를 한 스레드만 수행하게 하는 게이트.
	//
	// OnDisconnectRequest 는 여러 경로에서 동시에 들어온다.
	//   - 앱 스레드의 StopClient()
	//   - ConnectEx 취소 완료 처리(HandleConnectCancelled)
	//   - recv/send 소켓 오류 -> ClientSession::NotifyDisconnect
	//
	// 가드가 없으면 들어온 스레드마다 종료 절차를 중복 수행한다. 소켓을
	// 두 번 닫고 OnDisconnect 를 두 번 부른다.
	// (서버 쪽은 ClientSessionPool 의 poolState CAS 가 같은 역할을 한다)
	//
	// 예전에는 그 중복이 각자 10초짜리 WaitForIOCancelComplete 였다.
	// 이제 그 대기 자체가 없어졌지만(지연 반납) 중복 수행은 여전히
	// 막아야 하므로 게이트는 남는다.
	volatile LONG m_disconnecting = 0;

	SOCKET m_clientSocket = INVALID_SOCKET;

	ClientSession* m_session = nullptr;

	TlsMemoryPool* m_jobMemoryPool = nullptr;
	TlsMemoryPool* m_packetMemoryPool = nullptr;

	// 송신 큐 엔트리 전용 풀. 분리 이유는 IOCPServer 의 같은 멤버 주석 참고.
	TlsMemoryPool* m_sendQueueMemoryPool = nullptr;
	ClientSessionScheduler* m_clientSessionScheduler = nullptr;

	// 서버가 조용해진 것을 알아차리는 감시 스레드.
	// 왜 잡 스케줄러가 아니라 별도 스레드인지는 ClientWatchdogThread 주석.
	ClientWatchdogThread* m_clientWatchdog = nullptr;

	// StartClient 에서 받아 보관한다. 인증 응답이 도착했을 때 서버가 알려준
	// 하트비트 주기와 합쳐 실제 유휴 타임아웃을 정한다.
	ClientLivenessConfig m_livenessConfig;
	PacketHandlerTable* m_packetHandlerTable = nullptr;
	HandlerContext m_handlerContext = {};

	// IP Address & Port
	char m_serverIPAddress[INET_ADDRSTRLEN] = { 0, };
	uint16_t m_serverPort = 0;

private:
	// for ConnectEx
	using LPFN_CONNECTEX = BOOL(__stdcall*)(
		_In_ SOCKET s,
		_In_reads_bytes_(namelen) const struct sockaddr FAR* name,
		_In_ int namelen,
		_In_reads_bytes_opt_(dwSendDataLength) PVOID lpSendBuffer,
		_In_ DWORD dwSendDataLength,
		_Out_ LPDWORD lpdwBytesSent,
		_Inout_ LPOVERLAPPED lpOverlapped
		);

	// ConnectEx 함수 포인터
	LPFN_CONNECTEX m_connectEx = nullptr;

private:
	// GQCS 에 통지 받은 IO 처리
	void HandleCompletion(ULONG_PTR completionKey, LPOVERLAPPED overlapped, DWORD bytesTransferred, BOOL completionStatus) override;
	// 아래 핸들러들의 세션 인자는 전부 ClientSession 이다.
	// 이 클라이언트가 가진 세션은 m_session 하나뿐이다.
	void HandleSocketError(OverlappedEx* overlappedEx, ClientSession* session, int errorCode, IO_OPERATION ioOperation);

	void HandleConnect(uint32_t sessionId, DWORD bytesTransferred);

	// 접속 완료 후의 소켓 옵션 / OnConnect / 인증 요청까지.
	// 실패를 반환값 하나로 모으려고 떼어냈다. 이유는 서버 쪽
	// RunAcceptedConnectSequence 와 같다.
	bool RunClientConnectSequence();

	void HandleConnectCancelled(OverlappedEx* overlappedEx, ClientSession* session, int errorCode);

	void HandleRecv(OverlappedEx* overlappedEx, ClientSession* session, DWORD bytesTransferred);
	void HandleRecvCancelled(OverlappedEx* overlappedEx, ClientSession* session);

	void HandleSend(OverlappedEx* overlappedEx, ClientSession* session, DWORD bytesTransferred);
	void HandleSendCancelled(OverlappedEx* overlappedEx, ClientSession* session);

	void HandleSessionDisconnected(OverlappedEx* overlappedEx, ClientSession* session, DWORD bytesTransferred);

private:
	bool CreateConnectSocket();
	void DestroyConnectSocket();

	bool BindClientSocket(SOCKET clientSocket);
	bool ListenServerSocket(SOCKET clientSocket);

	bool InitializeGUIDConnectEx(SOCKET clientSocket);
	void FinalizeGUIDConnectEx();

	bool PrepareConnect();
	bool PostConnect(ClientSession* clientSession);
	bool SendSystemAuthRequest(ClientSession* session);
	bool HandleSystemPacket(ClientSession* session, uint16_t packetId, const char* packetData, uint32_t packetSize);

	void OnDisconnectRequest(ISession* session) override;

	// 종료 절차의 뒷부분. 소켓을 닫고 세션 상태를 정리한다.
	// OnDisconnectRequest 가 그 자리에서 부르거나(남은 I/O 가 없을 때),
	// 마지막 DecrementIO 가 부른다.
	void CompleteDisconnect(ClientSession* clientSession);

	// BaseSession 이 마지막 DecrementIO 에서 부르는 진입점.
	static void OnReleaseReady(void* context, BaseSession* session);

	// ISessionEvent 의 시그니처가 ISession* 로 고정돼 있어 기반 타입이
	// 들어오는 유일한 지점이다. 그 포인터가 정말 이 클라이언트의 세션인지
	// 확인해서 돌려준다. 아니면 nullptr 과 함께 위반을 남긴다.
	ClientSession* ResolveOwnSession(ISession* session, const char* calledFrom);

public:
	PacketHandlerTable* GetPacketHandlerTable() const;
	TlsMemoryPool* GetJobMemoryPool() const;
	TlsMemoryPool* GetPacketMemoryPool() const;

	// 서버 쪽 같은 이름과 대칭으로 둔다. (사정은 IOCPServer.h 주석 참고)
	// 클라는 세션이 하나라 브로드캐스트가 없지만, 공유 송신 자체는 막혀
	// 있지 않으므로 한쪽만 여는 편이 더 헷갈린다.
	TlsMemoryPool* GetSendQueueMemoryPool() const;

	const HandlerContext& GetHandlerContext() const;
	ClientSession* GetClientSession() const;

	// 이 클라이언트 세션이 들고 있는 미완료 I/O 수.
	// 조용한 상태에서 0 으로 돌아오지 않으면 Increment 와 Decrement 의
	// 짝이 맞지 않는다는 신호다.
	uint32_t GetOutstandingIOCount() const;
	virtual void* GetServiceContext();


protected:
	// 받은 패킷을 세션의 Job 큐에 올린다. OnReceive 에서 부르는 것이 정상
	// 사용이다. 계약은 IOCPServer::SubmitPacketJob 과 같다 —
	// packetData 의 소유권은 성공/실패와 무관하게 이 함수가 가져간다.
	//
	// 클라이언트는 세션이 하나이고 소비자도 하나(ClientSessionScheduler)라
	// ReadySessionQueue 도 처리 중 플래그도 쓰지 않는다. 큐에 넣는 것으로
	// 끝이고, 잠들어 있는 소비자를 깨우는 일은 EnqueueJob 이 한다.
	bool SubmitPacketJob(ISession* session, uint16_t packetId, const char* packetData, uint32_t packetSize);

	virtual void OnClientConnect(ISession* session) {};
	virtual void OnSessionEstablished(ISession* session) {};

	// reason 은 이 연결이 왜 끝났는지다.
	//
	// 예전에는 인자가 세션 하나뿐이라 서비스가 알 수 있는 것이 "끝났다"
	// 뿐이었다. 그런데 해야 할 일이 사유마다 정반대다 — 만석이면 잠시 뒤
	// 다시 붙어야 하고, 버전이 안 맞으면 다시 붙어 봐야 같고, 내가 부른
	// StopClient 면 아무것도 하면 안 된다. 엔진은 셋을 전부 알면서 로그로만
	// 남기고 버렸다.
	//
	// 재접속 여부만 알면 되면 IsRetryableDisconnect(reason) 를 쓰면 된다.
	// (값 목록은 Session/DisconnectReason.h)
	virtual void OnClientDisconnect(ISession* session, DisconnectReason reason) {};

	// 접속 자체가 성립하지 않았다. errorCode 는 WSA 오류 코드다.
	//
	// OnClientDisconnect 와 짝이 아니라 별개인 이유는, 접속한 적이 없으면
	// OnClientConnect 도 부른 적이 없기 때문이다. 짝 없는 종료 통지는
	// 서비스의 "접속당 하나" 계수를 어긋나게 한다.
	//
	// 이게 없던 동안 서버가 떠 있지 않으면 서비스는 콜백을 하나도 받지
	// 못했다. StartClient 는 ConnectEx 를 건 시점에 true 를 돌려주므로,
	// 실패를 알 방법도 재시도를 걸 근거도 없었다.
	virtual void OnConnectFailed(int errorCode) {};
	virtual void OnReceive(ISession* session, uint16_t packetId, const char* packetData, uint32_t packetSize) {};
	virtual void OnSend(ISession* session, uint32_t bytesTransferred) {};
};





