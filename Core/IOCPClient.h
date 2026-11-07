#pragma once

#include <atomic> // for std::atomic
#include <memory> // for std::unique_ptr
#include <mutex> // for std::mutex

#include "IOCPCore.h"
#include "ClientLivenessConfig.h"
#include "../Session/DisconnectReason.h"
#include "../Job/HandlerContext.h"
#include "../Buffer/SessionBufferConfig.h"
#include "../Memory/EnginePoolConfig.h"

#include <stdint.h>

#include "../Buffer/PacketHandle.h"
#include "../Buffer/SharedPacketHandle.h"
#include "../Session/ISession.h"

enum class IO_OPERATION;

struct OverlappedEx;

class ISession;
class ClientSession;
class TlsMemoryPool;
class ClientJobWorker;
class ClientWatchdogTask;
class BackgroundTaskGroup;
class PacketHandlerTable;
class SessionContext;

#include "../EngineApi.h"

#define INET_ADDRSTRLEN  22

class IOCPClient : public IOCPCore
{
public:
	// 생성·소멸과 복사 제한
	IOCP_ENGINE_API IOCPClient();
	IOCPClient(const IOCPClient&) = delete;
	IOCPClient& operator=(const IOCPClient&) = delete;
	IOCP_ENGINE_API virtual ~IOCPClient();

	// 시작과 종료
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
	// 잡을 실행하는 ClientJobWorker 스레드 하나다.
	//
	// poolConfig 의 사정은 IOCPServer::StartServer 의 같은 인자 주석과 같다.
	//
	// livenessConfig 는 "서버가 죽었다" 를 이쪽이 스스로 판정하는 방법이다.
	// 예전에는 그 수단이 아예 없어서, 랜선이 빠지거나 서버 프로세스가
	// 행되면 클라가 영원히 기다렸다. 기본값으로 켜져 있다.
	// (자세한 사정은 Core/ClientLivenessConfig.h)
	// 외부 제어 스레드에서 호출한다. 실행 중이거나 다른 제어 호출이 진행 중이면 실패한다.
	IOCP_ENGINE_API bool StartClient(const char* serverIp, const uint16_t port,
		const SessionBufferConfig& bufferConfig = SessionBufferPreset::Client(),
		uint32_t iocpThreadCount = 0,
		const EnginePoolConfig& poolConfig = EnginePoolPreset::Client(),
		const ClientLivenessConfig& livenessConfig = ClientLivenessConfig());
	// 외부 호출은 정리가 끝날 때까지 대기한다. 엔진 콜백·Job·타이머 콜백 안의 호출은 거절한다.
	// 객체 파괴 및 서비스의 패킷 핸들 반환은 호출자가 별도로 동기화한다.
	IOCP_ENGINE_API void StopClient();

	// 핸들러 등록과 패킷 생성
	IOCP_ENGINE_API bool RegisterPacketHandler(uint16_t packetId, PacketHandlerFunc handler);
	[[nodiscard]] IOCP_ENGINE_API PacketHandle CreatePacket(std::size_t size) const;
	[[nodiscard]] IOCP_ENGINE_API SharedPacketHandle CreateSharedPacket(PacketHandle packet) const;
	IOCP_ENGINE_API ISession* GetSession() const;

protected:
	// 서비스 작업 등록과 컨텍스트
	// 패킷으로 Job을 초기화하고 큐에 등록한다. 실패 시 전달받은 패킷은 자동 반환한다.
	IOCP_ENGINE_API bool EnqueueJob(ISession* session, uint16_t packetId, PacketHandle packetHandle);
	// 핸들러에 전달할 서비스 객체를 파생 클래스에서 지정한다.
	IOCP_ENGINE_API virtual void* GetServiceContext();

	// 서비스 이벤트
	virtual void OnClientConnect(ISession*) {};
	virtual void OnSessionEstablished(ISession*) {};
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
	virtual void OnClientDisconnect(ISession*, DisconnectReason) {};
	// 접속 자체가 성립하지 않았다. errorCode 는 WSA 오류 코드다.
	//
	// OnClientDisconnect 와 짝이 아니라 별개인 이유는, 접속한 적이 없으면
	// OnClientConnect 도 부른 적이 없기 때문이다. 짝 없는 종료 통지는
	// 서비스의 "접속당 하나" 계수를 어긋나게 한다.
	//
	// 이게 없던 동안 서버가 떠 있지 않으면 서비스는 콜백을 하나도 받지
	// 못했다. StartClient 는 ConnectEx 를 건 시점에 true 를 돌려주므로,
	// 실패를 알 방법도 재시도를 걸 근거도 없었다.
	virtual void OnConnectFailed(int) {};
	virtual void OnReceive(ISession*, uint16_t, PacketHandle) {};
	virtual void OnSend(ISession*, uint32_t) {};

private:
	// 내부 타입
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

	// 제어 락을 보유한 상태에서 자원을 준비하거나 역순으로 정리한다.
	bool StartResources(const char* serverIp, const uint16_t port, const SessionBufferConfig& bufferConfig, uint32_t iocpThreadCount, const EnginePoolConfig& poolConfig, const ClientLivenessConfig& livenessConfig);
	void StopResources();

	// 메모리 풀 정리
	// 서비스의 패킷 핸들이 모두 반환된 후에만 풀을 파괴합니다.
	// 종료 시 풀을 정리하며, 엔진 파괴·재시작 시에는 풀 객체도 삭제합니다.
	void DestroyMemoryPools();

	// 접속 준비와 발행
	bool StartConnection(const SessionBufferConfig& bufferConfig, const EnginePoolConfig& poolConfig);
	bool BindClientSocket(SOCKET clientSocket);
	bool InitializeGUIDConnectEx(SOCKET clientSocket);
	bool PostConnect(ClientSession* clientSession);

	// I/O 완료 분배와 오류 처리
	// GQCS 에 통지 받은 IO 처리
	IOCP_ENGINE_API void HandleCompletion(ULONG_PTR completionKey, LPOVERLAPPED overlapped, DWORD bytesTransferred, BOOL completionStatus) override;
	// 아래 핸들러들의 세션 인자는 전부 ClientSession 이다.
	// 이 클라이언트가 가진 세션은 m_session 하나뿐이다.
	void HandleSocketError(ClientSession* session, int errorCode, IO_OPERATION ioOperation);

	// 접속 완료와 실패
	void HandleConnect();
	// 접속 완료 후의 소켓 옵션 / OnConnect / 인증 요청까지.
	// 실패를 반환값 하나로 모으려고 떼어냈다. 이유는 서버 쪽
	// RunAcceptedConnectSequence 와 같다.
	bool RunClientConnectSequence();
	void HandleConnectFailure(ClientSession* clientSession, int errorCode);

	// 송수신 완료와 패킷 분배
	// 수신 완료를 세션에 전달하고 완성된 패킷의 분배 경로를 연결한다.
	void HandleReceive(ClientSession* session, DWORD bytesTransferred);
	bool HandleReceivedPacket(ClientSession& session, uint16_t packetId, PacketHandle&& packet);
	void HandleSend(ClientSession* session, DWORD bytesTransferred);

	// 인증과 하트비트
	DisconnectReason HandleSystemPacket(ClientSession* session, uint16_t packetId, const char* packetData, uint32_t packetSize);
	bool SendSystemAuthRequest(ClientSession* session);
	// 패킷별 크기·상태 검사와 인증 또는 하트비트 처리를 수행한다.
	DisconnectReason HandleAuthResponse(ClientSession* session, const char* packetData, uint32_t packetSize);
	DisconnectReason HandleHeartbeatRequest(ClientSession* session, const char* packetData, uint32_t packetSize);

	// 세션 종료 요청과 반환 통지
	static void OnDisconnectRequest(void* context, ClientSession* session);
	// 마지막 I/O와 수명 참조가 반환되면 소켓을 닫고 서비스에 통지한다.
	static void OnReleaseReady(void* context, ClientSession* session);

private:
	// 시작·종료만 직렬화한다. 송수신 워커는 이 락을 사용하지 않는다.
	std::mutex m_controlMutex;
	bool m_running = false;

	// 연결 종료 선점
	// 같은 연결에서 종료 절차를 한 번만 시작한다.
	std::atomic<bool> m_disconnecting{false};

	// 연결 대상과 접속 함수
	// IP Address & Port
	char m_serverIPAddress[INET_ADDRSTRLEN] = { 0, };
	uint16_t m_serverPort = 0;
	// ConnectEx 함수 포인터
	LPFN_CONNECTEX m_connectEx = nullptr;

	// 패킷과 Job 메모리 풀
	std::unique_ptr<TlsMemoryPool> m_jobMemoryPool;
	std::unique_ptr<TlsMemoryPool> m_packetMemoryPool;
	// 공유 패킷 참조 제어 객체용 풀입니다.
	std::unique_ptr<TlsMemoryPool> m_sharedPacketMemoryPool;

	// 세션과 작업 실행
	std::unique_ptr<ClientSession> m_session;
	std::unique_ptr<ClientJobWorker> m_clientJobWorker;

	// 핸들러와 서비스 컨텍스트
	std::unique_ptr<PacketHandlerTable> m_packetHandlerTable;
	HandlerContext m_handlerContext = {};

	// 연결 생존 감시
	// StartClient 에서 받아 보관한다. 인증 응답이 도착했을 때 서버가 알려준
	// 하트비트 주기와 합쳐 실제 유휴 타임아웃을 정한다.
	ClientLivenessConfig m_livenessConfig;
	// 그룹이 작업을 소유한다. 인증 완료 시 Arm에 사용하는 포인터는 빌린 참조다.
	std::unique_ptr<BackgroundTaskGroup> m_backgroundTasks;
	ClientWatchdogTask* m_clientWatchdog = nullptr;
};
