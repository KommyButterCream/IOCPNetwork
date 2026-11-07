#pragma once

#include <atomic> // for std::atomic
#include <memory> // for std::unique_ptr
#include <mutex> // for std::mutex
#include <cstddef> // for std::byte
#include <span> // for std::span

#include "IOCPCore.h"
#include "ClientLivenessConfig.h"
#include "../Session/DisconnectReason.h"
#include "../HandlerTable/PacketHandler.h"
#include <new> // for std::bad_alloc
#include "../Buffer/SessionBufferConfig.h"
#include "../Memory/EnginePoolConfig.h"

#include <stdint.h>

#include "../Buffer/PacketHandle.h"
#include "../Buffer/SharedPacketHandle.h"
#include "../Session/ISession.h"

enum class IO_OPERATION;

struct OverlappedEx;

class ClientSession;
namespace ENGINE_POOL { struct EnginePools; }
class ClientJobWorker;
class ClientWatchdogTask;
class BackgroundTaskGroup;
class PacketHandlerTable;

#include "../EngineApi.h"

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
	// 그 하나가 필요한 이유는 접속·송신 완료 콜백이 워커 스레드에서
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

	// 서비스 패킷은 등록된 핸들러의 Job으로 자동 전달합니다. 미등록 패킷·Job 등록 실패 시 연결을 종료합니다.
	// 핸들러 등록과 패킷 생성
	// 정지 상태에서 등록한다. 같은 핸들러 재등록은 성공하며, 등록은 재시작 후에도 유지한다.
	IOCP_ENGINE_API bool RegisterPacketHandler(uint16_t packetId, PacketHandlerFunc handler);

	// 객체는 Stop 완료까지 유지하며, 패킷 참조는 콜백 중에만 유효합니다. 동일 등록은 허용합니다.
	template<FixedServicePacket Packet, typename Owner, typename Result>
	[[nodiscard]] bool RegisterHandler(Owner* owner, Result (Owner::*method)(ISession&, const Packet&))
	{
		try { return RegisterHandlerBinding(PacketTraits<Packet>::id, PacketHandler::Bind<Packet>(owner, method)); }
		catch (const std::bad_alloc&) { return false; }
	}
	// 가변 길이 패킷은 헤더를 포함한 바이트 범위를 빌려 받으며, 세부 형식은 서비스에서 검사합니다.
	template<typename Owner, typename Result>
	[[nodiscard]] bool RegisterHandler(uint16_t packetId, Owner* owner,
		Result (Owner::*method)(ISession&, std::span<const std::byte>))
	{
		try { return RegisterHandlerBinding(packetId, PacketHandler::Bind(owner, method)); }
		catch (const std::bad_alloc&) { return false; }
	}

	[[nodiscard]] IOCP_ENGINE_API PacketHandle CreatePacket(std::size_t size) const;
	[[nodiscard]] IOCP_ENGINE_API SharedPacketHandle CreateSharedPacket(PacketHandle packet) const;
	IOCP_ENGINE_API ISession* GetSession() const;

	// 서비스 송신입니다. Queued는 큐 등록 성공이며 전달한 핸들은 실패해도 소비합니다.
	// 외부 송신과 Stop은 호출자가 동기화하며, 송신 대상 세션은 엔진이 선택합니다.
	[[nodiscard]] IOCP_ENGINE_API SendResult Send(PacketHandle packet);
	[[nodiscard]] IOCP_ENGINE_API SendResult Send(SharedPacketHandle packet);
	// 고정 패킷은 반환 전에 엔진 버퍼로 복사하며, ID와 길이는 타입 정의로 설정합니다.
	template<FixedServicePacket Packet>
	[[nodiscard]] SendResult Send(const Packet& packet)
	{
		return SendPacketCopy(PacketTraits<Packet>::id, std::as_bytes(std::span(&packet, 1)));
	}

protected:
	// 기존 자유 함수 핸들러에 전달할 서비스 컨텍스트입니다.
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
	virtual void OnSend(ISession*, uint32_t) {};

private:
	// 템플릿을 사용하는 소비 DLL에서도 호출하므로 내부 연결 함수는 export합니다.
	IOCP_ENGINE_API bool RegisterHandlerBinding(uint16_t packetId, PacketHandler handler);
	IOCP_ENGINE_API SendResult SendPacketCopy(uint16_t packetId, std::span<const std::byte> bytes);
	bool EnqueueJob(ISession* session, uint16_t packetId, PacketHandle packet);

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

	// 접속 준비와 발행
	bool StartConnection(const SessionBufferConfig& bufferConfig, const EnginePoolConfig& poolConfig);
	bool BindClientSocket(SOCKET clientSocket);
	bool InitializeGUIDConnectEx(SOCKET clientSocket);
	bool PostConnect(ClientSession* clientSession);

	// I/O 완료 분배와 오류 처리
	// GQCS 에 통지 받은 IO 처리
	// 소비 DLL의 파생 클래스 vtable이 참조하므로 private이어도 export가 필요하다.
	IOCP_ENGINE_API void HandleCompletion(ULONG_PTR completionKey, LPOVERLAPPED overlapped, DWORD bytesTransferred, DWORD completionError) override;
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
	DisconnectReason HandleSystemPacket(ClientSession& session, uint16_t packetId, std::span<const std::byte> packet);
	bool SendSystemAuthRequest(ClientSession* session);
	// 패킷별 크기·상태 검사와 인증 또는 하트비트 처리를 수행한다.
	DisconnectReason HandleAuthResponse(ClientSession& session, std::span<const std::byte> packet);
	DisconnectReason HandleHeartbeatRequest(ClientSession& session, std::span<const std::byte> packet);

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
	SOCKADDR_IN m_serverAddress{};
	// ConnectEx 함수 포인터
	LPFN_CONNECTEX m_connectEx = nullptr;

	// 풀 세 개와 핸들러 컨텍스트를 함께 소유하며, 세션보다 늦게 파괴한다.
	std::unique_ptr<ENGINE_POOL::EnginePools> m_memoryPools;

	// 세션과 작업 실행
	std::unique_ptr<ClientSession> m_session;
	std::unique_ptr<ClientJobWorker> m_clientJobWorker;

	// 패킷 핸들러 등록
	std::unique_ptr<PacketHandlerTable> m_packetHandlerTable;

	// 연결 생존 감시
	// StartClient 에서 받아 보관한다. 인증 응답이 도착했을 때 서버가 알려준
	// 하트비트 주기와 합쳐 실제 유휴 타임아웃을 정한다.
	ClientLivenessConfig m_livenessConfig;
	// 그룹이 작업을 소유한다. 인증 완료 시 Arm에 사용하는 포인터는 빌린 참조다.
	std::unique_ptr<BackgroundTaskGroup> m_backgroundTasks;
	ClientWatchdogTask* m_clientWatchdog = nullptr;
};
