#pragma once

#include <atomic> // for std::atomic
#include <memory> // for std::unique_ptr
#include <mutex> // for std::mutex
#include <shared_mutex> // for std::shared_mutex
#include <utility> // for std::move
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
enum class ServiceEvent;
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
	// 완료 처리는 최소 2개 워커를 유지하고, 서비스 이벤트와 핸들러는 ClientJobWorker가 직렬 실행합니다.
	// 워커 수를 늘려도 같은 연결의 서비스 작업이 병렬 실행되지는 않습니다.
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

	// 외부에서 만든 핸들은 Stop 전에 모두 반환하고, 생성 작업도 Stop과 겹치지 않게 합니다.
	[[nodiscard]] IOCP_ENGINE_API PacketHandle CreatePacket(std::size_t size) const;
	[[nodiscard]] IOCP_ENGINE_API SharedPacketHandle CreateSharedPacket(PacketHandle packet) const;
	IOCP_ENGINE_API ConnectionId GetConnectionId() const;
	IOCP_ENGINE_API bool IsConnected() const;

	// 식별자로 연결을 조회하고 작업이 끝날 때까지 보호합니다. 만료된 ID는 Closed를 반환합니다.
	[[nodiscard]] IOCP_ENGINE_API SendResult Send(ConnectionId id, PacketHandle packet);
	[[nodiscard]] IOCP_ENGINE_API SendResult Send(ConnectionId id, SharedPacketHandle packet);
	template<FixedServicePacket Packet>
	[[nodiscard]] SendResult Send(ConnectionId id, const Packet& packet)
	{
		return SendPacketCopy(id, PacketTraits<Packet>::id, std::as_bytes(std::span(&packet, 1)));
	}
	// 종료 요청은 기다리지 않습니다. Post의 true는 등록 성공이며 실행 완료를 뜻하지 않습니다.
	// Post는 준비된 연결에서만 받으며, 이후 종료되면 대기 중인 작업을 실행하지 않습니다.
	[[nodiscard]] IOCP_ENGINE_API bool RequestDisconnect(ConnectionId id, DisconnectReason reason = DisconnectReason::LocalRequest);
	[[nodiscard]] IOCP_ENGINE_API bool Post(ConnectionId id, SessionTask task);
	// 현재 연결을 대상으로 하며, 아직 연결이 없으면 NotReady를 반환합니다.
	[[nodiscard]] IOCP_ENGINE_API SendResult Send(PacketHandle packet);
	[[nodiscard]] IOCP_ENGINE_API SendResult Send(SharedPacketHandle packet);
	template<FixedServicePacket Packet>
	[[nodiscard]] SendResult Send(const Packet& packet)
	{
		const auto id = GetConnectionId();
		return id ? Send(id, packet) : SendResult::NotReady;
	}

protected:
	// 기존 자유 함수 핸들러에 전달할 서비스 컨텍스트입니다.
	// 핸들러에 전달할 서비스 객체를 파생 클래스에서 지정한다.
	IOCP_ENGINE_API virtual void* GetServiceContext();

	// 같은 연결의 접속 → 준비 → 패킷·Post·송신 통지는 직렬 실행합니다(스레드 고정 아님).
	// 종료 요청 후 대기 중 일반 작업은 실행하지 않으며, 실행 중 작업·I/O 반환 후 종료를 한 번 통지합니다.
	// 종료 콜백은 마지막 참조를 반환하는 스레드에서 실행될 수 있습니다.
	// 서비스 이벤트
	// TCP 연결 통지입니다. 서비스 송신은 OnSessionEstablished부터 가능합니다.
	virtual void OnClientConnect(ISession*) {};
	// 인증 성공 후 서비스 통신을 시작할 수 있을 때 한 번 호출합니다.
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
	// 준비 완료 후 성공한 송신 I/O의 바이트 통지입니다. 상대의 처리 완료를 의미하지 않습니다.
	virtual void OnSend(ISession*, uint32_t) {};

private:
	// 템플릿을 사용하는 소비 DLL에서도 호출하므로 내부 연결 함수는 export합니다.
	IOCP_ENGINE_API bool RegisterHandlerBinding(uint16_t packetId, PacketHandler handler);
	IOCP_ENGINE_API SendResult SendPacketCopy(ConnectionId id, uint16_t packetId, std::span<const std::byte> bytes);
	bool EnqueueJob(ISession* session, uint16_t packetId, PacketHandle packet);
	bool EnqueueServiceEvent(ClientSession& session, ServiceEvent event, SessionTask task);
	// 반환된 포인터의 참조 하나를 즉시 ClientSession::Reference로 인수합니다.
	ClientSession* AcquireConnection(ConnectionId id) const;

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
	mutable std::shared_mutex m_apiMutex;
	bool m_apiOpen = false;
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
