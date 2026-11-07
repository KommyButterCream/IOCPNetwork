#pragma once

#include <atomic> // for std::atomic
#include <memory> // for std::unique_ptr
#include <mutex> // for std::mutex

#include "IOCPCore.h"
#include "../Network/UniqueSocket.h"
#include "HeartbeatConfig.h"
#include "../Job/HandlerContext.h"
#include "../Session/DisconnectReason.h"
#include "../Buffer/SessionBufferConfig.h"
#include "../Memory/EnginePoolConfig.h"

#include <stdint.h>

#include "../Buffer/PacketHandle.h"
#include "../Buffer/SharedPacketHandle.h"
#include "../Session/ISession.h"

enum class IO_OPERATION;
enum class SYSTEM_AUTH_RESULT : uint16_t;

class ISession;
class ClientSession;
class AcceptSession;
class PacketHandlerTable;
class SessionManager;
class ServerReadySessionQueue;
class ServerJobScheduler;
class TlsMemoryPool;
class HeartbeatThread;

struct OverlappedEx;

#include "../EngineApi.h"

// 접속을 받아들일지 판단하는 정책.
//
// 버퍼 크기(SessionBufferConfig)와 달리 이건 "누구를 얼마나 받을 것인가" 의
// 문제라 따로 둔다. 기본값은 둘 다 0 = 제한 없음이며, 그 경우 예전과 똑같이
// 동작한다.
struct ConnectionPolicyConfig
{
	// 서비스가 실제로 붙잡아 둘 접속 수의 상한. 0 이면 세션 풀 용량과 같다.
	//
	// 세션 풀 용량보다 작게 두는 이유는 "거부 사유를 전달할 여유" 다.
	// 이 값을 넘긴 접속도 세션을 받아 인증 단계까지 오되 SERVER_FULL 로
	// 거절되고 곧바로 정리된다.
	//
	// 풀 자체가 비면 소켓을 즉시 닫을 수밖에 없다. 그때 클라이언트는 RST 만
	// 보므로 "만석" 과 "서버 장애" 를 구분할 수 없다. 여유를 두면 흔한 경우
	// (만석)에서는 이유가 전달되고, 풀 고갈은 설정 실수나 공격일 때만 남는다.
	uint32_t serviceCapacity = 0;

	// 한 원격 주소가 동시에 유지할 수 있는 접속 수. 0 이면 제한 없음.
	//
	// 이게 없으면 한 주소가 세션 풀을 통째로 채울 수 있고, 그 상태가
	// 하트비트 타임아웃까지 유지되어 정상 사용자가 들어올 수 없다.
	uint32_t maxConnectionsPerAddress = 0;
};

class IOCPServer : public IOCPCore
{
public:
	// 생성·소멸과 복사 제한
	IOCP_ENGINE_API IOCPServer();
	IOCPServer(const IOCPServer&) = delete;
	IOCPServer& operator=(const IOCPServer&) = delete;
	IOCP_ENGINE_API virtual ~IOCPServer();

	// 시작과 종료
	// poolConfig 는 메모리 풀 세 개의 빈 목록과 커밋 상한이다.
	//
	// 예전에는 이 값들이 StartServer 안에 상수로 박혀 있었다. 풀에 담기는
	// 것은 서비스의 데이터인데 크기 분포를 엔진이 정하고 있었던 셈이다.
	// 채팅형 서비스는 패킷의 대부분이 수백 바이트인데 32K 빈까지 선할당했고,
	// 반대로 큰 패킷을 받는 서비스는 최대 빈을 넘어 조용히 힙을 쳤다.
	//
	// 기본값은 예전에 박혀 있던 값 그대로이므로, 넘기지 않으면 동작이 바뀌지
	// 않는다. (값과 조정 방법은 Memory/EnginePoolConfig.h 참고)
	//
	// heartbeatConfig 는 세션이 살아 있는지 묻는 주기와 답을 기다리는 시간이다.
	// 예전에는 StartServer 안에 상수 두 개로 박혀 있었고, 그래서 실시간
	// 서비스가 15초짜리 침묵을 줄일 방법이 없었다. 기본값은 예전 값 그대로다.
	// (값과 세 번째 항목의 사정은 Core/HeartbeatConfig.h 참고)
	// 외부 제어 스레드에서 호출한다. 실행 중이거나 다른 제어 호출이 진행 중이면 실패한다.
	IOCP_ENGINE_API bool StartServer(const char* ipAddress, const uint16_t port, const uint32_t maxConnectionCount,
		const SessionBufferConfig& bufferConfig = SessionBufferPreset::Server(),
		const ConnectionPolicyConfig& policyConfig = ConnectionPolicyConfig(),
		const EnginePoolConfig& poolConfig = EnginePoolPreset::Server(),
		const HeartbeatConfig& heartbeatConfig = HeartbeatConfig());
	// 외부 호출은 정리가 끝날 때까지 대기한다. 엔진 콜백·Job 안의 호출은 거절한다.
	// 객체 파괴 및 서비스의 패킷 핸들 반환은 호출자가 별도로 동기화한다.
	IOCP_ENGINE_API void StopServer();

	// 핸들러 등록과 패킷 생성
	IOCP_ENGINE_API bool RegisterPacketHandler(uint16_t packetId, PacketHandlerFunc handler);
	[[nodiscard]] IOCP_ENGINE_API PacketHandle CreatePacket(std::size_t size) const;
	[[nodiscard]] IOCP_ENGINE_API SharedPacketHandle CreateSharedPacket(PacketHandle packet) const;

protected:
	// 서비스 작업 등록과 컨텍스트
	// 패킷으로 Job을 초기화하고 큐에 등록한다. 실패 시 전달받은 패킷은 자동 반환한다.
	IOCP_ENGINE_API bool EnqueueJob(ISession* session, uint16_t packetId, PacketHandle packetHandle);
	// 핸들러에 전달할 서비스 객체를 파생 클래스에서 지정한다.
	IOCP_ENGINE_API virtual void* GetServiceContext();

	// 서비스 이벤트
	virtual void OnClientConnect(ISession* session) = 0;
	// reason 은 이 세션이 왜 끝났는지다.
	//
	// 예전에는 인자가 세션 하나뿐이라 서비스가 알 수 있는 것이 "끝났다"
	// 뿐이었다. 만석 거절과 프로토콜 위반과 하트비트 타임아웃과 서버 종료가
	// 같은 신호로 도착했고, 엔진은 넷을 전부 알면서 로그로만 남기고 버렸다.
	// (값 목록은 Session/DisconnectReason.h)
	virtual void OnClientDisconnect(ISession* session, DisconnectReason reason) = 0;
	virtual void OnReceive(ISession* session, uint16_t packetId, PacketHandle packetHandle) = 0;
	virtual void OnSend(ISession* session, uint32_t bytesTransferred) = 0;

private:
	// 내부 타입
	// for AcceptEX
	using LPFN_ACCEPTEX = BOOL(__stdcall*)(
		_In_ SOCKET sListenSocket,
		_In_ SOCKET sAcceptSocket,
		_Out_writes_bytes_(dwReceiveDataLength + dwLocalAddressLength + dwRemoteAddressLength) PVOID lpOutputBuffer,
		_In_ DWORD dwReceiveDataLength,
		_In_ DWORD dwLocalAddressLength,
		_In_ DWORD dwRemoteAddressLength,
		_Out_ LPDWORD lpdwBytesReceived,
		_Inout_ LPOVERLAPPED lpOverlapped
		);
	// for GetAcceptEXSockAddrs
	using LPFN_GETACCEPTEXSOCKADDRS = VOID(__stdcall*)(
		_In_reads_bytes_(dwReceiveDataLength + dwLocalAddressLength + dwRemoteAddressLength) PVOID lpOutputBuffer,
		_In_ DWORD dwReceiveDataLength,
		_In_ DWORD dwLocalAddressLength,
		_In_ DWORD dwRemoteAddressLength,
		_Outptr_result_bytebuffer_(*LocalSockaddrLength) struct sockaddr** LocalSockaddr,
		_Out_ LPINT LocalSockaddrLength,
		_Outptr_result_bytebuffer_(*RemoteSockaddrLength) struct sockaddr** RemoteSockaddr,
		_Out_ LPINT RemoteSockaddrLength
		);
	// 시스템 패킷 처리 결과.
	//
	// 예전에는 bool 이라 "정상적인 거절" 과 "처리 실패" 가 구분되지 않았다.
	// 그래서 거절 응답을 보낸 세션이 true 로 돌아가 살아남았고, 만석으로
	// 거절한 접속이 하트비트 타임아웃까지 연결 세션을 점유했다.
	enum class SystemPacketResult
	{
		Ok,        // 처리 완료. 세션 유지
		Rejected,  // 거절 응답과 종료를 이미 요청했다.
		Failed,    // 처리 실패. 세션을 정리한다 (오류)
	};

	// 제어 락을 보유한 상태에서 자원을 준비하거나 역순으로 정리한다.
	bool StartResources(const char* ipAddress, const uint16_t port, const uint32_t maxConnectionCount, const SessionBufferConfig& bufferConfig, const ConnectionPolicyConfig& policyConfig, const EnginePoolConfig& poolConfig, const HeartbeatConfig& heartbeatConfig);
	void StopResources();

	// 메모리 풀 정리
	// 서비스의 패킷 핸들이 모두 반환된 후에만 풀을 파괴합니다.
	// 종료 시 풀을 정리하며, 엔진 파괴·재시작 시에는 풀 객체도 삭제합니다.
	void DestroyMemoryPools();

	// 리슨 소켓 준비와 해제
	bool CreateListenSocket(const char* ipAddress, uint16_t port);
	void DestroyListenSocket();
	bool BindServerSocket(SOCKET serverSocket, const char* ipAddress, uint16_t port);
	bool ListenServerSocket(SOCKET serverSocket);
	bool InitializeGUIDAcceptEx(SOCKET serverSocket);

	// 수락 I/O 발행과 유지
	bool PrepareAccept();
	bool PostAccept(AcceptSession* acceptSession);
	void RepostAfterAccept(AcceptSession& session);
	// 주기 점검은 발행 권한을 얻은 수락 세션에만 AcceptEx를 다시 건다.
	void RefillAcceptSessions();
	bool CancelAllAcceptIO();
	// 걸려 있는 AcceptEx 가 0 이면 크게 울린다. 걸기를 시도한 직후에 부른다.
	void ReportAcceptStarvationIfNeeded();
	uint32_t GetPostedAcceptCount() const;

	// I/O 완료 분배와 오류 처리
	// GQCS 에 통지 받은 IO 처리
	IOCP_ENGINE_API void HandleCompletion(ULONG_PTR completionKey, LPOVERLAPPED overlapped, DWORD bytesTransferred, BOOL completionStatus) override;
	// 아래 핸들러들의 세션 인자는 전부 ClientSession 이다.
	// RECV/SEND 완료 키에 등록되는 것이 ClientSession 뿐이기 때문이다.
	// (ACCEPT 는 완료 키가 아니라 overlappedEx->sessionId 로 라우팅한다)
	void HandleSocketError(OverlappedEx* overlappedEx, ClientSession* session, int errorCode, IO_OPERATION ioOperation);

	// 접속 수락과 취소 완료
	void HandleAccept(uint32_t sessionId);
	// 수락한 소켓에 옵션을 적용하고 첫 수신과 서비스 접속 통지를 시작한다.
	bool RunAcceptedConnectSequence(ClientSession* clientSession, SOCKET socket);
	void HandleAcceptIOCancelled(uint32_t sessionId);

	// 송수신 완료와 패킷 분배
	// 수신 완료를 세션에 전달하고 완성된 패킷의 분배 경로를 연결한다.
	void HandleReceive(ClientSession* session, DWORD bytesTransferred);
	bool HandleReceivedPacket(ClientSession& session, uint16_t packetId, PacketHandle&& packet);
	void HandleSend(ClientSession* session, DWORD bytesTransferred);

	// 인증과 하트비트
	SystemPacketResult HandleSystemPacket(ClientSession* session, uint16_t packetId, const char* packetData, uint32_t packetSize);
	// 패킷별 크기·상태 검사와 인증 또는 하트비트 처리를 수행한다.
	SystemPacketResult HandleAuthRequest(ClientSession* session, const char* packetData, uint32_t packetSize);
	PacketHandle CreateAuthResponse(SYSTEM_AUTH_RESULT authResult);
	SystemPacketResult HandleHeartbeatResponse(ClientSession* session, const char* packetData, uint32_t packetSize);

	// 세션 종료 통지
	// ClientSessionPool 이 세션을 실제로 반납할 때 부르는 진입점.
	// 여기서 서비스의 OnClientDisconnect 로 넘긴다.
	static void OnSessionDisconnectNotify(void* context, ClientSession* session, DisconnectReason reason);

private:
	// 시작·종료만 직렬화한다. 송수신 워커는 이 락을 사용하지 않는다.
	std::mutex m_controlMutex;
	bool m_running = false;

	// 서버 종료 상태
	std::atomic<bool> m_serverShutdownRequested{false};

	// 리슨 소켓과 수락 함수
	UniqueSocket m_serverSocket;
	// AcceptEx 함수 포인터
	LPFN_ACCEPTEX m_acceptEx = nullptr;
	// GetAcceptEXSockAddrs 함수 포인터
	LPFN_GETACCEPTEXSOCKADDRS m_getAcceptExSockAddrs = nullptr;

	// 수락 발행 상태와 진단
	// 지금 리슨 소켓에 걸려 있는 AcceptEx 개수.
	//
	// 이게 0 이 되면 서버는 살아 있는 채로 새 접속을 하나도 받지 못한다.
	// 예전에는 그 상태와 정상 동작이 로그로 구분되지 않았다.
	std::atomic<LONG> m_postedAcceptCount{0};
	// 유지하려는 개수. 자원 부족 등으로 걸기가 실패해 이보다 줄어들면
	// 주기 점검이 유휴 수락 세션을 다시 채운다.
	uint32_t m_desiredAcceptCount = 0;
	// 한 번만 크게 울리기 위한 래치. 매 실패마다 같은 경보를 쏟지 않는다.
	std::atomic<bool> m_acceptStarvationReported{false};

	// 패킷과 Job 메모리 풀
	std::unique_ptr<TlsMemoryPool> m_jobMemoryPool;
	std::unique_ptr<TlsMemoryPool> m_packetMemoryPool;
	// 공유 패킷 참조 제어 객체용 풀입니다. 큐 배열은 세션이 직접 소유합니다.
	std::unique_ptr<TlsMemoryPool> m_sharedPacketMemoryPool;

	// 세션 관리와 작업 실행
	std::unique_ptr<SessionManager> m_sessionManager;
	std::unique_ptr<ServerReadySessionQueue> m_serverReadySessionQueue;
	std::unique_ptr<ServerJobScheduler> m_serverJobScheduler;

	// 핸들러와 서비스 컨텍스트
	std::unique_ptr<PacketHandlerTable> m_packetHandlerTable;
	HandlerContext m_handlerContext = {};

	// 접속 정책과 하트비트
	// 접속 수용 정책. StartServer 에서 받아 보관한다.
	ConnectionPolicyConfig m_connectionPolicy;
	// 하트비트 설정. 스레드에 넘기고 끝이 아니라 여기에도 남긴다 —
	// 인증 응답에 주기를 실어 보내야 하기 때문이다.
	// (왜 클라가 그 값을 알아야 하는지는 SC_SYSTEM_AUTH_RESPONSE_PACKET 주석)
	HeartbeatConfig m_heartbeatConfig;
	std::unique_ptr<HeartbeatThread> m_heartbeatThread;
};
