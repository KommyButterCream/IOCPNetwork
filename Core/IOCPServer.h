#pragma once

#include "IOCPCore.h"
#include "HeartbeatConfig.h"
#include "../Job/JobDefs.h"
#include "../Protocol/SystemPacket.h"
#include "../Buffer/SessionBufferConfig.h"
#include "../Memory/EnginePoolConfig.h"
#include "../Session/ConnectionPolicyConfig.h"

#include <stdint.h>

#include "../Buffer/PacketHandle.h"

enum class IO_OPERATION;

class ISession;
class ClientSession;
class AcceptSession;
class PacketHandlerTable;
class SessionManager;
class ReadySessionQueue;
class ReadySessionScheduler;
class TlsMemoryPool;
class HeartbeatThread;

struct OverlappedEx;

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

class IOCP_ENGINE_API IOCPServer : public IOCPCore
{
public:
	IOCPServer();
	virtual ~IOCPServer();

public:
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
	bool StartServer(const char* ipAddress, const uint16_t port, const uint32_t maxConnectionCount,
		const SessionBufferConfig& bufferConfig = SessionBufferPreset::Server(),
		const ConnectionPolicyConfig& policyConfig = ConnectionPolicyConfig(),
		const EnginePoolConfig& poolConfig = EnginePoolPreset::Server(),
		const HeartbeatConfig& heartbeatConfig = HeartbeatConfig());
	void StopServer();

private:
	// 메모리 풀 객체를 실제로 지운다.
	//
	// StopServer 는 Finalize 까지만 한다. 풀 포인터가 서비스 핸들러와
	// SharedPacketHandle 으로 나가 있어서, 늦게 도착하는 반납이 해제된 객체를
	// 만질 수 있기 때문이다. (사정은 ENGINE_POOL::FinalizePools 주석)
	//
	// 그래서 지우는 자리는 소멸자와, 다시 기동하기 직전 두 곳뿐이다.
	void DestroyMemoryPools();

	SOCKET m_serverSocket = INVALID_SOCKET;

	SessionManager* m_sessionManager = nullptr;
	ReadySessionQueue* m_readySessionQueue = nullptr;
	ReadySessionScheduler* m_readySessionScheduler = nullptr;
	HeartbeatThread* m_heartbeatThread = nullptr;
	PacketHandlerTable* m_packetHandlerTable = nullptr;
	TlsMemoryPool* m_jobMemoryPool = nullptr;
	TlsMemoryPool* m_packetMemoryPool = nullptr;

	// 송신 큐 엔트리 전용 풀.
	//
	// m_packetMemoryPool 안의 빈 하나로 대신하지 않는 이유는, 그 풀이 수신
	// 경로도 쓰기 때문이다(RecvPacketBuffer::ReadPacket). 같이 쓰면 송신
	// 버스트가 수신 실패로 번진다. 따로 두면 LogStats("sendQueue") 의
	// grow / acquireFail 이 송신 경로만의 신호가 되는 이점도 있다.
	//
	// 예전에는 HybridSendPacketPool 이 이 자리에 있었다.
	TlsMemoryPool* m_sendQueueMemoryPool = nullptr;

	HandlerContext m_handlerContext = {};

	volatile LONG m_serverShutdownRequested = FALSE;

	// 지금 리슨 소켓에 걸려 있는 AcceptEx 개수.
	//
	// 이게 0 이 되면 서버는 살아 있는 채로 새 접속을 하나도 받지 못한다.
	// 예전에는 그 상태와 정상 동작이 로그로 구분되지 않았다.
	volatile LONG m_postedAcceptCount = 0;

	// 유지하려는 개수. 자원 부족 등으로 걸기가 실패해 이보다 줄어들면
	// 주기 점검이 빈 슬롯을 다시 채운다.
	uint32_t m_desiredAcceptCount = 0;

	// 한 번만 크게 울리기 위한 래치. 매 실패마다 같은 경보를 쏟지 않는다.
	volatile LONG m_acceptStarvationReported = FALSE;

	// 접속 수용 정책. StartServer 에서 받아 보관한다.
	ConnectionPolicyConfig m_connectionPolicy;

	// 하트비트 설정. 스레드에 넘기고 끝이 아니라 여기에도 남긴다 —
	// 인증 응답에 주기를 실어 보내야 하기 때문이다.
	// (왜 클라가 그 값을 알아야 하는지는 SC_SYSTEM_AUTH_RESPONSE_PACKET 주석)
	HeartbeatConfig m_heartbeatConfig;

private:
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


	// AcceptEx 함수 포인터
	LPFN_ACCEPTEX m_acceptEx = nullptr;

	// GetAcceptEXSockAddrs 함수 포인터
	LPFN_GETACCEPTEXSOCKADDRS m_getAcceptExSockAddrs = nullptr;

private:
	// GQCS 에 통지 받은 IO 처리
	void HandleCompletion(ULONG_PTR completionKey, LPOVERLAPPED overlapped, DWORD bytesTransferred, BOOL completionStatus) override;
	// 아래 핸들러들의 세션 인자는 전부 ClientSession 이다.
	// RECV/SEND 완료 키에 등록되는 것이 ClientSession 뿐이기 때문이다.
	// (ACCEPT 는 완료 키가 아니라 overlappedEx->sessionId 로 라우팅한다)
	void HandleSocketError(OverlappedEx* overlappedEx, ClientSession* session, int errorCode, IO_OPERATION ioOperation);

	void HandleAccept(uint32_t sessionId, DWORD bytesTransferred);

	// 수락한 소켓에 옵션을 걸고 IOCP 에 등록해 수신을 시작한다.
	// 실패를 반환값 하나로 모으려고 떼어냈다. 예전에는 실패 지점마다
	// ReleaseClientSession 이 붙어 있었는데, 그러면 반납 시점이 여러
	// 갈래로 흩어져 "세션을 붙잡고 있는 구간" 을 만들 수 없다.
	bool RunAcceptedConnectSequence(ClientSession* clientSession);
	void HandleAcceptIOCancelled(uint32_t sessionId);
	void HandleRecv(OverlappedEx* overlappedEx, ClientSession* session, DWORD bytesTransferred);
	void HandleRecvCancelled(OverlappedEx* overlappedEx, ClientSession* session);
	void HandleSend(OverlappedEx* overlappedEx, ClientSession* session, DWORD bytesTransferred);
	void HandleSendCancelled(OverlappedEx* overlappedEx, ClientSession* session);
	void HandleSessionDisconnected(OverlappedEx* overlappedEx, ClientSession* session, DWORD bytesTransferred);

private:
	bool CreateListenSocket(const char* ipAddress, uint16_t port);
	void DestroyListenSocket();

	bool BindServerSocket(SOCKET serverSocket, const char* ipAddress, uint16_t port);
	bool ListenServerSocket(SOCKET serverSocket);

	bool InitializeGUIDAcceptEx(SOCKET serverSocket);
	void FinalizeGUIDAcceptEx();
	bool CancelAllAcceptIO();
	bool PrepareAccept();
	bool PrepareAccept(uint32_t sessionId);
	bool PostAccept(AcceptSession* acceptSession);

	// ClientSessionPool 이 세션을 실제로 반납할 때 부르는 진입점.
	// 여기서 서비스의 OnClientDisconnect 로 넘긴다.
	static void OnSessionDisconnectNotify(void* context, ClientSession* session);

	// 걸기가 실패해 비어 버린 슬롯을 다시 채운다.
	//
	// 주기 점검 스레드에서만 호출한다. accept 슬롯의 상태는 원자적이지
	// 않으므로, 여러 스레드가 같은 빈 슬롯을 동시에 집으면 같은
	// OVERLAPPED 로 AcceptEx 를 두 번 걸어 완료 통지가 짝을 잃는다.
	// 슬롯 자신의 완료를 처리하는 워커가 그 슬롯을 다시 거는 경로와는
	// 겹치지 않는다 — 그 워커는 실패한 뒤에야 슬롯을 비운 채로 떠난다.
	void RefillAcceptSlots();

	// 걸려 있는 AcceptEx 가 0 이면 크게 울린다. 걸기를 시도한 직후에 부른다.
	void ReportAcceptStarvationIfNeeded();
	bool SendSystemAuthResponse(ClientSession* session, SYSTEM_AUTH_RESULT authResult);
	// 시스템 패킷 처리 결과.
	//
	// 예전에는 bool 이라 "정상적인 거절" 과 "처리 실패" 가 구분되지 않았다.
	// 그래서 거절 응답을 보낸 세션이 true 로 돌아가 살아남았고, 만석으로
	// 거절한 접속이 하트비트 타임아웃까지 슬롯을 붙들었다.
	enum class SystemPacketResult
	{
		Ok,        // 처리 완료. 세션 유지
		Rejected,  // 정상적인 거절. 응답을 보냈으니 세션을 정리한다 (오류 아님)
		Failed,    // 처리 실패. 세션을 정리한다 (오류)
	};

	SystemPacketResult HandleSystemPacket(ClientSession* session, uint16_t packetId, const char* packetData, uint32_t packetSize);

protected:
	ReadySessionQueue* GetReadySessionQueue() const;
	PacketHandlerTable* GetPacketHandlerTable() const;

public:
	// 지금 걸려 있는 AcceptEx 개수와 유지 목표. 운영 지표라 서비스가 읽는다.
	// 이 값이 목표보다 오래 낮게 머물면 자원 부족을 의심해야 하고,
	// 0 이면 새 접속을 전혀 받지 못하는 상태다.
	uint32_t GetPostedAcceptCount() const;
	uint32_t GetDesiredAcceptCount() const;

	// 지금 이 서버의 세션들이 들고 있는 미완료 I/O 총합.
	//
	// 이 값이 0 이라는 것은 어떤 스레드도 완료 핸들러 안에서 세션을
	// 만지고 있지 않다는 뜻이고, 엔진의 수명 규약 전체가 이 하나에
	// 얹혀 있다. 조용한 상태에서 0 으로 돌아오지 않으면 Increment 와
	// Decrement 의 짝이 맞지 않는다는 신호다.
	uint32_t GetOutstandingIOCount() const;

	// 지금 임대되어 있는 클라이언트 세션 수. 운영 지표이자, 반납이
	// 실제로 이루어졌는지 확인하는 유일한 외부 관측점이다.
	// 미완료 I/O 가 0 이어도 세션이 풀로 돌아오지 않았을 수 있다 —
	// 그 둘은 별개의 실패다.
	uint32_t GetInUseSessionCount() const;

	// 지금 가장 밀린 세션의 잡 큐가 도달했던 최고 깊이.
	//
	// 잡 큐에는 상한이 없다. 핸들러가 유입보다 느리면 잡이 계속 쌓이고,
	// 잡 하나가 패킷 하나를 붙들고 있으므로 메모리도 함께 자란다.
	// 상한을 어디에 둘지는 이 값을 실제로 보고 정해야 한다.
	//
	// 합이 아니라 최댓값인 이유는 ClientSessionPool 쪽 주석 참고.
	uint32_t GetPeakJobQueueDepth() const;

	// 수신 백프레셔가 걸린 총 횟수.
	//
	// 잡 큐가 SessionBufferConfig::recvPauseJobDepth 에 닿아 다음 WSARecv 를
	// 걸지 않은 횟수의 전 세션 합이다. 이 값이 0 이면 부하가 고수위까지
	// 가지 않은 것이고, 0 이 아니면서 세션이 끊기지 않았다면 백프레셔가
	// 의도대로 동작한 것이다 — 예전에는 그 상황에서 세션을 잃었다.
	uint32_t GetRecvPauseCount() const;

protected:
	TlsMemoryPool* GetJobMemoryPool() const;
	TlsMemoryPool* GetPacketMemoryPool() const;

	// 송신 경로 부기용 풀. 64바이트 빈 하나를 갖고 있다.
	//
	// 브로드캐스트에서 SharedPacketHandle 을 담을 곳이 필요해서 열었다.
	// packet 풀은 수신 경로와 공유하므로 작은 송신 부기 객체는 여기에 둔다.
	// (분리 이유는 m_sendQueueMemoryPool 선언부 주석 참고)
	TlsMemoryPool* GetSendQueueMemoryPool() const;

	const HandlerContext& GetHandlerContext() const;
	virtual void* GetServiceContext();

protected:
	// Consumes packetHandle on both success and failure.
	// Success transfers ownership to the Job; failure returns the packet automatically.
	bool SubmitPacketJob(ISession* session, uint16_t packetId, PacketHandle packetHandle);

protected:
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
};





