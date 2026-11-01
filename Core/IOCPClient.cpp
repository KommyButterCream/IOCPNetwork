#include "IOCPClient.h"
#include "ClientWatchdogThread.h"

#include <WinSock2.h>

#include "../Diagnostics/EngineAssert.h"
#include <MSWSock.h> // for ConnectEx
#include <ws2tcpip.h> // for inet_pton

#include "IOCPCore.h"


#include "../Job/Job.h"
#include "../Scheduler/ClientSessionScheduler.h"
#include "../HandlerTable/PacketHandlerTable.h"

#include "../Network/SocketOption.h"

#include "../Memory/TlsMemoryPool.h"
#include "../Memory/EngineMemoryPoolHelper.h"
#include "../Memory/EnginePoolBuilder.h"

#include "../Buffer/RecvPacketBuffer.h"
#include "../Buffer/SendPacketEntry.h"
#include "../Buffer/PreDefine.h"
#include "../Protocol/PacketID.h"
#include "../Protocol/SystemPacket.h"

#include "../Session/ClientSession.h"
#include "../Session/SessionManager.h"

#include "../../Core/Util/Logger.h"

#pragma comment(lib, "ws2_32.lib") // for WinSock2
#pragma comment(lib, "mswsock.lib") // for AcceptEX / ConnectEx

using namespace Core::Util;

namespace
{
	// 연결이 죽어서 난 I/O 실패인가.
	//
	// ERROR_OPERATION_ABORTED 는 여기 없다. 그건 우리가 건 취소이고,
	// 취소를 건 쪽이 이미 종료 절차를 밟고 있다.
	//
	// !! IOCPServer.cpp 에 같은 이름의 사본이 있다. 목록을 고치면 양쪽을
	//    같이 고쳐야 한다. (갈라졌던 경위는 그쪽 주석 참고)
	bool IsConnectionDeadError(int errorCode)
	{
		switch (errorCode)
		{
		case WSAECONNRESET:           // 상대가 강제 종료 (RST)
		case WSAECONNABORTED:         // 연결 중단
		case WSAENOTCONN:             // 이미 끊김
		case WSAESHUTDOWN:            // 송수신 불가
		case ERROR_NETNAME_DELETED:   // 네트워크 이름 삭제 = 연결 끊김
		case ERROR_CONNECTION_ABORTED:
			return true;
		default:
			return false;
		}
	}
}

IOCPClient::IOCPClient()
{
}

IOCPClient::~IOCPClient()
{
	StopClient();

	// 풀 객체를 실제로 지우는 유일한 자리다.
	// StopClient 는 Finalize 만 한다. (이유는 ENGINE_POOL::FinalizePools 주석)
	DestroyMemoryPools();
}

void IOCPClient::DestroyMemoryPools()
{
	ENGINE_POOL::EnginePools pools;
	pools.packet = m_packetMemoryPool;
	pools.job = m_jobMemoryPool;
	pools.sendQueue = m_sendQueueMemoryPool;

	ENGINE_POOL::DestroyPools(pools);

	m_packetMemoryPool = nullptr;
	m_jobMemoryPool = nullptr;
	m_sendQueueMemoryPool = nullptr;
}

bool IOCPClient::StartClient(const char* serverIp, const uint16_t port, const SessionBufferConfig& bufferConfig, uint32_t iocpThreadCount, const EnginePoolConfig& poolConfig, const ClientLivenessConfig& livenessConfig)
{
	// 지난 주기의 한 번짜리 게이트를 되돌린다.
	//
	// m_destroyFlag 는 StopClient 가 올리기만 하고 내리지 않았다. 그래서
	// Start -> Stop -> Start 를 한 객체에서 하면 두 번째 StopClient 이 통째로
	// 무시됐다 — 스케줄러 스레드도, GQCS 워커도, 세션도, 풀도 정리되지 않는다.
	// 기동이 실패해 다시 시도하는 서비스가 그대로 이 경로를 밟는다.
	::InterlockedExchange(&m_destroyFlag, 0);
	::InterlockedExchange(&m_disconnecting, 0);

	// 앞선 주기의 풀 객체가 남아 있으면 여기서 지운다. StopClient 는 Finalize
	// 까지만 하고 객체를 남기기 때문이다. (이유는 ENGINE_POOL::FinalizePools 주석)
	DestroyMemoryPools();

	// 설정 오류는 아무것도 잡기 전에 걸러낸다.
	if (!bufferConfig.IsValid())
	{
		LOGE("invalid session buffer config : recv %u / ring %u, send %u, queue %u",
			bufferConfig.maxRecvPacketSize, bufferConfig.recvRingSize,
			bufferConfig.maxSendPacketSize, bufferConfig.sendQueueDepth);
		return false;
	}

	if (!livenessConfig.IsValid())
	{
		LOGE("invalid client liveness config : missed heartbeat limit %u, min idle timeout %llu ms, "
			"keepalive %d (%u ms / %u ms)",
			livenessConfig.missedHeartbeatLimit, livenessConfig.minIdleTimeout_ms,
			livenessConfig.useKeepAlive ? 1 : 0,
			livenessConfig.keepAliveTime_ms, livenessConfig.keepAliveInterval_ms);
		return false;
	}

	m_livenessConfig = livenessConfig;

	// 주소를 복사하기 '전' 에 본다.
	//
	// 예전에는 곧바로 strcpy_s 였다. 널이거나 버퍼보다 긴 문자열이 들어오면
	// CRT 의 잘못된 인자 처리기가 불리고, 기본 처리기는 프로세스를 그 자리에서
	// 죽인다(0xC0000409). 서비스가 설정 파일에서 읽은 값을 그대로 넘기는 것은
	// 흔한 형태인데, 그 오타 하나가 진단 한 줄 없는 즉사가 된다.
	//
	// 형식이 IPv4 인지까지는 여기서 보지 않는다. 그건 PostConnect 의
	// inet_pton 이 판정하고 거기서 위반으로 남긴다. 여기서 막는 것은
	// '복사 자체가 프로세스를 죽이는' 입력이다.
	if (serverIp == nullptr || serverIp[0] == '\0')
	{
		LOGE("StartClient was given no server address");
		return false;
	}

	const size_t addressLength = ::strnlen(serverIp, sizeof(m_serverIPAddress));
	if (addressLength >= sizeof(m_serverIPAddress))
	{
		LOGE("the server address is longer than %zu characters", sizeof(m_serverIPAddress) - 1);
		return false;
	}

	::memcpy(m_serverIPAddress, serverIp, addressLength);
	m_serverIPAddress[addressLength] = '\0';

	m_serverPort = port;

	// 워커 2개.
	//
	// 예전에는 이 숫자가 데드락 회피책이었다. disconnect 경로가 완료 핸들러
	// 안에서 WaitForIOCancelComplete 로 최대 10초를 기다렸고, 그 대기를 풀어
	// 줄 취소 완료 통지를 처리할 스레드가 바로 그 막힌 워커 자신이었다.
	// 워커가 1개면 확정 데드락, 2개면 "둘 다 대기에 들어가지만 않으면" 이라는
	// 조건부 회피였다. 실제로는 둘 다 들어가는 일이 잦아서 실행당 200회 넘게
	// 10초 타임아웃을 태웠다.
	//
	// 이제 그 대기가 없다. disconnect 는 예약만 하고, 마지막 완료가 마무리한다.
	// 2개를 유지하는 이유는 다른 것이다 — 서비스 콜백(OnReceive 등)이 워커
	// 스레드에서 실행되므로, 하나가 서비스 코드에 붙들려 있어도 나머지 하나가
	// 완료 통지를 계속 꺼낼 수 있어야 한다.
	//
	// 그래서 1 은 받지 않는다. 그 이유가 사라지기 때문이다.
	// (기본값과 상한의 사정은 IOCPClient.h 의 선언 주석 참고)
	constexpr uint32_t DEFAULT_CLIENT_IOCP_THREADS = 2;
	constexpr uint32_t MIN_CLIENT_IOCP_THREADS = 2;

	uint32_t workerCount = (iocpThreadCount == 0) ? DEFAULT_CLIENT_IOCP_THREADS : iocpThreadCount;

	if (workerCount < MIN_CLIENT_IOCP_THREADS)
	{
		LOGW("client iocp thread count %u raised to %u : one worker must stay free to drain completions "
			"while another is inside a service callback",
			workerCount, MIN_CLIENT_IOCP_THREADS);
		workerCount = MIN_CLIENT_IOCP_THREADS;
	}

	LOGI("client iocp workers : %u", workerCount);

	SetIOCPThreadCount(workerCount);

	if (!IOCPCore::Start())
	{
		return false;
	}

	// 클라이언트 소켓 생성
	if (!CreateConnectSocket())
	{
		return false;
	}

	// GQCS 사용을 위한 포트와 소켓 바인드
	if (!BindClientSocket(m_clientSocket))
	{
		return false;
	}

	// ConnectEx 사용을 위한 함수 포인터 초기화
	if (!InitializeGUIDConnectEx(m_clientSocket))
	{
		return false;
	}

	// 메모리 풀 세 개. 구성은 서버와 같은 경로를 쓴다.
	// (사정은 IOCPServer::StartServer 의 같은 자리와 Memory/EnginePoolConfig.h 참고)
	//
	// 패킷 풀이 수신 상한을 덮는지 확인하는 검사도 그 안으로 들어갔다.
	// 예전에는 이 검사가 클라에만 있었다 — 서버는 지금 프리셋에서 우연히
	// 안전할 뿐이고, 서비스가 maxRecvPacketSize 를 올리면 조용히 힙으로 샜다.
	ENGINE_POOL::EnginePools pools;
	if (!ENGINE_POOL::CreatePools(poolConfig, bufferConfig.maxRecvPacketSize, "client", pools))
	{
		return false;
	}

	m_jobMemoryPool = pools.job;
	m_packetMemoryPool = pools.packet;
	m_sendQueueMemoryPool = pools.sendQueue;

	m_handlerContext.jobMemoryPool = GetJobMemoryPool();
	m_handlerContext.packetMemoryPool = GetPacketMemoryPool();
	m_handlerContext.serviceContext = GetServiceContext();

	m_packetHandlerTable = new PacketHandlerTable();
	if (!m_packetHandlerTable)
		return false;


	// 서버와의 통신을 위한 세션 생성
	m_session = new ClientSession;
	if (!m_session)
		return false;
	// Initialize 는 CreateEvent 가 실패하면 false 다. 그 세션은
	// m_ioCancelCompleteEvent 없이 살아남고, WaitForIOCancelComplete 가
	// 널 이벤트를 보고 무조건 true 를 반환해 취소 대기가 무의미해진다.
	if (!m_session->Initialize(SESSION_ROLE::CLIENT, 0))
	{
		LOGE("failed to initialize the client session");
		return false;
	}
	if (!m_session->InitializeMemoryPool(m_sendQueueMemoryPool, m_jobMemoryPool, m_packetMemoryPool, bufferConfig))
		return false;
	m_session->SetEventHandler(this);

	// 마지막 완료가 종료 절차를 마무리할 수 있도록 진입점을 걸어 둔다.
	m_session->SetReleaseReadyFunc(&IOCPClient::OnReleaseReady, this);

	// 세션에 소켓 설정
	m_session->SetClientSocket(m_clientSocket);

	// 세션 잡큐의 Job 을 처리 하기 위한 스케쥴러/스레드 생성
	m_clientSessionScheduler = new ClientSessionScheduler;
	if (!m_clientSessionScheduler)
		return false;

	if (!m_clientSessionScheduler->Initialize(m_session, m_jobMemoryPool, m_packetMemoryPool))
	{
		LOGE("failed to initialize the client session scheduler");
		return false;
	}

	// 서버가 조용해진 것을 알아차리는 감시 스레드.
	//
	// 여기서 만들되 무장은 하지 않는다. 기준이 되는 하트비트 주기를
	// 서버가 인증 응답으로 알려주기 때문이다. 무장은 그 응답을 처리하는
	// 자리에서 한다. (HandleSystemPacket 의 SC_SYSTEM_AUTH_RESPONSE)
	m_clientWatchdog = new ClientWatchdogThread(m_session, m_livenessConfig);
	if (!m_clientWatchdog)
		return false;

	if (!m_clientWatchdog->Start())
	{
		LOGE("failed to start the client liveness watchdog");
		delete m_clientWatchdog;
		m_clientWatchdog = nullptr;
		return false;
	}

	// 비동기 Connect To Server
	// 연결에 대한 통지를 GQCS 에서 처리 한다.
	if (!PrepareConnect())
	{
		return false;
	}

	return true;
}

void IOCPClient::StopClient()
{
	if (::InterlockedCompareExchange(&m_destroyFlag, 1, 0) == 1)
		return;

	LOGI("client shutting down");

	// 감시 스레드를 가장 먼저 내린다.
	//
	// 이 스레드는 세션 포인터를 들고 주기적으로 깨어나므로, 아래에서
	// 세션을 지우기 전에 반드시 조인되어 있어야 한다. 그리고 우리가
	// 내리는 종료와 이 스레드가 내리는 종료가 겹치는 것도 막는다
	// (겹쳐도 m_disconnecting 이 거르지만, 겹칠 이유가 없다).
	if (m_clientWatchdog)
	{
		m_clientWatchdog->Disarm();
		delete m_clientWatchdog;
		m_clientWatchdog = nullptr;
	}

	if (m_clientSessionScheduler)
	{
		delete m_clientSessionScheduler;
		m_clientSessionScheduler = nullptr;
	}

	// [1] I/O 취소 요청 및 취소 완료 대기.
	//     이 대기는 IOCP 워커 스레드가 DecrementIO 에서 이벤트를 Set 해주어야 풀리므로
	//     반드시 IOCPCore::Stop() 보다 먼저 수행되어야 한다.
	if (m_session)
	{
		m_session->NoteDisconnectReason(DisconnectReason::LocalShutdown);
		OnDisconnectRequest(m_session);

		// 기다리는 자리는 여기 하나로 남겼다.
		//
		// OnDisconnectRequest 는 이제 예약만 하고 즉시 돌아온다. 완료
		// 핸들러 안에서 불릴 때 자기를 기다리지 않게 하려는 것이었고,
		// 그 대신 누군가는 실제로 끝나기를 기다려야 한다. 그 자리가
		// 여기다 — 이 함수는 앱 스레드에서 불리므로 자기 대기가 생기지
		// 않고, 바로 아래 IOCPCore::Stop() 이후에는 남은 취소 완료 통지를
		// 꺼내 줄 워커가 없다.
		//
		// 이벤트는 disconnect 를 요청한 쪽이 CancelPendingIO 로 무장해
		// 두었으므로, 이미 다른 스레드가 절차를 진행 중이어도 세워진다.
		if (!m_session->WaitForIOCancelComplete(10'000))
		{
			ENGINE_VIOLATION("session %u IO cancel did not complete during shutdown (io count %ld)",
				m_session->GetSessionID(), m_session->GetOutstandingIOCount());
		}
	}

	// [2] IOCP 정지. GQCS 워커 스레드를 조인한다.
	//     이 시점 이후로는 완료 통지가 발생하지 않으므로
	//     아래의 세션 및 메모리풀 해제가 안전해진다.
	IOCPCore::Stop();

	// [3] 세션 해제
	if (m_session)
	{
		delete m_session;
		m_session = nullptr;
	}

	// 세션(m_session)을 이미 지웠으므로 엔트리는 모두 반납된 상태다.
	if (m_sendQueueMemoryPool)
	{
		m_sendQueueMemoryPool->LogStats("sendQueue");
	}

	// 재고만 돌려주고 객체는 남긴다. 늦게 도착하는 반납이 해제된 풀을
	// 역참조하지 않도록 하기 위한 것이고, 실제 delete 는 소멸자가 한다.
	// (사정은 ENGINE_POOL::FinalizePools 주석)
	//
	// 반드시 세션을 지운 뒤여야 한다. 세션 정리가 담고 있던 패킷과 송신
	// 엔트리를 이 풀들로 되돌리기 때문이다.
	{
		ENGINE_POOL::EnginePools pools;
		pools.packet = m_packetMemoryPool;
		pools.job = m_jobMemoryPool;
		pools.sendQueue = m_sendQueueMemoryPool;

		ENGINE_POOL::FinalizePools(pools);
	}

	if (m_packetHandlerTable)
	{
		delete m_packetHandlerTable;
		m_packetHandlerTable = nullptr;
	}

	memset(m_serverIPAddress, 0, sizeof(m_serverIPAddress));
	m_serverPort = 0;

	FinalizeGUIDConnectEx();
}

void IOCPClient::HandleCompletion(ULONG_PTR completionKey, LPOVERLAPPED overlapped, DWORD bytesTransferred, BOOL completionStatus)
{
	// 완료 키에 넣은 것이 ClientSession* 다 (PostConnect 의
	// RegisterSocketToIOCP 참고). 넣은 타입 그대로 받는다.
	ClientSession* session = reinterpret_cast<ClientSession*>(completionKey);
	OverlappedEx* overlappedEx = reinterpret_cast<OverlappedEx*>(overlapped);

	// GQCS 자체가 실패하면 overlapped 가 nullptr 로 온다.
	// 종료 중 IOCP 핸들이 닫히는 경우가 대표적이다.
	if (session == nullptr || overlappedEx == nullptr)
	{
		LOGW("completion arrived with session %p overlapped %p (status %d, error %lu)",
			session, overlappedEx, completionStatus, ::GetLastError());
		return;
	}

	if (!completionStatus)
	{
		HandleSocketError(overlappedEx, session, ::WSAGetLastError(), overlappedEx->operation);

		return;
	}

	switch (overlappedEx->operation)
	{
	case IO_OPERATION::CONNECT:
		HandleConnect(overlappedEx->sessionId, bytesTransferred);
		break;
	case IO_OPERATION::RECV:
		HandleRecv(overlappedEx, session, bytesTransferred);
		break;
	case IO_OPERATION::SEND:
		HandleSend(overlappedEx, session, bytesTransferred);
		break;
	default:
		ENGINE_VIOLATION("completion arrived with an unknown io operation %d", static_cast<int>(overlappedEx->operation));
		break;
	}
}

void IOCPClient::HandleSocketError(OverlappedEx* overlappedEx, ClientSession* session, int errorCode, IO_OPERATION ioOperation)
{
	LOGW("session %u socket error %d on io %d", session->GetSessionID(), errorCode, (int)ioOperation);

	// 공용으로 처리 되어야 하는 예외 처리

	if (ioOperation == IO_OPERATION::CONNECT)
	{
		switch (errorCode)
		{
		case ERROR_HOST_UNREACHABLE:   // 호스트 접근 불가
		case ERROR_CONNECTION_REFUSED: // 서버가 연결 거부
		case ERROR_NETWORK_UNREACHABLE: // 네트워크 단절
		case ERROR_PORT_UNREACHABLE: // 서버가 갑자기 죽었거나 리셋한 경우
		case WSAECONNRESET: // SYN 보내는 도중 서버가 포트를 닫은 경우
		case WSAHOST_NOT_FOUND: // ConnectEx 이전 단계에서 발생된 문제 1
		case WSATRY_AGAIN:  // ConnectEx 이전 단계에서 발생된 문제 2
		case WSANO_DATA:  // ConnectEx 이전 단계에서 발생된 문제 3
		case WSAEADDRNOTAVAIL: // 로컬 인터페이스가 해당 주소 패밀리를 지원하지 않을 때
		case WSAEADDRINUSE: // 중복 포트 바인딩
			HandleConnectCancelled(overlappedEx, session, errorCode);
			break;
		case ERROR_OPERATION_ABORTED:
			HandleConnectCancelled(overlappedEx, session, errorCode);
			break;
		default:
			// 분류되지 않은 에러 코드. 그냥 빠져나가면 IncrementIO 로 올려둔
			// 카운트가 내려가지 않아 이 세션이 영구히 취소 대기에 묶인다.
			// 취소 처리로 보내 카운트를 정리한다.
			ENGINE_VIOLATION("session %u unhandled connect error %d, treating it as a cancellation", session->GetSessionID(), errorCode);
			HandleConnectCancelled(overlappedEx, session, errorCode);
			break;
		}
		return;
	}
	else if (ioOperation == IO_OPERATION::RECV)
	{
		if (errorCode == ERROR_OPERATION_ABORTED)
		{
			// 우리가 건 취소다. 취소를 건 쪽이 종료 절차를 이미 밟고 있다.
			HandleRecvCancelled(overlappedEx, session);
			return;
		}

		if (!IsConnectionDeadError(errorCode))
		{
			ENGINE_VIOLATION("session %u unhandled recv error %d, treating the connection as dead",
				session->GetSessionID(), errorCode);
		}

		// 서버와의 연결이 죽었다. 취소와 다르다 — 아무도 정리하고 있지 않다.
		//
		// 예전에는 여기도 HandleRecvCancelled 로만 보냈다. 그 함수는 IO
		// 카운트만 내린다. 그래서 클라이언트는 걸린 수신 없이 연결된 것처럼
		// 남아, 아무것도 받지 못한 채 조용히 멈췄다. 서버 쪽과 같은 결함이다.
		//
		// 종료를 먼저 요청하고 카운트는 마지막에 내린다.
		session->NoteDisconnectReason(DisconnectReason::SocketError);
		OnDisconnectRequest(session);

		HandleRecvCancelled(overlappedEx, session);

		return;
	}
	else if (ioOperation == IO_OPERATION::SEND)
	{
		if (errorCode == ERROR_OPERATION_ABORTED)
		{
			HandleSendCancelled(overlappedEx, session);
			return;
		}

		if (!IsConnectionDeadError(errorCode))
		{
			ENGINE_VIOLATION("session %u unhandled send error %d, treating the connection as dead",
				session->GetSessionID(), errorCode);
		}

		// 송신이 죽은 연결도 마찬가지다. 수신 쪽 주석 참고.
		session->NoteDisconnectReason(DisconnectReason::SocketError);
		OnDisconnectRequest(session);

		HandleSendCancelled(overlappedEx, session);

		return;
	}
	else
	{
		ENGINE_VIOLATION("socket error reported for an unknown io operation %d (error %d)", static_cast<int>(ioOperation), errorCode);
		return;
	}
}

void IOCPClient::HandleConnect(uint32_t sessionId, DWORD bytesTransferred)
{
	ENGINE_CHECK_RETVOID(m_session != nullptr, "connect completion arrived but there is no session");

	// ConnectEx 몫의 카운트는 이 함수 끝에서 내린다.
	//
	// 예전에는 여기 맨 앞에서 내렸다. 그때는 그게 옳았다 — 아래의
	// OnDisconnectRequest 가 카운트 0 을 기다렸으므로, 우리 몫을 먼저
	// 내려놓지 않으면 자기를 기다리는 꼴이 됐다.
	//
	// 이제 기다리지 않는다. 그래서 반대가 됐다. 여기서 먼저 내리면
	// 카운트가 0 이 되어 실패 경로의 disconnect 가 그 자리에서 소켓을
	// 닫아 버리고, 그 뒤 남은 코드가 닫힌 소켓을 쓴다.
	//
	// 실패 여부와 무관하게 완료 통지 1건은 소비했으므로 반드시 내린다.
	// 그 자리가 이 함수의 마지막 줄이 됐을 뿐이다.
	const bool connectSequenceOk = RunClientConnectSequence();

	if (!connectSequenceOk)
	{
		OnDisconnectRequest(m_session);
	}

	// 이 함수의 마지막 줄이어야 한다. 여기서 지연된 disconnect 마무리가
	// 실행될 수 있으므로 이후로 m_session 을 만지면 안 된다.
	m_session->DecrementIO();
}

bool IOCPClient::RunClientConnectSequence()
{
	if (!SocketOption::SetClientContext(m_clientSocket))
	{
		LOGE("session %u SO_UPDATE_CONNECT_CONTEXT failed, dropping the connection", m_session->GetSessionID());
		return false;
	}

	if (!SocketOption::SetNoDelay(m_clientSocket))
	{
		LOGE("session %u TCP_NODELAY failed, dropping the connection", m_session->GetSessionID());
		return false;
	}

	// TCP keepalive.
	//
	// 서버는 수락한 소켓에 이미 걸고 있었는데 클라만 빠져 있었다.
	// 유휴 타임아웃과 겹치는 장치가 아니다 — keepalive 는 커널이 응답하므로
	// 경로 단절과 호스트 사망은 잡아도 프로세스 행은 못 잡고, 반대로
	// 유휴 타임아웃은 인증 전 구간(하트비트가 아직 없다)을 덮지 못한다.
	//
	// 실패해도 접속을 버리지 않는다. 이건 있으면 좋은 보조 장치이고,
	// 없다고 해서 연결이 동작하지 않는 것은 아니다.
	if (m_livenessConfig.useKeepAlive)
	{
		if (!SocketOption::SetKeepAliveEx(m_clientSocket,
			m_livenessConfig.keepAliveTime_ms, m_livenessConfig.keepAliveInterval_ms))
		{
			LOGW("session %u could not set TCP keepalive. the connection stays up, but a "
				"broken path will only be noticed by the idle timeout", m_session->GetSessionID());
		}
	}

	if (!m_session->OnConnect())
	{
		LOGE("session %u OnConnect failed, dropping the connection", m_session->GetSessionID());
		return false;
	}

	// 접속 통지를 인증 요청 '전' 에 보낸다.
	//
	// 순서가 반대였다. 그런데 SendSystemAuthRequest 는 그 자리에서 WSASend 를
	// 발행하고, 루프백이면 응답이 마이크로초 안에 돌아와 다른 GQCS 워커가
	// OnSessionEstablished 를 부른다. 그래서 서비스가 접속 통지보다 established
	// 통지를 먼저 받을 수 있었다.
	//
	// 서비스가 접속 통지에서 세션별 자원을 잡고 established 에서 쓴다면 그건
	// 곧 널 참조다. 게다가 그 사이에 세션이 정리되면 MarkServiceConnectNotified
	// 가 이미 끝난 종료 뒤에 래치를 세워, 종료 통지의 짝이 어긋난다.
	//
	// 아직 아무것도 보내지 않았으므로 이 자리에서는 응답이 올 수 없다.
	// 서버 쪽 RunAcceptedConnectSequence 와 같은 모양이 된다.
	m_session->MarkServiceConnectNotified();

	OnClientConnect(m_session);

	if (!SendSystemAuthRequest(m_session))
	{
		LOGE("session %u system auth request send failed", m_session->GetSessionID());
		return false;
	}

	return true;
}

void IOCPClient::HandleConnectCancelled(OverlappedEx* overlappedEx, ClientSession* clientSession, int errorCode)
{
	LOGW("ConnectEx ended with error %d", errorCode);

	if (!clientSession)
	{
		ENGINE_VIOLATION("connect cancellation arrived with no session");
		return;
	}

	clientSession->SetClientSessionState(ClientSessionState::CONNECT_ABORTED);

	// 여기서 OnClientDisconnect 를 부르지 않는다.
	//
	// 접속이 성립한 적이 없으므로 OnClientConnect 도 부른 적이 없다.
	// 짝 없는 종료 통지는 서비스 쪽에서 "접속당 하나" 를 세는 코드를
	// 어긋나게 한다. 그래서 종료가 아니라 별도의 신호로 알린다.
	//
	// 예전에는 그 별도 신호가 없었다. 주석에 "별도의 신호여야 한다" 라고
	// 적어 두고 끝이었고, 그래서 서버가 떠 있지 않으면 서비스는 콜백을
	// 하나도 받지 못했다 — 재시도를 걸 근거 자체가 없었다.
	//
	// ERROR_OPERATION_ABORTED 는 뺀다. 그건 우리가 StopClient 등에서
	// 건 취소이지 접속 실패가 아니다.
	if (errorCode != ERROR_OPERATION_ABORTED)
	{
		clientSession->NoteDisconnectReason(DisconnectReason::ConnectFailed);
		OnConnectFailed(errorCode);
	}
	else
	{
		clientSession->NoteDisconnectReason(DisconnectReason::LocalShutdown);
	}

	// disconnect 를 먼저 요청하고, ConnectEx 몫의 카운트는 마지막에 내린다.
	// 순서를 바꾸면 카운트가 0 이 된 사이에 disconnect 가 소켓을 닫고,
	// 그 뒤 이 함수가 세션을 계속 만지게 된다.
	OnDisconnectRequest(clientSession);

	clientSession->DecrementIO();
}

void IOCPClient::HandleRecv(OverlappedEx* overlappedEx, ClientSession* session, DWORD bytesTransferred)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	if (bytesTransferred == 0)
	{
		// 클라이언트쪽에서 소켓 Close 를 보낸경우
		// 정상적인 클라이언트 접속 종료
		const DWORD error = ::GetLastError();
		const int nError = ::WSAGetLastError();

		// disconnect 를 먼저 요청하고 카운트를 마지막에 내린다.
		// HandleSessionDisconnected 가 OnDisconnectRequest 를 부르는데,
		// 그보다 먼저 카운트를 0 으로 만들면 그 자리에서 소켓이 닫히고
		// 세션 정리가 끝나 버린다.
		HandleSessionDisconnected(overlappedEx, session, bytesTransferred);

		// 이 분기의 마지막 줄이어야 한다.
		session->DecrementIO();

		return;
	}

	ClientSession* clientSession = static_cast<ClientSession*>(session);

	// DecrementIO 는 이 함수 끝에서 한다. 서버 쪽 HandleRecv 와 같은 이유다.
	// 카운트를 먼저 내리면 "카운트 0 = 아무도 세션을 만지지 않음" 이 깨지고,
	// 그 틈에 다른 스레드가 취소 대기를 통과해 세션을 정리할 수 있다.
	bool disconnectAfterHandling = false;

	clientSession->UpdateLastRecvTick();

	RecvPacketBuffer& recvBuf = clientSession->GetReceiveBuffer();

	if (!recvBuf.CommitWrite(bytesTransferred))
	{
		ENGINE_VIOLATION("session %u recv ring commit failed : %lu bytes would overflow (stored %u / capacity %u)",
			clientSession->GetSessionID(), bytesTransferred,
			recvBuf.GetStoredSize(), recvBuf.GetCapacity());

		clientSession->DecrementIO();
		return;
	}

	// 패킷 처리 루프
	while (!recvBuf.IsEmpty())
	{
		uint32_t packetSize = 0;
		uint16_t packetId = 0;
		char* packetDataByMemoryPool = nullptr;

		const PacketReadResult readResult = recvBuf.ReadPacket(packetDataByMemoryPool, packetSize, packetId);

		if (readResult == PacketReadResult::NeedMoreData)
		{
			// 아직 다 안 왔다. 다음 수신 완료에서 이어서 파싱한다.
			break;
		}

		if (readResult != PacketReadResult::Ok)
		{
			// Invalid 는 서버가 규칙 밖의 크기를 적어 보낸 것이고,
			// OutOfMemory 는 패킷 풀이 고갈된 것이다. 둘 다 이 스트림을
			// 더 이상 신뢰할 수 없으므로 연결을 끊는다.
			LOGE("session %u recv stream is unusable (%s). dropping the connection",
				clientSession->GetSessionID(),
				readResult == PacketReadResult::Invalid ? "invalid packet size" : "packet pool exhausted");

			// 두 사유는 서비스가 할 일이 다르다. 앞쪽은 서버가 이상한
			// 것이고, 뒤쪽은 우리 풀이 모자란 것이다.
			clientSession->NoteDisconnectReason(readResult == PacketReadResult::Invalid
				? DisconnectReason::ProtocolError
				: DisconnectReason::ResourceExhausted);

			disconnectAfterHandling = true;
			break;
		}

		if (!IsValidPacketId(packetId))
		{
			// 스트림이 어긋났다. 디스패치하지 않고 연결을 끊는다.
			MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), packetDataByMemoryPool);
			LOGE("session %u received an invalid packet id %u (size %u). dropping the connection",
				clientSession->GetSessionID(), packetId, packetSize);
			clientSession->NoteDisconnectReason(DisconnectReason::ProtocolError);
			disconnectAfterHandling = true;
			break;
		}

		if (IsSystemPacketId(packetId))
		{
			const bool handled = HandleSystemPacket(clientSession, packetId, packetDataByMemoryPool, packetSize);

			MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), packetDataByMemoryPool);

			if (!handled)
			{
				// 인증 거절 같은 구체적인 사유는 HandleSystemPacket 이 이미
				// 남겼다. 먼저 쓴 값이 이기므로 이 줄이 덮지 않는다.
				LOGE("session %u engine packet handling failed (PacketID : %u, %s)",
					clientSession->GetSessionID(), packetId,
					ToString(clientSession->GetDisconnectReason()));
				clientSession->NoteDisconnectReason(DisconnectReason::ProtocolError);
				disconnectAfterHandling = true;
				break;
			}

			continue;
		}

		if (!clientSession->IsEstablished())
		{
			MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), packetDataByMemoryPool);
			LOGW("session %u service packet received before session established (PacketID : %u)", clientSession->GetSessionID(), packetId);
			clientSession->NoteDisconnectReason(DisconnectReason::UnexpectedPacket);
			disconnectAfterHandling = true;
			break;
		}

		// 패킷 완성! 실제 처리 호출
		OnReceive(clientSession, packetId, packetDataByMemoryPool, packetSize);
	}

	// 처리 도중 종료가 예약됐으면 다음 수신을 걸지 않는다.
	// (이유는 서버 쪽 HandleRecv 의 같은 자리 주석 참고)
	if (!disconnectAfterHandling && !clientSession->IsReleasePending())
	{
		// 다시 다음 수신 요청
		// 실패하면 이 세션은 pending recv 가 없는 상태로 남아 통신이 조용히 멈추므로
		// 그대로 방치하지 않고 연결을 정리한다.
		// 백프레셔는 서버 쪽 HandleRecv 와 같다. (사정은 그쪽 주석)
		// 클라이언트는 스케줄러 스레드가 하나뿐이라 소비가 더 쉽게 밀린다.
		if (!clientSession->PostReceiveOrPause())
		{
			LOGE("session %u failed to post the next recv, disconnecting instead of going silent",
				clientSession->GetSessionID());
			clientSession->NoteDisconnectReason(DisconnectReason::ResourceExhausted);
			disconnectAfterHandling = true;
		}
	}

	// disconnect 를 먼저 요청하고 카운트는 마지막에 내린다. (서버 쪽 HandleRecv 와 같다)
	if (disconnectAfterHandling)
	{
		OnDisconnectRequest(clientSession);
	}

	// 이 함수의 마지막 줄이어야 한다.
	clientSession->DecrementIO();
}

void IOCPClient::HandleRecvCancelled(OverlappedEx* overlappedEx, ClientSession* session)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	// 아이디를 먼저 읽어 둔다. DecrementIO 가 마지막 카운트를 내리면
	// 지연된 disconnect 마무리가 그 자리에서 실행된다.
	const uint32_t sessionId = session->GetSessionID();

	// 걸어 두었던 WSARecv 에 대한 IO 감소
	session->DecrementIO();

	LOGI("session %u recv cancelled", sessionId);
}

void IOCPClient::HandleSend(OverlappedEx* overlappedEx, ClientSession* session, DWORD bytesTransferred)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	ClientSession* clientSession = static_cast<ClientSession*>(session);

	if (clientSession->GetSessionRole() != SESSION_ROLE::CLIENT)
	{
		ENGINE_VIOLATION("session %u send completion arrived but the role is %d, not CLIENT",
			clientSession->GetSessionID(), static_cast<int>(clientSession->GetSessionRole()));

		// 카운트를 반드시 내려놓아야 한다. 그러지 않으면 취소 대기가 풀리지 않는다.
		clientSession->DecrementIO();
		return;
	}

	// 선언만 되어 있고 엔진이 부르지 않던 훅이다. OnReceive 와 대칭이 맞아야
	// 서비스가 송신 완료를 관측할 수 있다.
	//
	// 서버 쪽(IOCPServer::HandleSend)은 이미 고쳐져 있었는데 이쪽만 빠져
	// 있었다. 같은 처리가 두 곳에 따로 구현되어 있어서 생긴 누락이다.
	OnSend(session, bytesTransferred);

	// OnSendCompleted 가 다음 패킷의 WSASend 까지 발행하므로 처리 후에 내린다.
	clientSession->OnSendCompleted(bytesTransferred);

	clientSession->DecrementIO();
}

void IOCPClient::HandleSendCancelled(OverlappedEx* overlappedEx, ClientSession* session)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	// 아이디를 먼저 읽는 이유는 HandleRecvCancelled 와 같다.
	const uint32_t sessionId = session->GetSessionID();

	// 걸어 두었던 WSASend 에 대한 IO 감소
	session->DecrementIO();

	LOGI("session %u send cancelled", sessionId);
}

void IOCPClient::HandleSessionDisconnected(OverlappedEx* overlappedEx, ClientSession* session, DWORD bytesTransferred)
{
	// 세션의 정상 종료 시퀀스

	ENGINE_CHECK_RETVOID(session != nullptr && overlappedEx != nullptr,
		"disconnect handler called with session %p overlapped %p", session, overlappedEx);

	if (overlappedEx->operation == IO_OPERATION::RECV)
	{
		session->NoteDisconnectReason(DisconnectReason::PeerClosed);
		OnDisconnectRequest(session);
	}
	else
	{
		// 0바이트 완료는 RECV 에서만 발생한다.
		ENGINE_VIOLATION("session %u reported a zero byte completion on io %d, which should only happen for RECV",
			session->GetSessionID(), static_cast<int>(overlappedEx->operation));
	}
}

bool IOCPClient::CreateConnectSocket()
{
	m_clientSocket = ::WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);

	if (m_clientSocket == INVALID_SOCKET)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] WSASocket Connect socket failed");
		return false;
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] WSASocket Connect socket success");

	return true;
}

void IOCPClient::DestroyConnectSocket()
{
	IOCPCore::CloseSocketHandle(m_session->DetachSocket());
}

bool IOCPClient::BindClientSocket(SOCKET clientSocket)
{
	SOCKADDR_IN localAddr;
	localAddr.sin_family = AF_INET;
	localAddr.sin_port = 0;
	localAddr.sin_addr.s_addr = ::htonl(INADDR_ANY);

	if (::bind(clientSocket, (SOCKADDR*)&localAddr, sizeof(localAddr)) == SOCKET_ERROR)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] bind failed - ErroCode : %d", WSAGetLastError());

		return false;
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] bind success");

	return true;
}

bool IOCPClient::InitializeGUIDConnectEx(SOCKET clientSocket)
{
	// ConnectEx 를 사용하기 위해 함수 포인터를 얻어오는 함수
	// WSAIoctl 를 사용하여 ConnectEx 함수 포인터를 얻어올 수 있다.

	DWORD bytes = 0;

	// ConnectEX 함수 포인터 얻기
	GUID GuidConnectEx = WSAID_CONNECTEX;
	if (::WSAIoctl(clientSocket,
		SIO_GET_EXTENSION_FUNCTION_POINTER,
		&GuidConnectEx,
		sizeof(GuidConnectEx),
		&m_connectEx,
		sizeof(m_connectEx),
		&bytes,
		nullptr,
		nullptr) == SOCKET_ERROR)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] WSAIoctl Get ConnectEx Pointer Failed - ErroCode : %d", WSAGetLastError());

		return false;
	}

	return true;
}

void IOCPClient::FinalizeGUIDConnectEx()
{
	m_connectEx = nullptr;
}

bool IOCPClient::PrepareConnect()
{
	if (!m_session)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] Failed to acquire session");
		ENGINE_BREAK_IF_DEBUGGER();
		return false;
	}

	if (m_session->GetClientSessionState() != ClientSessionState::CONNECT_READY)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] Session is not Ready");
		ENGINE_BREAK_IF_DEBUGGER();
		return false;
	}

	if (!PostConnect(m_session))
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] Failed to post ConnectEx");
		return false;
	}

	return true;
}

bool IOCPClient::PostConnect(ClientSession* clientSession)
{
	if (!clientSession)
	{
		ENGINE_VIOLATION("PostConnect called with no session");
		return false;
	}

	SOCKADDR_IN serveraddr = { 0 };
	serveraddr.sin_family = AF_INET;
	serveraddr.sin_port = htons(m_serverPort);
	int nResult = ::inet_pton(AF_INET, m_serverIPAddress, &serveraddr.sin_addr);

	if (nResult != 1)
	{
		// inet_pton 은 0(형식이 틀림)과 -1(그 밖의 오류)을 따로 돌려준다.
		//
		// 예전에는 -1 분기가 오류 코드를 읽고 디버거만 세운 뒤 그대로 아래로
		// 떨어졌다. 그러면 serveraddr 의 sin_addr 이 0 인 채로 ConnectEx 가
		// 나간다 — 0.0.0.0 은 윈도우에서 로컬 호스트로 해석되므로, 주소가
		// 틀렸다고 알리는 대신 '엉뚱한 곳에 붙은 클라이언트' 가 된다.
		// 어느 쪽도 접속할 수 없는 주소라는 사실은 같으므로 함께 실패시킨다.
		// 위반이 아니라 오류로 남긴다. 이건 엔진의 불변식이 깨진 것이 아니라
		// 서비스가 준 인자가 틀린 것이고, 서버 쪽 BindServerSocket 의 같은
		// 판정도 LOGE 다. 위반 계수기는 하네스가 건전성 판정에 쓰므로
		// 인자 오류로 더럽히지 않는다.
		LOGE("server address '%s' is not a valid IPv4 address (inet_pton %d, error %d)",
			m_serverIPAddress, nResult, ::WSAGetLastError());
		return false;
	}

	// GQCS 사용을 위한 IOCP 등록
	if (!IOCPCore::RegisterSocketToIOCP((ULONG_PTR)clientSession, clientSession->GetClientSocket()))
		return false;

	// IO 수량 증가(ConnectEx 에 대한 IO)
	clientSession->IncrementIO();

	// Overlapped 구조체 초기화
	OverlappedEx& overlappedEx = clientSession->GetConnectOverlapped();  // session이 미리 생성한 OverlappedEx 포인터 반환

	// ConnectEx 는 보낼 데이터를 넘기지 않으므로 wsaBuffer 는 비워 둔다.
	overlappedEx.ResetForNextIO(clientSession->GetSessionID());

	clientSession->SetClientSessionState(ClientSessionState::CONNECTING);

	DWORD bytesSent = 0;
	BOOL bResult = m_connectEx(
		m_clientSocket,
		(SOCKADDR*)&serveraddr,
		sizeof(serveraddr),
		NULL,
		0,
		&bytesSent,
		(LPOVERLAPPED)&overlappedEx
	);

	if (bResult == FALSE)
	{
		const int nError = ::WSAGetLastError();
		if (nError != ERROR_IO_PENDING)
		{
			clientSession->SetClientSessionState(ClientSessionState::CONNECT_ABORTED);

			// 접속이 성립한 적이 없으므로 종료 통지도 없다.
			// (HandleConnectCancelled 의 같은 자리 주석 참고)

			OnDisconnectRequest(clientSession);

			// ConnectEx 가 실패했으므로 위에서 올린 몫을 내린다.
			// 다른 경로와 같은 규칙으로 마지막에 둔다.
			clientSession->DecrementIO();

			return false;
		}
	}

	return true;
}

// 이 클라이언트가 가진 세션은 m_session 하나뿐이다.
//
// 예전에는 이 자리에서 dynamic_cast<ClientSession*> 로 되돌렸다. 유일한
// 세션이 ClientSession 이라 그 RTTI 조회는 실패할 수 없었고, 그런데도
// 실패 분기가 "캐스팅 실패" 라는 원인과 무관한 문구를 남기고 있었다.
//
// 정말 확인해야 할 것은 타입이 아니라 신원이다. 들어온 포인터가 이 객체의
// 세션이 아니라면 라우팅이 깨진 것이고, 그건 캐스팅으로 덮을 일이 아니다.
ClientSession* IOCPClient::ResolveOwnSession(ISession* session, const char* calledFrom)
{
	if (!session)
	{
		ENGINE_VIOLATION("%s received a null session", calledFrom);
		return nullptr;
	}

	if (session != m_session)
	{
		ENGINE_VIOLATION("%s received session %p but this client owns %p",
			calledFrom, static_cast<const void*>(session), static_cast<const void*>(m_session));
		return nullptr;
	}

	return m_session;
}

void IOCPClient::OnDisconnectRequest(ISession* session)
{
	ClientSession* clientSession = ResolveOwnSession(session, "OnDisconnectRequest");
	if (!clientSession)
		return;

	// 종료 절차는 한 스레드만 수행한다.
	//
	// 이 함수는 앱 스레드의 StopClient, ConnectEx 취소 완료 처리, 그리고
	// recv/send 오류의 NotifyDisconnect 에서 동시에 들어온다. 가드가 없으면
	// 들어온 스레드가 전부 각자 10초 WaitForIOCancelComplete 를 수행하는데,
	// 이 클라이언트의 GQCS 워커는 2개뿐이라 그 둘이 대기에 들어가면 취소
	// 완료 통지를 꺼낼 스레드가 남지 않는다. 카운트가 0 이 되지 못해 모두
	// 10초를 꽉 채우고, 그 뒤 "취소가 끝나지 않았는데도 소켓을 닫는"
	// 경로로 넘어간다. 남은 완료 통지가 닫힌 소켓을 참조할 수 있는 상태다.
	//
	// 실측으로 bench 실행당 200회 이상이었다. 서버 쪽은 ClientSessionPool 의
	// poolState CAS 가 같은 역할을 하고 있었고("release skipped" 로그),
	// 클라이언트 쪽에만 그 가드가 없었다.
	if (::InterlockedExchange(&m_disconnecting, 1) != 0)
	{
		// 다른 스레드가 이미 수행 중이다. 여기서 함께 기다릴 이유가 없다.
		LOGI("session %u disconnect already in progress, skipping", clientSession->GetSessionID());
		return;
	}

	if (clientSession->GetClientSocket() != INVALID_SOCKET)
	{
		// Connect, Recv, Send IO 를 모두 취소한다.
		if (!clientSession->CancelPendingIO())
		{
			ENGINE_VIOLATION("session %u CancelPendingIO failed during disconnect", clientSession->GetSessionID());
		}
	}

	// 여기서 기다리지 않는다.
	//
	// 예전에는 WaitForIOCancelComplete(10초) 였다. 이 함수는 완료 핸들러
	// 안에서도 불리는데(recv/send 오류 -> HandleSocketError ->
	// NotifyDisconnect), 그때 그 핸들러는 자기 몫의 카운트를 아직 들고
	// 있다. 그 카운트를 내려 줄 스레드가 바로 지금 대기 중인 자신이므로
	// 대기는 풀리지 않는다.
	//
	// 클라이언트는 GQCS 워커가 2개뿐이라 이 자기 대기가 특히 나빴다.
	// 둘 다 대기에 들어가면 취소 완료 통지를 꺼낼 스레드가 하나도 남지
	// 않아, 다른 경로에서 들어온 스레드까지 10초를 꽉 채웠다.
	//
	// 대신 예약해 두고, 마지막 완료가 CompleteDisconnect 를 수행한다.
	if (!clientSession->RequestRelease())
	{
		return;
	}

	CompleteDisconnect(clientSession);
}

void IOCPClient::OnReleaseReady(void* context, BaseSession* session)
{
	IOCPClient* client = static_cast<IOCPClient*>(context);

	client->CompleteDisconnect(static_cast<ClientSession*>(session));
}

void IOCPClient::CompleteDisconnect(ClientSession* clientSession)
{
	// 남은 완료 통지가 없는 것이 확인된 뒤다. 이제 소켓을 닫아도
	// 진행 중인 I/O 가 닫힌 핸들을 참조할 일이 없다.
	if (clientSession->GetClientSocket() != INVALID_SOCKET)
	{
		DestroyConnectSocket();
	}

	// 서비스에 종료를 알린다.
	//
	// 여기가 클라이언트의 유일한 종료 지점이다. 예전에는 접속에 실패한
	// 경우(HandleConnectCancelled, PostConnect 실패)에만 OnClientDisconnect
	// 가 불렸고, 정작 붙어서 쓰던 연결이 끊길 때는 아무 말이 없었다.
	// 짝이 거꾸로였다.
	//
	// 래치가 그 짝을 보장한다. 접속을 알린 적 없으면 여기서도 알리지 않고,
	// 알린 적 있으면 정확히 한 번 알린다.
	if (clientSession->ConsumeServiceConnectNotified())
	{
		// 사유는 세션이 들고 있다. 끊기로 결정한 자리에서 표시해 두고
		// (BaseSession::NoteDisconnectReason) 통지하는 자리에서 꺼낸다.
		const DisconnectReason reason = clientSession->GetDisconnectReason();

		// 사정은 서버 쪽 OnSessionDisconnectNotify 의 같은 자리 주석 참고.
		if (reason == DisconnectReason::Unknown)
		{
			LOGW("session %u is being reported as disconnected with no reason recorded. "
				"some path called disconnect without NoteDisconnectReason",
				clientSession->GetSessionID());
		}

		OnClientDisconnect(clientSession, reason);
	}

	// 소켓을 닫은 이후 세션에 대한 상태 및 정리를 수행
	if (!clientSession->OnDisconnect())
	{
		ENGINE_VIOLATION("session %u OnDisconnect reported failure", clientSession->GetSessionID());
	}
}

PacketHandlerTable* IOCPClient::GetPacketHandlerTable() const
{
	return m_packetHandlerTable;
}

TlsMemoryPool* IOCPClient::GetJobMemoryPool() const
{
	return m_jobMemoryPool;
}

TlsMemoryPool* IOCPClient::GetPacketMemoryPool() const
{
	return m_packetMemoryPool;
}

TlsMemoryPool* IOCPClient::GetSendQueueMemoryPool() const
{
	return m_sendQueueMemoryPool;
}

const HandlerContext& IOCPClient::GetHandlerContext() const
{
	return m_handlerContext;
}

bool IOCPClient::SubmitPacketJob(ISession* session, uint16_t packetId, const char* packetData, uint32_t packetSize)
{
	ClientSession* clientSession = ResolveOwnSession(session, "SubmitPacketJob");

	auto dropPacket = [this, packetData]()
		{
			if (packetData && m_packetMemoryPool)
			{
				MEMORY_POOL::ReleasePacket(*m_packetMemoryPool, packetData);
			}
		};

	if (!clientSession || !packetData)
	{
		dropPacket();
		return false;
	}

	if (!m_packetHandlerTable || !m_jobMemoryPool)
	{
		ENGINE_VIOLATION("cannot submit a job : the client is not fully initialized");
		dropPacket();
		return false;
	}

	PacketHandlerFunc handler = m_packetHandlerTable->GetHandler(packetId);
	if (!handler)
	{
		LOGW("received packet id %u with no registered handler, dropping it", packetId);
		dropPacket();
		return false;
	}

	Job* job = MEMORY_POOL::CreateJob(*m_jobMemoryPool);
	if (!job)
	{
		LOGE("could not allocate a job, dropping packet id %u", packetId);
		dropPacket();
		return false;
	}

	job->SetPacketJob(JobType::PACKET, handler, session, packetId, packetData, packetSize, m_handlerContext);

	// SubmitJob 이 실패하면 job 과 패킷의 소유권은 여기 그대로 남는다.
	// 잡은 쪽이 반납한다 — 이쪽 풀은 위에서 유효성이 확인되어 있으므로
	// 항상 되돌릴 수 있다. (사정은 ClientSession::SubmitJob 주석 참고)
	//
	// 서버 쪽 SubmitPacketJob 의 EnqueueJob 실패 자리와 구조가 같다.
	if (!clientSession->SubmitJob(job))
	{
		LOGE("failed to enqueue a job for packet id %u", packetId);

		dropPacket();
		MEMORY_POOL::ReleaseJob(*m_jobMemoryPool, job);
		return false;
	}

	return true;
}

ClientSession* IOCPClient::GetClientSession() const
{
	return m_session;
}

uint32_t IOCPClient::GetOutstandingIOCount() const
{
	if (!m_session)
		return 0;

	const LONG outstanding = m_session->GetOutstandingIOCount();

	// 음수는 짝이 맞지 않는다는 뜻이고 이미 위반으로 잡힌다.
	return outstanding < 0 ? 0 : static_cast<uint32_t>(outstanding);
}

void* IOCPClient::GetServiceContext()
{
	return this;
}

bool IOCPClient::SendSystemAuthRequest(ClientSession* session)
{
	if (!session || !session->IsTransportConnected())
	{
		return false;
	}

	void* memory = MEMORY_POOL::CreatePacket(*GetPacketMemoryPool(), sizeof(CS_SYSTEM_AUTH_REQUEST_PACKET));
	if (!memory)
	{
		return false;
	}

	CS_SYSTEM_AUTH_REQUEST_PACKET* request = reinterpret_cast<CS_SYSTEM_AUTH_REQUEST_PACKET*>(memory);
	*request = CS_SYSTEM_AUTH_REQUEST_PACKET();

	// 상태를 보내기 '전' 에 올린다.
	//
	// 순서가 반대였다. 큐에 넣고 나서 AUTH_PENDING 을 썼는데, EnqueueSendPacket 은
	// 그 자리에서 WSASend 까지 발행한다. 루프백이면 서버가 요청을 받아 응답을
	// 돌려주는 왕복이 마이크로초 단위로 끝나고, 그 응답을 처리하는 것은 이
	// 스레드가 아니라 클라의 '다른' GQCS 워커다 (접속 완료와 수신 완료는 서로
	// 다른 완료 통지다).
	//
	// 그래서 저쪽이 ESTABLISHED 를 쓴 뒤에 이쪽이 AUTH_PENDING 으로 덮어쓸 수
	// 있었다. 그러면 세션은 영원히 AUTH_PENDING 에 남는다 — ESTABLISHED 를 다시
	// 쓰는 경로가 없기 때문이다. 그 뒤로는
	//   서비스 패킷 송신이 "established 전" 으로 거부되고,
	//   서버가 보낸 에코가 도착하면 "service packet received before session
	//   established" 로 판정해 클라가 스스로 연결을 끊는다.
	// 읽지 않은 데이터를 안은 채 소켓이 닫히므로 RST 가 나가고, 서버의 걸려
	// 있던 WSASend 가 10054 로 실패해 서버도 그 세션을 잃는다.
	//
	// 실측: bench Phase 6 이 93회 중 1회 이 경로로 세션을 잃었다. 창을 넓혀
	// (이 자리에 Sleep(1)) 확인하면 40개 세션 전부가 AUTH_PENDING 으로
	// 되돌아갔다. (tools/authrace)
	//
	// 먼저 올려 두면 덮어쓸 것이 없다. 시스템 패킷 송신은 IsTransportConnected
	// 만 보므로(CanSendPacket) AUTH_PENDING 상태에서도 이 요청은 나간다.
	session->SetClientSessionState(ClientSessionState::AUTH_PENDING);

	void* packetData = request;
	if (!session->EnqueueSendPacket(&packetData, sizeof(CS_SYSTEM_AUTH_REQUEST_PACKET)))
	{
		// 보내지 못했으니 되돌린다. 나간 것이 없으므로 응답이 올 수도 없고,
		// 따라서 이 쓰기가 ESTABLISHED 를 덮을 일도 없다.
		session->SetClientSessionState(ClientSessionState::CONNECTED);

		MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), request);
		return false;
	}

	return true;
}

bool IOCPClient::HandleSystemPacket(ClientSession* session, uint16_t packetId, const char* packetData, uint32_t packetSize)
{
	if (!session || !packetData)
	{
		return false;
	}

	switch (static_cast<PACKET_ID>(packetId))
	{
	case PACKET_ID::SC_SYSTEM_AUTH_RESPONSE:
	{
		if (packetSize != sizeof(SC_SYSTEM_AUTH_RESPONSE_PACKET))
		{
			return false;
		}

		const SC_SYSTEM_AUTH_RESPONSE_PACKET* response = reinterpret_cast<const SC_SYSTEM_AUTH_RESPONSE_PACKET*>(packetData);
		const SYSTEM_AUTH_RESULT authResult = static_cast<SYSTEM_AUTH_RESULT>(response->authResult);

		if (response->protocolVersion != IOCP_ENGINE_PROTOCOL_VERSION)
		{
			LOGE("session %u protocol version mismatch (server=%u, client=%u)", session->GetSessionID(), response->protocolVersion, IOCP_ENGINE_PROTOCOL_VERSION);
			session->NoteDisconnectReason(DisconnectReason::AuthRejectedProtocolMismatch);
			return false;
		}

		if (authResult != SYSTEM_AUTH_RESULT::SUCCESS)
		{
			LOGW("session %u auth rejected by server (result=%u)", session->GetSessionID(), response->authResult);

			// 서버가 준 거절 사유를 그대로 서비스까지 올린다.
			//
			// SERVER_FULL 을 따로 만든 이유가 "RST 만 보면 만석과 장애를
			// 구분할 수 없다" 였는데, 정작 그 값이 여기서 로그로만 남고
			// 끝나 서비스는 재접속 여부를 판단할 수 없었다.
			session->NoteDisconnectReason(ToDisconnectReason(authResult));
			return false;
		}

		session->SetClientSessionState(ClientSessionState::ESTABLISHED);
		LOGI("session %u session established", session->GetSessionID());

		// 여기가 유휴 감시를 켜는 자리다.
		//
		// 서버의 하트비트는 established 세션에만 나가므로 그 전에는 잴
		// 기준이 없다. 그리고 기준이 되는 주기는 서버 설정이라 이 응답에
		// 실려 온다 — 상수로 복제하면 서버가 주기를 바꾸는 순간 클라가
		// 멀쩡한 연결을 끊는다.
		//
		// 서비스 훅보다 먼저 부른다. OnSessionEstablished 가 오래 붙들려도
		// 그 사이의 침묵이 감시에서 빠지지 않게 한다.
		if (m_clientWatchdog)
		{
			m_clientWatchdog->Arm(response->heartbeatIntervalMs);
		}

		OnSessionEstablished(session);
		return true;
	}

	case PACKET_ID::SC_SYSTEM_HEARTBEAT_REQUEST:
	{
		if (packetSize != sizeof(SC_SYSTEM_HEARTBEAT_REQUEST_PACKET))
		{
			return false;
		}

		if (!session->IsEstablished())
		{
			return false;
		}

		const SC_SYSTEM_HEARTBEAT_REQUEST_PACKET* request = reinterpret_cast<const SC_SYSTEM_HEARTBEAT_REQUEST_PACKET*>(packetData);

		// 응답을 보내지 못해도 연결을 끊지 않는다.
		//
		// 예전에는 이 함수의 반환값을 그대로 돌려줬고, 호출부(HandleRecv)는
		// false 를 "엔진 패킷 처리 실패" 로 보고 연결을 정리했다. 그런데
		// SendSystemHeartbeatResponse 가 false 를 내는 남은 이유는 전부
		// 일시적인 혼잡이다 — 송신 큐 포화, 패킷 풀 일시 고갈. (역할과
		// established 는 바로 위에서 이미 확인했다)
		//
		// 즉 "지금 이 순간 보낼 자리가 없다" 를 프로토콜 위반으로 처리하고
		// 있었다. 송신 큐가 가득 차면 즉시 false 를 돌려주는 것은 이 엔진의
		// 설계된 동작인데(대기하지 않고 호출부에 정책을 맡긴다), 정작 엔진
		// 자신이 그걸 치명으로 받았다.
		//
		// 실측: 클라가 전력으로 송신하면 송신 큐(기본 깊이 4096)가 계속
		// 차 있다. 그 상태에서 서버 하트비트(5초 주기)가 도착하면 응답을
		// 큐에 넣지 못하고, 클라가 스스로 끊는다. 그러면 읽지 않은 데이터를
		// 안은 채 소켓이 닫히므로 RST 가 나가고, 서버의 걸려 있던 WSASend 가
		// 10054 로 실패해 서버도 그 세션을 정리한다. 6초 부하에서 클라 4대 중
		// 3대가 정확히 t=5.0s(하트비트 주기)에 함께 사라졌다.
		//
		// 응답 한 번을 놓치는 것은 안전하다. 생존 판정은 서버의 하트비트
		// 타임아웃(기본 15초)이 하고, 그 기준은 max(마지막 수신, 마지막
		// 하트비트) 다. 클라가 데이터를 보내고 있으면 수신 시각이 계속
		// 갱신되므로 타임아웃이 돌지 않는다 — 실제로 살아 있으니 맞는 판정이다.
		// 반대로 정말 조용해지면 그때는 타임아웃이 제 몫을 한다.
		if (!session->SendSystemHeartbeatResponse(request->tick))
		{
			LOGW("session %u could not queue the heartbeat response (the send queue is likely full). "
				"keeping the connection : liveness is decided by the server's heartbeat timeout",
				session->GetSessionID());
		}

		return true;
	}

	default:
		LOGW("session %u unhandled system packet id: %u", session->GetSessionID(), packetId);
		return false;
	}
}
