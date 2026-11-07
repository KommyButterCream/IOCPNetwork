#include <cstring> // for std::memcpy
#include "IOCPClient.h"
#include "EngineExecutionScope.h"
#include "BackgroundTaskGroup.h"
#include "BackgroundTasks.h"
#include "../../Core/Concurrency/ThreadTimer.h"

#include <WinSock2.h>

#include "../Diagnostics/EngineAssert.h"
#include <MSWSock.h> // for ConnectEx
#include <ws2tcpip.h> // for inet_pton

#include "IOCPCore.h"

#include "../Job/Job.h"
#include "../Scheduler/ClientJobWorker.h"
#include "../HandlerTable/PacketHandlerTable.h"

#include "../Network/SocketOption.h"
#include "../Network/UniqueSocket.h"
#include "../Network/SocketError.h"

#include "../Memory/TlsMemoryPool.h"
#include "../Memory/EngineMemoryPoolHelper.h"
#include "../Memory/EnginePoolBuilder.h"

#include "../Protocol/PacketID.h"
#include "../Protocol/SystemPacket.h"
#include "../Job/JobHandle.h"
#include "../Session/ClientSession.h"

#include "../../Core/Util/Logger.h"

#include <utility> // for std::move
#include <mutex> // for std::unique_lock, std::lock_guard, std::try_to_lock
#include <new> // for std::bad_alloc
#include <stdexcept> // for std::length_error
#include <memory> // for std::make_unique

#pragma comment(lib, "ws2_32.lib") // for WinSock2
#pragma comment(lib, "mswsock.lib") // for AcceptEX / ConnectEx

using namespace Core::Util;

// 생성·시작·종료

IOCPClient::IOCPClient()
{
}

IOCPClient::~IOCPClient()
{
	StopClient();

	// 풀 묶음은 세션과 워커가 정리된 뒤 멤버 소멸 과정에서 파괴한다.
}

bool IOCPClient::StartClient(const char* serverIp, const uint16_t port, const SessionBufferConfig& bufferConfig, uint32_t iocpThreadCount, const EnginePoolConfig& poolConfig, const ClientLivenessConfig& livenessConfig)
{
	if (EngineExecutionScope::IsActive() || Core::Concurrency::ThreadTimer::IsCallbackThread())
	{
		LOGE("StartClient must be called outside engine callbacks, jobs and timer callbacks");
		return false;
	}
	std::unique_lock lock(m_controlMutex, std::try_to_lock);
	if (!lock.owns_lock() || m_running)
		return false;
	EngineExecutionScope execution;
	try
	{
		if (StartResources(serverIp, port, bufferConfig, iocpThreadCount, poolConfig, livenessConfig))
		{
			m_running = true;
			return true;
		}
	}
	catch (const std::bad_alloc&)
	{
		LOGE("StartClient failed to allocate resources");
	}
	catch (const std::length_error&)
	{
		LOGE("StartClient resource count exceeds container capacity");
	}
	catch (...)
	{
		StopResources();
		m_memoryPools.reset();
		throw;
	}
	StopResources();
	m_memoryPools.reset();
	return false;
}

bool IOCPClient::StartResources(const char* serverIp, const uint16_t port, const SessionBufferConfig& bufferConfig, uint32_t iocpThreadCount, const EnginePoolConfig& poolConfig, const ClientLivenessConfig& livenessConfig)
{
	m_disconnecting.store(false);

	// 앞선 주기의 풀 객체가 남아 있으면 여기서 지운다. StopClient 는 Finalize
	// 까지만 하고 객체를 남기기 때문이다. (이유는 ENGINE_POOL::EnginePools::Finalize 주석)
	m_memoryPools.reset();

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

	// 주소는 자원 생성 전에 한 번만 파싱한다.
	m_serverAddress = {};
	m_serverAddress.sin_family = AF_INET;
	m_serverAddress.sin_port = ::htons(port);
	if (!serverIp || !serverIp[0] || ::inet_pton(AF_INET, serverIp, &m_serverAddress.sin_addr) != 1)
	{
		LOGE("invalid server IPv4 address");
		return false;
	}

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
	// 2개를 유지하는 이유는 다른 것이다 — 접속·송신 완료 콜백이 워커
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

	if (IOCPCore::Start() && StartConnection(bufferConfig, poolConfig))
		return true;

	return false;
}

void IOCPClient::StopClient()
{
	if (EngineExecutionScope::IsActive() || Core::Concurrency::ThreadTimer::IsCallbackThread())
	{
		LOGE("StopClient must be requested through an external control thread");
		return;
	}
	std::lock_guard lock(m_controlMutex);
	if (!m_running)
		return;
	EngineExecutionScope execution;
	StopResources();
	m_running = false;
}

void IOCPClient::StopResources()
{
	LOGI("client shutting down");

	// 예약과 실행은 먼저 정지한다. 늦은 인증 응답이 참조할 작업 객체는 워커 조인까지 유지한다.
	if (m_backgroundTasks)
		m_backgroundTasks->StopAll();

	m_clientJobWorker.reset();

	if (m_session)
	{
		m_session->RequestDisconnect(DisconnectReason::LocalShutdown);
		// 완료를 처리할 워커가 살아 있는 동안 실제 I/O가 모두 끝날 때까지 기다린다.
		for (;;)
		{
			const auto wait = m_session->WaitForIOCancelComplete(10'000);
			if (wait.status == IOWaitStatus::Completed)
				break;
			LOGW("client shutdown waiting for session %u IO (count %ld, error %lu)",
				wait.sessionId, m_session->GetOutstandingIOCount(), wait.error);
			if (wait.status == IOWaitStatus::Failed)
				::Sleep(50);
		}
	}

	// [2] IOCP 정지. GQCS 워커 스레드를 조인한다.
	//     이 시점 이후로는 완료 통지가 발생하지 않으므로
	//     아래의 세션 및 메모리풀 해제가 안전해진다.
	IOCPCore::Stop();

	m_clientWatchdog = nullptr;
	m_backgroundTasks.reset();

	// [3] 세션 해제
	m_session.reset();

	// 세션과 Job이 반환한 뒤 저장 공간을 정리한다. 풀 객체는 재시작·파괴까지 유지한다.
	if (m_memoryPools)
		m_memoryPools->Finalize();

	// 핸들러 등록은 정지·재시작 사이에도 유지한다.

	m_serverAddress = {};

	m_connectEx = nullptr;
}

// 핸들러 등록과 패킷 생성

bool IOCPClient::RegisterPacketHandler(uint16_t id, PacketHandlerFunc handler)
{
	try { return RegisterHandlerBinding(id, PacketHandler(handler)); }
	catch (const std::bad_alloc&) { return false; }
}

bool IOCPClient::RegisterHandlerBinding(uint16_t id, PacketHandler handler)
{
	// 워커 또는 콜백에서 제어 락을 기다리지 않는다. 실행 중인 테이블은 불변이다.
	if (EngineExecutionScope::IsActive() || Core::Concurrency::ThreadTimer::IsCallbackThread())
		return false;
	std::unique_lock lock(m_controlMutex, std::try_to_lock);
	if (!lock.owns_lock() || m_running)
		return false;
	try
	{
		if (!m_packetHandlerTable)
			m_packetHandlerTable = std::make_unique<PacketHandlerTable>();
		return m_packetHandlerTable->Register(id, std::move(handler));
	}
	catch (const std::bad_alloc&)
	{
		LOGE("failed to allocate the packet handler table");
		return false;
	}
}

SendResult IOCPClient::Send(PacketHandle packet)
{
	return m_session ? m_session->Send(std::move(packet)) : SendResult::NotReady;
}

SendResult IOCPClient::Send(SharedPacketHandle packet)
{
	return m_session ? m_session->Send(std::move(packet)) : SendResult::NotReady;
}

SendResult IOCPClient::SendPacketCopy(uint16_t packetId, std::span<const std::byte> bytes)
{
	if (!m_memoryPools || !m_memoryPools->IsInitialized())
		return SendResult::NotReady;
	if (bytes.size() < sizeof(PACKET_HEADER) || !IsServicePacketId(packetId))
		return SendResult::InvalidPacket;
	if (bytes.size() > PACKET_SIZE_LIMIT)
		return SendResult::PacketTooLarge;
	auto packet = CreatePacket(bytes.size());
	if (!packet)
		return SendResult::NoMemory;
	std::memcpy(packet.MutableView().data(), bytes.data(), bytes.size());
	const PACKET_HEADER header(packetId, static_cast<uint16_t>(bytes.size()));
	std::memcpy(packet.MutableView().data(), &header, sizeof(header));
	return Send(std::move(packet));
}

PacketHandle IOCPClient::CreatePacket(std::size_t size) const
{
	return m_memoryPools ? m_memoryPools->context.CreatePacket(size) : PacketHandle{};
}

SharedPacketHandle IOCPClient::CreateSharedPacket(PacketHandle packet) const
{
	return m_memoryPools ? m_memoryPools->context.CreateSharedPacket(std::move(packet)) : SharedPacketHandle{};
}

ISession* IOCPClient::GetSession() const
{
	return m_session.get();
}

// 서비스 작업 등록과 컨텍스트

bool IOCPClient::EnqueueJob(ISession* session, uint16_t packetId, PacketHandle packetHandle)
{
	// 서비스가 전달한 세션이 이 클라이언트 소유인지 제출 지점에서 확인한다.
	if (!session || session != m_session.get())
	{
		ENGINE_VIOLATION("EnqueueJob received session %p but this client owns %p",
			static_cast<const void*>(session), static_cast<const void*>(m_session.get()));
		return false;
	}
	ClientSession* clientSession = m_session.get();

	if (!packetHandle)
	{
		return false;
	}

	if (!m_packetHandlerTable || !m_memoryPools)
	{
		ENGINE_VIOLATION("cannot submit a job : the client is not fully initialized");
		return false;
	}

	auto jobHandle = CreatePacketJob(*m_memoryPools->job, *m_packetHandlerTable,
		*clientSession, packetId, std::move(packetHandle), m_memoryPools->context);
	if (!jobHandle)
		return false;

	// 성공하면 큐가 소유하고, 실패하면 지역 핸들이 Job과 패킷을 반환한다.
	if (!clientSession->EnqueueJob(std::move(jobHandle)))
	{
		LOGE("failed to enqueue a job for packet id %u", packetId);
		return false;
	}

	return true;
}

void* IOCPClient::GetServiceContext()
{
	return this;
}

// 접속 준비와 발행

bool IOCPClient::StartConnection(const SessionBufferConfig& bufferConfig, const EnginePoolConfig& poolConfig)
{
	UniqueSocket socket(::WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED));
	if (socket.Get() == INVALID_SOCKET || !BindClientSocket(socket.Get()) || !InitializeGUIDConnectEx(socket.Get()))
		return false;

	// 메모리 풀 세 개. 구성은 서버와 같은 경로를 쓴다.
	// (사정은 IOCPServer::StartServer 의 같은 자리와 Memory/EnginePoolConfig.h 참고)
	//
	// 패킷 풀이 수신 상한을 덮는지 확인하는 검사도 그 안으로 들어갔다.
	// 예전에는 이 검사가 클라에만 있었다 — 서버는 지금 프리셋에서 우연히
	// 안전할 뿐이고, 서비스가 maxRecvPacketSize 를 올리면 조용히 힙으로 샜다.
	m_memoryPools = ENGINE_POOL::CreatePools(poolConfig, bufferConfig.maxRecvPacketSize, "client", GetServiceContext());
	if (!m_memoryPools)
		return false;

	if (!m_packetHandlerTable)
		m_packetHandlerTable = std::make_unique<PacketHandlerTable>();

	// 서버와의 통신을 위한 세션 생성
	m_session = std::make_unique<ClientSession>();
	if (!m_session->Initialize(SESSION_ROLE::CLIENT, 0))
	{
		LOGE("failed to initialize the client session");
		return false;
	}
	if (!m_session->InitializeMemoryPool(m_memoryPools->job.get(), m_memoryPools->packet.get(), bufferConfig))
		return false;
	m_session->SetOwnerCallbacks({this, &IOCPClient::OnDisconnectRequest, &IOCPClient::OnReleaseReady});

	// 세션에 소켓 설정
	if (!m_session->AttachSocket(std::move(socket)))
		return false;

	// 세션 잡큐의 Job 을 처리 하기 위한 스케쥴러/스레드 생성
	m_clientJobWorker = std::make_unique<ClientJobWorker>();

	if (!m_clientJobWorker->Initialize(m_session.get()))
	{
		LOGE("failed to initialize the client session scheduler");
		return false;
	}

	m_backgroundTasks = std::make_unique<BackgroundTaskGroup>();
	auto watchdog = std::make_unique<ClientWatchdogTask>(*m_session, m_livenessConfig);
	auto* watchdogObserver = watchdog.get();
	if (!m_backgroundTasks->Add(std::move(watchdog), ClientWatchdogTask::CheckPeriodMs))
		return false;
	m_clientWatchdog = watchdogObserver;
	if (!m_backgroundTasks->StartAll())
		return false;

	// 비동기 Connect To Server
	// 연결에 대한 통지를 GQCS 에서 처리 한다.
	if (!PostConnect(m_session.get()))
	{
		return false;
	}

	return true;
}

bool IOCPClient::BindClientSocket(SOCKET clientSocket)
{
	SOCKADDR_IN localAddr{};
	localAddr.sin_family = AF_INET;
	localAddr.sin_port = 0;
	localAddr.sin_addr.s_addr = ::htonl(INADDR_ANY);

	if (::bind(clientSocket, reinterpret_cast<const SOCKADDR*>(&localAddr), sizeof(localAddr)) == SOCKET_ERROR)
	{
		LOGE("client bind failed (error %d)", ::WSAGetLastError());

		return false;
	}

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
		LOGE("could not load ConnectEx (error %d)", ::WSAGetLastError());

		return false;
	}

	return true;
}

bool IOCPClient::PostConnect(ClientSession* clientSession)
{
	if (!clientSession || !clientSession->IsReadyToConnect())
	{
		ENGINE_VIOLATION("PostConnect requires a ready session");
		return false;
	}

	// GQCS 사용을 위한 IOCP 등록
	if (!IOCPCore::RegisterSocketToIOCP((ULONG_PTR)clientSession, clientSession->GetClientSocket()))
		return false;

	return clientSession->PostConnect(m_connectEx, m_serverAddress);
}

// I/O 완료 분배와 오류 처리

void IOCPClient::HandleCompletion(ULONG_PTR completionKey, LPOVERLAPPED overlapped, DWORD bytesTransferred, DWORD completionError)
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
			session, overlappedEx, completionError == ERROR_SUCCESS, completionError);
		return;
	}

	if (overlappedEx->operation != IO_OPERATION::CONNECT && overlappedEx->operation != IO_OPERATION::RECV &&
		overlappedEx->operation != IO_OPERATION::SEND)
	{
		ENGINE_VIOLATION("unknown client completion %d", static_cast<int>(overlappedEx->operation));
		return;
	}

	const auto completion = session->TakeIOCompletion();
	if (completionError != ERROR_SUCCESS)
	{
		HandleSocketError(session, static_cast<int>(completionError), overlappedEx->operation);

		return;
	}

	switch (overlappedEx->operation)
	{
	case IO_OPERATION::CONNECT:
		HandleConnect();
		break;
	case IO_OPERATION::RECV:
		HandleReceive(session, bytesTransferred);
		break;
	case IO_OPERATION::SEND:
		HandleSend(session, bytesTransferred);
		break;
	default:
		ENGINE_VIOLATION("completion arrived with an unknown io operation %d", static_cast<int>(overlappedEx->operation));
		break;
	}
}

void IOCPClient::HandleSocketError(ClientSession* session, int errorCode, IO_OPERATION ioOperation)
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
			HandleConnectFailure(session, errorCode);
			break;
		case ERROR_OPERATION_ABORTED:
			HandleConnectFailure(session, errorCode);
			break;
		default:
			ENGINE_VIOLATION("session %u unhandled connect error %d, treating it as a cancellation", session->GetSessionID(), errorCode);
			HandleConnectFailure(session, errorCode);
			break;
		}
		return;
	}
	if (ioOperation == IO_OPERATION::RECV || ioOperation == IO_OPERATION::SEND)
	{
		if (errorCode == ERROR_OPERATION_ABORTED)
			return;
		if (!IsConnectionClosedError(errorCode))
			ENGINE_VIOLATION("session %u unexpected io error %d", session->GetSessionID(), errorCode);
		session->RequestDisconnect(DisconnectReason::SocketError);
		return;
	}
	ENGINE_VIOLATION("unknown io operation %d (error %d)", static_cast<int>(ioOperation), errorCode);
}

// 접속 완료와 실패

void IOCPClient::HandleConnect()
{
	if (!RunClientConnectSequence())
		m_session->RequestDisconnect(DisconnectReason::SocketError);
}

bool IOCPClient::RunClientConnectSequence()
{
	const SOCKET socket = m_session->GetClientSocket();
	if (!SocketOption::SetClientContext(socket))
	{
		LOGE("session %u SO_UPDATE_CONNECT_CONTEXT failed, dropping the connection", m_session->GetSessionID());
		return false;
	}

	if (!SocketOption::SetNoDelay(socket))
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
		if (!SocketOption::SetKeepAliveEx(socket,
			m_livenessConfig.keepAliveTime_ms, m_livenessConfig.keepAliveInterval_ms))
		{
			LOGW("session %u could not set TCP keepalive. the connection stays up, but a "
				"broken path will only be noticed by the idle timeout", m_session->GetSessionID());
		}
	}

	m_session->OnConnect();

	// 인증 응답보다 서비스의 접속 통지가 먼저 도착해야 한다.
	if (!InvokeServiceCallback("OnClientConnect", [&] { OnClientConnect(m_session.get()); }))
	{
		m_session->RequestDisconnect(DisconnectReason::ServiceError);
		return false;
	}

	// 접속 콜백이 만든 컨텍스트를 수신 워커에 공개한 뒤 첫 수신을 건다.
	m_session->ContinueReceive();
	if (!SendSystemAuthRequest(m_session.get()))
	{
		LOGE("session %u system auth request send failed", m_session->GetSessionID());
		return false;
	}

	return true;
}

void IOCPClient::HandleConnectFailure(ClientSession* clientSession, int errorCode)
{
	LOGW("ConnectEx ended with error %d", errorCode);

	if (!clientSession)
	{
		ENGINE_VIOLATION("connect cancellation arrived with no session");
		return;
	}

	if (errorCode != ERROR_OPERATION_ABORTED)
	{
		// 서비스 콜백이 다른 종료를 요청해도 접속 실패 사유가 먼저 보존된다.
		clientSession->RequestDisconnect(DisconnectReason::ConnectFailed);
		(void)InvokeServiceCallback("OnConnectFailed", [&] { OnConnectFailed(errorCode); });
	}
	else
		clientSession->RequestDisconnect(DisconnectReason::LocalShutdown);
}

// 송수신 완료와 패킷 분배

void IOCPClient::HandleReceive(ClientSession* session, DWORD bytesTransferred)
{
	if (!session->OnReceiveCompleted(bytesTransferred))
		return;

	for (;;)
	{
		auto result = session->ReadReceivedPacket();
		if (result.status == ClientSession::ReceiveStatus::Stopped)
			return;
		if (result.status == ClientSession::ReceiveStatus::NeedMoreData)
			break;
		if (!HandleReceivedPacket(*session, result.packetId, std::move(result.packet)))
			return;
	}
	session->ContinueReceive();
}

bool IOCPClient::HandleReceivedPacket(ClientSession& session, uint16_t packetId, PacketHandle&& packet)
{
	if (IsServicePacketId(packetId))
	{
		// 서비스 패킷은 등록된 핸들러의 Job으로 한 번만 전달합니다.
		if (EnqueueJob(&session, packetId, std::move(packet)))
			return true;
		session.RequestDisconnect(DisconnectReason::ServiceError);
		return false;
	}

	const auto reason = HandleSystemPacket(session, packetId, packet.View());
	if (reason == DisconnectReason::Unknown)
		return true;
	session.RequestDisconnect(reason);
	return false;
}

void IOCPClient::HandleSend(ClientSession* session, DWORD bytesTransferred)
{
	ENGINE_CHECK_RETVOID(session->GetSessionRole() == SESSION_ROLE::CLIENT,
		"unexpected session role for send completion on session %u", session->GetSessionID());
	// 콜백 실패 시 새 송신을 막되, 이번 완료의 송신 상태와 패킷은 반드시 정리한다.
	if (!InvokeServiceCallback("OnSend", [&] { OnSend(session, bytesTransferred); }))
		session->RequestDisconnect(DisconnectReason::ServiceError);
	session->OnSendCompleted(bytesTransferred);
}

// 인증과 하트비트

DisconnectReason IOCPClient::HandleSystemPacket(ClientSession& session, uint16_t packetId, std::span<const std::byte> packet)
{
	if (packet.empty())
	{
		return DisconnectReason::ProtocolError;
	}

	switch (static_cast<PACKET_ID>(packetId))
	{
	case PACKET_ID::SC_SYSTEM_AUTH_RESPONSE:
		return HandleAuthResponse(session, packet);

	case PACKET_ID::SC_SYSTEM_HEARTBEAT_REQUEST:
		return HandleHeartbeatRequest(session, packet);

	default:
		LOGW("session %u unhandled system packet id: %u", session.GetSessionID(), packetId);
		return DisconnectReason::ProtocolError;
	}
}

bool IOCPClient::SendSystemAuthRequest(ClientSession* session)
{
	if (!session || !session->IsTransportConnected())
	{
		return false;
	}

	// 인증에 사용하는 패킷 풀은 엔진이 소유합니다.

	if (!m_memoryPools)
	{
		return false;
	}

	auto packetHandle = MEMORY_POOL::CreatePacketHandle<CS_SYSTEM_AUTH_REQUEST_PACKET>(*m_memoryPools->packet);

	if (!packetHandle)
	{
		return false;
	}

	// 송신 직후 다른 IOCP 워커가 인증 응답을 처리할 수 있다.
	// 응답 처리가 설정한 상태를 덮어쓰지 않도록 송신 전에 변경한다.
	if (!session->BeginAuthentication())
		return false;

	return session->EnqueueSendPacket(std::move(packetHandle));
}

DisconnectReason IOCPClient::HandleAuthResponse(ClientSession& session, std::span<const std::byte> packet)
{
	if (packet.size() != sizeof(SC_SYSTEM_AUTH_RESPONSE_PACKET))
	{
		return DisconnectReason::ProtocolError;
	}

	const SC_SYSTEM_AUTH_RESPONSE_PACKET* response = reinterpret_cast<const SC_SYSTEM_AUTH_RESPONSE_PACKET*>(packet.data());
	const SYSTEM_AUTH_RESULT authResult = static_cast<SYSTEM_AUTH_RESULT>(response->authResult);

	if (response->protocolVersion != IOCP_ENGINE_PROTOCOL_VERSION)
	{
		LOGE("session %u protocol version mismatch (server=%u, client=%u)", session.GetSessionID(), response->protocolVersion, IOCP_ENGINE_PROTOCOL_VERSION);
		return DisconnectReason::AuthRejectedProtocolMismatch;
	}

	if (authResult != SYSTEM_AUTH_RESULT::SUCCESS)
	{
		LOGW("session %u auth rejected by server (result=%u)", session.GetSessionID(), response->authResult);

		// 서버가 준 거절 사유를 그대로 서비스까지 올린다.
		//
		// SERVER_FULL 을 따로 만든 이유가 "RST 만 보면 만석과 장애를
		// 구분할 수 없다" 였는데, 정작 그 값이 여기서 로그로만 남고
		// 끝나 서비스는 재접속 여부를 판단할 수 없었다.
		return ToDisconnectReason(authResult);
	}

	if (!session.Establish())
		return DisconnectReason::ProtocolError;
	LOGI("session %u session established", session.GetSessionID());

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

	if (!InvokeServiceCallback("OnSessionEstablished", [&] { OnSessionEstablished(&session); }))
		return DisconnectReason::ServiceError;
	return DisconnectReason::Unknown;
}

DisconnectReason IOCPClient::HandleHeartbeatRequest(ClientSession& session, std::span<const std::byte> packet)
{
	if (packet.size() != sizeof(SC_SYSTEM_HEARTBEAT_REQUEST_PACKET))
	{
		return DisconnectReason::ProtocolError;
	}

	if (!session.IsEstablished())
	{
		return DisconnectReason::ProtocolError;
	}

	const SC_SYSTEM_HEARTBEAT_REQUEST_PACKET* request = reinterpret_cast<const SC_SYSTEM_HEARTBEAT_REQUEST_PACKET*>(packet.data());

	if (!session.SendSystemHeartbeatResponse(request->tick))
	{
		LOGW("session %u could not queue the heartbeat response (the send queue is likely full). "
			"keeping the connection : liveness is decided by the server's heartbeat timeout",
			session.GetSessionID());
	}

	return DisconnectReason::Unknown;
}

// 세션 종료 요청과 반환 통지

void IOCPClient::OnDisconnectRequest(void* context, ClientSession* session)
{
	auto* client = static_cast<IOCPClient*>(context);
	if (client->m_disconnecting.exchange(true))
		return;
	session->CloseWhenIdle();
}

void IOCPClient::OnReleaseReady(void* context, ClientSession* session)
{
	EngineExecutionScope execution;
	session->FinishDisconnect(&IOCPCore::CloseSocketHandle,
		[](void* owner, ClientSession* ended, DisconnectReason reason)
		{
			(void)InvokeServiceCallback("OnClientDisconnect", [&] { static_cast<IOCPClient*>(owner)->OnClientDisconnect(ended, reason); });
		}, context);
}
