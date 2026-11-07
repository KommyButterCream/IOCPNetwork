#include <cstring> // for std::memcpy
#include "IOCPServer.h"
#include "EngineExecutionScope.h"
#include "BackgroundTaskGroup.h"
#include "BackgroundTasks.h"
#include "../../Core/Concurrency/ThreadTimer.h"

#include "../Diagnostics/EngineAssert.h"

#include <WinSock2.h>
#include <MSWSock.h> // for AcceptEX
#include <Ws2tcpip.h> // for inet_ntop

#include <stdio.h>
#include <stdlib.h>

#include "IOCPCore.h"

#include "../Job/Job.h"
#include "../Scheduler/ServerReadySessionQueue.h"
#include "../Scheduler/ServerJobScheduler.h"
#include "../HandlerTable/PacketHandlerTable.h"

#include "../Memory/TlsMemoryPool.h"
#include "../Memory/EngineMemoryPoolHelper.h"
#include "../Memory/EnginePoolBuilder.h"

#include "../Network/SocketOption.h"
#include "../Network/UniqueSocket.h"
#include "../Network/SocketError.h"

#include "../Protocol/PacketID.h"
#include "../Protocol/SystemPacket.h"

#include "../Session/ISession.h"
#include "../Session/SessionManager.h"
#include "../Session/ClientSession.h"
#include "../Session/AcceptSession.h"
#include "../Job/JobHandle.h"
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

IOCPServer::IOCPServer()
{
}

IOCPServer::~IOCPServer()
{
	StopServer();

	// 풀 묶음은 세션과 워커가 정리된 뒤 멤버 소멸 과정에서 파괴한다.
}

bool IOCPServer::StartServer(const char* ipAddress, const uint16_t port, const uint32_t maxConnectionCount, const SessionBufferConfig& bufferConfig, const ConnectionPolicyConfig& policyConfig, const EnginePoolConfig& poolConfig, const HeartbeatConfig& heartbeatConfig)
{
	if (EngineExecutionScope::IsActive() || Core::Concurrency::ThreadTimer::IsCallbackThread())
	{
		LOGE("StartServer must be called outside engine callbacks, jobs and timer callbacks");
		return false;
	}
	std::unique_lock lock(m_controlMutex, std::try_to_lock);
	if (!lock.owns_lock() || m_running)
		return false;
	EngineExecutionScope execution;
	try
	{
		if (StartResources(ipAddress, port, maxConnectionCount, bufferConfig, policyConfig, poolConfig, heartbeatConfig))
		{
			m_running = true;
			return true;
		}
	}
	catch (const std::bad_alloc&)
	{
		LOGE("StartServer failed to allocate resources");
	}
	catch (const std::length_error&)
	{
		LOGE("StartServer resource count exceeds container capacity");
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

bool IOCPServer::StartResources(const char* ipAddress, const uint16_t port, const uint32_t maxConnectionCount, const SessionBufferConfig& bufferConfig, const ConnectionPolicyConfig& policyConfig, const EnginePoolConfig& poolConfig, const HeartbeatConfig& heartbeatConfig)
{
	// 설정 오류는 아무것도 잡기 전에 걸러낸다. 세션 생성 단계까지 끌고 가면
	// 세션을 하나도 못 잡는 서버가 정상 기동한 것처럼 보인다.
	if (maxConnectionCount == 0 || !bufferConfig.IsValid())
	{
		LOGE("invalid session buffer config : recv %u / ring %u, send %u, queue %u",
			bufferConfig.maxRecvPacketSize, bufferConfig.recvRingSize,
			bufferConfig.maxSendPacketSize, bufferConfig.sendQueueDepth);
		return false;
	}

	// serviceCapacity 가 세션 풀보다 크면 아무 효과가 없다. 그런 설정은
	// "제한을 걸었다" 고 믿게 만들므로 기동을 실패시킨다.
	if (policyConfig.serviceCapacity > maxConnectionCount)
	{
		LOGE("service capacity %u exceeds the session pool capacity %u",
			policyConfig.serviceCapacity, maxConnectionCount);
		return false;
	}

	if (policyConfig.acceptRetryInterval_ms == 0)
	{
		LOGE("accept retry interval must be greater than zero");
		return false;
	}

	// 하트비트도 다른 설정과 같은 자리에서 걸러낸다. 타임아웃이 점검 주기보다
	// 짧으면 첫 주기에 전 세션이 좀비로 판정된다.
	if (!heartbeatConfig.IsValid())
	{
		LOGE("invalid heartbeat config : interval %llu ms, timeout %llu ms, stalled peer timeout %llu ms",
			heartbeatConfig.checkInterval_ms, heartbeatConfig.timeout_ms,
			heartbeatConfig.stalledPeerTimeout_ms);
		return false;
	}

	m_connectionPolicy = policyConfig;
	m_heartbeatConfig = heartbeatConfig;

	// 지난 주기의 한 번짜리 래치를 되돌린다.
	//
	// StopServer 가 올리기만 하고 내리지 않는 값들이다. 그대로 두면 다시
	// 기동한 서버가 첫 AcceptEx 부터 "종료 중" 으로 판단해 접속을 하나도
	// 받지 못한다. 기동이 한 번 실패해 다시 시도하는 서비스가 이 경로를 밟는다.
	m_serverShutdownRequested.store(false);
	m_acceptStarvationReported.store(false);
	m_postedAcceptCount.store(0);

	LOGI("connection policy : service capacity %u (0 = pool capacity %u), max per address %u (0 = unlimited)",
		m_connectionPolicy.serviceCapacity, maxConnectionCount, m_connectionPolicy.maxConnectionsPerAddress);

	const DWORD processorCount = ::GetMaximumProcessorCount(ALL_PROCESSOR_GROUPS);
	SetIOCPThreadCount(processorCount ? processorCount : 1);

	if (!IOCPCore::Start())
	{
		return false;
	}

	if (!CreateListenSocket())
	{
		return false;
	}

	if (!BindServerSocket(m_serverSocket.Get(), ipAddress, port))
	{
		return false;
	}

	if (!ListenServerSocket(m_serverSocket.Get()))
	{
		return false;
	}

	if (!InitializeGUIDAcceptEx(m_serverSocket.Get()))
	{
		return false;
	}

	// 메모리 풀 세 개. 빈 목록과 커밋 상한은 전부 poolConfig 가 정한다.
	//
	// 예전에는 이 자리에 빈 목록이 상수로 박혀 있었다. 담기는 것이 서비스의
	// 데이터인데 크기 분포를 엔진이 정하고 있었던 셈이고, 서비스는 손댈
	// 방법이 없었다. (사정과 기본값은 Memory/EnginePoolConfig.h 참고)
	//
	// 만들기는 전부 성공하거나 전부 지워진다. 예전에는 실패 지점마다 이미
	// 만든 풀이 그대로 남은 채 false 만 돌아갔다.

	// 앞선 주기의 풀 객체가 남아 있으면 여기서 지운다. StopServer 는 Finalize
	// 까지만 하고 객체를 남기기 때문이다. (이유는 ENGINE_POOL::EnginePools::Finalize 주석)
	m_memoryPools.reset();

	m_memoryPools = ENGINE_POOL::CreatePools(poolConfig, bufferConfig.maxRecvPacketSize, "server", GetServiceContext());
	if (!m_memoryPools)
		return false;

	m_serverReadySessionQueue = std::make_unique<ServerReadySessionQueue>();

	const uint32_t sessionQueueBufferCount = maxConnectionCount;
	if (!m_serverReadySessionQueue->Initialize(sessionQueueBufferCount))
	{
		LOGE("failed to initialize the ready session queue");
		return false;
	}

	if (!m_packetHandlerTable)
		m_packetHandlerTable = std::make_unique<PacketHandlerTable>();

	m_serverJobScheduler = std::make_unique<ServerJobScheduler>();

	uint32_t sessionProcessorThreadCount = ::GetMaximumProcessorCount(ALL_PROCESSOR_GROUPS);
	if (sessionProcessorThreadCount == 0)
		sessionProcessorThreadCount = ::GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
	if (sessionProcessorThreadCount == 0)
		sessionProcessorThreadCount = 1;
	if (!m_serverJobScheduler->Initialize(sessionProcessorThreadCount, m_serverReadySessionQueue.get()))
	{
		LOGE("failed to initialize the ready session scheduler");
		return false;
	}

	m_sessionManager = std::make_unique<SessionManager>();

	constexpr uint32_t PreferredAcceptSessionCount = 16;

	uint32_t acceptSessionCount = maxConnectionCount < PreferredAcceptSessionCount
		? maxConnectionCount
		: PreferredAcceptSessionCount;

	if (acceptSessionCount == 0)
		acceptSessionCount = 1;

	m_desiredAcceptCount = acceptSessionCount;

	LOGI("accept sessions %u (max connections %u)", acceptSessionCount, maxConnectionCount);

	if (!m_sessionManager->Initialize(
		acceptSessionCount,
		maxConnectionCount,
		m_memoryPools->job.get(),
		m_memoryPools->packet.get(),
		IOCPCore::CloseSocketHandle,
		bufferConfig,
		&IOCPServer::OnSessionDisconnectNotify,
		this))
		return false;

	{ std::unique_lock lock(m_apiMutex); m_apiOpen = true; }
	if (!PrepareAccept())
	{
		return false;
	}

	// 의존 자원이 모두 준비된 뒤 작업을 구성하고 마지막에 실행을 허용한다.
	m_backgroundTasks = std::make_unique<BackgroundTaskGroup>();
	const auto heartbeatPeriod = static_cast<uint32_t>(heartbeatConfig.checkInterval_ms < 50 ? 50 : heartbeatConfig.checkInterval_ms);
	if (!m_backgroundTasks->Add(std::make_unique<ServerHeartbeatTask>(*m_sessionManager, heartbeatConfig), heartbeatPeriod) ||
		!m_backgroundTasks->Add(std::make_unique<AcceptRecoveryTask>(*this), policyConfig.acceptRetryInterval_ms) ||
		!m_backgroundTasks->StartAll())
		return false;

	return true;
}

void IOCPServer::StopServer()
{
	if (EngineExecutionScope::IsActive() || Core::Concurrency::ThreadTimer::IsCallbackThread())
	{
		LOGE("StopServer must be requested through an external control thread");
		return;
	}
	std::lock_guard lock(m_controlMutex);
	if (!m_running)
		return;
	EngineExecutionScope execution;
	StopResources();
	m_running = false;
}

void IOCPServer::StopResources()
{
	LOGI("server shutdown requested");
	{ std::unique_lock lock(m_apiMutex); m_apiOpen = false; }

	// 서버가 닫히기 전에 AcceptIO 가 취소되기 전에 그 찰나에 받아진
	// Accept IO 에 대한 예외 처리를 위해 플래그 추가.
	m_serverShutdownRequested.store(true);

	if (m_backgroundTasks)
	{
		m_backgroundTasks->StopAll();
		m_backgroundTasks.reset();
	}

	if (m_sessionManager)
	{
		m_sessionManager->CancelAllAcceptIORequests();
		if (!CancelAllAcceptIO())
			ENGINE_VIOLATION("failed to cancel outstanding AcceptEx on the listen socket");

		m_sessionManager->DisconnectAllSessions();
		m_sessionManager->RequestAllRecvSendIOCancel();

		// 10초는 진단 주기다. 완료가 늦어져도 워커와 세션 풀은 유지한다.
		constexpr DWORD diagnosticIntervalMs = 10'000;
		const ULONGLONG started = ::GetTickCount64();
		ULONGLONG nextDiagnostic = started + diagnosticIntervalMs;
		auto waitForCompletion = [&](const char* operation, auto wait)
		{
			bool reportedError = false;
			for (;;)
			{
				const ULONGLONG now = ::GetTickCount64();
				const DWORD remaining = now < nextDiagnostic ? static_cast<DWORD>(nextDiagnostic - now) : 0;
				const auto result = (m_sessionManager.get()->*wait)(remaining);
				if (result.status == IOWaitStatus::Completed)
					return;

				const ULONGLONG checked = ::GetTickCount64();
				const bool failed = result.status == IOWaitStatus::Failed;
				if (checked >= nextDiagnostic || (failed && !reportedError))
				{
					LOGW("shutdown waiting for %s: session=%u wait=%s error=%u outstanding=%u elapsed=%llu ms",
						operation, result.sessionId, failed ? "failed" : "timeout", result.error,
						m_sessionManager->GetOutstandingIOCount(), checked - started);
					nextDiagnostic = checked + diagnosticIntervalMs;
					reportedError = failed;
				}

				// 이벤트 오류 시에도 실제 카운트가 0이 되면 다음 검사에서 통과한다.
				if (failed)
					::Sleep(50);
			}
		};

		waitForCompletion("AcceptEx", &SessionManager::WaitForAllAcceptIOComplete);

		// 첫 취소 순회와 겹친 Accept 완료가 연결을 인계했을 수 있다.
		m_sessionManager->DisconnectAllSessions();
		m_sessionManager->RequestAllRecvSendIOCancel();
		waitForCompletion("recv/send", &SessionManager::WaitForAllRecvSendIOCancelComplete);
	}

	// I/O 개수가 0이어도 완료 함수의 마지막 반환 처리가 남아 있을 수 있습니다.
	// 생산자인 완료 워커를 먼저 조인하되, 소켓 정리를 위해 Winsock은 유지합니다.
	StopCompletionWorkers();

	DestroyListenSocket();
	m_acceptEx = nullptr;
	m_getAcceptExSockAddrs = nullptr;

	// I/O 생산자가 멈춘 뒤에도 Job 워커를 유지하여 참조와 종료 콜백을 모두 반환합니다.
	if (m_sessionManager)
	{
		m_sessionManager->DisconnectAllSessions();
		auto nextDiagnostic = ::GetTickCount64() + 10'000;
		while (m_sessionManager->GetClientSessionInUseCount() != 0)
		{
			if (::GetTickCount64() >= nextDiagnostic)
			{
				LOGW("shutdown waiting for service jobs and connection references");
				nextDiagnostic += 10'000;
			}
			::Sleep(1);
		}
	}

	// 실행 중인 Job이 끝난 뒤에만 준비 큐와 세션을 정리합니다.
	m_serverJobScheduler.reset();

	// 준비 큐에 남은 참조도 세션 풀이 살아 있을 때 반환합니다.
	if (m_serverReadySessionQueue)
	{
		m_serverReadySessionQueue->Finalize();
	}

	if (m_sessionManager)
	{
		m_sessionManager->DisconnectAllSessions();
	}

	LOGI("shutting down the IOCP core");
	IOCPCore::Stop();

	m_sessionManager.reset();

	// 준비 큐는 스케줄러보다 뒤에 지운다.
	// ServerJobScheduler::Finalize 가 워커를 깨우려고 이 큐의 WakeAll 을
	// 부르므로, 조인이 끝날 때까지 살아 있어야 한다.
	m_serverReadySessionQueue.reset();

	// 핸들러 등록은 정지·재시작 사이에도 유지한다.

	// 세션과 Job이 반환한 뒤 저장 공간을 정리한다. 풀 객체는 재시작·파괴까지 유지한다.
	if (m_memoryPools)
		m_memoryPools->Finalize();

	Logger::Flush();
}

// 핸들러 등록과 패킷 생성

bool IOCPServer::RegisterPacketHandler(uint16_t id, PacketHandlerFunc handler)
{
	try { return RegisterHandlerBinding(id, PacketHandler(handler)); }
	catch (const std::bad_alloc&) { return false; }
}

bool IOCPServer::RegisterHandlerBinding(uint16_t id, PacketHandler handler)
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

ClientSession* IOCPServer::AcquireConnection(ConnectionId id) const
{
	std::shared_lock lock(m_apiMutex);
	if (!m_apiOpen || !id || !m_sessionManager)
		return nullptr;
	return m_sessionManager->AcquireConnection(id).release();
}

SendResult IOCPServer::Send(ConnectionId id, PacketHandle packet)
{
	ClientSession::Reference session(AcquireConnection(id));
	return session ? session->Send(std::move(packet)) : SendResult::Closed;
}

SendResult IOCPServer::Send(ConnectionId id, SharedPacketHandle packet)
{
	ClientSession::Reference session(AcquireConnection(id));
	return session ? session->Send(std::move(packet)) : SendResult::Closed;
}

SendResult IOCPServer::SendPacketCopy(ConnectionId id, uint16_t packetId, std::span<const std::byte> bytes)
{
	ClientSession::Reference session(AcquireConnection(id));
	return session ? session->SendCopy(packetId, bytes) : SendResult::Closed;
}

bool IOCPServer::RequestDisconnect(ConnectionId id, DisconnectReason reason)
{
	ClientSession::Reference session(AcquireConnection(id));
	if (!session)
		return false;
	session->RequestDisconnect(reason);
	return true;
}

bool IOCPServer::Post(ConnectionId id, SessionTask task)
{
	ClientSession::Reference session(AcquireConnection(id));
	return session && session->IsServiceReady() && EnqueueServiceEvent(*session, ServiceEvent::Task, std::move(task));
}

bool IOCPServer::EnqueueServiceEvent(ClientSession& session, ServiceEvent event, SessionTask task)
{
	auto job = MEMORY_POOL::CreateJobHandle(*m_memoryPools->job);
	if (!job || !job->InitializeServiceEvent(session, event, std::move(task)))
		return false;
	return m_serverJobScheduler->EnqueueJob(session, std::move(job));
}

PacketHandle IOCPServer::CreatePacket(std::size_t size) const
{
	return m_memoryPools ? m_memoryPools->context.CreatePacket(size) : PacketHandle{};
}

SharedPacketHandle IOCPServer::CreateSharedPacket(PacketHandle packet) const
{
	return m_memoryPools ? m_memoryPools->context.CreateSharedPacket(std::move(packet)) : SharedPacketHandle{};
}

// 서비스 작업 등록과 컨텍스트

bool IOCPServer::EnqueueJob(ISession* session, uint16_t packetId, PacketHandle packetHandle)
{
	// recv 완료 키에 등록되는 것은 ClientSession 뿐이므로 이 경로의 세션은
	// 항상 ClientSession 이다.
	ClientSession* clientSession = static_cast<ClientSession*>(session);

	if (!clientSession || !packetHandle)
	{
		ENGINE_VIOLATION("EnqueueJob called with session %p packet %p",
			static_cast<const void*>(session), static_cast<const void*>(packetHandle.View().data()));
		return false;
	}

	if (!m_packetHandlerTable || !m_memoryPools || !m_serverJobScheduler)
	{
		ENGINE_VIOLATION("session %u cannot submit a job : the server is not fully initialized",
			clientSession->GetSessionID());
		return false;
	}

	auto jobHandle = CreatePacketJob(*m_memoryPools->job, *m_packetHandlerTable,
		*clientSession, packetId, std::move(packetHandle), m_memoryPools->context);
	if (!jobHandle)
		return false;

	// 성공하면 큐가 소유하고, 실패하면 지역 핸들이 Job과 패킷을 반환한다.
	return m_serverJobScheduler->EnqueueJob(*clientSession, std::move(jobHandle));
}

void* IOCPServer::GetServiceContext()
{
	return this;
}

// 리슨 소켓 준비와 해제

bool IOCPServer::CreateListenSocket()
{
	m_serverSocket.Reset(::WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED));

	if (m_serverSocket.Get() == INVALID_SOCKET)
	{
		LOGE("listen socket creation failed (error %d)", ::WSAGetLastError());
		return false;
	}

	// 수락 완료는 overlapped의 세션 ID로 분배하므로 완료 키는 0을 사용한다.
	if (!RegisterSocketToIOCP(0, m_serverSocket.Get()))
		return false;

	return true;
}

void IOCPServer::DestroyListenSocket()
{
	IOCPCore::CloseSocketHandle(m_serverSocket.Release());
}

bool IOCPServer::BindServerSocket(SOCKET serverSocket, const char* ipAddress, uint16_t port)
{
	SOCKADDR_IN serverAddr = {};
	serverAddr.sin_family = AF_INET;
	serverAddr.sin_port = ::htons(port);

	if (ipAddress == nullptr || ipAddress[0] == '\0')
	{
		serverAddr.sin_addr.s_addr = ::htonl(INADDR_ANY);
	}
	else
	{
		const int nResult = ::inet_pton(AF_INET, ipAddress, &serverAddr.sin_addr);
		if (nResult != 1)
		{
			LOGE("invalid bind address : %s", ipAddress);
			return false;
		}
	}

	if (::bind(serverSocket, reinterpret_cast<const SOCKADDR*>(&serverAddr), sizeof(serverAddr)) == SOCKET_ERROR)
	{
		LOGE("server bind failed (error %d)", ::WSAGetLastError());

		return false;
	}

	return true;
}

bool IOCPServer::ListenServerSocket(SOCKET serverSocket)
{
	if (::listen(serverSocket, SOMAXCONN) == SOCKET_ERROR)
	{
		LOGE("listen socket setup failed (error %d)", ::WSAGetLastError());

		return false;
	}

	return true;
}

bool IOCPServer::InitializeGUIDAcceptEx(SOCKET serverSocket)
{
	// AcceptEX 를 사용하기 위해 함수 포인터를 얻어오는 함수
	// WSAIoctl 를 사용하여 AcceptEx 함수 포인터를 얻어올 수 있다.

	DWORD bytes = 0;

	// AcceptEX 함수 포인터 얻기
	GUID GuidAcceptEx = WSAID_ACCEPTEX;
	if (::WSAIoctl(serverSocket,
		SIO_GET_EXTENSION_FUNCTION_POINTER,
		&GuidAcceptEx,
		sizeof(GuidAcceptEx),
		&m_acceptEx,
		sizeof(m_acceptEx),
		&bytes,
		nullptr,
		nullptr) == SOCKET_ERROR)
	{
		LOGE("could not load AcceptEx (error %d)", ::WSAGetLastError());

		return false;
	}

	// GetAcceptExSockaddrs 함수 포인터 얻기
	GUID GuidGetAddrs = WSAID_GETACCEPTEXSOCKADDRS;
	if (::WSAIoctl(serverSocket, SIO_GET_EXTENSION_FUNCTION_POINTER,
		&GuidGetAddrs, sizeof(GuidGetAddrs),
		&m_getAcceptExSockAddrs, sizeof(m_getAcceptExSockAddrs),
		&bytes, nullptr, nullptr) == SOCKET_ERROR)
	{
		LOGE("could not load GetAcceptExSockaddrs (error %d)", ::WSAGetLastError());

		return false;
	}

	return true;
}

// 수락 I/O 발행과 유지

bool IOCPServer::PrepareAccept()
{
	// Listner Session Post Accept 걸어 주기
	const uint32_t maxAcceptSessionCount = m_sessionManager->GetAcceptSessionCount();

	for (uint32_t i = 0; i < maxAcceptSessionCount; i++)
	{
		AcceptSession* session = m_sessionManager->GetAcceptSession(i);

		if (!session)
		{
			LOGE("could not get the accept session from the pool");
			ENGINE_BREAK_IF_DEBUGGER();
			return false;
		}

		// 기동 시점이라 경쟁자는 없지만, 세션 소유권을 얻고 거는 규칙은
		// 발행부 전체에 예외 없이 적용한다.
		if (!session->TryAcquireSession())
		{
			ENGINE_VIOLATION("accept session %u is already owned before the first post", i);
			return false;
		}

		if (!PostAccept(session))
		{
			session->ReleaseSession();
			LOGE("PostAccept failed for the accept session");
			ENGINE_BREAK_IF_DEBUGGER();
			return false;
		}
	}

	return true;
}

bool IOCPServer::PostAccept(AcceptSession* acceptSession)
{
	if (!acceptSession)
	{
		LOGE("PostAccept was given no accept session");

		return false;
	}

	LOGT("accept session %u posting AcceptEx", acceptSession->GetSessionID());

	if (m_serverShutdownRequested.load(std::memory_order_acquire))
	{
		return false;
	}

	const SOCKET socket = ::WSASocket(AF_INET, SOCK_STREAM, 0, nullptr, 0, WSA_FLAG_OVERLAPPED);
	if (socket == INVALID_SOCKET)
	{
		LOGE("accept session %u WSASocket failed (error %d)", acceptSession->GetSessionID(), ::WSAGetLastError());
		return false;
	}

	// 즉시 완료가 먼저 처리돼도 발행 카운트가 음수가 되지 않도록 미리 올린다.
	m_postedAcceptCount.fetch_add(1);
	const int error = acceptSession->PostAcceptSocket(m_serverSocket.Get(), socket, m_acceptEx);
	if (error != 0)
	{
		m_postedAcceptCount.fetch_sub(1);
		IOCPCore::CloseSocketHandle(socket);
		if (error != WSA_OPERATION_ABORTED)
			LOGW("accept session %u could not post AcceptEx (error %d)", acceptSession->GetSessionID(), error);
		return false;
	}
	return true;
}

void IOCPServer::RepostAfterAccept(AcceptSession& session)
{
	if (!PostAccept(&session))
	{
		session.ReleaseSession();
		if (!m_serverShutdownRequested.load(std::memory_order_acquire))
			LOGW("could not repost accept session %u", session.GetSessionID());
	}
	ReportAcceptStarvationIfNeeded();
}

void IOCPServer::RefillAcceptSessions()
{
	if (!m_sessionManager)
		return;

	if (m_serverShutdownRequested.load(std::memory_order_acquire))
		return;

	const uint32_t acceptSessionCount = m_sessionManager->GetAcceptSessionCount();
	uint32_t refilled = 0;

	for (uint32_t i = 0; i < acceptSessionCount; ++i)
	{
		AcceptSession* acceptSession = m_sessionManager->GetAcceptSession(i);
		if (!acceptSession)
			continue;

		// 완료 워커와 주기 점검 중 하나만 다음 AcceptEx를 발행한다.
		if (!acceptSession->TryAcquireSession())
			continue;

		if (!PostAccept(acceptSession))
		{
			// 아직 자원이 부족하다. 세션을 놓고 다음 주기에 다시 시도한다.
			acceptSession->ReleaseSession();
			break;
		}

		++refilled;
	}

	if (refilled > 0)
	{
		LOGI("refilled %u accept session(s), now %u of %u posted",
			refilled, GetPostedAcceptCount(), m_desiredAcceptCount);

		// 다시 채워졌으므로 다음 고갈 때 또 울릴 수 있도록 래치를 푼다.
		m_acceptStarvationReported.store(false);
	}
}

bool IOCPServer::CancelAllAcceptIO()
{
	if (m_serverSocket.Get() == INVALID_SOCKET)
	{
		return true;
	}

	// lpOverlapped 가 null 이면
	// 모든 IO 작업을 취소시킨다.
	if (!::CancelIoEx(reinterpret_cast<HANDLE>(m_serverSocket.Get()), nullptr))
	{
		DWORD error = ::GetLastError();

		if (error == ERROR_NOT_FOUND)
		{
			// 미완료 I/O 가 없어서 취소할 것이 없는 경우
			// 아무것도 하지 않고 빠져나가면 된다.
			// 정상
			return true;
		}
		else if (error == ERROR_INVALID_HANDLE)
		{
			// 이미 닫혔거나 유효하지 않은 핸들인 경우
			// 무시 가능
			return true;
		}
		else if (error == ERROR_OPERATION_ABORTED)
		{
			// I/O 가 이미 완료 직전에 있어서
			// 취소되지 못한 경우 (드물다)
			return false;
		}
		else
		{
			return false;
		}
	}

	return true;
}

void IOCPServer::ReportAcceptStarvationIfNeeded()
{
	if (m_serverShutdownRequested.load(std::memory_order_acquire))
		return;

	if (m_postedAcceptCount.load(std::memory_order_acquire) != 0)
		return;

	// 같은 고갈에 대해 한 번만 울린다. RefillAcceptSessions 가 성공하면 풀린다.
	if (m_acceptStarvationReported.exchange(true))
		return;

	ENGINE_VIOLATION("no AcceptEx is posted. the server is running but will not take any new connection until an accept session is refilled");
}

uint32_t IOCPServer::GetPostedAcceptCount() const
{
	const LONG count = m_postedAcceptCount.load(std::memory_order_acquire);

	return count < 0 ? 0 : static_cast<uint32_t>(count);
}

// I/O 완료 분배와 오류 처리

void IOCPServer::HandleCompletion(
	ULONG_PTR completionKey,
	LPOVERLAPPED overlapped,
	DWORD bytesTransferred,
	DWORD completionError)
{
	OverlappedEx* overlappedEx = reinterpret_cast<OverlappedEx*>(overlapped);

	// GQCS 자체가 실패하면 overlapped 가 nullptr 로 온다.
	// 종료 중 IOCP 핸들이 닫히는 경우가 대표적이므로 위반으로 다루지 않는다.
	if (overlappedEx == nullptr)
	{
		LOGW("completion arrived with no overlapped (status %d, error %lu). the IOCP handle was most likely closed",
			completionError == ERROR_SUCCESS, completionError);
		return;
	}

	switch (overlappedEx->operation)
	{
	case IO_OPERATION::ACCEPT:
		if (completionError != ERROR_SUCCESS)
		{
			HandleSocketError(overlappedEx, nullptr, static_cast<int>(completionError), IO_OPERATION::ACCEPT);
			return;
		}

		HandleAccept(overlappedEx->sessionId);
		return;

	case IO_OPERATION::RECV:
	case IO_OPERATION::SEND:
	{
		// 완료 키에 넣은 것이 ClientSession* 다 (HandleAccept 의
		// RegisterSocketToIOCP 참고). 예전에는 ISession* 로 되받아서
		// 계층을 가로지르는 reinterpret_cast 를 했고, 단일 상속이라
		// 주소가 우연히 같아서 동작했을 뿐이다. 넣은 타입 그대로 받는다.
		ClientSession* session = reinterpret_cast<ClientSession*>(completionKey);
		ENGINE_CHECK_RETVOID(session != nullptr,
			"io %d completion carried no session in the completion key",
			static_cast<int>(overlappedEx->operation));

		const auto completion = session->TakeIOCompletion();
		if (completionError != ERROR_SUCCESS)
		{
			HandleSocketError(overlappedEx, session, static_cast<int>(completionError), overlappedEx->operation);
			return;
		}

		if (overlappedEx->operation == IO_OPERATION::RECV)
			HandleReceive(session, bytesTransferred);
		else
			HandleSend(session, bytesTransferred);

		return;
	}

	default:
		ENGINE_VIOLATION("completion arrived with an unknown io operation %d",
			static_cast<int>(overlappedEx->operation));
		return;
	}
}

void IOCPServer::HandleSocketError(OverlappedEx* overlappedEx, ClientSession* session, int errorCode, IO_OPERATION ioOperation)
{
	// 공용으로 처리 되어야 하는 예외 처리

	if (ioOperation == IO_OPERATION::ACCEPT)
	{
		LOGW("accept io %d failed (error %d)", (int)ioOperation, errorCode);

		if (errorCode == ERROR_OPERATION_ABORTED)
		{
			// CancelIoEx 로 인한 I/O 를 강제로 취소한 경우
			// 또는 서버 리슨 소켓을 닫은 경우
			// 이전에 걸어 두었던 Accept 에 대해 취소된 IO 작업 완료 알림 수신

			HandleAcceptIOCancelled(overlappedEx->sessionId);

			return;
		}

		// 취소가 아닌 accept 실패다. 해당 accept 세션의 IO 카운트를 정리해야
		// 종료 시 취소 대기가 풀린다.
		ENGINE_VIOLATION("accept failed with error %d (not a cancellation), releasing the accept session", errorCode);
		HandleAcceptIOCancelled(overlappedEx->sessionId);

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

// 접속 수락과 취소 완료

void IOCPServer::HandleAccept(uint32_t sessionId)
{
	LOGT("accept completed on accept session %u", sessionId);

	AcceptSession* acceptSession = m_sessionManager->GetAcceptSession(sessionId);

	if (!acceptSession)
	{
		LOGE("accept session %u not found", sessionId);

		ENGINE_BREAK_IF_DEBUGGER();
		return;
	}

	// 종료 스레드는 연결 인계와 첫 수신 등록까지 끝난 뒤 연결 세션을 정리한다.
	const auto finishAccept = [](AcceptSession* session) { session->CompleteAcceptIO(); };
	const std::unique_ptr<AcceptSession, decltype(finishAccept)> completion(acceptSession, finishAccept);
	m_postedAcceptCount.fetch_sub(1);

	SOCKADDR_IN peer{};
	auto socket = acceptSession->DetachAcceptedConnection(m_getAcceptExSockAddrs, peer);
	if (m_serverShutdownRequested.load(std::memory_order_acquire) || socket.Get() == INVALID_SOCKET)
	{
		if (socket.Get() == INVALID_SOCKET && !m_serverShutdownRequested.load(std::memory_order_acquire))
			ENGINE_VIOLATION("accept session %u completed without a socket", sessionId);
		acceptSession->ReleaseSession();
		return;
	}

	char ipAddress[INET_ADDRSTRLEN]{};
	uint16_t port = 0;
	if (peer.sin_family == AF_INET && ::inet_ntop(AF_INET, &peer.sin_addr, ipAddress, sizeof(ipAddress)))
		port = ::ntohs(peer.sin_port);

	if (!ipAddress[0])
	{
		RepostAfterAccept(*acceptSession);
		return;
	}
	ClientSession* clientSession = nullptr;
	try
	{
		clientSession = m_sessionManager->AcquireClientSession(peer.sin_addr.s_addr, m_connectionPolicy.maxConnectionsPerAddress);
	}
	catch (const std::bad_alloc&)
	{
		LOGE("could not reserve the connection address");
	}
	if (!clientSession)
	{
		RepostAfterAccept(*acceptSession);
		return;
	}

	// 소켓 인계부터 첫 수신 및 서비스 콜백까지 이번 연결의 세션을 보유한다.
	const auto reference = clientSession->AcquireReference();
	const SOCKET connectedSocket = socket.Get();
	const bool attached = clientSession->AttachSocket(std::move(socket), ipAddress, port);
	RepostAfterAccept(*acceptSession);

	if (m_serverShutdownRequested.load(std::memory_order_acquire))
		clientSession->RequestDisconnect(DisconnectReason::LocalShutdown);
	else if (!attached || !RunAcceptedConnectSequence(clientSession, connectedSocket))
		clientSession->RequestDisconnect(DisconnectReason::SocketError);
}

bool IOCPServer::RunAcceptedConnectSequence(ClientSession* clientSession, SOCKET socket)
{
	LOGT("session %u running the server-side connect sequence", clientSession->GetSessionID());

	if (m_serverShutdownRequested.load(std::memory_order_acquire))
	{
		// 이미 서버가 Shutdown 모드면 이 Accept 결과는 버린다.
		return false;
	}

	if (!SocketOption::SetAcceptContext(socket, m_serverSocket.Get()))
	{
		LOGE("session %u SO_UPDATE_ACCEPT_CONTEXT failed, dropping the connection", clientSession->GetSessionID());
		return false;
	}

	if (!SocketOption::SetNoDelay(socket))
	{
		LOGE("session %u TCP_NODELAY failed, dropping the connection", clientSession->GetSessionID());
		return false;
	}

	if (!SocketOption::SetKeepAliveEx(socket, 10'000, 1'000))
	{
		LOGE("session %u keepalive setup failed, dropping the connection", clientSession->GetSessionID());
		return false;
	}

	if (!IOCPCore::RegisterSocketToIOCP((ULONG_PTR)clientSession, socket))
	{
		LOGE("session %u could not be associated with the IOCP, dropping the connection", clientSession->GetSessionID());
		return false;
	}

	clientSession->OnConnect();

	if (!EnqueueServiceEvent(*clientSession, ServiceEvent::Connected,
		[this](ISession& connected) { OnClientConnect(&connected); }))
	{
		clientSession->RequestDisconnect(DisconnectReason::ServiceError);
		return false;
	}

	// 접속 이벤트를 먼저 등록했으므로 이후의 서비스 Job은 컨텍스트 준비 뒤 실행됩니다.
	clientSession->ContinueReceive();
	return true;
}

void IOCPServer::HandleAcceptIOCancelled(uint32_t sessionId)
{
	LOGW("AcceptEx was cancelled on the listen socket");

	AcceptSession* acceptSession = m_sessionManager->GetAcceptSession(sessionId);

	if (!acceptSession)
	{
		LOGE("accept session %u not found", sessionId);

		return;
	}

	IOCPCore::CloseSocketHandle(acceptSession->DetachAcceptSocket());

	// 취소된 AcceptEx 는 더 이상 걸려 있지 않다.
	m_postedAcceptCount.fetch_sub(1);

	// 이 경로는 다시 걸지 않는다. 세션이 비었으므로 소유권을 놓는다.
	// 종료 중이면 주기 점검이 어차피 걸지 않고, 종료가 아닌 accept 실패라면
	// 이 세션은 주기 점검이 되살려야 한다.
	acceptSession->ReleaseSession();
	acceptSession->CompleteAcceptIO();
}

// 송수신 완료와 패킷 분배

void IOCPServer::HandleReceive(ClientSession* session, DWORD bytesTransferred)
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

bool IOCPServer::HandleReceivedPacket(ClientSession& session, uint16_t packetId, PacketHandle&& packet)
{
	if (IsServicePacketId(packetId))
	{
		// 서비스 패킷은 등록된 핸들러의 Job으로 한 번만 전달합니다.
		if (EnqueueJob(&session, packetId, std::move(packet)))
			return true;
		session.RequestDisconnect(DisconnectReason::ServiceError);
		return false;
	}

	const auto result = HandleSystemPacket(session, packetId, packet.View());
	if (result == SystemPacketResult::Failed)
		session.RequestDisconnect(DisconnectReason::ProtocolError);
	return result == SystemPacketResult::Ok;
}

void IOCPServer::HandleSend(ClientSession* session, DWORD bytesTransferred)
{
	ENGINE_CHECK_RETVOID(session->GetSessionRole() == SESSION_ROLE::SERVER,
		"unexpected session role for send completion on session %u", session->GetSessionID());
	// 통지를 Job 큐에 등록한 뒤, 이번 I/O의 송신 상태와 패킷을 정리합니다.
	if (session->IsServiceReady() && !EnqueueServiceEvent(*session, ServiceEvent::Sent,
		[this, bytesTransferred](ISession& sent) { OnSend(&sent, bytesTransferred); }))
		session->RequestDisconnect(DisconnectReason::ServiceError);
	session->OnSendCompleted(bytesTransferred);
}

// 인증과 하트비트

IOCPServer::SystemPacketResult IOCPServer::HandleSystemPacket(ClientSession& session, uint16_t packetId, std::span<const std::byte> packet)
{
	if (packet.empty())
	{
		return SystemPacketResult::Failed;
	}

	switch (static_cast<PACKET_ID>(packetId))
	{
	case PACKET_ID::CS_SYSTEM_AUTH_REQUEST:
		return HandleAuthRequest(session, packet);

	case PACKET_ID::CS_SYSTEM_HEARTBEAT_RESPONSE:
		return HandleHeartbeatResponse(session, packet);

	default:
		LOGW("session %u unhandled system packet id: %u", session.GetSessionID(), packetId);
		return SystemPacketResult::Failed;
	}
}

IOCPServer::SystemPacketResult IOCPServer::HandleAuthRequest(ClientSession& session, std::span<const std::byte> packet)
{
	if (packet.size() != sizeof(CS_SYSTEM_AUTH_REQUEST_PACKET))
	{
		return SystemPacketResult::Failed;
	}

	const CS_SYSTEM_AUTH_REQUEST_PACKET* request = reinterpret_cast<const CS_SYSTEM_AUTH_REQUEST_PACKET*>(packet.data());

	if (!session.BeginAuthentication())
	{
		session.RejectAuthentication(DisconnectReason::AuthRejectedInvalidState, CreateAuthResponse(SYSTEM_AUTH_RESULT::INVALID_STATE));
		return SystemPacketResult::Rejected;
	}

	if (request->protocolVersion != IOCP_ENGINE_PROTOCOL_VERSION)
	{
		session.RejectAuthentication(DisconnectReason::AuthRejectedProtocolMismatch, CreateAuthResponse(SYSTEM_AUTH_RESULT::PROTOCOL_MISMATCH));
		return SystemPacketResult::Rejected;
	}

	// 인증을 허용할 연결 수를 원자적으로 예약한다. 세션 반환 시 예약도 해제한다.
	if (!m_sessionManager->TryAdmitClientSession(session, m_connectionPolicy.serviceCapacity))
	{
		session.RejectAuthentication(DisconnectReason::AuthRejectedServerFull, CreateAuthResponse(SYSTEM_AUTH_RESULT::SERVER_FULL));
		return SystemPacketResult::Rejected;
	}

	if (!session.EnqueueSendPacket(CreateAuthResponse(SYSTEM_AUTH_RESULT::SUCCESS)))
	{
		return SystemPacketResult::Failed;
	}

	if (!session.Establish())
		return SystemPacketResult::Failed;
	LOGI("session %u session established", session.GetSessionID());
	return EnqueueServiceEvent(session, ServiceEvent::Ready,
		[this](ISession& ready) { OnSessionEstablished(&ready); })
		? SystemPacketResult::Ok : SystemPacketResult::Failed;
}

PacketHandle IOCPServer::CreateAuthResponse(SYSTEM_AUTH_RESULT authResult)
{
	auto* packetPool = m_memoryPools ? m_memoryPools->packet.get() : nullptr;
	if (!packetPool)
	{
		return {};
	}

	auto packetHandle = MEMORY_POOL::CreatePacketHandle<SC_SYSTEM_AUTH_RESPONSE_PACKET>(*packetPool);

	if (!packetHandle)
	{
		return {};
	}

	auto* response = reinterpret_cast<SC_SYSTEM_AUTH_RESPONSE_PACKET*>(packetHandle.MutableView().data());

	response->authResult = static_cast<uint16_t>(authResult);

	// 클라이언트의 유휴 타임아웃이 이 값에서 파생된다. 타임아웃이 0 이면
	// 이 서버는 좀비 정리를 하지 않는다는 뜻이므로 주기도 알릴 필요가 없다.
	response->heartbeatIntervalMs =
		(m_heartbeatConfig.timeout_ms != 0)
		? static_cast<uint32_t>(m_heartbeatConfig.checkInterval_ms)
		: 0;

	return packetHandle;
}

IOCPServer::SystemPacketResult IOCPServer::HandleHeartbeatResponse(ClientSession& session, std::span<const std::byte> packet)
{
	if (packet.size() != sizeof(CS_SYSTEM_HEARTBEAT_RESPONSE_PACKET))
	{
		return SystemPacketResult::Failed;
	}

	if (!session.IsEstablished())
	{
		return SystemPacketResult::Failed;
	}

	session.UpdateLastHeartbeatTick();
	return SystemPacketResult::Ok;
}

// 세션 종료 통지

void IOCPServer::OnSessionDisconnectNotify(void* context, ClientSession* session, DisconnectReason reason)
{
	EngineExecutionScope execution;
	(void)InvokeServiceCallback("OnClientDisconnect", [&] { static_cast<IOCPServer*>(context)->OnClientDisconnect(session, reason); });
}
