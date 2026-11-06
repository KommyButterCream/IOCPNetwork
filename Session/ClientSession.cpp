#include "ClientSession.h"

#include <WinSock2.h>

#include "../Diagnostics/EngineAssert.h"

#include "SessionJobQueue.h"

#include "../Buffer/RecvPacketBuffer.h"
#include "../Buffer/SendPacketQueue.h"
#include "../Memory/EngineMemoryPoolHelper.h"
#include "../../Core/Util/Logger.h"

#include "../Job/Job.h"
#include "../Protocol/PacketID.h"
#include "../Protocol/SystemPacket.h"
#include "../Buffer/PacketHandle.h"
#include "../Buffer/SharedPacketHandle.h"
#include "../../Core/Sync/SRWLockGuard.h"
#include <cstring> // for std::strncmp, std::memcpy

#include <utility> // for std::move
#include <memory>  // for std::construct_at, std::make_unique
#pragma comment(lib, "ws2_32.lib") // for WinSock2

using namespace Core::Util;

ClientSession::ClientSession()
{
	m_connectOverlapped.operation = IO_OPERATION::CONNECT;
	m_recvOverlapped.operation = IO_OPERATION::RECV;
	m_sendOverlapped.operation = IO_OPERATION::SEND;
}

ClientSession::~ClientSession()
{
	CheckCleanupPreconditions();
	if (m_ioCancelCompleteEvent)
		::CloseHandle(m_ioCancelCompleteEvent);

	SetSessionContext(nullptr);
	// 현재 송신을 먼저 반환한 뒤 송신 큐, Job 큐, 수신 링을 해제한다.
	m_currentSendPacket.Reset();
	ReleaseMemoryResources();
}

bool ClientSession::Initialize(SESSION_ROLE sessionType, uint32_t sessionId)
{
	m_sessionRole = sessionType;
	m_sessionId = sessionId;

	if (!m_ioCancelCompleteEvent)
	{
		m_ioCancelCompleteEvent = ::CreateEvent(nullptr, TRUE, FALSE, nullptr);
		if (!m_ioCancelCompleteEvent)
			return false;
	}
	else
	{
		::ResetEvent(m_ioCancelCompleteEvent);
	}

	SetConnectReadyState();

	return true;
}

bool ClientSession::InitializeMemoryPool(TlsMemoryPool* jobMemoryPool, TlsMemoryPool* packetMemoryPool, const SessionBufferConfig& bufferConfig)
{
	if (!jobMemoryPool)
		return false;

	if (!bufferConfig.IsValid())
	{
		ENGINE_VIOLATION("session %u received an invalid buffer config (recv %u/ring %u, send %u, queue %u)",
			GetSessionID(), bufferConfig.maxRecvPacketSize, bufferConfig.recvRingSize,
			bufferConfig.maxSendPacketSize, bufferConfig.sendQueueDepth);
		return false;
	}

	// 세 자원을 모두 준비한 뒤 세션에 등록한다.
	auto recvPacketBuffer = std::make_unique<RecvPacketBuffer>();
	if (!recvPacketBuffer->Initialize(packetMemoryPool, bufferConfig.recvRingSize, bufferConfig.maxRecvPacketSize))
		return false;

	auto jobQueue = std::make_unique<SessionJobQueue>(GetSessionRole(), *jobMemoryPool);

	auto sendPacketQueue = std::make_unique<SendPacketQueue>();
	if (!sendPacketQueue->Initialize(bufferConfig.sendQueueDepth))
		return false;

	m_bufferConfig = bufferConfig;
	m_packetMemoryPool = packetMemoryPool;
	m_recvPacketBuffer = std::move(recvPacketBuffer);
	m_jobQueue = std::move(jobQueue);
	m_sendPacketQueue = std::move(sendPacketQueue);
	return true;
}

bool ClientSession::OnConnect()
{
	if (GetSessionRole() == SESSION_ROLE::CLIENT)
	{
		SetClientSessionState(ClientSessionState::CONNECTED);
	}
	else if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		SetServerSessionState(ServerSessionState::CONNECTED);
	}

	char ipAddress[sizeof(m_clientIPAddress)] = {};
	uint16_t port = 0;

	{
		Core::Sync::SRWReadLockGuard lock(m_remoteAddressLock);
		std::memcpy(ipAddress, m_clientIPAddress, sizeof(ipAddress));
		port = m_clientPort;
	}

	if (ipAddress[0] != '\0')
	{
		LOGI("session %u connected from %s:%u (role %d)",
			GetSessionID(), ipAddress, port, static_cast<int>(GetSessionRole()));
	}
	else
	{
		LOGI("session %u connected (role %d)", GetSessionID(), static_cast<int>(GetSessionRole()));
	}

	UpdateLastRecvTick();
	UpdateLastHeartbeatTick();

	if (!PostReceive())
	{
		// 첫 수신을 걸지 못하면 이 세션은 아무것도 받을 수 없다.
		LOGE("session %u failed to post the first recv right after connect", GetSessionID());
		return false;
	}

	return true;
}

void ClientSession::OnDisconnect()
{
	ClearRemoteAddress();
	if (m_clientSocket != INVALID_SOCKET)
	{
		// 소켓은 반드시 DetachSocket 으로 먼저 떼어낸 뒤 여기 와야 한다.
		LOGE("session %u disconnecting with a live socket %d. it was not detached",
			GetSessionID(), static_cast<int>(m_clientSocket));
		ENGINE_BREAK_IF_DEBUGGER();
	}

	switch (m_sessionRole)
	{
	case SESSION_ROLE::CLIENT:
		SetClientSessionState(ClientSessionState::DISCONNECTED);
		break;
	case SESSION_ROLE::SERVER:
		SetServerSessionState(ServerSessionState::DISCONNECTED);
		break;
	default:
		break;
	}

	LOGI("session %u disconnected (role %d)", GetSessionID(), static_cast<int>(m_sessionRole));
}

void ClientSession::ResetSession()
{
	SetClientSessionState(ClientSessionState::NONE);
	SetServerSessionState(ServerSessionState::NONE);

	// 지난 접속의 종료 사유를 지운다. 남겨 두면 이 슬롯을 다음에 쓰는
	// 접속이 끝날 때 남의 사유를 물려받는다.
	//
	// 서비스 통지보다 뒤여야 한다. ClientSessionPool::CompleteRelease 가
	// NotifyServiceDisconnect -> OnDisconnect -> ResetSession 순으로 부르므로
	// 지금 순서는 맞다.
	::InterlockedExchange(&m_disconnectReason, static_cast<LONG>(DisconnectReason::Unknown));

	CheckCleanupPreconditions();

	::InterlockedExchange(&m_cancelIo, 0);

	// 취소 완료 이벤트는 여기서 되돌리지 않는다.
	//
	// 되돌렸었다. 그런데 이 함수는 지연 반납의 마무리 경로에서도 불리고,
	// 그 경로는 방금 DecrementIO 가 이벤트를 세운 직후다. 종료 절차의
	// 대기자가 그 사이에 깨어나지 못하면 신호를 놓치고 10초를 태운다.
	//
	// 무장하는 쪽(CancelPendingIO)에서 되돌린다. 그러면 세워진 이벤트가
	// 대기자 밑에서 사라지는 일이 없다. 대기는 언제나 무장 뒤에 오므로
	// 재사용된 세션이 이전 주기의 신호를 물려받지도 않는다.

	SetConnectReadyState();

	::InterlockedExchange(&m_sending, 0);
	::InterlockedExchange(&m_processing, 0);

	// 백프레셔 상태도 지운다.
	//
	// 여기 도달했다는 것은 IO 카운트가 0 이라는 뜻이고, 멈춰 있는 세션은
	// 카운트를 하나 들고 있으므로 이 값은 이미 0 이어야 한다. 그래도 지우는
	// 이유는 다음 접속이 이 슬롯을 그대로 쓰기 때문이다 — 1 이 남아 있으면
	// 새 접속이 첫 수신부터 "이미 멈춰 있다" 로 오해된다.
	::InterlockedExchange(&m_recvPaused, 0);
	m_diagnostics.Reset();

	// 래치가 남아 있으면 다음 접속이 이 세션을 쓰다가, 접속을 알린 적도
	// 없는데 종료 통지를 받는다. 정상 경로에서는 반납 직전에 이미
	// 소비되지만 통지 배선이 없는 구성도 있어 여기서 확실히 지운다.
	::InterlockedExchange(&m_serviceConnectNotified, 0);

	SetSessionContext(nullptr);

	if (m_sendPacketQueue)
	{
		m_sendPacketQueue->Reset();
	}

	// 수신 링을 비운다. 이게 없으면 세션이 끊길 때 링에 남아 있던 바이트가
	// 그대로 살아남아, 이 슬롯을 재사용하는 다음 클라이언트의 스트림 앞에
	// 붙는다. 그 클라이언트는 첫 바이트부터 남의 데이터를 헤더로 읽는다.
	//
	// GetReceiveBuffer 는 널 검사 없이 역참조하는데, ResetSession 은 정리
	// 경로에서도 불리므로 포인터로 직접 다룬다.
	if (m_recvPacketBuffer)
	{
		m_recvPacketBuffer->Reset();
	}

	// 미완료 IO가 정리된 뒤 세션이 보관한 송신 패킷을 반환합니다.
	m_currentSendPacket.Reset();
	m_sendOffset = 0;

	if (m_jobQueue)
	{
		m_jobQueue->Reset();
	}

	::InterlockedExchange64(&m_lastRecvTick, 0);
	::InterlockedExchange64(&m_lastHeartbeatTick, 0);

	if (GetSessionRole() == SESSION_ROLE::CLIENT)
	{
		m_connectOverlapped.Clear();
	}
	else if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		m_recvOverlapped.Clear();
		m_sendOverlapped.Clear();
	}
}

OverlappedEx& ClientSession::GetConnectOverlapped()
{
	return m_connectOverlapped;
}

// 소켓이 붙어 있는가. 인증 여부는 보지 않는다.
//
// IsEstablished(인증까지 끝났는가) 와 짝을 이루는 두 단계 중 아래쪽이다.
// 시스템 패킷(인증 요청, 하트비트)은 인증 전에도 오가야 하므로 이쪽을 보고,
// 서비스 패킷은 IsEstablished 를 본다. CanSendPacket 이 그 분기다.
//
// 예전에는 이 함수의 별칭인 IsConnected 가 따로 있었다. 두 이름이 같은 것을
// 계산하니 독자는 없는 구분을 찾게 되고, 한쪽만 고치면 조용히 갈라진다.
bool ClientSession::IsTransportConnected() const
{
	// 상태를 지역 변수에 한 번만 받는다.
	//
	// 예전에는 비교마다 getter 를 다시 불렀다. 상태가 평범한 멤버이던
	// 시절에도 옳지 않았고(다른 스레드가 그 사이에 바꾼다) 지금은 호출마다
	// 실제로 다시 읽으므로 분명한 결함이다. 비교 도중에 값이 바뀌면
	// 어느 항목에도 걸리지 않아, 실제로는 접속되어 있는 세션이
	// "접속 아님" 으로 판정될 수 있다.
	if (GetSessionRole() == SESSION_ROLE::CLIENT)
	{
		const ClientSessionState state = GetClientSessionState();
		return (state == ClientSessionState::CONNECTED
			|| state == ClientSessionState::AUTH_PENDING
			|| state == ClientSessionState::ESTABLISHED);
	}

	if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		const ServerSessionState state = GetServerSessionState();
		return (state == ServerSessionState::CONNECTED
			|| state == ServerSessionState::AUTH_PENDING
			|| state == ServerSessionState::ESTABLISHED
			|| state == ServerSessionState::HEARTBEAT_TIMEOUT);
	}

	return false;
}

bool ClientSession::IsEstablished() const
{
	if (GetSessionRole() == SESSION_ROLE::CLIENT)
		return (GetClientSessionState() == ClientSessionState::ESTABLISHED);

	if (GetSessionRole() == SESSION_ROLE::SERVER)
		return (GetServerSessionState() == ServerSessionState::ESTABLISHED);

	return false;
}

void ClientSession::MarkServiceConnectNotified()
{
	::InterlockedExchange(&m_serviceConnectNotified, 1);
}

bool ClientSession::ConsumeServiceConnectNotified()
{
	return (::InterlockedExchange(&m_serviceConnectNotified, 0) == 1);
}

void ClientSession::NotifyDisconnect()
{
	if (m_ownerCallbacks.requestDisconnect)
	{
		m_ownerCallbacks.requestDisconnect(m_ownerCallbacks.owner, this);
	}
}

bool ClientSession::SubmitJob(JobHandle&& jobHandle)
{
	if (!m_jobQueue)
	{
		ENGINE_VIOLATION("session %u has no job queue", GetSessionID());
		return false;
	}
	return m_jobQueue->Enqueue(std::move(jobHandle));
}

SessionJobQueue& ClientSession::GetJobQueue() const
{
	return *m_jobQueue;
}

uint32_t ClientSession::GetJobQueuePeakDepth() const
{
	return m_jobQueue ? m_jobQueue->GetPeakCount() : 0;
}

bool ClientSession::TryClaimProcessing()
{
	// 0에서 1로 바꾼 호출자만 세션을 준비 큐에 등록한다.
	return (::InterlockedCompareExchange(&m_processing, 1, 0) == 0);
}

void ClientSession::ReleaseProcessing()
{
	::InterlockedExchange(&m_processing, 0);
}

void ClientSession::SetCurrentJob(Job* job)
{
	// Borrowed while the client handler runs; TakeCurrentPacket moves ownership.
	m_currentJob = job;
}

PacketHandle ClientSession::TakeCurrentPacket()
{
	// 현재 Job을 연결하는 스케줄러는 클라이언트 쪽뿐이다.
	if (GetSessionRole() != SESSION_ROLE::CLIENT)
	{
		ENGINE_VIOLATION(
			"session %u TakeCurrentPacket is client-only",
			GetSessionID());

		return {};
	}

	// 잡 사이에는 널이다. 스케줄러가 반납 직전에 끊어 둔다.
	if (!m_currentJob)
		return {};

	return m_currentJob->TakePacket();
}

void ClientSession::SetSessionContext(SessionContext* sessionContext)
{
	if (m_sessionContext != nullptr && m_sessionContext != sessionContext)
	{
		delete m_sessionContext;
	}

	m_sessionContext = sessionContext;
}

SessionContext* ClientSession::GetSessionContext() const
{
	return m_sessionContext;
}

void ClientSession::SetRemoteAddress(const char* ipAddress, uint16_t port)
{
	if (ipAddress == nullptr)
		return;

	Core::Sync::SRWWriteLockGuard lock(m_remoteAddressLock);
	::strncpy_s(m_clientIPAddress, sizeof(m_clientIPAddress), ipAddress, _TRUNCATE);
	m_clientPort = port;
}

bool ClientSession::HasRemoteAddress(const char* ipAddress) const
{
	if (!ipAddress || ipAddress[0] == '\0')
	{
		return false;
	}

	Core::Sync::SRWReadLockGuard lock(m_remoteAddressLock);
	return std::strncmp(m_clientIPAddress, ipAddress, sizeof(m_clientIPAddress)) == 0;
}

void ClientSession::UpdateLastRecvTick()
{
	::InterlockedExchange64(&m_lastRecvTick, static_cast<LONGLONG>(::GetTickCount64()));
}

uint64_t ClientSession::GetLastRecvTick() const
{
	return static_cast<uint64_t>(::ReadAcquire64(&m_lastRecvTick));
}

void ClientSession::UpdateLastHeartbeatTick()
{
	::InterlockedExchange64(&m_lastHeartbeatTick, static_cast<LONGLONG>(::GetTickCount64()));
}

bool ClientSession::IsHeartbeatTimedOut(uint64_t nowTick, uint64_t timeout_ms) const
{
	if (timeout_ms == 0)
	{
		return false;
	}

	const uint64_t lastActiveTick = GetLastActiveTick();
	if (lastActiveTick == 0 || nowTick < lastActiveTick)
	{
		return false;
	}

	return ((nowTick - lastActiveTick) >= timeout_ms);
}

void ClientSession::MarkHeartbeatTimeout()
{
	if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		SetServerSessionState(ServerSessionState::HEARTBEAT_TIMEOUT);
	}
}

bool ClientSession::SendSystemHeartbeatRequest()
{
	if (GetSessionRole() != SESSION_ROLE::SERVER || !IsEstablished() || !m_packetMemoryPool)
	{
		return false;
	}

	auto packetHandle = MEMORY_POOL::CreatePacketHandle(*m_packetMemoryPool, sizeof(SC_SYSTEM_HEARTBEAT_REQUEST_PACKET));

	if (!packetHandle)
	{
		return false;
	}

	auto* request = std::construct_at(reinterpret_cast<SC_SYSTEM_HEARTBEAT_REQUEST_PACKET*>(packetHandle.MutableView().data()));

	request->tick = ::GetTickCount64();

	return EnqueueSendPacket(std::move(packetHandle));
}

bool ClientSession::SendSystemHeartbeatResponse(uint64_t requestTick)
{
	if (GetSessionRole() != SESSION_ROLE::CLIENT || !IsEstablished())
	{
		return false;
	}

	auto packetHandle = MEMORY_POOL::CreatePacketHandle(*m_packetMemoryPool, sizeof(CS_SYSTEM_HEARTBEAT_RESPONSE_PACKET));

	if (!packetHandle)
	{
		return false;
	}

	auto* response = std::construct_at(reinterpret_cast<CS_SYSTEM_HEARTBEAT_RESPONSE_PACKET*>(packetHandle.MutableView().data()));

	response->tick = requestTick;

	return EnqueueSendPacket(std::move(packetHandle));
}

void ClientSession::SetConnectReadyState()
{
	if (GetSessionRole() == SESSION_ROLE::CLIENT)
	{
		SetClientSessionState(ClientSessionState::CONNECT_READY);
	}
	else if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		SetServerSessionState(ServerSessionState::CONNECT_READY);
	}
}

void ClientSession::ClearRemoteAddress()
{
	Core::Sync::SRWWriteLockGuard lock(m_remoteAddressLock);
	memset(m_clientIPAddress, 0, sizeof(m_clientIPAddress));
	m_clientPort = 0;
}

void ClientSession::ReleaseMemoryResources()
{
	m_sendPacketQueue.reset();
	m_jobQueue.reset();
	m_recvPacketBuffer.reset();
}

uint64_t ClientSession::GetLastHeartbeatTick() const
{
	return static_cast<uint64_t>(::ReadAcquire64(&m_lastHeartbeatTick));
}

uint64_t ClientSession::GetLastActiveTick() const
{
	const uint64_t lastRecvTick = GetLastRecvTick();
	const uint64_t lastHeartbeatTick = GetLastHeartbeatTick();
	return (lastRecvTick > lastHeartbeatTick) ? lastRecvTick : lastHeartbeatTick;
}
