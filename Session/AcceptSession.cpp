#include "AcceptSession.h"

#include <WinSock2.h>

#include "../Diagnostics/EngineAssert.h"
#include "../../Core/Sync/SRWLockGuard.h"
#include <utility> // for std::exchange
#include "../../Core/Util/Logger.h"
using namespace Core::Util;

#pragma comment(lib, "ws2_32.lib")

AcceptSession::AcceptSession()
{
	m_request.overlapped.operation = IO_OPERATION::ACCEPT;
}

AcceptSession::~AcceptSession()
{
	const LONG outstanding = GetAcceptIOCount();

	if (outstanding != 0)
		ENGINE_VIOLATION("accept session %u finalized with %ld outstanding IO", GetSessionID(), outstanding);

	if (m_acceptIOCompleteEvent)
	{
		::CloseHandle(m_acceptIOCompleteEvent);
		m_acceptIOCompleteEvent = nullptr;
	}
}

bool AcceptSession::Initialize(uint32_t sessionId)
{
	if (m_acceptIOCompleteEvent)
		return false;

	m_acceptIOCompleteEvent = ::CreateEvent(nullptr, TRUE, FALSE, nullptr);
	if (!m_acceptIOCompleteEvent)
		return false;

	m_sessionId = sessionId;
	SetState(State::ACCEPT_READY);

	return true;
}

uint32_t AcceptSession::GetSessionID() const noexcept
{
	return m_sessionId;
}

AcceptSession::State AcceptSession::GetState() const
{
	return static_cast<State>(::ReadAcquire(&m_state));
}

void AcceptSession::SetState(State state)
{
	::InterlockedExchange(&m_state, static_cast<LONG>(state));
}

bool AcceptSession::TryAcquireSession()
{
	return ::InterlockedCompareExchange(&m_sessionOwned, 1, 0) == 0;
}

void AcceptSession::ReleaseSession()
{
	if (::InterlockedExchange(&m_sessionOwned, 0) == 0)
	{
		// 발행 권한이 없는 수락 세션을 반환했다. 소유권 인계가 어긋났다는 뜻이라
		// 조용히 넘기면 다음에 둘이 함께 거는 상태로 이어진다.
		ENGINE_VIOLATION("accept session %u released without owning the session", GetSessionID());
	}
}

void AcceptSession::ResetForRepost()
{
	SetState(State::ACCEPT_READY);
	::ZeroMemory(m_request.addressBuffer, sizeof(m_request.addressBuffer));
	m_request.overlapped.Clear();
}

char* AcceptSession::GetAcceptBuffer()
{
	return m_request.addressBuffer;
}

int AcceptSession::PostAcceptSocket(SOCKET listenSocket, SOCKET acceptSocket, LPFN_ACCEPTEX acceptEx)
{
	Core::Sync::SRWWriteLockGuard lock(m_acceptSocketLock);

	if (m_acceptPostingStopped)
		return WSA_OPERATION_ABORTED;

	if (m_acceptSocket != INVALID_SOCKET)
	{
		ENGINE_VIOLATION("accept session %u already owns a socket", GetSessionID());
		return WSAEALREADY;
	}

	m_acceptSocket = acceptSocket;
	++m_acceptIOCount;

	m_request.overlapped.ResetForNextIO(GetSessionID());
	SetState(State::ACCEPT_WAIT);

	DWORD bytesReceived = 0;
	const BOOL posted = acceptEx(
		listenSocket,
		acceptSocket,
		m_request.addressBuffer,
		0,
		AddressLength,
		AddressLength,
		&bytesReceived,
		&m_request.overlapped.wsaOverlapped);

	// AcceptEx 요청 성공
	if (posted)
		return 0;

	// IO_PENDING 상태도 정상 처리
	const int error = ::WSAGetLastError();
	if (error == WSA_IO_PENDING)
		return 0;

	// 발행되지 않았으므로 완료 통지는 오지 않는다. 이번 발행의 몫을 되돌린다.
	m_acceptSocket = INVALID_SOCKET;
	SetState(State::ACCEPT_ABORTED);
	ResetForRepost();
	--m_acceptIOCount;

	return error;
}

SOCKET AcceptSession::DetachAcceptSocket()
{
	Core::Sync::SRWWriteLockGuard lock(m_acceptSocketLock);

	return std::exchange(m_acceptSocket, INVALID_SOCKET);
}

SOCKET AcceptSession::StopPostingAndDetachAcceptSocket()
{
	Core::Sync::SRWWriteLockGuard lock(m_acceptSocketLock);

	m_acceptPostingStopped = true;

	if (m_acceptIOCount == 0 && m_acceptIOCompleteEvent)
	{
		::SetEvent(m_acceptIOCompleteEvent);
	}

	return std::exchange(m_acceptSocket, INVALID_SOCKET);
}

void AcceptSession::CompleteAcceptIO()
{
	Core::Sync::SRWWriteLockGuard lock(m_acceptSocketLock);

	if (m_acceptIOCount == 0)
	{
		ENGINE_VIOLATION("accept session %u completed IO with none outstanding", GetSessionID());
		return;
	}

	--m_acceptIOCount;

	if (m_acceptPostingStopped && m_acceptIOCount == 0)
	{
		::SetEvent(m_acceptIOCompleteEvent);
	}
}

LONG AcceptSession::GetAcceptIOCount() const
{
	Core::Sync::SRWReadLockGuard lock(m_acceptSocketLock);

	return m_acceptIOCount;
}

bool AcceptSession::WaitForAcceptIOComplete(uint32_t timeout_ms) const
{
	if (GetAcceptIOCount() == 0)
		return true;

	return m_acceptIOCompleteEvent && ::WaitForSingleObject(m_acceptIOCompleteEvent, timeout_ms) == WAIT_OBJECT_0;
}