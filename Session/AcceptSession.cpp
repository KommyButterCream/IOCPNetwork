#include "AcceptSession.h"
#include "../Network/UniqueSocket.h"

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

	return true;
}

uint32_t AcceptSession::GetSessionID() const noexcept
{
	return m_sessionId;
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

UniqueSocket AcceptSession::DetachAcceptedConnection(LPFN_GETACCEPTEXSOCKADDRS getAddresses, SOCKADDR_IN& peer)
{
	// 이 완료 워커만 재발행 권한을 보유한다. 주소는 다음 AcceptEx를 걸기 전에 복사한다.
	peer = {};
	if (getAddresses)
	{
		sockaddr* local = nullptr;
		sockaddr* remote = nullptr;
		int localLength = 0, remoteLength = 0;
		getAddresses(m_request.addressBuffer, 0, AddressLength, AddressLength,
			&local, &localLength, &remote, &remoteLength);
		if (remote && remote->sa_family == AF_INET && remoteLength >= static_cast<int>(sizeof(peer)))
			peer = *reinterpret_cast<const SOCKADDR_IN*>(remote);
	}
	return UniqueSocket(DetachAcceptSocket());
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

	::ZeroMemory(m_request.addressBuffer, sizeof(m_request.addressBuffer));
	m_request.overlapped.ResetForNextIO(GetSessionID());

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

IOWaitResult AcceptSession::WaitForAcceptIOComplete(uint32_t timeout_ms) const
{
	if (GetAcceptIOCount() == 0)
		return { IOWaitStatus::Completed };

	if (!m_acceptIOCompleteEvent)
		return { IOWaitStatus::Failed, GetSessionID(), ERROR_INVALID_HANDLE };

	const DWORD result = ::WaitForSingleObject(m_acceptIOCompleteEvent, timeout_ms);
	if (result == WAIT_FAILED)
	{
		const DWORD error = ::GetLastError();
		return { IOWaitStatus::Failed, GetSessionID(), error };
	}

	if (GetAcceptIOCount() == 0)
		return { IOWaitStatus::Completed };

	return { IOWaitStatus::TimedOut, GetSessionID() };
}
