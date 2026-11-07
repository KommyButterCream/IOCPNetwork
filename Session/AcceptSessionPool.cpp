#include "AcceptSessionPool.h"

#include "AcceptSession.h"

#include "../Diagnostics/EngineAssert.h"

#include <WinSock2.h>
#include <memory>  // for std::make_unique, std::unique_ptr
#include <utility> // for std::move

using namespace Core::Util;

std::unique_ptr<AcceptSessionPool> AcceptSessionPool::Create(uint32_t capacity, CloseSocketFunc closeSocketFunc)
{
	auto sessions = std::make_unique<AcceptSession[]>(capacity);
	for (uint32_t i = 0; i < capacity; ++i)
	{
		if (!sessions[i].Initialize(i))
		{
			LOGE("failed to initialize accept session %u of %u", i, capacity);
			return nullptr;
		}
	}

	return std::unique_ptr<AcceptSessionPool>(new AcceptSessionPool(std::move(sessions), capacity, closeSocketFunc));
}

AcceptSessionPool::AcceptSessionPool(std::unique_ptr<AcceptSession[]> sessions, uint32_t capacity, CloseSocketFunc closeSocketFunc)
	: m_sessions(std::move(sessions)), m_capacity(capacity), m_closeSocketFunc(closeSocketFunc)
{
}

AcceptSessionPool::~AcceptSessionPool()
{
	// StopServer에서 발행을 중지하고 완료 워커를 조인한 뒤 잔여 소켓을 정리한다.
	CloseRemainingAcceptSockets();
}

AcceptSession* AcceptSessionPool::GetSession(uint32_t sessionId)
{
	if (sessionId >= m_capacity)
		return nullptr;

	return &m_sessions[sessionId];
}

uint32_t AcceptSessionPool::GetSessionCount() const
{
	return m_capacity;
}

uint32_t AcceptSessionPool::GetAcceptIOCount() const
{
	uint32_t total = 0;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		const LONG outstanding = m_sessions[i].GetAcceptIOCount();

		// 음수는 짝이 맞지 않는다는 뜻이고 이미 위반으로 잡힌다.
		// 여기서 더해 상쇄시키면 합계가 0 으로 보여 오히려 숨는다.
		if (outstanding > 0)
			total += static_cast<uint32_t>(outstanding);
	}

	return total;
}

void AcceptSessionPool::CancelAllAcceptIORequests()
{
	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		const SOCKET socket = m_sessions[i].StopPostingAndDetachAcceptSocket();
		if (socket != INVALID_SOCKET)
			CloseAcceptSocket(socket);
	}
}

IOWaitResult AcceptSessionPool::WaitForAllAcceptIOComplete(uint32_t timeout_ms)
{
	// 모든 세션이 이번 대기 주기의 같은 마감 시각을 사용한다.
	const ULONGLONG deadline = ::GetTickCount64() + timeout_ms;
	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		const ULONGLONG now = ::GetTickCount64();
		const DWORD remaining = now < deadline ? static_cast<DWORD>(deadline - now) : 0;
		const auto result = m_sessions[i].WaitForAcceptIOComplete(remaining);
		if (result.status != IOWaitStatus::Completed)
			return result;
	}

	return { IOWaitStatus::Completed };
}

// 풀 소멸 시 수락 세션이 아직 보유한 소켓을 진단하고 닫는다.
void AcceptSessionPool::CloseRemainingAcceptSockets()
{
	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		auto& session = m_sessions[i];

		const SOCKET socket = session.DetachAcceptSocket();
		if (socket == INVALID_SOCKET)
			continue;

		ENGINE_VIOLATION("accept session %u still holds a socket at pool destruction", session.GetSessionID());
		CloseAcceptSocket(socket);
	}
}

void AcceptSessionPool::CloseAcceptSocket(SOCKET socket)
{
	if (m_closeSocketFunc)
		m_closeSocketFunc(socket);
	else if (::closesocket(socket) == SOCKET_ERROR)
		LOGE("failed to close accept socket (error %d)", ::WSAGetLastError());
}
