#include "ServerReadySessionQueue.h"

#include "../Session/ClientSession.h"

#include <new> // for std::nothrow
#include <utility> // for std::move

#include "../../Core/Sync/SRWLockGuard.h"
#include "../../Core/Util/Logger.h"

using namespace Core::Util;

ServerReadySessionQueue::~ServerReadySessionQueue()
{
	Finalize();
}

bool ServerReadySessionQueue::Initialize(uint32_t maxSessionCount)
{
	if (maxSessionCount == 0 || m_queue)
	{
		return false;
	}

	auto queue = std::unique_ptr<ClientSession*[]>(new (std::nothrow) ClientSession*[maxSessionCount]{});

	if (!queue)
	{
		return false;
	}

	m_queue = std::move(queue);
	m_capacity = maxSessionCount;
	m_head = m_tail = m_count = 0;
	m_stopFlag = false;

	return true;
}

void ServerReadySessionQueue::Finalize()
{
	// 생산자 중지와 워커 Join 뒤, 세션 풀을 파괴하기 전에 호출합니다.
	while (auto* session = Dequeue(0))
	{
		session->UpdateProcessingFlag(0);
		session->ReleaseLifetimeReference();
	}
	m_queue.reset();
	m_capacity = m_head = m_tail = m_count = 0;
	m_stopFlag = true;
}

bool ServerReadySessionQueue::Enqueue(ClientSession* session)
{
	if (!session)
	{
		return false;
	}

	Core::Sync::SRWWriteLockGuard lock(m_srwLock);

	if (m_count >= m_capacity)
	{
		LOGE("[ServerReadySessionQueue] Queue full or uninitialized - Enqueue Failed.");
		return false;
	}

	// 호출자는 수신 I/O 또는 실행 중인 워커의 참조로 세션 수명을 보장합니다.
	session->AddLifetimeReference();
	m_queue[m_tail] = session;
	m_tail = (m_tail + 1) % m_capacity;
	++m_count;

	::WakeConditionVariable(&m_cv);

	return true;
}

ClientSession* ServerReadySessionQueue::Dequeue(uint32_t timeout_ms)
{
	Core::Sync::SRWWriteLockGuard lock(m_srwLock);

	if (!m_queue)
	{
		return nullptr;
	}

	while (m_count == 0)
	{
		if (m_stopFlag)
		{
			return nullptr;
		}

		// 대기 중에는 락을 풀고, 반환 전에 다시 획득합니다.
		const BOOL ok = ::SleepConditionVariableSRW(&m_cv, &m_srwLock, timeout_ms, 0);
		if (!ok)
		{
			const DWORD error = ::GetLastError();
			if (error != ERROR_TIMEOUT)
			{
				LOGE("[ServerReadySessionQueue] Wait failed (error %lu).", error);
			}
			return nullptr;
		}
	}

	ClientSession* session = m_queue[m_head];
	m_queue[m_head] = nullptr;
	m_head = (m_head + 1) % m_capacity;
	--m_count;

	return session;
}

void ServerReadySessionQueue::WakeAll()
{
	{
		Core::Sync::SRWWriteLockGuard lock(m_srwLock);
		m_stopFlag = true;
	}
	::WakeAllConditionVariable(&m_cv);
}

bool ServerReadySessionQueue::IsEmpty() const
{
	Core::Sync::SRWReadLockGuard lock(m_srwLock);
	return m_count == 0;
}

uint32_t ServerReadySessionQueue::GetCount() const
{
	Core::Sync::SRWReadLockGuard lock(m_srwLock);
	return m_count;
}
