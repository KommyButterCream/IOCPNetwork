#include "ReadySessionQueue.h"

#include "../Session/ISession.h"

#include "../../Core/Util/Logger.h"

using namespace Core::Util;

ReadySessionQueue::ReadySessionQueue()
{
}

ReadySessionQueue::~ReadySessionQueue()
{
	Finalize();
}

bool ReadySessionQueue::Initialize(const uint32_t maxSessionCount)
{
	if (maxSessionCount == 0)
		return false;

	m_queue = new ISession * [maxSessionCount];
	if (!m_queue)
	{
		return false;
	}

	::AcquireSRWLockExclusive(&m_srwLock);

	m_capacity = maxSessionCount;
	m_head = 0;
	m_tail = 0;
	m_count = 0;
	m_stopFlag = false;

	memset(m_queue, 0, sizeof(ISession*) * maxSessionCount);

	::ReleaseSRWLockExclusive(&m_srwLock);

	return true;
}

void ReadySessionQueue::Finalize()
{
	if (m_queue)
	{
		delete[] m_queue;
		m_queue = nullptr;
	}

	m_capacity = 0;
	m_head = 0;
	m_tail = 0;
	m_count = 0;
}

bool ReadySessionQueue::Push(ISession* session)
{
	::AcquireSRWLockExclusive(&m_srwLock);

	if (m_count >= m_capacity)
	{
		::ReleaseSRWLockExclusive(&m_srwLock);
		LOGE("[ReadySessionQueue] Queue full - Push Failed.");
		return false;
	}

	m_queue[m_tail] = session;
	m_tail = (m_tail + 1) % m_capacity;
	++m_count;

	::WakeConditionVariable(&m_cv);
	::ReleaseSRWLockExclusive(&m_srwLock);

	return true;
}

ISession* ReadySessionQueue::Pop(const uint32_t timeout_ms)
{
	::AcquireSRWLockExclusive(&m_srwLock);

	while (m_count == 0)
	{
		if (m_stopFlag)
		{
			::ReleaseSRWLockExclusive(&m_srwLock);
			return nullptr;
		}

		const BOOL ok = ::SleepConditionVariableSRW(&m_cv, &m_srwLock, timeout_ms, 0);

		if (!ok && ::GetLastError() == ERROR_TIMEOUT)
		{
			::ReleaseSRWLockExclusive(&m_srwLock);
			return nullptr;
		}
	}

	ISession* session = m_queue[m_head];
	m_queue[m_head] = nullptr;
	m_head = (m_head + 1) % m_capacity;
	--m_count;

	::ReleaseSRWLockExclusive(&m_srwLock);

	return session;
}

void ReadySessionQueue::WakeAll()
{
	// 플래그는 락 안에서. (이유는 SessionJobQueue::WakeUp 주석과 같다)
	::AcquireSRWLockExclusive(&m_srwLock);
	m_stopFlag = true;
	::ReleaseSRWLockExclusive(&m_srwLock);

	::WakeAllConditionVariable(&m_cv);
}

bool ReadySessionQueue::IsEmpty() const
{
	::AcquireSRWLockShared(&m_srwLock);
	bool isEmpty = (m_count == 0);
	::ReleaseSRWLockShared(&m_srwLock);
	return isEmpty;
}

int32_t ReadySessionQueue::GetCount() const
{
	::AcquireSRWLockShared(&m_srwLock);
	int32_t count = m_count;
	::ReleaseSRWLockShared(&m_srwLock);
	return count;
}
