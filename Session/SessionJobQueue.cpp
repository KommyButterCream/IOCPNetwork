#include "SessionJobQueue.h"
#include "../Job/Job.h"
#include "../Diagnostics/EngineAssert.h"
#include "../../Core/Sync/SRWLockGuard.h"
#include <utility> // for std::exchange

using namespace Core::Sync;
using namespace Core::Util;

SessionJobQueue::SessionJobQueue(SESSION_ROLE role, TlsMemoryPool& jobPool)
	: m_role(role)
	, m_jobPool(jobPool)
{
}

SessionJobQueue::~SessionJobQueue()
{
	Reset();
}

bool SessionJobQueue::Enqueue(JobHandle&& job)
{
	if (!job)
	{
		return false;
	}

	if (job.get_deleter().GetPool() != &m_jobPool)
	{
		ENGINE_VIOLATION("cannot enqueue a job allocated from a different pool");
		return false;
	}

	{
		SRWWriteLockGuard lock(m_lock);

		if (m_stopped)
		{
			return false;
		}

		Job* node = job.release();

		node->m_next = nullptr;

		if (m_tail)
			m_tail->m_next = node;
		else
			m_head = node;

		m_tail = node;

		++m_count;

		if (m_count > m_peakCount)
		{
			m_peakCount = m_count;
		}
	}

	if (m_role == SESSION_ROLE::CLIENT)
	{
		::WakeConditionVariable(&m_cv);
	}

	return true;
}

JobHandle SessionJobQueue::Dequeue()
{
	if (m_role != SESSION_ROLE::SERVER)
	{
		ENGINE_VIOLATION("Dequeue requires a server session");
		return JobHandle(nullptr, JobDeleter{ m_jobPool });
	}

	SRWWriteLockGuard lock(m_lock);

	return DequeueLocked();
}

JobHandle SessionJobQueue::WaitDequeue()
{
	if (m_role != SESSION_ROLE::CLIENT)
	{
		ENGINE_VIOLATION("WaitDequeue requires a client session");
		return JobHandle(nullptr, JobDeleter{ m_jobPool });
	}

	SRWWriteLockGuard lock(m_lock);

	while (!m_stopped && !m_head)
	{
		if (!::SleepConditionVariableSRW(&m_cv, &m_lock, INFINITE, 0))
		{
			const DWORD error = ::GetLastError();
			ENGINE_VIOLATION("WaitDequeue failed (error %lu)", error);
			return JobHandle(nullptr, JobDeleter{ m_jobPool });
		}
	}

	return DequeueLocked();
}

void SessionJobQueue::RequestStop()
{
	{
		SRWWriteLockGuard lock(m_lock);
		m_stopped = true;
	}

	::WakeAllConditionVariable(&m_cv);
}

bool SessionJobQueue::HasRunnableJobs() const
{
	SRWReadLockGuard lock(m_lock);

	return !m_stopped && m_head;
}

uint32_t SessionJobQueue::GetCount() const
{
	SRWReadLockGuard lock(m_lock);

	return m_count;
}

uint32_t SessionJobQueue::GetPeakCount() const
{
	SRWReadLockGuard lock(m_lock);

	return m_peakCount;
}

void SessionJobQueue::Reset()
{
	Job* pending;
	{
		SRWWriteLockGuard lock(m_lock);

		pending = std::exchange(m_head, nullptr);

		m_tail = nullptr;
		m_count = 0;
		m_peakCount = 0;
		m_stopped = false;
	}

	while (pending)
	{
		JobHandle job(pending, JobDeleter{ m_jobPool });
		pending = std::exchange(job->m_next, nullptr);
	}
}

JobHandle SessionJobQueue::DequeueLocked()
{
	if (m_stopped || !m_head)
	{
		return JobHandle(nullptr, JobDeleter{ m_jobPool });
	}

	Job* job = m_head;

	m_head = std::exchange(job->m_next, nullptr);

	if (!m_head)
	{
		m_tail = nullptr;
	}

	--m_count;
	
	return JobHandle(job, JobDeleter{ m_jobPool });
}
