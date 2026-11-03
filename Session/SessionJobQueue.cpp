#include "SessionJobQueue.h"

#include "ISession.h"

#include "../Diagnostics/EngineAssert.h"

using namespace Core::Util;
#include "SessionDefs.h"

#include "../Job/Job.h"
#include "../Memory/EngineMemoryPoolHelper.h"
#include "../Job/JobHandle.h"

SessionJobQueue::SessionJobQueue(SESSION_ROLE sessionRole, TlsMemoryPool* jobMemoryPool, TlsMemoryPool* packetMemoryPool)
{
	m_sessionRole = sessionRole;
	m_jobMemoryPool = jobMemoryPool;
	m_packetMemoryPool = packetMemoryPool;
}

SessionJobQueue::~SessionJobQueue()
{
	Reset();
}

bool SessionJobQueue::EnqueueJob(JobHandle&& jobHandle, bool& wasEmpty)
{
	wasEmpty = false;

	if (!jobHandle)
	{
		return false;
	}

	// 큐는 꺼내기와 정리 시 자신의 Job 풀로 반환한다.
	if (jobHandle.get_deleter().GetPool() != m_jobMemoryPool)
	{
		ENGINE_VIOLATION("cannot enqueue a job allocated from a different pool");

		return false;
	}

	::AcquireSRWLockExclusive(&m_srwLock);

	wasEmpty = (m_head == nullptr);

	// 유효한 Job은 이 지점부터 실패 없이 큐에 연결된다.
	// 소비자가 접근하기 전에 호출자의 핸들을 비운다.
	Job* job = jobHandle.release();

	job->m_next = nullptr;

	if (m_tail)
	{
		m_tail->m_next = job;
		m_tail = job;
	}
	else
	{
		m_head = m_tail = job;
	}

	++m_count;

	// 최고 수위. 이미 잡고 있는 락 안이라 비용이 비교 하나다.
	if (m_count > m_peakCount)
		m_peakCount = m_count;

	if (m_sessionRole == SESSION_ROLE::CLIENT)
	{
		::WakeConditionVariable(&m_cv);
	}

	::ReleaseSRWLockExclusive(&m_srwLock);
	return true;
}

JobHandle SessionJobQueue::DequeueJob()
{
	JobHandle result(nullptr, JobDeleter{ *m_jobMemoryPool });

	if (m_sessionRole != SESSION_ROLE::SERVER)
	{
		// DequeueJob 은 서버 역할 전용이다. 클라이언트 역할은 WaitDequeueJob 을 쓴다.
		ENGINE_VIOLATION("DequeueJob called on a session whose role is %d, not SERVER", static_cast<int>(m_sessionRole));

		return result;
	}

	::AcquireSRWLockExclusive(&m_srwLock);

	if (m_head)
	{
		Job* job = m_head;
		m_head = job->m_next;

		if (!m_head)
		{
			m_tail = nullptr;
		}

		--m_count;

		job->m_next = nullptr;

		// 큐에서 분리한 Job을 반환용 핸들이 소유한다.
		result.reset(job);
	}

	::ReleaseSRWLockExclusive(&m_srwLock);

	return result;
}

void SessionJobQueue::WakeUp()
{
	if (m_sessionRole != SESSION_ROLE::CLIENT)
		return;

	// 플래그는 락 안에서 세운다.
	//
	// 읽는 쪽(WaitDequeueJob)이 락 안이라 밖에서 쓰면 형식상 데이터 경합이고,
	// 평범한 bool 이라 원자성도 없다. 더 중요한 건 대기자의 술어 검사와
	// 이 쓰기가 겹치지 않게 만드는 것이다 — 그게 없으면 "플래그를 봤는데
	// 아직 false" 인 상태로 잠드는 창이 남는다.
	::AcquireSRWLockExclusive(&m_srwLock);
	m_stopFlag = true;
	::ReleaseSRWLockExclusive(&m_srwLock);

	// 신호는 락을 놓은 뒤에 보낸다. 락 안에서 보내면 깨어난 스레드가
	// 곧바로 그 락에서 다시 막힌다.
	::WakeConditionVariable(&m_cv);
}

JobHandle SessionJobQueue::WaitDequeueJob()
{
	JobHandle result(nullptr, JobDeleter{ *m_jobMemoryPool });

	if (m_sessionRole != SESSION_ROLE::CLIENT)
	{
		// WaitDequeueJob 은 클라이언트 역할 전용이다. 서버 역할은 DequeueJob 을 쓴다.
		ENGINE_VIOLATION("WaitDequeueJob called on a session whose role is %d, not CLIENT", static_cast<int>(m_sessionRole));

		return result;
	}

	::AcquireSRWLockExclusive(&m_srwLock);

	while (!m_head)
	{
		if (m_stopFlag)
		{
			::ReleaseSRWLockExclusive(&m_srwLock);
			return result;
		}

		::SleepConditionVariableSRW(&m_cv, &m_srwLock, INFINITE, 0);
	}

	Job* job = m_head;
	m_head = job->m_next;

	if (!m_head)
	{
		m_tail = nullptr;
	}

	--m_count;

	job->m_next = nullptr;
	result.reset(job);

	::ReleaseSRWLockExclusive(&m_srwLock);

	return result;
}

void SessionJobQueue::Reset()
{
	::AcquireSRWLockExclusive(&m_srwLock);

	Job* job = m_head;
	while (job)
	{
		JobHandle jobHandle(job, JobDeleter{ *m_jobMemoryPool });

		// 현재 Job을 반환하기 전에 다른 Job의 주소를 확보한다.
		job = jobHandle->m_next;

		// 현재 Job을 연결 목록에서 분리한다.
		jobHandle->m_next = nullptr;

		if (const char* data = jobHandle->GetNonPacketData())
		{
			MEMORY_POOL::ReleasePacket(*m_packetMemoryPool, data);
		}
	}

	m_head = nullptr;
	m_tail = nullptr;
	m_count = 0;

	// 세션이 다음 접속에 재배포되면 수위도 새로 센다.
	// 남겨 두면 남의 접속에서 쌓인 값을 자기 것으로 보고하게 된다.
	m_peakCount = 0;

	m_stopFlag = false;

	::ReleaseSRWLockExclusive(&m_srwLock);
}

bool SessionJobQueue::IsEmpty() const
{
	::AcquireSRWLockShared(&m_srwLock);
	bool isEmpty = (m_count == 0);
	::ReleaseSRWLockShared(&m_srwLock);
	return isEmpty;
}

uint32_t SessionJobQueue::GetCount() const
{
	::AcquireSRWLockShared(&m_srwLock);
	const int32_t count = m_count;
	::ReleaseSRWLockShared(&m_srwLock);

	return (count > 0) ? static_cast<uint32_t>(count) : 0;
}

uint32_t SessionJobQueue::GetPeakCount() const
{
	::AcquireSRWLockShared(&m_srwLock);
	const int32_t peak = m_peakCount;
	::ReleaseSRWLockShared(&m_srwLock);

	return (peak > 0) ? static_cast<uint32_t>(peak) : 0;
}

void SessionJobQueue::ResetPeakCount()
{
	::AcquireSRWLockExclusive(&m_srwLock);
	m_peakCount = m_count;
	::ReleaseSRWLockExclusive(&m_srwLock);
}
