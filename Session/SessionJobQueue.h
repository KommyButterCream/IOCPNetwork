#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

#include "SessionDefs.h"
#include "../Job/JobHandle.h"


class Job;
class TlsMemoryPool;
enum class SESSION_ROLE;

// 세션별 JobQueue (singly-linked list) - SRWLock 으로 보호
class SessionJobQueue
{
public:
	SessionJobQueue(SESSION_ROLE sessionRole, TlsMemoryPool* jobMemoryPool, TlsMemoryPool* packetMemoryPool);
	~SessionJobQueue();

private:
	SESSION_ROLE m_sessionRole = SESSION_ROLE::NONE;

	Job* m_head = nullptr;
	Job* m_tail = nullptr;
	mutable SRWLOCK m_srwLock = SRWLOCK_INIT;
	CONDITION_VARIABLE m_cv = CONDITION_VARIABLE_INIT;
	int32_t m_count = 0;

	// 여태 도달한 최고 깊이. m_count 와 같은 락이 지킨다.
	int32_t m_peakCount = 0;

	TlsMemoryPool* m_jobMemoryPool = nullptr;
	TlsMemoryPool* m_packetMemoryPool = nullptr;

	bool m_stopFlag = false;

public:
	// 성공하면 핸들이 비워지고 큐가 소유한다.
	// 실패하면 호출자가 소유권을 유지한다.
	[[nodiscard]]
	bool EnqueueJob(JobHandle&& jobHandle, bool& wasEmpty);

	// 대기 없이 꺼낸다. 큐가 비어 있으면 빈 핸들을 반환한다.
	[[nodiscard]]
	JobHandle DequeueJob();
	void WakeUp();

	// Job이 들어오거나 중지 요청이 올 때까지 기다린다.
	// 큐가 비어 있고 중지 요청이 있으면 빈 핸들을 반환한다.
	[[nodiscard]]
	JobHandle WaitDequeueJob();

	void Reset();

	bool IsEmpty() const;

	// Valid Jobs from this queue's pool are accepted without a depth limit.
	// Empty handles and handles from a different pool are rejected.
	// A slow consumer can retain queued Jobs and their packet buffers.
	// Count and peak updates are protected by the queue lock.
	uint32_t GetCount() const;

	// 이 큐가 여태 도달한 최고 깊이. 세션이 재사용되면 Reset 이 지운다.
	uint32_t GetPeakCount() const;

	// 구간 측정용. 하네스가 시나리오 경계에서 쓴다.
	void ResetPeakCount();
};
