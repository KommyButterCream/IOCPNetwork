#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include "SessionDefs.h"
#include "../Job/JobHandle.h"

class SessionJobQueue final
{
public:
	SessionJobQueue(SESSION_ROLE role, TlsMemoryPool& jobPool);
	~SessionJobQueue();

	// 락과 연결 목록을 소유하므로 큐 객체의 복사를 금지한다.
	SessionJobQueue(const SessionJobQueue&) = delete;
	SessionJobQueue& operator=(const SessionJobQueue&) = delete;

	// 성공할 때만 Job의 소유권을 받아 큐 끝에 넣는다.
	[[nodiscard]] bool Enqueue(JobHandle&& job);

	// 서버 워커가 첫 Job을 꺼낸다. 비었거나 정지했으면 빈 핸들을 반환한다.
	[[nodiscard]] JobHandle Dequeue();

	// 클라이언트 워커가 첫 Job을 기다린다. 정지하면 빈 핸들을 반환한다.
	[[nodiscard]] JobHandle WaitDequeue();

	// 새 등록과 인출을 막고 대기 중인 워커를 깨운다.
	void RequestStop();

	// 서버 워커가 다시 스케줄할 수 있는 Job이 있는지 확인한다.
	[[nodiscard]] bool HasRunnableJobs() const;

	// 정지 전에 등록된 잔여 Job을 포함한 현재 개수를 반환한다.
	[[nodiscard]] uint32_t GetCount() const;

	// 이번 연결에서 기록한 최대 대기 개수를 반환한다.
	[[nodiscard]] uint32_t GetPeakCount() const;

private:
	friend class ClientSession;

	// 이전 연결의 사용자가 모두 떠난 뒤 잔여 Job을 버리고 재사용을 준비한다.
	void Reset();

	// 쓰기 락을 보유한 호출자가 첫 Job의 소유권을 가져간다.
	JobHandle DequeueLocked();

	const SESSION_ROLE m_role;
	TlsMemoryPool& m_jobPool;
	mutable SRWLOCK m_lock = SRWLOCK_INIT;
	CONDITION_VARIABLE m_cv = CONDITION_VARIABLE_INIT;
	Job* m_head = nullptr;
	Job* m_tail = nullptr;
	uint32_t m_count = 0;
	uint32_t m_peakCount = 0;
	bool m_stopped = false;
};
