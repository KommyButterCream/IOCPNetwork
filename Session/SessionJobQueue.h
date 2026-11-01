#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

#include "SessionDefs.h"

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

struct Job;
class TlsMemoryPool;
enum class SESSION_ROLE;

// 세션별 JobQueue (singly-linked list) - SRWLock 으로 보호
class IOCP_ENGINE_API SessionJobQueue
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
	bool EnqueueJob(Job* job, bool& wasEmpty);
	bool DequeueJob(Job*& outJob);
	void WakeUp();
	bool WaitDequeueJob(Job*& outJob);

	void Reset();

	bool IsEmpty() const;

	// --- 수위 계측 ---
	//
	// 이 큐에는 깊이 상한이 없다. EnqueueJob 은 언제나 성공하고, 핸들러가
	// 유입보다 느리면 잡이 계속 쌓인다. 잡 하나가 패킷 하나를 붙들고 있으므로
	// 메모리는 그만큼 함께 자란다.
	//
	// 상한을 정하기 전에 실제 수위를 봐야 한다. 지금까지는 볼 수가 없었다 —
	// IsEmpty 만 있고 개수를 묻는 방법이 없었다.
	//
	// m_count 는 이미 락 안에서 갱신되므로 최고 수위 추적은 그 임계구역 안의
	// 비교 한 번이다. 락도 원자 연산도 늘지 않는다.
	uint32_t GetCount() const;

	// 이 큐가 여태 도달한 최고 깊이. 세션이 재사용되면 Reset 이 지운다.
	uint32_t GetPeakCount() const;

	// 구간 측정용. 하네스가 시나리오 경계에서 쓴다.
	void ResetPeakCount();
};
