#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>
#include <memory> // for std::unique_ptr

class ClientSession;

class ServerReadySessionQueue
{
public:
	ServerReadySessionQueue() = default;
	~ServerReadySessionQueue();

	ServerReadySessionQueue(const ServerReadySessionQueue&) = delete;
	ServerReadySessionQueue& operator=(const ServerReadySessionQueue&) = delete;
	ServerReadySessionQueue(ServerReadySessionQueue&&) = delete;
	ServerReadySessionQueue& operator=(ServerReadySessionQueue&&) = delete;

	// 생산자와 소비자가 중지된 상태에서 호출합니다.
	// 재초기화하려면 먼저 Finalize로 기존 큐를 해제해야 합니다.
	[[nodiscard]] bool Initialize(uint32_t maxSessionCount);
	void Finalize();

	// Enqueue 성공 시 수명 참조를 확보합니다. Dequeue는 그 참조를 호출자에게 넘깁니다.
	// 반환받은 세션의 마지막 사용 후 ReleaseLifetimeReference를 호출해야 합니다.
	[[nodiscard]] bool Enqueue(ClientSession* session);
	[[nodiscard]] ClientSession* Dequeue(uint32_t timeout_ms);

	// 빈 큐에서 대기하는 워커를 깨우고 이후 빈 큐에서 다시 대기하지 않게 합니다.
	// 이미 들어 있는 항목의 Dequeue과 워커의 마지막 재등록은 기존처럼 허용합니다.
	void WakeAll();

	[[nodiscard]] bool IsEmpty() const;
	[[nodiscard]] uint32_t GetCount() const;

private:
	std::unique_ptr<ClientSession*[]> m_queue;
	uint32_t m_capacity = 0;
	uint32_t m_head = 0;
	uint32_t m_tail = 0;
	uint32_t m_count = 0;

	mutable SRWLOCK m_srwLock = SRWLOCK_INIT;
	CONDITION_VARIABLE m_cv = CONDITION_VARIABLE_INIT;
	bool m_stopFlag = false;
};
