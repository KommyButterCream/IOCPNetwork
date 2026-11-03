#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

#include "../../Core/Concurrency/ThreadBase.h"

class ReadySessionQueue;
class TlsMemoryPool;
class ReadySessionScheduler;

// ThreadBase 는 객체 1개당 스레드 1개이므로, 워커 N개를 두려면
// 스레드 객체를 N개 만들어야 한다.
// 실제 처리 루프는 스케줄러가 갖고 있고 이 클래스는 그 루프로 진입만 시킨다.
class ReadySessionWorker final : public Core::Concurrency::ThreadBase
{
public:
	explicit ReadySessionWorker(const wchar_t* name) : ThreadBase(name) {}

	void Bind(ReadySessionScheduler* owner, uint32_t workerIndex)
	{
		m_owner = owner;
		m_workerIndex = workerIndex;
	}

protected:
	void Run() override;

private:
	ReadySessionScheduler* m_owner = nullptr;
	uint32_t m_workerIndex = 0;
};

class ReadySessionScheduler
{
public:
	ReadySessionScheduler();
	~ReadySessionScheduler();

public:
	bool Initialize(uint32_t workerCount, ReadySessionQueue* readySessionQueue, TlsMemoryPool* packetMemoryPool);
	void Finalize();

	// ReadySessionWorker 가 호출한다. 정지 판정은 워커의 정지 이벤트로 한다.
	void WorkerThreadLoop(ReadySessionWorker& worker, uint32_t workerIndex);

private:
	ReadySessionWorker** m_workers = nullptr;
	uint32_t m_workerCount = 0;
	ReadySessionQueue* m_readySessionQueue = nullptr;
	TlsMemoryPool* m_packetMemoryPool = nullptr;

	void DestroyWorkers();
};
