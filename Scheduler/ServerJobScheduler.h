#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>
#include <memory> // for std::unique_ptr
#include <vector> // for std::vector

#include "../../Core/Concurrency/ThreadBase.h"

class ServerReadySessionQueue;
class TlsMemoryPool;
class ServerJobScheduler;

// ThreadBase 는 객체 1개당 스레드 1개이므로, 워커 N개를 두려면
// 스레드 객체를 N개 만들어야 한다.
// 실제 처리 루프는 스케줄러가 갖고 있고 이 클래스는 그 루프로 진입만 시킨다.
class ServerJobWorker final : public Core::Concurrency::ThreadBase
{
public:
	explicit ServerJobWorker(const wchar_t* name) : ThreadBase(name) {}

	void Bind(ServerJobScheduler* owner, uint32_t workerIndex)
	{
		m_owner = owner;
		m_workerIndex = workerIndex;
	}

protected:
	void Run() override;

private:
	ServerJobScheduler* m_owner = nullptr;
	uint32_t m_workerIndex = 0;
};

class ServerJobScheduler
{
public:
	ServerJobScheduler();
	~ServerJobScheduler();

	ServerJobScheduler(const ServerJobScheduler&) = delete;
	ServerJobScheduler& operator=(const ServerJobScheduler&) = delete;
	ServerJobScheduler(ServerJobScheduler&&) = delete;
	ServerJobScheduler& operator=(ServerJobScheduler&&) = delete;

public:
	// 수명 관리 함수는 외부 제어 스레드에서 직렬로 호출합니다.
	// 종료 시 큐도 중지되므로 재시작하려면 새로 초기화한 큐를 전달합니다.
	bool Initialize(uint32_t workerCount, ServerReadySessionQueue* readySessionQueue, TlsMemoryPool* packetMemoryPool);
	void Finalize();

	// ServerJobWorker 가 호출한다. 정지 판정은 워커의 정지 이벤트로 한다.
	void WorkerThreadLoop(ServerJobWorker& worker, uint32_t workerIndex);

private:
	std::vector<std::unique_ptr<ServerJobWorker>> m_workers;
	ServerReadySessionQueue* m_serverReadySessionQueue = nullptr;
	TlsMemoryPool* m_packetMemoryPool = nullptr;

	void StopAndDestroyWorkers();
};
