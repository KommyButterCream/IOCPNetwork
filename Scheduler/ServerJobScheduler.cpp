#include "ServerJobScheduler.h"

#include <stdio.h> // for swprintf_s
#include <memory> // for std::make_unique, std::unique_ptr
#include <new> // for std::bad_alloc
#include <stdexcept> // for std::length_error

#include "ServerReadySessionQueue.h"
#include "../Job/Job.h"
#include "../Session/ClientSession.h"
#include "../Session/SessionJobQueue.h"
#include "../Job/JobHandle.h"
#include "../../Core/Util/Logger.h"

using namespace Core::Util;

namespace
{
	struct SessionReferenceDeleter
	{
		void operator()(ClientSession* session) const noexcept
		{
			session->ReleaseLifetimeReference();
		}
	};
}

void ServerJobWorker::Run()
{
	if (m_owner)
	{
		m_owner->WorkerThreadLoop(*this, m_workerIndex);
	}
}

ServerJobScheduler::ServerJobScheduler()
{
}

ServerJobScheduler::~ServerJobScheduler()
{
	Finalize();
}

bool ServerJobScheduler::Initialize(uint32_t workerCount, ServerReadySessionQueue* readySessionQueue)
{
	// 실행 중인 워커의 큐와 소유 테이블을 덮어쓰지 않습니다.
	if (!m_workers.empty())
	{
		LOGE("ready session scheduler is already initialized");
		return false;
	}

	if (workerCount == 0 || readySessionQueue == nullptr)
	{
		LOGE("invalid scheduler arguments");
		return false;
	}

	m_serverReadySessionQueue = readySessionQueue;

	try
	{
		// 워커를 시작하기 전에 모든 소유 슬롯을 확보합니다.
		m_workers.resize(workerCount);

		for (uint32_t i = 0; i < workerCount; ++i)
		{
			// 스레드에 이름을 붙여 두면 디버거와 ETW 추적에서 바로 식별된다.
			wchar_t threadName[64] = {};
			::swprintf_s(threadName, L"ServerJobWorker-%u", i);

			m_workers[i] = std::make_unique<ServerJobWorker>(threadName);

			m_workers[i]->Bind(this, i);

			if (!m_workers[i]->Start())
			{
				LOGE("failed to start worker %u", i);
				Finalize();
				return false;
			}
		}
	}
	catch (const std::bad_alloc&)
	{
		LOGE("failed to allocate workers (count %u)", workerCount);
		Finalize();
		return false;
	}
	catch (const std::length_error&)
	{
		LOGE("worker count exceeds container capacity (count %u)", workerCount);
		Finalize();
		return false;
	}

	LOGI("ready session scheduler started with %u workers", workerCount);

	return true;
}

void ServerJobScheduler::Finalize()
{
	StopAndDestroyWorkers();
	m_serverReadySessionQueue = nullptr;
}

void ServerJobScheduler::StopAndDestroyWorkers()
{
	if (m_workers.empty())
	{
		return;
	}

	// 정상 종료와 초기화 실패 모두 같은 순서를 따릅니다.
	// 한 워커를 먼저 조인하지 않고 모든 워커에 정지 요청을 전달합니다.
	for (const auto& worker : m_workers)
	{
		if (worker) worker->RequestStop();
	}

	// 정지 이벤트는 조건변수 대기를 깨우지 않습니다.
	// Dequeue에서 잠든 워커가 종료 조건을 확인하도록 큐도 중지하고 깨웁니다.
	if (m_serverReadySessionQueue)
	{
		m_serverReadySessionQueue->WakeAll();
	}

	// 워커 객체가 살아 있는 동안 스레드를 모두 종료합니다.
	// 파생 객체 소멸 후 기반 클래스 소멸자에서 조인하는 것에 의존하지 않습니다.
	for (const auto& worker : m_workers)
	{
		if (worker) worker->Join();
	}

	// 모든 스레드의 종료를 확인한 뒤 워커와 소유 슬롯을 해제합니다.
	decltype(m_workers){}.swap(m_workers);

	LOGI("ready session scheduler stopped");
}

void ServerJobScheduler::WorkerThreadLoop(ServerJobWorker& worker, uint32_t workerIndex)
{
	LOGI("worker %u entering loop", workerIndex);

	while (!worker.IsStopRequested())
	{
		constexpr uint32_t timeout_ms = INFINITE;

		ClientSession* session = m_serverReadySessionQueue->Dequeue(timeout_ms);
		if (!session)
			continue;

		// 큐에서 넘겨받은 수명 참조는 이 반복의 마지막까지 유지합니다.
		std::unique_ptr<ClientSession, SessionReferenceDeleter> sessionReference(session);

		while (auto jobHandle = session->GetJobQueue().Dequeue())
		{
			jobHandle->Execute();
		}

		session->UpdateProcessingFlag(0);

		// 드레인 중에 새로 들어온 Job 이 있으면 다시 큐에 올린다.
		if (!session->GetJobQueue().IsEmpty())
		{
			if (session->IsProcessingReady())
			{
				if (!m_serverReadySessionQueue->Enqueue(session))
				{
					// Enqueue 실패 시 소유권 플래그를 되돌려야 세션이 영구히 묶이지 않는다.
					session->UpdateProcessingFlag(0);
					LOGE("failed to re-queue session %u : ready queue is full", session->GetSessionID());
				}
			}
		}

		// 수신 재개에서 I/O 참조가 반환되어도 sessionReference가 세션을 보호합니다.
		// 반복 종료 시 마지막 수명 참조가 반환되면 세션이 풀로 돌아갈 수 있습니다.
		session->ResumeReceiveIfDrained();
	}

	LOGI("worker %u leaving loop", workerIndex);
}
