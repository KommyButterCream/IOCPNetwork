#include "ClientSessionScheduler.h"

#include "../Job/Job.h"
#include "../Session/ClientSession.h"
#include "../Session/SessionJobQueue.h"
#include "../Memory/EngineMemoryPoolHelper.h"

#include "../../Core/Util/Logger.h"

using namespace Core::Util;

ClientSessionScheduler::ClientSessionScheduler()
	: ThreadBase(L"ClientSessionScheduler")
{
}

ClientSessionScheduler::~ClientSessionScheduler()
{
	Finalize();
}

bool ClientSessionScheduler::Initialize(ClientSession* clientSession, TlsMemoryPool* jobMemoryPool, TlsMemoryPool* packetMemoryPool)
{
	if (clientSession == nullptr || jobMemoryPool == nullptr)
	{
		LOGE("invalid arguments (session %p, jobPool %p)", clientSession, jobMemoryPool);
		return false;
	}

	m_clientSession = clientSession;
	m_jobMemoryPool = jobMemoryPool;
	m_packetMemoryPool = packetMemoryPool;

	if (!Start())
	{
		LOGE("failed to start the scheduler thread");
		return false;
	}

	LOGI("client session scheduler started (thread id %u)", GetThreadId());

	return true;
}

void ClientSessionScheduler::Finalize()
{
	if (!IsRunning() && m_clientSession == nullptr)
	{
		return;
	}

	// 정지 요청만 먼저 보낸다.
	// Run() 은 SessionJobQueue::WaitDequeueJob 에서 조건 변수로 블로킹되므로
	// 정지 이벤트만으로는 깨어나지 않는다. WakeUp 으로 깨워야 조인이 성립한다.
	RequestStop();

	if (m_clientSession)
	{
		m_clientSession->GetJobQueue().WakeUp();
	}

	Join();

	LOGI("client session scheduler stopped");

	m_jobMemoryPool = nullptr;
	m_clientSession = nullptr;
	m_packetMemoryPool = nullptr;
}

void ClientSessionScheduler::Run()
{
	while (!IsStopRequested())
	{
		Job* job = nullptr;
		const bool gotJob = m_clientSession->GetJobQueue().WaitDequeueJob(job);

		if (!gotJob)
			break;

		if (job)
		{
			m_clientSession->SetCurrentJob(job);
			job->Execute();

			// Clear the borrowed Job pointer before its destructor returns the packet.
			m_clientSession->SetCurrentJob(nullptr);

			MEMORY_POOL::ReleaseJob(*m_jobMemoryPool, job);
		}

		// 백프레셔 해제. 서버 쪽 ReadySessionScheduler 와 같은 자리다.
		//
		// 이 스레드가 클라이언트의 유일한 소비자라, 멈춰 뒀던 수신을 다시
		// 걸어 줄 수 있는 것도 여기뿐이다. 반드시 루프 본문의 마지막 줄이어야
		// 한다 — 안에서 일시정지의 IO 카운트를 내려놓는다.
		m_clientSession->ResumeReceiveIfDrained();
	}
}
