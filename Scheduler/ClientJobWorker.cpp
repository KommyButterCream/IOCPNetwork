#include "ClientJobWorker.h"

#include "../Job/Job.h"
#include "../Session/ClientSession.h"
#include "../Session/SessionJobQueue.h"

#include "../../Core/Util/Logger.h"
#include "../Job/JobHandle.h"

using namespace Core::Util;

namespace
{
	// JobHandle보다 나중에 생성하여, Job 반환 전에 현재 Job 연결을 해제합니다.
	class ClientJobScope final
	{
	public:
		ClientJobScope(ClientSession& session, Job& job)
			: m_session(session)
		{
			m_session.SetCurrentJob(&job);
		}

		~ClientJobScope()
		{
			m_session.SetCurrentJob(nullptr);
		}

		ClientJobScope(const ClientJobScope&) = delete;
		ClientJobScope& operator=(const ClientJobScope&) = delete;

	private:
		ClientSession& m_session;
	};
}

ClientJobWorker::ClientJobWorker()
	: ThreadBase(L"ClientJobWorker")
{
}

ClientJobWorker::~ClientJobWorker()
{
	Finalize();
}

bool ClientJobWorker::Initialize(ClientSession* clientSession)
{
	if (m_clientSession)
	{
		LOGE("client session scheduler is already initialized");
		return false;
	}

	if (!clientSession)
	{
		LOGE("client session is null");
		return false;
	}

	m_clientSession = clientSession;


	if (!Start())
	{
		LOGE("failed to start the scheduler thread");
		Finalize();
		return false;
	}

	LOGI("client session scheduler started (thread id %u)", GetThreadId());

	return true;
}

void ClientJobWorker::Finalize()
{
	if (!IsRunning() && m_clientSession == nullptr)
	{
		return;
	}

	// 정지 요청만 먼저 보낸다.
	// Run() 은 SessionJobQueue::WaitDequeue 에서 조건 변수로 블로킹되므로
	// 정지 이벤트만으로는 깨어나지 않는다. 큐 정지 요청으로 깨워야 조인이 성립한다.
	RequestStop();

	if (m_clientSession)
	{
		m_clientSession->GetJobQueue().RequestStop();
	}

	Join();

	LOGI("client session scheduler stopped");

	m_clientSession = nullptr;

}

void ClientJobWorker::Run()
{
	while (!IsStopRequested())
	{
		{
			auto jobHandle = m_clientSession->GetJobQueue().WaitDequeue();

			if (!jobHandle)
			{
				break;
			}

			ClientJobScope jobScope(*m_clientSession, *jobHandle);

			jobHandle->Execute();
		}

		// 저수위에 도달한 세션만 수신을 재개한다.
		m_clientSession->ResumeReceiveIfDrained();
	}
}
