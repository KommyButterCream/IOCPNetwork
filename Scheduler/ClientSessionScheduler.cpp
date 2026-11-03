#include "ClientSessionScheduler.h"

#include "../Job/Job.h"
#include "../Session/ClientSession.h"
#include "../Session/SessionJobQueue.h"

#include "../../Core/Util/Logger.h"
#include "../Job/JobHandle.h"

using namespace Core::Util;

ClientSessionScheduler::ClientSessionScheduler()
	: ThreadBase(L"ClientSessionScheduler")
{
}

ClientSessionScheduler::~ClientSessionScheduler()
{
	Finalize();
}

bool ClientSessionScheduler::Initialize(ClientSession* clientSession)
{
	if (clientSession == nullptr)
	{
		LOGE("client session is null");
		return false;
	}

	m_clientSession = clientSession;

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

	m_clientSession = nullptr;
}

void ClientSessionScheduler::Run()
{
	while (!IsStopRequested())
	{
		{
			auto jobHandle = m_clientSession->GetJobQueue().WaitDequeueJob();

			if (!jobHandle)
			{
				break;
			}

			// 세션은 실행 중인 Job을 잠시 참조한다.
			m_clientSession->SetCurrentJob(jobHandle.get());

			jobHandle->Execute();

			// 소유자인 핸들이 사라지기 전에 빌린 주소를 비운다.
			m_clientSession->SetCurrentJob(nullptr);
		}

		// 백프레셔 해제. 서버 쪽 ReadySessionScheduler 와 같은 자리다.
		//
		// 이 스레드가 클라이언트의 유일한 소비자라, 멈춰 뒀던 수신을 다시
		// 걸어 줄 수 있는 것도 여기뿐이다. 반드시 루프 본문의 마지막 줄이어야
		// 한다 — 안에서 일시정지의 IO 카운트를 내려놓는다.
		m_clientSession->ResumeReceiveIfDrained();
	}
}
