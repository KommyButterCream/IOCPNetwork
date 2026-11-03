#include "ClientJobWorker.h"

#include "../Job/Job.h"
#include "../Session/ClientSession.h"
#include "../Session/SessionJobQueue.h"

#include "../../Core/Util/Logger.h"
#include "../Job/JobHandle.h"
#include "../Memory/EngineMemoryPoolHelper.h"

using namespace Core::Util;

namespace
{
	// JobHandle보다 나중에 생성하여, Job 반환 전에 빌린 주소와 별도 데이터를 정리합니다.
	class ClientJobScope final
	{
	public:
		ClientJobScope(ClientSession& session, Job& job, TlsMemoryPool& packetMemoryPool)
			: m_session(session), m_job(job), m_packetMemoryPool(packetMemoryPool)
		{
			m_session.SetCurrentJob(&m_job);
		}

		~ClientJobScope()
		{
			m_session.SetCurrentJob(nullptr);
			// Packet은 Job의 핸들이 반환합니다. DB/System만 기존 패킷 풀 반환 정책을 따릅니다.
			if (const char* data = m_job.GetNonPacketData())
			{
				MEMORY_POOL::ReleasePacket(m_packetMemoryPool, data);
			}
		}

		ClientJobScope(const ClientJobScope&) = delete;
		ClientJobScope& operator=(const ClientJobScope&) = delete;

	private:
		ClientSession& m_session;
		Job& m_job;
		TlsMemoryPool& m_packetMemoryPool;
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

bool ClientJobWorker::Initialize(ClientSession* clientSession, TlsMemoryPool* packetMemoryPool)
{
	if (m_clientSession)
	{
		LOGE("client session scheduler is already initialized");
		return false;
	}

	if (!clientSession || !packetMemoryPool)
	{
		LOGE("client session or packet memory pool is null");
		return false;
	}

	m_clientSession = clientSession;
	m_packetMemoryPool = packetMemoryPool;

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
	m_packetMemoryPool = nullptr;
}

void ClientJobWorker::Run()
{
	while (!IsStopRequested())
	{
		{
			auto jobHandle = m_clientSession->GetJobQueue().WaitDequeueJob();

			if (!jobHandle)
			{
				break;
			}

			ClientJobScope jobScope(*m_clientSession, *jobHandle, *m_packetMemoryPool);

			jobHandle->Execute();
		}

		// 백프레셔 해제. 서버 쪽 ServerJobScheduler 와 같은 자리다.
		//
		// 이 스레드가 클라이언트의 유일한 소비자라, 멈춰 뒀던 수신을 다시
		// 걸어 줄 수 있는 것도 여기뿐이다. 반드시 루프 본문의 마지막 줄이어야
		// 한다 — 안에서 일시정지의 IO 카운트를 내려놓는다.
		m_clientSession->ResumeReceiveIfDrained();
	}
}
