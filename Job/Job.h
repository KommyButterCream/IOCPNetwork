#pragma once

#include <stdint.h>

#include "../Diagnostics/EngineAssert.h"
#include "JobDefs.h"

// Scheduler 가 Task 를 수행하기 위한 기본 단위
// 각 Packet, DB 마다 별도의 Job 을 생성하지 않고
// 아래에 구현된 Job 을 통합하여 사용한다.
// 대신 내부에서 JobType 에 따라 적절한 인자로 캐스팅하여 호출한다.
__declspec(align(64)) struct Job
{
	Job* next = nullptr;
	void* target = nullptr;
	const char* data = nullptr;

	union
	{
		PacketHandlerFunc packetFunc;
		DBHandlerFunc dbFunc;
		SystemHandlerFunc systemFunc;
	};

	const HandlerContext* context = nullptr;

	uint32_t size = 0;
	uint16_t packetId = 0;
	JobType jobType = JobType::NONE;

	void SetPacketJob(JobType jobType, PacketHandlerFunc handlerFunc, ISession* session, const uint16_t packetId, const char* packetData, uint32_t packetSize, const HandlerContext& context)
	{
		this->jobType = jobType;
		this->packetFunc = handlerFunc;
		this->target = session;
		this->packetId = packetId;
		this->data = packetData;
		this->size = packetSize;
		this->context = &context;
	}

	void Execute()
	{
		// context 는 SetPacketJob 등이 채운다. 비어 있으면 Job 이 설정되지
		// 않은 채로 큐에 들어갔다는 뜻이라, 역참조하지 않고 걸러낸다.
		if (!context)
		{
			LOGE("job (type %d, packet id %u) has no handler context, skipping",
				static_cast<int>(jobType), packetId);
			return;
		}

		switch (jobType)
		{
		case JobType::PACKET:
			if (packetFunc)
			{
				bool result = packetFunc(reinterpret_cast<ISession*>(target), data, size, *context);
				if (!result)
				{
					// 핸들러가 false 를 반환한 것은 정상적인 실패 표현이다.
					// 이전에는 여기서 __debugbreak 로 프로세스를 죽였다.
					LOGW("packet handler for id %u returned failure", packetId);
				}
			}
			break;
		case JobType::DB:
			if (dbFunc)
			{
				dbFunc(reinterpret_cast<IDBConnection*>(target), data, size, *context);
			}
			break;
		case JobType::SYSTEM:
			if (systemFunc)
			{
				systemFunc(reinterpret_cast<IOCPCore*>(target), data, size, *context);
			}
			break;
		}
	}
};