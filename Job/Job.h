#pragma once

#include <stdint.h>

#include "../Diagnostics/EngineAssert.h"
#include "../Buffer/PacketHandle.h"
#include "JobDefs.h"

#include <utility> // for std::move

// Scheduler 가 Task 를 수행하기 위한 기본 단위
// 각 Packet, DB 마다 별도의 Job 을 생성하지 않고
// 아래에 구현된 Job 을 통합하여 사용한다.
// 대신 내부에서 JobType 에 따라 적절한 인자로 캐스팅하여 호출한다.
__declspec(align(64)) struct Job
{
	Job* next = nullptr;
	void* target = nullptr;
	// Borrowed view for PACKET; ownership lives in m_packetHandle.
	const char* data = nullptr;

	union
	{
		PacketHandlerFunc packetFunc = nullptr;
		DBHandlerFunc dbFunc;
		SystemHandlerFunc systemFunc;
	};

	const HandlerContext* context = nullptr;

	uint32_t size = 0;
	uint16_t packetId = 0;
	JobType jobType = JobType::NONE;


	void SetPacketJob(
		PacketHandlerFunc handlerFunc,
		ISession* session,
		uint16_t packetId,
		PacketHandle&& packetHandle,
		const HandlerContext& context)
	{
		m_packetHandle = std::move(packetHandle);

		this->jobType = JobType::PACKET;
		this->packetFunc = handlerFunc;
		this->target = session;
		this->packetId = packetId;
		this->context = &context;


		this->data = reinterpret_cast<const char*>(m_packetHandle.View().data());
		this->size = static_cast<uint32_t>(m_packetHandle.Size());
	}

	// 실행 중인 Packet Job에서 소유권을 꺼낸다.
	// 호출 이후 기존 data 포인터는 Job이 소유하지 않는다.
	[[nodiscard]]
	PacketHandle TakePacket() noexcept
	{
		if (jobType != JobType::PACKET)
		{
			return {};
		}

		data = nullptr;
		size = 0;

		return std::move(m_packetHandle);
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

private:
	PacketHandle m_packetHandle;

};