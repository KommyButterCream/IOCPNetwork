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
	
	// DB/System 작업에서 사용하는 데이터 주소.
	// Packet 작업의 데이터는 m_packetHandle에서 읽는다.
	const char* data = nullptr;

	union
	{
		PacketHandlerFunc packetFunc = nullptr;
		DBHandlerFunc dbFunc;
		SystemHandlerFunc systemFunc;
	};

	const HandlerContext* context = nullptr;

	// DB/System 작업의 데이터 크기
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
	}

	// Packet Job의 소유권을 호출자에게 이전한다.
	// 호출 후 Job의 핸들은 빈 상태가 된다.
	[[nodiscard]]
	PacketHandle TakePacket() noexcept
	{
		if (jobType != JobType::PACKET)
		{
			return {};
		}

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
			if (!m_packetHandle)
			{
				LOGE("packet job for id %u has no packet, skipping", packetId);
				break;
			}

			if (packetFunc)
			{
				const auto bytes = m_packetHandle.View();
				
				bool result = packetFunc(
					reinterpret_cast<ISession*>(target), 
					reinterpret_cast<const char*>(bytes.data()),
					static_cast<uint32_t>(bytes.size()),
					*context);

				// 핸들러가 패킷을 송신 큐로 옮겼을 수 있으므로,
				// 호출 이후 bytes가 가리키는 메모리에 접근하지 않는다.
				if (!result)
				{
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