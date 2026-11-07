#pragma once

// 엔진의 풀 세 개와 핸들러 컨텍스트를 함께 소유한다. 생성 실패는 지역 소유권으로 정리한다.
// 내부 전용 정의이므로 서버·클라이언트의 공개 헤더에는 전방 선언만 둔다.

#include "TlsMemoryPool.h"
#include "EnginePoolConfig.h"

#include "../Buffer/SharedPacketHandle.h"
#include "../Job/Job.h"
#include "../Job/HandlerContext.h"

#include "../../Core/Util/Logger.h"

#include <span> // for std::span
#include <memory> // for std::unique_ptr, std::make_unique
#include <utility> // for std::move

namespace ENGINE_POOL
{
	struct EnginePools final
	{
		std::unique_ptr<TlsMemoryPool> packet;
		std::unique_ptr<TlsMemoryPool> job;
		std::unique_ptr<TlsMemoryPool> sharedPacket;
		HandlerContext context;

		EnginePools(std::unique_ptr<TlsMemoryPool> packetPool, std::unique_ptr<TlsMemoryPool> jobPool,
			std::unique_ptr<TlsMemoryPool> sharedPool, void* serviceContext)
			: packet(std::move(packetPool)), job(std::move(jobPool)), sharedPacket(std::move(sharedPool))
		{
			context.m_packetPool = packet.get();
			context.m_sharedPool = sharedPacket.get();
			context.serviceContext = serviceContext;
		}

		// Job이 context의 주소를 빌리므로 묶음 자체는 이동하지 않는다.
		EnginePools(const EnginePools&) = delete;
		EnginePools& operator=(const EnginePools&) = delete;
		EnginePools(EnginePools&&) = delete;
		EnginePools& operator=(EnginePools&&) = delete;

		// 워커·세션과 서비스가 보관한 핸들을 모두 정리한 뒤 호출한다.
		// 컨텍스트와 저장 공간을 정리하되, 풀 객체는 재시작·엔진 파괴까지 유지한다.
		void Finalize()
		{
			if (!context.m_packetPool)
				return;
			context = {};
			job->LogStats("job");
			packet->LogStats("packet");
			sharedPacket->LogStats("sharedPacket");
			job->Finalize();
			packet->Finalize();
			sharedPacket->Finalize();
		}
	};

	namespace Detail
	{
		// 단일·복수 빈의 생성과 실패 정리를 같은 경로에서 처리한다.
		[[nodiscard]] inline std::unique_ptr<TlsMemoryPool> CreatePool(
			std::span<const TlsMemoryPool::BinConfig> bins, uint64_t commitLimitBytes,
			const char* poolName, uint32_t payloadAlignment)
		{
			auto pool = std::make_unique<TlsMemoryPool>();
			if (!pool->Initialize(bins.data(), static_cast<uint32_t>(bins.size()), payloadAlignment))
			{
				LOGE("failed to initialize the %s memory pool", poolName);
				return nullptr;
			}
			pool->SetCommitLimit(commitLimitBytes);
			return pool;
		}

		// 공개 설정을 풀 내부의 빈 설정으로 변환한다.
		[[nodiscard]] inline std::unique_ptr<TlsMemoryPool> CreateBinSetPool(const PoolBinSetConfig& config,
			const char* poolName, uint32_t payloadAlignment = 16)
		{
			TlsMemoryPool::BinConfig bins[PoolBinSetConfig::MAX_ENTRIES]{};
			for (uint32_t i = 0; i < config.entryCount; ++i)
				bins[i] = { config.entries[i].blockSize, config.entries[i].blockCount };
			return CreatePool({ bins, config.entryCount }, config.commitLimitBytes, poolName, payloadAlignment);
		}

		// 고정 크기 객체는 빈 하나로 구성한다.
		[[nodiscard]] inline std::unique_ptr<TlsMemoryPool> CreateReservePool(const PoolReserveConfig& config,
			uint32_t blockSize, const char* poolName, uint32_t payloadAlignment = 16)
		{
			const TlsMemoryPool::BinConfig bin{ blockSize, config.blockCount };
			return CreatePool({ &bin, 1 }, config.commitLimitBytes, poolName, payloadAlignment);
		}
	}

	// 세 풀을 설정대로 만든다.
	//
	// 전부 성공하거나 전부 지워진다. 부분 생성 상태를 호출부가 정리할 일이
	// 없다는 것이 요점이다 — 예전에는 실패 지점마다 이미 만든 풀이 그대로
	// 남은 채 false 만 돌아갔다.
	//
	// maxRecvPacketSize 는 커버리지 검사에 쓴다. 세션 버퍼 설정과 풀 설정은
	// 서로 다른 곳에서 정해지므로 언제든 어긋날 수 있다.
	[[nodiscard]] inline std::unique_ptr<EnginePools> CreatePools(const EnginePoolConfig& config, uint32_t maxRecvPacketSize,
		const char* roleName, void* serviceContext)
	{
		if (!config.IsValid())
		{
			LOGE("[%s] the pool configuration is invalid (packet entries %u, "
				"job blocks %u, shared packet blocks %u)",
				roleName, config.packet.entryCount,
				config.job.blockCount, config.sharedPacket.blockCount);
			return nullptr;
		}

		// Job 은 alignas(64) 라 페이로드도 64바이트 정렬이어야 한다.
		// 예전 풀은 16바이트만 보장해서 4개 중 1개만 실제로 정렬되어 있었다.
		constexpr uint32_t AlignedJobObjectSize =
			static_cast<uint32_t>((sizeof(Job) + 63) & ~static_cast<size_t>(63));

		auto job = Detail::CreateReservePool(config.job, AlignedJobObjectSize, "job", alignof(Job));
		if (!job)
		{
			return nullptr;
		}

		auto packet = Detail::CreateBinSetPool(config.packet, "packet");
		if (!packet)
		{
			return nullptr;
		}

		// 풀이 설정된 수신 상한을 실제로 덮는지 확인한다.
		//
		// 덮지 못하면 그 크기의 패킷은 TlsMemoryPool::Acquire 가 우회 경로로
		// 보내므로 패킷마다 HeapAlloc / HeapFree 를 한 번씩 한다. 풀을 만든
		// 이유가 그걸 피하려던 것이다.
		//
		// 조용히 빠진다는 점이 더 나쁘다. 설정만 보면 상한까지 받는 줄 알고,
		// 로그도 나가지 않는다 (지표는 LogStats 의 bypass 카운터에만 남는다).
		// 그래서 기동 시점에 끊는다.
		if (packet->GetBinIndex(maxRecvPacketSize) >= packet->GetBinCount())
		{
			LOGE("[%s] the packet pool does not cover maxRecvPacketSize %u (largest bin is %u bytes). "
				"every packet above that size would bypass the pool and hit the heap",
				roleName, maxRecvPacketSize,
				packet->GetBinBlockSize(packet->GetBinCount() - 1));
			return nullptr;
		}

		auto sharedPacket = Detail::CreateReservePool(
			config.sharedPacket, static_cast<uint32_t>(SharedPacketHandle::GetControlSize()), "sharedPacket");
		if (!sharedPacket)
		{
			return nullptr;
		}

		// 기동 시점에 실제로 적용된 설정을 한 줄 남긴다.
		//
		// 설정이 밖에서 오게 되었으므로, 문제가 생겼을 때 "무엇으로 떴는가" 를
		// 로그만 보고 알 수 있어야 한다. 서비스 코드를 찾아 들어가지 않아도 된다.
		LOGI("[%s] memory pools ready : packet %u bins up to %u bytes (limit %lluMB), "
			"job %u blocks (limit %lluMB), "
			"sharedPacket %u blocks (limit %lluMB)",
			roleName,
			packet->GetBinCount(), config.packet.LargestBlockSize(),
			static_cast<unsigned long long>(config.packet.commitLimitBytes >> 20),
			config.job.blockCount,
			static_cast<unsigned long long>(config.job.commitLimitBytes >> 20),
			config.sharedPacket.blockCount,
			static_cast<unsigned long long>(config.sharedPacket.commitLimitBytes >> 20));

		return std::make_unique<EnginePools>(std::move(packet), std::move(job), std::move(sharedPacket), serviceContext);
	}
}
