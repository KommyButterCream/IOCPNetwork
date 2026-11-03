#pragma once

// EnginePoolConfig 로 엔진의 메모리 풀 세 개를 만든다. 기동 전용이다.
//
// 왜 별도 파일인가
//   EnginePoolConfig.h 는 서비스가 포함하는 순수 데이터다. 이쪽은 풀 구현을
//   끌어오므로 엔진만 포함한다. 둘을 한 파일에 두면 서비스가 풀 내부까지
//   딸려 들어온다.
//
//   EngineMemoryPoolHelper.h 에 얹지 않은 이유는 그쪽 성격이 다르기 때문이다.
//   그 파일은 패킷 하나당 불리는 핫패스라 로그 한 줄도 못 넣는다는 규칙이
//   붙어 있다. 기동 시점에 한 번 도는 코드를 거기 섞으면 그 규칙이 흐려진다.
//
// 왜 서버와 클라가 같이 쓰는가
//   두 곳에 같은 코드가 따로 있었고, 실제로 어긋났다 — 패킷 풀이 수신
//   상한을 덮는지 확인하는 검사가 클라에만 있었다. 서버는 지금 프리셋에서
//   우연히 안전할 뿐이고, 서비스가 maxRecvPacketSize 를 올리는 순간
//   조용히 힙으로 새기 시작한다. 설정을 밖으로 열면서 그 위험이 커지므로
//   검사를 한 곳으로 모은다.
//
//   (IOCPServer / IOCPClient 의 중복 자체를 없애는 작업은 별개다.
//    여기서는 새로 만드는 코드가 두 벌이 되는 것만 막는다)

#include "TlsMemoryPool.h"
#include "EnginePoolConfig.h"

#include "../Buffer/SendPacketQueue.h"
#include "../Job/Job.h"

#include "../../Core/Util/Logger.h"

#include <new>

namespace ENGINE_POOL
{
	// 엔진이 들고 다니는 풀 묶음. 만들기와 지우기를 한 단위로 다루려고 둔다.
	struct EnginePools
	{
		TlsMemoryPool* packet = nullptr;
		TlsMemoryPool* job = nullptr;
		TlsMemoryPool* sendQueue = nullptr;
	};

	inline void DestroyPools(EnginePools& pools)
	{
		// 지우는 순서는 상관없다. 서로를 참조하지 않는다.
		delete pools.packet;    pools.packet = nullptr;
		delete pools.job;       pools.job = nullptr;
		delete pools.sendQueue; pools.sendQueue = nullptr;
	}

	// 재고만 OS 로 돌려주고 풀 객체는 남긴다.
	//
	// 왜 지우지 않는가
	// PacketHandle과 SharedPacketHandle은 풀 포인터를 비공개 멤버로 보관합니다.
	// 생성 함수 호출이 끝나도 마지막 소유 핸들이 해제될 때 메모리를 반환합니다.
	//
	//   그 상태에서 Stop 이 풀을 delete 하면, 늦게 도착한 반납 하나가
	//   해제된 객체를 역참조한다. 게다가 소멸자의 Finalize 가 세그먼트를
	//   VirtualFree 로 이미 돌려주었으므로, 반납이 블록 헤더에 쓰는 순간
	//   커밋 해제된 주소에 쓰는 접근 위반이 된다.
	//
	//   "서비스가 Stop 전에 다 정리하면 된다" 로 넘길 수 없다. 브로드캐스트
	//   팬아웃의 마지막 참조가 언제 떨어지는지는 송신 완료 시점이 정하고,
	//   그건 종료와 경합한다. SharedPacketHandle 이 서버가 아니라 풀을 직접
	//   드는 것도 원래 이 문제를 피하려던 것이었는데, 그 풀 자체가 지워지니
	//   의도한 보호가 성립하지 않았다.
	//
	// 남겨 두면 무엇이 달라지는가
	//   TlsMemoryPool::Acquire / Release 는 맨 앞에서 m_initialized 를 본다.
	//   Finalize 만 해 두면 늦은 반납이 그 검사에 걸려 위반 한 줄을 남기고
	//   돌아간다. 크래시가 진단으로 바뀐다.
	//
	//   객체 자체는 수십 바이트다. 세그먼트는 Finalize 가 이미 전부 돌려주므로
	//   붙들리는 메모리는 없다. 실제 delete 는 소유자의 소멸자가 한다.
	inline void FinalizePools(EnginePools& pools)
	{
		if (pools.packet)    pools.packet->Finalize();
		if (pools.job)       pools.job->Finalize();
		if (pools.sendQueue) pools.sendQueue->Finalize();
	}

	namespace Detail
	{
		// 빈 목록형 풀 하나를 만든다.
		inline TlsMemoryPool* CreateBinSetPool(const PoolBinSetConfig& config,
			const char* poolName, uint32_t payloadAlignment = 16)
		{
			// 설정 배열을 풀이 받는 모양으로 옮긴다. 둘을 같은 타입으로
			// 두지 않는 이유는 서비스가 풀 헤더를 포함하지 않게 하려는 것이다.
			TlsMemoryPool::BinConfig binConfigs[PoolBinSetConfig::MAX_ENTRIES] = {};
			for (uint32_t i = 0; i < config.entryCount; ++i)
			{
				binConfigs[i].blockSize = config.entries[i].blockSize;
				binConfigs[i].blockCount = config.entries[i].blockCount;
			}

			TlsMemoryPool* pool = new (std::nothrow) TlsMemoryPool;
			if (!pool)
			{
				LOGE("failed to allocate the %s memory pool", poolName);
				return nullptr;
			}

			if (!pool->Initialize(binConfigs, config.entryCount, payloadAlignment))
			{
				LOGE("failed to initialize the %s memory pool", poolName);
				delete pool;
				return nullptr;
			}

			pool->SetCommitLimit(config.commitLimitBytes);
			return pool;
		}

		// 블록 크기가 고정된 풀 하나를 만든다.
		inline TlsMemoryPool* CreateReservePool(const PoolReserveConfig& config,
			uint32_t blockSize, const char* poolName, uint32_t payloadAlignment = 16)
		{
			const TlsMemoryPool::BinConfig binConfig{ blockSize, config.blockCount };

			TlsMemoryPool* pool = new (std::nothrow) TlsMemoryPool;
			if (!pool)
			{
				LOGE("failed to allocate the %s memory pool", poolName);
				return nullptr;
			}

			if (!pool->Initialize(&binConfig, 1, payloadAlignment))
			{
				LOGE("failed to initialize the %s memory pool", poolName);
				delete pool;
				return nullptr;
			}

			pool->SetCommitLimit(config.commitLimitBytes);
			return pool;
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
	inline bool CreatePools(const EnginePoolConfig& config, uint32_t maxRecvPacketSize,
		const char* roleName, EnginePools& outPools)
	{
		DestroyPools(outPools);

		if (!config.IsValid())
		{
			LOGE("[%s] the pool configuration is invalid (packet entries %u, "
				"job blocks %u, send queue blocks %u)",
				roleName, config.packet.entryCount,
				config.job.blockCount, config.sendQueue.blockCount);
			return false;
		}

		// Job 은 alignas(64) 라 페이로드도 64바이트 정렬이어야 한다.
		// 예전 풀은 16바이트만 보장해서 4개 중 1개만 실제로 정렬되어 있었다.
		constexpr uint32_t AlignedJobObjectSize =
			static_cast<uint32_t>((sizeof(Job) + 63) & ~static_cast<size_t>(63));

		outPools.job = Detail::CreateReservePool(config.job, AlignedJobObjectSize, "job", alignof(Job));
		if (!outPools.job)
		{
			DestroyPools(outPools);
			return false;
		}

		outPools.packet = Detail::CreateBinSetPool(config.packet, "packet");
		if (!outPools.packet)
		{
			DestroyPools(outPools);
			return false;
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
		if (outPools.packet->GetBinIndex(maxRecvPacketSize) >= outPools.packet->GetBinCount())
		{
			LOGE("[%s] the packet pool does not cover maxRecvPacketSize %u (largest bin is %u bytes). "
				"every packet above that size would bypass the pool and hit the heap",
				roleName, maxRecvPacketSize,
				outPools.packet->GetBinBlockSize(outPools.packet->GetBinCount() - 1));
			DestroyPools(outPools);
			return false;
		}

		outPools.sendQueue = Detail::CreateReservePool(
			config.sendQueue, static_cast<uint32_t>(SendPacketQueue::GetNodeSize()), "sendQueue");
		if (!outPools.sendQueue)
		{
			DestroyPools(outPools);
			return false;
		}

		// 기동 시점에 실제로 적용된 설정을 한 줄 남긴다.
		//
		// 설정이 밖에서 오게 되었으므로, 문제가 생겼을 때 "무엇으로 떴는가" 를
		// 로그만 보고 알 수 있어야 한다. 서비스 코드를 찾아 들어가지 않아도 된다.
		LOGI("[%s] memory pools ready : packet %u bins up to %u bytes (limit %lluMB), "
			"job %u blocks (limit %lluMB), "
			"sendQueue %u blocks (limit %lluMB)",
			roleName,
			outPools.packet->GetBinCount(), config.packet.LargestBlockSize(),
			static_cast<unsigned long long>(config.packet.commitLimitBytes >> 20),
			config.job.blockCount,
			static_cast<unsigned long long>(config.job.commitLimitBytes >> 20),
			config.sendQueue.blockCount,
			static_cast<unsigned long long>(config.sendQueue.commitLimitBytes >> 20));

		return true;
	}
}
