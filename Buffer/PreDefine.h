#pragma once
#include <stdint.h>

constexpr static uint32_t MEMORY_SIZE_1K = 1024;
constexpr static uint32_t MEMORY_SIZE_2K = 1024 * 2;
constexpr static uint32_t MEMORY_SIZE_4K = 1024 * 4;
constexpr static uint32_t MEMORY_SIZE_8K = 1024 * 8;
constexpr static uint32_t MEMORY_SIZE_16K = 1024 * 16;
constexpr static uint32_t MEMORY_SIZE_32K = 1024 * 32;
constexpr static uint32_t MEMORY_SIZE_64K = 1024 * 64;
constexpr static uint32_t MEMORY_SIZE_128K = 1024 * 128;
constexpr static uint32_t MEMORY_SIZE_256K = 1024 * 256;
constexpr static uint32_t MEMORY_SIZE_512K = 1024 * 512;
constexpr static uint32_t MEMORY_SIZE_1024K = 1024 * 1024;

constexpr static uint32_t MEMORY_SIZE_1MB = MEMORY_SIZE_1024K;
constexpr static uint32_t MEMORY_SIZE_2MB = MEMORY_SIZE_1024K * 2;
constexpr static uint32_t MEMORY_SIZE_4MB = MEMORY_SIZE_1024K * 4;
constexpr static uint32_t MEMORY_SIZE_8MB = MEMORY_SIZE_1024K * 8;
constexpr static uint32_t MEMORY_SIZE_16MB = MEMORY_SIZE_1024K * 16;
constexpr static uint32_t MEMORY_SIZE_32MB = MEMORY_SIZE_1024K * 32;
constexpr static uint32_t MEMORY_SIZE_64MB = MEMORY_SIZE_1024K * 64;
constexpr static uint32_t MEMORY_SIZE_128MB = MEMORY_SIZE_1024K * 128;
constexpr static uint32_t MEMORY_SIZE_256MB = MEMORY_SIZE_1024K * 256;
constexpr static uint32_t MEMORY_SIZE_512MB = MEMORY_SIZE_1024K * 512;
constexpr static uint32_t MEMORY_SIZE_1024MB = MEMORY_SIZE_1024K * 1024;


constexpr static uint32_t BLOCK_COUNT_1K = 1024;
constexpr static uint32_t BLOCK_COUNT_2K = 1024 * 2;
constexpr static uint32_t BLOCK_COUNT_4K = 1024 * 4;
constexpr static uint32_t BLOCK_COUNT_8K = 1024 * 8;
constexpr static uint32_t BLOCK_COUNT_16K = 1024 * 16;
constexpr static uint32_t BLOCK_COUNT_32K = 1024 * 32;
constexpr static uint32_t BLOCK_COUNT_64K = 1024 * 64;


// PACKET_HEADER::packetSize 가 uint16_t 라 이 값을 넘는 패킷은 애초에
// 표현할 수 없다. 모든 크기 설정의 절대 상한이다.
constexpr static uint32_t PACKET_SIZE_LIMIT = 65535;

// 세션당 수신 링과 송신 큐 크기는 SessionBufferConfig 로 옮겼다.
// 역할마다 적정값이 한 자릿수 배 이상 벌어져서 상수 하나로 덮을 수 없다.
// (Buffer/SessionBufferConfig.h 참고)
// 송신 큐 엔트리(SendPacketEntry) 풀의 초기 블록 수.
//
// 상한이 아니라 출발점이다. 모자라면 EngineMemoryPool 이 런타임에 확장하고,
// 그 사실은 LogStats("sendQueue") 의 grow 값으로 드러난다.
//
// 예전 이름은 HYBRID_SEND_PACKET_POOL_SIZE 였고 그때는 진짜 상한이었다.
// 총량을 샤드로 쪼개 세션 아이디로 고정 배정했으므로, 이 값을 늘리는 것
// 말고는 한 세션의 송신 상한을 올릴 방법이 없었다.
constexpr static uint32_t SEND_QUEUE_ENTRY_COUNT = BLOCK_COUNT_64K;

// 풀별 커밋 상한의 기본값.
//
// 상한이 없으면 재고가 마를 때마다 세그먼트를 새로 잡고, 거부당하는 조건은
// OS 가 메모리를 못 주는 것뿐이다. 잡 큐에 깊이 상한이 없으므로 소비자가
// 느려지면 그대로 OOM 까지 간다.
//
// 이 값들은 "정상 운영이라면 절대 닿지 않는 곳" 에 둔다. 튜닝 손잡이가
// 아니라 폭주 차단기다. 실측 기준선(bench, 8세션 부하)은 이렇다.
//   packet   32.5MB 커밋 (10~11개 빈 선할당)
//   job       2.2MB
//   sendQueue 5.0MB
// 선할당의 8~16배를 상한으로 잡으면, 정상 버스트로는 닿지 않고 폭주는 막힌다.
//
// 서비스가 더 필요하면 StartServer 뒤에 SetCommitLimit 으로 올리면 된다.
// 0 을 주면 예전처럼 무제한이다.
constexpr static uint64_t POOL_COMMIT_LIMIT_PACKET = 512ull * 1024 * 1024;  // 512MB
constexpr static uint64_t POOL_COMMIT_LIMIT_JOB = 64ull * 1024 * 1024;   //  64MB
constexpr static uint64_t POOL_COMMIT_LIMIT_SENDQUEUE = 128ull * 1024 * 1024;  // 128MB
constexpr static uint32_t INFERENCE_CHUNK_SIZE = MEMORY_SIZE_4K;