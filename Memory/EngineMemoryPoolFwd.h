#pragma once

// 엔진이 쓰는 메모리 풀 구현을 고르는 단일 스위치.
//
//   1 : TlsMemoryPool  - TLS 2단계 크기별 풀 (기본)
//   0 : SlabMemoryPool - 기존 전역 락 슬랩 풀
//
// 두 클래스는 공개 API 가 같으므로 이 값만 바꾸면 엔진 전체가 갈아탄다.
// A/B 측정을 할 때 이 값만 뒤집고 다시 빌드하면 된다.
//
// 측정 결과 (tools/poolbench, 32 논리 프로세서)
//   단일 스레드 왕복       21.0 ns/op   -> 10.3 ns/op       2.05배
//   8 스레드 동시 접근     11.7 M ops/s -> 665 M ops/s     56.8배
//   생산자/소비자 4:4       4.1 M ops/s -> 5.0 M ops/s      1.23배
//                          (이 구간은 벤치의 핸드오프 큐가 병목이라
//                           풀이 더 이상 병목이 아니라는 뜻으로 읽어야 한다)
//   Job 페이로드 64B 정렬  256개 중 64개 -> 256개 전부
#ifndef ENGINE_USE_TLS_MEMORY_POOL
#define ENGINE_USE_TLS_MEMORY_POOL 1
#endif

// 포인터로만 쓰는 자리를 위한 전방 선언용 헤더.
// 별칭이 가리키는 대상이 불완전 타입이어도 포인터 선언에는 문제가 없으므로,
// 예전의 전방 선언 자리를 그대로 대체할 수 있다.
// (Windows.h 를 끌어오지 않는다는 점이 이 헤더의 존재 이유다)
#if ENGINE_USE_TLS_MEMORY_POOL
class TlsMemoryPool;
using EngineMemoryPool = TlsMemoryPool;
#else
class SlabMemoryPool;
using EngineMemoryPool = SlabMemoryPool;
#endif
