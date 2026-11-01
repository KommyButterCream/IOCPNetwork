#pragma once

// 엔진이 쓰는 메모리 풀의 완전한 정의.
// 스위치와 전방 선언은 EngineMemoryPoolFwd.h 에 있다.

#include "EngineMemoryPoolFwd.h"

#if ENGINE_USE_TLS_MEMORY_POOL
#include "TlsMemoryPool.h"
#else
#include "SlabMemoryPool.h"
#endif
