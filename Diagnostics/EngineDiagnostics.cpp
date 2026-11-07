#include "EngineDiagnostics.h"

#include <atomic> // for std::atomic, std::memory_order_acquire

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <Windows.h>

namespace
{
	// 프로세스 전역이다. 엔진 인스턴스가 여럿이어도 하나로 센다.
	// 하네스는 시나리오 하나를 한 번에 하나씩 돌리므로 이걸로 충분하고,
	// 인스턴스별로 나누면 "어느 인스턴스도 아닌" 정적 경로의 위반을 놓친다.
	std::atomic<LONG64> g_violationCount{0};
}

namespace Engine
{
	namespace Diagnostics
	{
		void NoteViolation()
		{
			g_violationCount.fetch_add(1);
		}

		uint64_t GetViolationCount()
		{
			return static_cast<uint64_t>(g_violationCount.load(std::memory_order_acquire));
		}

		void ResetViolationCount()
		{
			g_violationCount.store(0);
		}
	}
}
