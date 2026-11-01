#pragma once

#include <stdint.h>

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

// ---------------------------------------------------------------------------
// 엔진 자기 진단 계수기
//
// ENGINE_VIOLATION 은 불변식이 깨진 자리를 로그로 남긴다. 그런데 로그는
// 사람이 읽는 물건이라, 하네스가 "이번 시나리오에서 위반이 있었는가" 를
// 판정에 쓸 수 없었다. 실제로 지금까지는 실행 후 로그를 grep 해서 눈으로
// 확인했고, 그래서 위반이 나도 테스트는 PASS 로 끝났다.
//
// 숫자로 물을 수 있게 한다. 올라가는 곳이 위반 경로뿐이므로 정상 경로의
// 비용은 0 이다.
// ---------------------------------------------------------------------------

namespace Engine
{
	namespace Diagnostics
	{
		// ENGINE_VIOLATION / ENGINE_CORRUPTION 이 부른다. 직접 부를 일은 없다.
		IOCP_ENGINE_API void NoteViolation();

		// 지금까지 누적된 위반 수.
		IOCP_ENGINE_API uint64_t GetViolationCount();

		// 시나리오 경계에서 0 으로 되돌린다. 하네스 전용이다.
		IOCP_ENGINE_API void ResetViolationCount();
	}
}
