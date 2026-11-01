#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <Windows.h>

#include "../../Core/Util/Logger.h"
#include "EngineDiagnostics.h"

// ---------------------------------------------------------------------------
// 엔진 불변식 위반 처리 정책
//
// 이전에는 위반 지점마다 __debugbreak() 를 두었다. 이 코드는 릴리스 빌드에도
// 그대로 남으므로, 디버거가 붙어 있지 않으면 STATUS_BREAKPOINT(0xC0000003) 로
// 호스트 프로세스가 즉사한다. 문제가 두 가지였다.
//
//   1) 라이브러리가 호스트 프로세스를 죽인다.
//      엔진은 DLL 이고, 내부 상태가 이상하다는 이유로 호스트를 끝내는 것은
//      라이브러리의 권한이 아니다. 호출부가 실패를 처리할 기회를 주어야 한다.
//
//   2) 원인을 남기지 못한다.
//      부하 테스트에서 프로세스가 사라지면 stdio 버퍼도 함께 날아가서
//      "어디서 죽었는지" 조차 알 수 없었다. 실제로 이 문제 때문에
//      벤치에 벡터드 예외 핸들러를 붙여 심볼을 찍어야 했다.
//
// 새 정책
//   - 위반은 항상 로그로 남긴다. 원인 추적이 가능해야 한다.
//   - 디버거가 붙어 있으면 그 자리에서 중단한다. 개발 중 조사는 그대로 유지.
//   - 디버거가 없으면 계속 진행하고, 호출부가 반환값으로 실패를 처리한다.
// ---------------------------------------------------------------------------

// 디버거가 붙어 있을 때만 중단한다.
#define ENGINE_BREAK_IF_DEBUGGER()                     \
	do {                                               \
		if (::IsDebuggerPresent()) { __debugbreak(); } \
	} while (0)

// 불변식 위반. 로그를 남기고 디버거가 있으면 중단한다. 흐름은 계속된다.
//
// 계수기도 함께 올린다. 로그만 남기던 시절에는 하네스가 위반을 판정에
// 쓸 수 없었다. 위반이 쏟아지는 실행도 테스트는 PASS 로 끝냈고, 사람이
// 로그를 grep 할 때만 드러났다.
#define ENGINE_VIOLATION(...)                    \
	do {                                         \
		Engine::Diagnostics::NoteViolation();    \
		LOGE(__VA_ARGS__);                       \
		ENGINE_BREAK_IF_DEBUGGER();              \
	} while (0)

// 복구 불가능한 손상(메모리 훼손 등). 반드시 남아야 하므로 LOGC 를 쓴다.
// 프로세스를 죽이지는 않는다. 죽여도 로그 이상의 정보를 얻지 못한다.
#define ENGINE_CORRUPTION(...)                   \
	do {                                         \
		Engine::Diagnostics::NoteViolation();    \
		LOGC(__VA_ARGS__);                       \
		ENGINE_BREAK_IF_DEBUGGER();              \
	} while (0)

// 조건이 거짓이면 위반으로 처리한다.
#define ENGINE_CHECK(cond, ...)                     \
	do {                                            \
		if (!(cond)) { ENGINE_VIOLATION(__VA_ARGS__); } \
	} while (0)

// 조건이 거짓이면 위반 처리 후 지정한 값을 반환한다.
#define ENGINE_CHECK_RET(cond, ret, ...)                \
	do {                                                \
		if (!(cond)) {                                  \
			ENGINE_VIOLATION(__VA_ARGS__);              \
			return ret;                                 \
		}                                               \
	} while (0)

// void 함수용.
#define ENGINE_CHECK_RETVOID(cond, ...)                 \
	do {                                                \
		if (!(cond)) {                                  \
			ENGINE_VIOLATION(__VA_ARGS__);              \
			return;                                     \
		}                                               \
	} while (0)
