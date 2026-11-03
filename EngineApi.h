#pragma once
// 엔진 DLL을 빌드할 때만 BUILD_IOCP_ENGINE_DLL을 정의합니다.
#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif
