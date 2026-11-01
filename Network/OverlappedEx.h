#pragma once

#include <WinSock2.h>

#include "../Session/SessionDefs.h"

// 세션이 I/O 종류별로 하나씩 들고 있는 overlapped.
// wsaOverlapped 가 반드시 첫 멤버여야 한다.
struct OverlappedEx
{
	WSAOVERLAPPED wsaOverlapped = {};
	WSABUF wsaBuffer = {};
	IO_OPERATION operation = IO_OPERATION::INVALID;
	uint32_t sessionId = INVALID_SESSION_ID;

	// operation 만 남기고 초기 상태로 되돌린다.
	// 세션이 유휴로 돌아갈 때(ResetSession / Finalize) 쓴다.
	// 주의: 이 구조체에 걸린 I/O 가 아직 대기 중이면 부르면 안 된다.
	// 대기 중에는 wsaOverlapped 의 소유권이 커널에 있다.
	void Clear()
	{
		const IO_OPERATION keptOperation = operation;

		*this = OverlappedEx{};

		operation = keptOperation;
	}

	// 다음 I/O 를 걸기 직전에 부른다.
	void ResetForNextIO(uint32_t id, char* buffer = nullptr, ULONG length = 0)
	{
		Clear();

		sessionId = id;
		wsaBuffer.buf = buffer;
		wsaBuffer.len = length;
	}
};
