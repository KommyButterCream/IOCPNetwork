#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <Windows.h>

class ClientSession;

// 세션이 풀에 대해 가지는 소유권 상태.
// ClientSessionPool 이 "이 세션을 반납해도 되는가" 를 판단하는 유일한 근거이며
// 반드시 Interlocked 연산으로만 전이시킨다.
enum SESSION_POOL_STATE : LONG
{
	SESSION_POOL_FREE = 0,      // 프리 리스트에 있음. Acquire 로 임대 가능
	SESSION_POOL_IN_USE = 1,      // 임대되어 사용 중
	SESSION_POOL_RELEASING = 2,   // 반납 처리 중. IN_USE -> RELEASING 전이를 이긴 스레드 한 명만 진입
};

struct SessionNode
{
	// 이 노드는 ClientSessionPool 만 쓴다. 담기는 것도 ClientSession 뿐이라
	// 기반 타입으로 둘 이유가 없다. 좁혀 두면 임대/반납 경로에서
	// 다시 구체 타입으로 되돌리는 캐스팅이 사라진다.
	ClientSession* session = nullptr;
	SessionNode* nextNode = nullptr;    // 프리 리스트의 다음 노드

	// 풀 소유권 상태. Release 의 멱등성을 보장하는 게이트 역할을 한다.
	volatile LONG poolState = SESSION_POOL_FREE;

	SessionNode() = default;

	~SessionNode()
	{
		session = nullptr;
		nextNode = nullptr;
	}
};
