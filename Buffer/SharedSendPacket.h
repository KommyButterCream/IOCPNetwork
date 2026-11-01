#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <Windows.h>

#include "SendPacketEntry.h"

#include "../Memory/TlsMemoryPool.h"

#include <new>   // placement new

// 한 버퍼를 여러 세션이 함께 보낼 때(브로드캐스트) 쓰는 참조 계수.
//
// 엔진의 계약은 바뀌지 않는다.
//
//   SendPacketQueue 는 여전히 참조 계수를 모른다. 마지막 사용자가
//   SendPacketReleaseFunc 를 부를 뿐이고, 그 안에서 무슨 일이 일어나는지는
//   소유자 사정이다. 그 결정(SendPacketEntry.h 주석 참고)은 그대로 둔다.
//
//   여기 있는 것은 그 콜백의 기성 구현이다. 쓰고 싶으면 쓰고, 다른 방식으로
//   소유하고 싶으면 자기 콜백을 넘기면 된다. MEMORY_POOL:: 헬퍼가 풀에 대해
//   갖는 관계와 같다 — 엔진도 쓰고 서비스도 직접 쓴다.
//
// 왜 서비스에 두지 않는가
//
//   DesktopStreamingApp 이 이걸 손으로 구현해서 쓰고 있었고, 회계 자체는
//   정확했다. 문제는 두 가지였고 둘 다 서비스가 잘못 짠 것이 아니라
//   엔진 자원에 닿을 방법이 없어서 생긴 것이었다.
//
//   1) 청크마다 new / delete 를 했다. 프레임 하나가 청크 N개로 쪼개지므로
//      초당 수백 번의 힙 왕복이다. 엔진이 TlsMemoryPool 로 패킷 경로에서
//      걷어낸 힙이 브로드캐스트 팬아웃 지점에서 그대로 돌아왔다.
//
//   2) 서버 객체를 들고 있다가 반납 시점에 그쪽 GetPacketMemoryPool() 을
//      불렀다. 늦게 도착한 반납이 StopServer 뒤면 죽은 풀을 역참조한다.
//      (엔진의 SendPacketQueue::ReleaseEntry 는 같은 상황을 위반으로
//       명시해 두고 있다 — "the pools are gone")
//
//   그래서 여기서는 서버가 아니라 풀 자체를 들고, 자기 자신도 풀에서 받는다.
//
// 참조 계수 규약
//
//   Create 가 1 로 시작한다. 이 1 은 "배포하는 쪽" 의 몫이다.
//   세션에 넘길 때마다 AddRef, 넘기지 못했으면 그 자리에서 Release.
//   다 배포하고 나면 마지막에 자기 몫을 Release 한다.
//
//     SharedSendPacket* shared = SHARED_SEND_PACKET::Create(entryPool, packetPool, packet);
//     for (session : targets)
//     {
//         SHARED_SEND_PACKET::AddRef(shared);
//         if (!session->EnqueueSharedSendPacket(packet, size,
//                 SHARED_SEND_PACKET::ReleaseCallback, shared))
//             SHARED_SEND_PACKET::Release(shared);
//     }
//     SHARED_SEND_PACKET::Release(shared);   // 배포자 몫
//
//   마지막 Release 를 빠뜨리면 패킷이 영영 반납되지 않는다. 그건
//   LogStats 의 live 가 0 으로 돌아오지 않는 것으로 드러난다.

struct SharedSendPacket
{
	volatile LONG refCount = 1;

	// 공유되는 패킷. packetPool 에서 나왔고 마지막 참조가 그쪽으로 되돌린다.
	const void* packet = nullptr;

	// 서버 객체가 아니라 풀을 직접 든다. 수명이 서버에 묶이지 않는다.
	TlsMemoryPool* packetPool = nullptr;

	// 이 구조체 자신이 나온 풀. new / delete 를 쓰지 않기 위한 것이다.
	TlsMemoryPool* selfPool = nullptr;
};

namespace SHARED_SEND_PACKET
{
	// selfPool 은 이 구조체를 담을 풀이다.
	//
	// 송신 큐 엔트리 풀(IOCPServer::GetSendQueueMemoryPool)을 권한다.
	// 이미 송신 경로 부기 전용이고 64바이트 빈을 갖고 있어 이 구조체가
	// 그대로 들어간다. 송신 부기 객체 전용 풀을 사용한다.
	inline SharedSendPacket* Create(TlsMemoryPool& selfPool, TlsMemoryPool& packetPool, const void* packet)
	{
		if (!packet)
			return nullptr;

		void* memory = selfPool.Acquire(sizeof(SharedSendPacket));
		if (!memory)
			return nullptr;

		// 풀에서 나온 블록은 앞선 사용자의 값을 그대로 들고 있다.
		// 기본 멤버 초기자로 전부 덮는다 (refCount 가 1 이 되는 자리이기도 하다).
		SharedSendPacket* shared = new (memory) SharedSendPacket();

		shared->packet = packet;
		shared->packetPool = &packetPool;
		shared->selfPool = &selfPool;

		return shared;
	}

	inline void AddRef(SharedSendPacket* shared)
	{
		if (!shared)
			return;

		::InterlockedIncrement(&shared->refCount);
	}

	inline void Release(SharedSendPacket* shared)
	{
		if (!shared)
			return;

		if (::InterlockedDecrement(&shared->refCount) != 0)
			return;

		// 마지막 참조다. 패킷을 먼저 되돌리고 자기를 되돌린다.
		//
		// 패킷을 할당한 풀에 직접 반납한다.
		if (shared->packet && shared->packetPool)
		{
			shared->packetPool->Release(shared->packet);
		}

		TlsMemoryPool* selfPool = shared->selfPool;

		shared->packet = nullptr;
		shared->packetPool = nullptr;
		shared->selfPool = nullptr;

		if (selfPool)
		{
			selfPool->Release(shared);
		}
	}

	// SendPacketReleaseFunc 시그니처 그대로다. 서비스가 정적 함수를 다시
	// 쓰지 않아도 되도록 여기 둔다.
	//
	// packetData 는 보지 않는다. 어느 패킷인지는 context 가 이미 알고 있고,
	// 세션이 넘겨주는 주소와 같아야 한다.
	inline void ReleaseCallback(const void* packetData, void* context)
	{
		(void)packetData;

		Release(static_cast<SharedSendPacket*>(context));
	}
}
