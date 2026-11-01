#pragma once

#include <stdint.h>

// 공유 패킷의 반납 진입점.
//
// 한 버퍼를 여러 세션이 함께 보내는 경우(브로드캐스트) 마지막 사용자가
// 실제 소유자에게 되돌려 주어야 한다. 그 방법은 소유자만 알고 있으므로
// 엔트리에 함께 실어 둔다.
using SendPacketReleaseFunc = void (*)(const void* packetData, void* context);

// 송신 대기 중인 패킷 하나를 가리키는 서술자.
// 내용을 복사하지 않고 패킷 풀의 주소를 그대로 들고 있는다.
//
// 예전 이름은 SendPacketBuffer 였고 SLIST_ENTRY 를 상속했다. 전용 고정
// 크기 풀(SendPacketPool)이 프리 리스트를 SList 로 들고 있었기 때문이다.
// 그 구조에는 두 가지 결함이 있었다.
//
//   - 세션이 쓸 수 있는 서술자가 설정한 큐 깊이와 무관했다.
//     HybridSendPacketPool 이 총량을 논리 프로세서 수만큼의 샤드로 나누고
//     세션 아이디로 샤드를 고정 배정했으므로, 실제 상한은 "총량 / 샤드 수"
//     를 그 샤드에 배정된 세션들이 나눠 쓴 몫이었다. 실측(tools/sendbench
//     Case 1)에서 설정값 4096 은 세션이 하나일 때조차 도달하지 못했고,
//     같은 샤드에 313개가 몰리면 세션당 6~7개였다.
//   - 한 샤드가 마른 순간에도 다른 샤드에 남은 재고(97%)를 쓸 수 없었다.
//
// 이제는 EngineMemoryPool(TlsMemoryPool)에서 엔트리를 받는다. 그 풀은 이미
// 스레드별 캐시로 샤딩되어 있고 — 할당하는 스레드 축이다. 세션 아이디보다
// 올바른 축이다 — 재고는 전역에 공유되며 부족하면 런타임에 확장한다.
// 그래서 SList 상속과 그것이 요구하던 정렬 제약이 함께 사라졌다.
//
// 이중 반납 가드(volatile LONG inUse 의 CAS)도 사라졌다. 같은 일을
// TlsMemoryPool 의 MAGIC_LIVE / MAGIC_FREE 블록 헤더가 이미 하고 있고,
// 그쪽은 "어느 풀에서 나온 블록인가" 까지 본다.
struct SendPacketEntry
{
	// 세션의 송신 큐가 이 필드로 엮인다.
	//
	// 큐가 포인터 배열을 따로 잡지 않는 이유다. 예전에는 깊이 x 8바이트를
	// 세션마다 기동 시점에 전부 잡았고(4096 이면 32KB), 한 칸도 쓰지 않는
	// 세션도 같은 값을 냈다.
	SendPacketEntry* next = nullptr;

	const char* packetData = nullptr;
	uint32_t packetSize = 0;
	SendPacketReleaseFunc releaseFunc = nullptr;
	void* releaseContext = nullptr;

	inline void Reset() noexcept
	{
		next = nullptr;
		packetData = nullptr;
		packetSize = 0;
		releaseFunc = nullptr;
		releaseContext = nullptr;
	}
};
