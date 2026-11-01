#pragma once

#include <stdint.h>

#include "BaseSession.h"
#include "../Network/OverlappedEx.h"

class AcceptSession final : public BaseSession
{
public:
	AcceptSession();
	~AcceptSession() override;

private:
	OverlappedEx m_acceptOverlapped = {};
	char m_acceptBuffer[sizeof(SOCKADDR_IN) + 16 + sizeof(SOCKADDR_IN) + 16] = {};

	// 이 슬롯을 지금 누가 쓰고 있는가. AcceptEx 를 걸 권리다.
	//
	// AcceptEx 를 다시 거는 주체가 둘이다 — accept 완료를 처리하는 IOCP
	// 워커와, 비어 버린 슬롯을 채우는 주기 점검(RefillAcceptSlots).
	// 예전에는 둘 다 GetAcceptSessionState() == ACCEPT_READY 만 보고
	// 판단했는데, 그 상태는 평범한 멤버라 원자적이지 않다.
	//
	// HandleAccept 의 거절 경로는 ResetSession(상태가 ACCEPT_READY 가 된다)
	// 을 지나고 나서 PostAccept 를 부른다. 그 사이에 주기 점검이 같은 슬롯을
	// 비었다고 읽으면 둘이 함께 건다. 같은 OverlappedEx 로 AcceptEx 가 두 번
	// 걸리고 WSASocket 도 두 번 불려 앞 핸들이 덮인다.
	// (실측: 창을 300ms 로 넓히면 슬롯 16개가 전부 32개로 걸렸다)
	//
	// 그래서 상태가 아니라 이 플래그가 유일한 근거다.
	volatile LONG m_slotOwned = 0;

public:
	bool Initialize(SESSION_ROLE sessionType, uint32_t sessionId) override;
	void ResetSession() override;

	// 완료된 AcceptEx 의 카운트를 부르는 쪽이 아직 들고 있는 상태에서
	// 이 슬롯을 되돌린다.
	//
	// ResetSession 을 쓸 수 없다. 그쪽은 정리 경로용이라 "미완료 I/O 가 없다"
	// 를 전제하고, 남아 있으면 위반을 남긴다. 그런데 완료 핸들러 안의 거절
	// 경로(주소당 제한 / 세션 풀 고갈 / 풀 가득 참)는 방금 완료된 AcceptEx 의
	// 몫을 함수 끝까지 들고 있어야 한다 — 그 카운트가 종료 시의 취소 대기를
	// 막는 장벽이기 때문이다. 그래서 그 자리에서 ResetSession 을 부르면
	// 정상적인 거절 1건마다 위반이 하나씩 쌓인다.
	// (실측 tools/churnbp : 접속 거절 28건에 "reset while 1 IO operations are
	//  still outstanding" 28건)
	//
	// 그리고 ResetSession 은 여기서 불리면 안 되는 일을 하나 더 한다 —
	// m_cancelIo 를 0 으로 되돌린다. 종료 절차가 막 취소를 걸어 둔 슬롯에서
	// 이게 지워지면 마지막 DecrementIO 가 완료 이벤트를 세우지 않아
	// WaitForAllAcceptIOCancelComplete 가 10초를 태운다.
	//
	// 그래서 다음 걸기까지 들고 갈 이유가 없는 것만 지운다. 상태와
	// OVERLAPPED 는 PostAccept 가 어차피 다시 세운다.
	void ResetForRepost();
	void Finalize() override;

	bool OnAccept() override;
	bool OnConnect() override;
	bool OnDisconnect() override;

	// 슬롯을 가져간다. 이긴 쪽만 PostAccept 를 부를 수 있다.
	//
	// 소유권은 발행자에게서 그 발행의 완료를 처리하는 쪽으로 그대로 넘어간다.
	// 완료는 발행당 정확히 한 번이므로 인계에 별도 조치가 필요 없고,
	// 그래서 HandleAccept 는 이미 가진 소유권으로 곧바로 다시 걸면 된다.
	//
	// 놓는 시점은 슬롯이 실제로 비는 순간뿐이다 — PostAccept 가 실패했거나,
	// 취소된 AcceptEx 를 처리했거나, 종료 경로에서 다시 걸지 않을 때.
	bool TryAcquireSlot();
	void ReleaseSlot();

	OverlappedEx& GetAcceptOverlapped();
	char* GetAcceptBuffer();
};
