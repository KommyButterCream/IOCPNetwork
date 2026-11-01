#include "AcceptSession.h"

#include <WinSock2.h>

#include "../Diagnostics/EngineAssert.h"

#include "../../Core/Util/Logger.h"

using namespace Core::Util;

#pragma comment(lib, "ws2_32.lib")

AcceptSession::AcceptSession()
{
	m_acceptOverlapped.operation = IO_OPERATION::ACCEPT;
}

AcceptSession::~AcceptSession()
{
	Finalize();
}

bool AcceptSession::Initialize(SESSION_ROLE sessionType, uint32_t sessionId)
{
	if (!BaseSession::Initialize(sessionType, sessionId))
		return false;

	::InterlockedExchange(&m_slotOwned, 0);

	SetAcceptSessionState(AcceptSessionState::ACCEPT_READY);
	return true;
}

void AcceptSession::ResetSession()
{
	BaseSession::ResetSession();
	SetAcceptSessionState(AcceptSessionState::ACCEPT_READY);

	// m_slotOwned 는 일부러 건드리지 않는다.
	//
	// 이 함수는 다시 걸기 직전에도 불린다. 여기서 소유권을 놓으면 바로
	// 그 순간 주기 점검이 같은 슬롯을 가져가 함께 걸 수 있다. 그게 이
	// 플래그를 만든 이유다. 세션 상태와 슬롯 소유권은 수명이 다르다.

	::ZeroMemory(&m_acceptBuffer, sizeof(m_acceptBuffer));

	m_acceptOverlapped.Clear();
}

void AcceptSession::ResetForRepost()
{
	// BaseSession::ResetSession 을 부르지 않는다. (이유는 헤더 주석)
	//
	// m_closing 은 지운다. OnDisconnect 의 "한 번만" 게이트가 이 플래그라,
	// 1 로 굳으면 다음 주기의 "소켓을 떼지 않고 종료한다" 진단이 영영 울리지
	// 않는다.
	::InterlockedExchange(&m_closing, 0);

	// m_cancelIo / m_releasePending / m_ioCount 는 건드리지 않는다.
	// accept 슬롯은 예약 반납을 쓰지 않고, 취소 플래그는 종료 절차의 것이다.

	SetAcceptSessionState(AcceptSessionState::ACCEPT_READY);

	// m_slotOwned 는 ResetSession 과 같은 이유로 건드리지 않는다.
	::ZeroMemory(&m_acceptBuffer, sizeof(m_acceptBuffer));

	m_acceptOverlapped.Clear();
}

void AcceptSession::Finalize()
{
	if (m_destroyFlag)
	{
		return;
	}

	BaseSession::Finalize();

	::InterlockedExchange(&m_slotOwned, 0);

	::ZeroMemory(&m_acceptBuffer, sizeof(m_acceptBuffer));

	m_acceptOverlapped.Clear();

	m_destroyFlag = true;
}

bool AcceptSession::OnAccept()
{
	SetAcceptSessionState(AcceptSessionState::ACCEPT_COMPLETE);

	// 접속 1건당 호출된다. HandleAccept 가 같은 사실을 더 자세히 남기므로
	// 여기서는 추적 레벨로만 둔다.
	LOGT("accept session %u complete", GetSessionID());

	return true;
}

bool AcceptSession::OnConnect()
{
	// accept 세션은 접속을 거는 쪽이 아니다. OnConnect 를 부르는 곳은
	// ConnectEx 완료 처리(IOCPClient)와 accept 완료 처리(IOCPServer)뿐이고
	// 둘 다 ClientSession 을 상대한다. 여기 왔다면 라우팅이 깨진 것이다.
	//
	// 예전에는 조용히 false 만 돌려줬다. 호출부는 그걸 "접속 훅이 실패했다"
	// 로 읽고 연결을 끊는데, 진짜 원인인 잘못된 라우팅은 아무 데도 남지 않는다.
	ENGINE_VIOLATION("accept session %u OnConnect was called. accept sessions never connect",
		GetSessionID());

	return false;
}

bool AcceptSession::OnDisconnect()
{
	if (::InterlockedExchange(&m_closing, 1) == 0)
	{
		LOGT("accept session %u disconnecting", GetSessionID());

		if (!IsSocketInvalid())
		{
			// 소켓은 반드시 DetachSocket 으로 먼저 떼어낸 뒤 여기 와야 한다.
			// 그러지 않으면 소켓이 닫히지 않고 새는 상태로 세션이 회수된다.
			LOGE("accept session %u disconnecting with a live socket %d. it was not detached",
				GetSessionID(), static_cast<int>(GetClientSocket()));
			ENGINE_BREAK_IF_DEBUGGER();
			return false;
		}

		if (GetAcceptSessionState() != AcceptSessionState::ACCEPT_ABORTED)
		{
			SetAcceptSessionState(AcceptSessionState::DISCONNECTED);
		}
	}

	return true;
}

bool AcceptSession::TryAcquireSlot()
{
	return ::InterlockedCompareExchange(&m_slotOwned, 1, 0) == 0;
}

void AcceptSession::ReleaseSlot()
{
	if (::InterlockedExchange(&m_slotOwned, 0) == 0)
	{
		// 갖고 있지 않은 슬롯을 놓았다. 소유권 인계가 어긋났다는 뜻이라
		// 조용히 넘기면 다음에 둘이 함께 거는 상태로 이어진다.
		ENGINE_VIOLATION("accept session %u released a slot it did not own", GetSessionID());
	}
}

OverlappedEx& AcceptSession::GetAcceptOverlapped()
{
	return m_acceptOverlapped;
}

char* AcceptSession::GetAcceptBuffer()
{
	return m_acceptBuffer;
}
