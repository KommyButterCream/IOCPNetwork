#pragma once

#include <stdint.h>
#include "ISessionEvent.h"

class AcceptSession;

class AcceptSessionPool
{
private:
	AcceptSession* m_sessions = nullptr;		 // 주소가 연속된 Session 객체 배열 생성
	uint32_t m_capacity = 0;
	CloseSocketFunc m_closeSocketFunc = nullptr;

	// 생성자는 실패를 반환할 수 없다. 세션 하나라도 준비에 실패하면 여기가
	// false 로 남고, SessionManager 가 그걸 보고 초기화를 실패시킨다.
	// (ClientSessionPool 과 같은 방식)
	bool m_ready = false;

public:
	explicit AcceptSessionPool(uint32_t capacity);
	~AcceptSessionPool();

	bool IsReady() const { return m_ready; }

	// 세션 아이디로 풀의 세션 직접 접근
	AcceptSession* GetSession(const uint32_t sessionId);

	// 세션 수량 반환
	uint32_t GetSessionCount() const;

	// 이 풀의 accept 세션들이 들고 있는 미완료 I/O 총합.
	// 정상 종료 후에는 0 이어야 한다. 하네스의 판정 근거다.
	uint32_t GetOutstandingIOCount() const;

	// 세션 연결 해제
	//
	// 이 둘의 소유권은 IOCPServer::StopServer 에 있다. 소멸자는 부르지 않는다.
	// (이유는 AcceptSessionPool.cpp 의 소멸자 주석 참고)
	void RequestAllAcceptIOCancel();
	bool WaitForAllAcceptIOCancelComplete(const uint32_t timeout_ms);

	// Session Close 함수 포인터 설정
	// closesocket 을 하나의 함수에서만 수행되도록 강제!
	void SetSocketCloseFunc(CloseSocketFunc closeSocketFunc);

private:
	// 소멸 시점에 남아 있는 소켓을 보고하고 닫는다. 기다리지는 않는다.
	void CloseLeftoverSockets();
};
