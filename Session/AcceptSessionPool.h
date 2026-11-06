#pragma once

#include <stdint.h>
#include <memory> // for std::unique_ptr

#include "ISessionEvent.h"

class AcceptSession;

class AcceptSessionPool
{
public:
	[[nodiscard]] static std::unique_ptr<AcceptSessionPool> Create(uint32_t capacity);
	~AcceptSessionPool();

	AcceptSession* GetSession(uint32_t sessionId);
	uint32_t GetSessionCount() const;

	// 발행부터 완료 핸들러 종료까지 남은 Accept I/O를 풀 전체에서 합산한다.
	uint32_t GetAcceptIOCount() const;

	void CancelAllAcceptIORequests();

	// 각 수락 세션에 timeout_ms를 적용하고, 모든 세션의 완료 여부를 확인한다.
	bool WaitForAllAcceptIOComplete(uint32_t timeout_ms);
	void SetSocketCloseFunc(CloseSocketFunc closeSocketFunc);

private:
	AcceptSessionPool(std::unique_ptr<AcceptSession[]> sessions, uint32_t capacity);
	void CloseRemainingAcceptSockets();
	void CloseAcceptSocket(uintptr_t socket);

	std::unique_ptr<AcceptSession[]> m_sessions;
	uint32_t m_capacity;
	CloseSocketFunc m_closeSocketFunc = nullptr;
};
