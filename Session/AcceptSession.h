#pragma once

#include "../Network/OverlappedEx.h"
#include <MSWSock.h> // for LPFN_ACCEPTEX

class AcceptSession final
{
public:
	enum class State { NONE, ACCEPT_READY, ACCEPT_WAIT, ACCEPT_COMPLETE, ACCEPT_ABORTED };
	static constexpr DWORD AddressLength = sizeof(SOCKADDR_IN) + 16;

	AcceptSession();
	~AcceptSession();
	AcceptSession(const AcceptSession&) = delete;
	AcceptSession& operator=(const AcceptSession&) = delete;

	[[nodiscard]] bool Initialize(uint32_t sessionId);
	[[nodiscard]] uint32_t GetSessionID() const noexcept;
	[[nodiscard]] State GetState() const;
	void SetState(State state);

	bool TryAcquireSession();
	void ReleaseSession();
	void ResetForRepost();
	char* GetAcceptBuffer();

	[[nodiscard]] int PostAcceptSocket(SOCKET listenSocket, SOCKET acceptSocket, LPFN_ACCEPTEX acceptEx);
	[[nodiscard]] SOCKET DetachAcceptSocket();
	[[nodiscard]] SOCKET StopPostingAndDetachAcceptSocket();

	void CompleteAcceptIO();
	[[nodiscard]] LONG GetAcceptIOCount() const;
	[[nodiscard]] IOWaitResult WaitForAcceptIOComplete(uint32_t timeout_ms) const;

private:
	struct AcceptRequest
	{
		OverlappedEx overlapped{};
		char addressBuffer[2 * AddressLength]{};
	};

	uint32_t m_sessionId = INVALID_SESSION_ID;
	volatile LONG m_state = static_cast<LONG>(State::NONE);
	// 완료 워커와 주기 점검 중 한 쪽만 다음 요청을 발행할 수 있다.
	volatile LONG m_sessionOwned = 0;
	
	AcceptRequest m_request{};
	mutable SRWLOCK m_acceptSocketLock = SRWLOCK_INIT;
	SOCKET m_acceptSocket = INVALID_SOCKET;
	bool m_acceptPostingStopped = false;
	LONG m_acceptIOCount = 0; // 발행부터 완료 핸들러가 끝날 때까지 남아 있는 건수
	HANDLE m_acceptIOCompleteEvent = nullptr;
};
