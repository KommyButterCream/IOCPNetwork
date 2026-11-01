#include "BaseSession.h"

#include "SessionDefs.h"

#include "../Diagnostics/EngineAssert.h"

#include "../../Core/Util/Logger.h"

using namespace Core::Util;

BaseSession::BaseSession()
{
}

BaseSession::~BaseSession()
{
	Finalize();
}

bool BaseSession::Initialize(SESSION_ROLE sessionType, uint32_t sessionId)
{
	m_sessionRole = sessionType;
	SetSessionID(sessionId);

	if (!m_ioCancelCompleteEvent)
	{
		m_ioCancelCompleteEvent = ::CreateEvent(nullptr, TRUE, FALSE, nullptr);
		if (!m_ioCancelCompleteEvent)
			return false;
	}
	else
	{
		::ResetEvent(m_ioCancelCompleteEvent);
	}

	return true;
}

void BaseSession::ResetSession()
{
	SetClientSessionState(ClientSessionState::NONE);
	SetServerSessionState(ServerSessionState::NONE);
	SetAcceptSessionState(AcceptSessionState::NONE);

	// 지난 접속의 종료 사유를 지운다. 남겨 두면 이 슬롯을 다음에 쓰는
	// 접속이 끝날 때 남의 사유를 물려받는다.
	//
	// 서비스 통지보다 뒤여야 한다. ClientSessionPool::CompleteRelease 가
	// NotifyServiceDisconnect -> OnDisconnect -> ResetSession 순으로 부르므로
	// 지금 순서는 맞다.
	ClearDisconnectReason();

	if (m_clientSocket != INVALID_SOCKET)
	{
		ENGINE_VIOLATION("session %u reset with a live socket %d. it was not detached",
			GetSessionID(), static_cast<int>(m_clientSocket));
	}

	// IO 카운트는 여기서 강제로 0 으로 만들지 않는다.
	//
	// 이전에는 InterlockedExchange(&m_ioCount, 0) 였다. 그런데 이 시점에
	// 다른 스레드의 완료 처리가 아직 돌고 있으면, 그 스레드의 DecrementIO 가
	// 0 에서 하나 더 내려가 -1 이 되고 단정에 걸렸다. 실제로 관측된 현상이다.
	//
	// 카운트를 그대로 두면 늦게 도착한 DecrementIO 가 정상적으로 0 으로 내려간다.
	// 0 이 아닌 상태로 여기 도달한 것 자체가 버그이므로 크게 남긴다.
	const LONG remainingIo = ::ReadAcquire(&m_ioCount);
	if (remainingIo != 0)
	{
		ENGINE_VIOLATION("session %u reset while %ld IO operations are still outstanding. the counter is left as is so a late DecrementIO does not go negative",
			GetSessionID(), remainingIo);
	}

	::InterlockedExchange(&m_closing, 0);
	::InterlockedExchange(&m_cancelIo, 0);

	// 반납을 마친 세션에 예약이 남아 있으면, 다음 접속이 이 세션을 쓰다가
	// 첫 완료에서 곧바로 반납된다. 마무리 경로를 거쳐 왔다면 토큰은 이미
	// 소비되어 0 이지만, 그렇지 않은 경로로 리셋될 수도 있어 여기서 지운다.
	::InterlockedExchange(&m_releasePending, 0);

	// 취소 완료 이벤트는 여기서 되돌리지 않는다.
	//
	// 되돌렸었다. 그런데 이 함수는 지연 반납의 마무리 경로에서도 불리고,
	// 그 경로는 방금 DecrementIO 가 이벤트를 세운 직후다. 종료 절차의
	// 대기자가 그 사이에 깨어나지 못하면 신호를 놓치고 10초를 태운다.
	//
	// 무장하는 쪽(CancelPendingIO)에서 되돌린다. 그러면 세워진 이벤트가
	// 대기자 밑에서 사라지는 일이 없다. 대기는 언제나 무장 뒤에 오므로
	// 재사용된 세션이 이전 주기의 신호를 물려받지도 않는다.
}

void BaseSession::Finalize()
{
	if (m_destroyFlag)
	{
		return;
	}

	// 종료 시점에는 IOCP 가 이미 멈춰 있을 수 있고, 그때 큐에 남아 있던 완료
	// 통지는 버려진다. 그러면 그 몫의 DecrementIO 가 영영 실행되지 않아
	// 카운트가 0 이 아닌 상태로 여기 도달한다.
	// 종료 경로이므로 치명적이지 않다. 남기고 계속 진행한다.
	const LONG remainingIo = ::ReadAcquire(&m_ioCount);
	if (remainingIo != 0)
	{
		ENGINE_VIOLATION("session %u finalized while %ld IO operations are still outstanding. completions were most likely dropped when the IOCP stopped",
			GetSessionID(), remainingIo);
	}

	SetClientSessionState(ClientSessionState::NONE);
	SetServerSessionState(ServerSessionState::NONE);
	SetAcceptSessionState(AcceptSessionState::NONE);

	if (m_clientSocket != INVALID_SOCKET)
	{
		ENGINE_VIOLATION("session %u finalized with a live socket %d. it was not detached",
			GetSessionID(), static_cast<int>(m_clientSocket));
	}

	::InterlockedExchange(&m_closing, 0);
	::InterlockedExchange(&m_ioCount, 0);
	::InterlockedExchange(&m_cancelIo, 0);
	::InterlockedExchange(&m_releasePending, 0);

	if (m_ioCancelCompleteEvent)
	{
		::ResetEvent(m_ioCancelCompleteEvent);
		::CloseHandle(m_ioCancelCompleteEvent);
		m_ioCancelCompleteEvent = nullptr;
	}

	m_destroyFlag = true;
}

void BaseSession::IncrementIO()
{
	::InterlockedIncrement(&m_ioCount);
}

void BaseSession::DecrementIO()
{
	// 아래에서 마무리 콜백을 부르고 나면 이 세션은 이미 풀에 돌아가 다른
	// 접속에 재배포될 수 있다. 그래서 콜백에 필요한 값은 지금 읽어 둔다.
	// (지금은 초기화 이후 바뀌지 않는 값이지만, 여기 순서에 기대는 코드가
	//  생기지 않도록 읽는 시점을 못박아 둔다)
	ReleaseReadyFunc releaseReadyFunc = m_releaseReadyFunc;
	void* releaseReadyContext = m_releaseReadyContext;

	const LONG ioCount = ::InterlockedDecrement(&m_ioCount);

	if (ioCount < 0)
	{
		// IncrementIO 없이 DecrementIO 가 불렸거나, 카운터가 외부에서 리셋됐다.
		// ResetSession 이 더 이상 카운터를 강제로 0 으로 만들지 않으므로
		// 이제 이 로그가 뜨면 Increment/Decrement 짝이 실제로 맞지 않는다는 뜻이다.
		ENGINE_VIOLATION("session %u IO count went negative (%ld). an Increment/Decrement pair is unbalanced",
			GetSessionID(), ioCount);
		return;
	}

	if (ioCount != 0)
	{
		return;
	}

	// 여기부터는 마지막 I/O 소유권을 방금 내려놓은 스레드다.

	if (::ReadAcquire(&m_cancelIo) == 1 && m_ioCancelCompleteEvent)
	{
		LOGI("session %u all pending IO cancelled", GetSessionID());
		::SetEvent(m_ioCancelCompleteEvent);
	}

	// 예약된 반납이 있으면 여기서 마무리한다.
	//
	// 토큰을 1 -> 0 으로 바꾸는 데 성공한 스레드만 부른다. RequestRelease 를
	// 부른 스레드가 "카운트 0" 을 보는 것과 이 스레드가 "예약됨" 을 보는 것이
	// 겹칠 수 있어, 둘 중 하나만 골라야 두 번 반납되지 않는다.
	if (::InterlockedExchange(&m_releasePending, 0) == 1 && releaseReadyFunc)
	{
		releaseReadyFunc(releaseReadyContext, this);
	}
}

void BaseSession::SetReleaseReadyFunc(ReleaseReadyFunc releaseReadyFunc, void* context)
{
	m_releaseReadyFunc = releaseReadyFunc;
	m_releaseReadyContext = context;
}

bool BaseSession::RequestRelease()
{
	// 예약을 먼저 세우고 카운트를 읽는다.
	//
	// 순서를 바꾸면, 카운트를 읽은 뒤 예약을 세우기 전에 마지막
	// DecrementIO 가 지나가 버리는 창이 생긴다. 그 스레드는 예약을 보지
	// 못해 그냥 돌아가고, 이쪽은 "남은 I/O 가 있으니 저쪽이 하겠지" 로
	// 판단해 돌아간다. 아무도 반납하지 않는 세션이 남는다.
	//
	// 이 store 는 InterlockedExchange 여야 한다. 아래 읽기는 ReadAcquire 라
	// StoreLoad 를 막지 못하고, 그 순서를 세워 주는 것이 이 lock 접두 명령
	// 하나뿐이다. WriteRelease 같은 것으로 바꾸면 아래 읽기가 이 store 앞으로
	// 올라갈 수 있고, 그러면 양쪽이 서로를 못 보는 창이 열린다.
	// (반대편 DecrementIO 도 같은 이유로 InterlockedDecrement 여야 한다)
	::InterlockedExchange(&m_releasePending, 1);

	if (::ReadAcquire(&m_ioCount) != 0)
	{
		// 아직 완료되지 않은 I/O 가 있다. 이 시점에 카운트가 0 이 아니었다면
		// 0 으로 내리는 DecrementIO 가 반드시 이 뒤에 오고, 그 스레드는
		// 위에서 세운 예약을 본다. 여기서 기다릴 이유가 없다.
		return false;
	}

	// 카운트가 0 이다. 다만 위 두 줄 사이에 마지막 DecrementIO 가 지나갔다면
	// 그쪽도 "0 이고 예약됨" 을 본다. 토큰을 집은 쪽만 마무리한다.
	return (::InterlockedExchange(&m_releasePending, 0) == 1);
}

bool BaseSession::IsReleasePending() const
{
	return (::ReadAcquire(&m_releasePending) == 1);
}

LONG BaseSession::GetOutstandingIOCount() const
{
	return ::ReadAcquire(&m_ioCount);
}

bool BaseSession::CancelPendingIO()
{
	if (m_clientSocket != INVALID_SOCKET)
	{
		if (::InterlockedExchange(&m_cancelIo, 1) == 0)
		{
			// 이 주기의 신호를 받기 전에 이전 주기의 잔상을 지운다.
			// 되돌리는 자리는 여기 하나뿐이다 (ResetSession 의 주석 참고).
			if (m_ioCancelCompleteEvent)
			{
				::ResetEvent(m_ioCancelCompleteEvent);
			}

			LOGI("session %u cancelling pending IO (count %ld)",
				GetSessionID(), ::ReadAcquire(&m_ioCount));

			if (!::CancelIoEx(reinterpret_cast<HANDLE>(m_clientSocket), nullptr))
			{
				const DWORD errorCode = ::GetLastError();

				if (errorCode == ERROR_NOT_FOUND || errorCode == ERROR_INVALID_HANDLE)
				{
					// CancelIoEx 가 취소할 대상을 찾지 못했다.
					//
					// 이전 코드는 여기서 IO 카운트를 보지 않고 무조건 완료 이벤트를
					// 세웠다. 그러면 실제로는 완료 통지를 기다리는 I/O 가 남아 있는데도
					// WaitForIOCancelComplete 가 즉시 통과해서, 호출부가 아직 사용 중인
					// 세션을 리셋하고 풀로 반납했다. 대기 자체가 무의미했다.
					//
					// 카운트가 0 일 때만 세운다. 0 이 아니면 남은 완료 통지가 도착해
					// DecrementIO 가 0 으로 내릴 때 그쪽에서 이벤트를 세운다.
					const LONG outstanding = ::ReadAcquire(&m_ioCount);

					if (outstanding == 0)
					{
						if (m_ioCancelCompleteEvent)
						{
							::SetEvent(m_ioCancelCompleteEvent);
						}

						LOGI("session %u CancelIoEx found no pending IO and the count is zero (error %lu)",
							GetSessionID(), errorCode);
					}
					else
					{
						// 흔한 정상 상태다. 완료 통지는 이미 큐에서 꺼내졌고
						// (그래서 취소할 대상이 없다) 그걸 처리 중인 핸들러가
						// 아직 자기 몫의 카운트를 들고 있다.
						//
						// 예전에는 이게 경보였다. 바로 뒤의 대기가 그 카운트를
						// 기다렸고, 그 카운트의 주인이 대기 중인 스레드 자신일
						// 때 10초를 태웠기 때문이다. 이제 아무도 기다리지 않고
						// 마지막 DecrementIO 가 마무리하므로 경보가 아니다.
						// (실측: bench 1회에 436번 — WARN 으로 둘 양이 아니다)
						LOGT("session %u CancelIoEx found nothing to cancel, %ld IO operations are still counted by their handlers",
							GetSessionID(), outstanding);
					}

					return true;
				}
				else if (errorCode == ERROR_OPERATION_ABORTED)
				{
					LOGW("session %u CancelIoEx returned ERROR_OPERATION_ABORTED", GetSessionID());
					return false;
				}
				else
				{
					LOGE("session %u CancelIoEx failed (error %lu)", GetSessionID(), errorCode);
					return false;
				}
			}
		}
	}

	// 취소가 걸렸으니, 발행 없이 들고만 있는 카운트는 여기서 놓는다.
	// 이 함수의 마지막 줄이어야 한다 — 안에서 마지막 카운트가 내려가면
	// 예약된 반납이 그 자리에서 끝난다.
	ReleaseUnpostedIO();

	return true;
}

void BaseSession::ReissueCancelIo()
{
	if (m_clientSocket == INVALID_SOCKET)
		return;

	// 실패는 정상이다. 취소할 것이 없으면 ERROR_NOT_FOUND 가 돌아오고,
	// 그건 낙오가 없었다는 뜻이다. 여기서는 보고하지 않는다 —
	// 판정은 아래 대기가 카운트로 한다.
	::CancelIoEx(reinterpret_cast<HANDLE>(m_clientSocket), nullptr);
}

bool BaseSession::WaitForIOCancelComplete(const uint32_t timeout_ms)
{
	// 기다리는 목적은 "미완료 I/O 가 없는 상태" 하나다. 이벤트는 그걸
	// 알리는 수단일 뿐이므로, 이미 0 이면 이벤트를 보지 않고 통과한다.
	// 신호를 놓쳤더라도 여기서 걸러진다.
	if (::ReadAcquire(&m_ioCount) == 0)
	{
		return true;
	}

	if (!m_ioCancelCompleteEvent)
	{
		return true;
	}

	// 한 번에 통째로 기다리지 않고 쪼개서 기다린다.
	//
	// 예전에는 여기서 timeout_ms 를 한 번에 걸었다. 그러면 게이트를 막
	// 통과한 스레드가 취소 뒤에 발행한 I/O 를 아무도 다시 취소하지 않으므로,
	// 그 한 건 때문에 10초를 통째로 태우고 타임아웃했다.
	// (실측: bench Phase 4 반복 8회 전부)
	//
	// 슬라이스마다 CancelIoEx 를 다시 건다. 게이트가 닫힌 뒤로는 새 I/O 가
	// 생기지 않으므로 낙오는 유한하고, 보통 두 번째 취소에서 걷힌다.
	constexpr DWORD CANCEL_RETRY_SLICE_MS = 50;

	const ULONGLONG deadline = ::GetTickCount64() + timeout_ms;

	for (;;)
	{
		const ULONGLONG now = ::GetTickCount64();
		const DWORD remaining = (now >= deadline)
			? 0
			: static_cast<DWORD>(deadline - now);

		if (remaining == 0)
			break;

		const DWORD slice = (remaining < CANCEL_RETRY_SLICE_MS) ? remaining : CANCEL_RETRY_SLICE_MS;

		const DWORD waitResult = ::WaitForSingleObject(m_ioCancelCompleteEvent, slice);

		if (waitResult == WAIT_OBJECT_0)
		{
			LOGI("session %u IO cancel completed", GetSessionID());
			return true;
		}

		if (waitResult != WAIT_TIMEOUT)
		{
			LOGE("session %u IO cancel wait returned %lu (error %lu)",
				GetSessionID(), waitResult, ::GetLastError());
			return false;
		}

		// 신호를 놓쳤을 수도 있으니 목적을 직접 확인한다.
		if (::ReadAcquire(&m_ioCount) == 0)
		{
			LOGW("session %u IO cancel event was missed but the count is zero", GetSessionID());
			return true;
		}

		// 게이트를 막 통과한 스레드가 취소 직후에 멈춤을 표시했을 수 있다.
		// 그 카운트는 완료 통지가 아니라 여기서만 걷힌다.
		ReleaseUnpostedIO();

		// 낙오가 있으면 여기서 걷힌다.
		ReissueCancelIo();
	}

	const LONG outstanding = ::ReadAcquire(&m_ioCount);

	if (outstanding == 0)
	{
		LOGW("session %u IO cancel event was missed but the count is zero", GetSessionID());
		return true;
	}

	// 호출부는 이 실패를 무시하고 세션 해제로 진행하므로
	// 남아 있는 IO 수까지 남겨야 원인 추적이 가능하다.
	LOGE("session %u IO cancel timed out after %u ms (io count still %ld)",
		GetSessionID(), timeout_ms, outstanding);
	return false;
}

bool BaseSession::IsIOCancelRequested() const
{
	return (::ReadAcquire(&m_cancelIo) == 1);
}

bool BaseSession::BeginIO()
{
	// 증가가 먼저다. 이유는 헤더 주석 참고 —
	// 이 카운트가 곧 "대기가 통과하지 못하게 하는 장벽" 이다.
	::InterlockedIncrement(&m_ioCount);

	if (::ReadAcquire(&m_cancelIo) == 0)
	{
		return true;
	}

	// 취소가 이미 걸려 있다. 발행하지 않고 카운트를 되돌린다.
	// 이 감소가 마지막이면 여기서 완료 이벤트가 서고 예약된 반납이 끝난다.
	DecrementIO();

	return false;
}

bool BaseSession::OnDisconnect()
{
	if (::InterlockedExchange(&m_closing, 1) == 0)
	{
		if (m_clientSocket != INVALID_SOCKET)
		{
			// 소켓은 반드시 DetachSocket 으로 먼저 떼어낸 뒤 여기 와야 한다.
			LOGE("session %u disconnecting with a live socket %d. it was not detached",
				GetSessionID(), static_cast<int>(m_clientSocket));
			ENGINE_BREAK_IF_DEBUGGER();
		}

		switch (m_sessionRole)
		{
		case SESSION_ROLE::CLIENT:
			SetClientSessionState(ClientSessionState::DISCONNECTED);
			break;
		case SESSION_ROLE::SERVER:
			SetServerSessionState(ServerSessionState::DISCONNECTED);
			break;
		case SESSION_ROLE::ACCEPT:
			SetAcceptSessionState(AcceptSessionState::DISCONNECTED);
			break;
		default:
			break;
		}

		LOGI("session %u disconnected (role %d)", GetSessionID(), static_cast<int>(m_sessionRole));
	}

	return true;
}

void BaseSession::AttachSocket(SOCKET socket)
{
	if (m_clientSocket != INVALID_SOCKET)
	{
		// 이미 소켓을 들고 있는 세션에 다시 붙이려 한다.
		// 사용 중인 세션이 풀에서 다시 배포되면 이 경로로 온다.
		ENGINE_VIOLATION("session %u already holds socket %d, cannot attach socket %d. the session was handed out while still in use",
			GetSessionID(), static_cast<int>(m_clientSocket), static_cast<int>(socket));
	}

	m_clientSocket = socket;
}

SOCKET BaseSession::DetachSocket()
{
	// 원자적으로 떼어낸다.
	//
	// 떼어낸 소켓은 곧바로 closesocket 으로 넘어간다. 두 스레드가 같은
	// 값을 읽어 가면 같은 핸들을 두 번 닫는다. 닫힌 직후 같은 번호가 다른
	// 소켓에 재할당되면 그 소켓이 대신 닫힌다 — 추적이 거의 불가능한 종류의
	// 고장이다.
	//
	// 지연 반납을 넣으면서 실제로 겹칠 수 있는 조합이 생겼다. 마지막
	// 완료를 처리하는 워커가 반납을 마무리하는 동안, 종료 절차의
	// DisconnectAllSessions 가 같은 세션을 훑는다.
	return reinterpret_cast<SOCKET>(::InterlockedExchangePointer(
		reinterpret_cast<PVOID volatile*>(&m_clientSocket),
		reinterpret_cast<PVOID>(INVALID_SOCKET)));
}