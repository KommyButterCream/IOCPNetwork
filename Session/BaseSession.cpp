#include "BaseSession.h"

#include "SessionDefs.h"

#include "../Diagnostics/EngineAssert.h"

#include "../../Core/Sync/SRWLockGuard.h"
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
	ResetLifetimeState();
	const LONG remainingIo = ::ReadAcquire(&m_ioCount);
	if (remainingIo != 0)
	{
		ENGINE_VIOLATION("session %u reset while %ld IO operations are still outstanding. the counter is left as is so a late DecrementIO does not go negative",
			GetSessionID(), remainingIo);
	}

	::InterlockedExchange(&m_closing, 0);
	::InterlockedExchange(&m_cancelIo, 0);

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
	ResetLifetimeState();
	const LONG remainingIo = ::ReadAcquire(&m_ioCount);
	if (remainingIo != 0)
	{
		ENGINE_VIOLATION("session %u finalized while %ld IO operations are still outstanding. completions were most likely dropped when the IOCP stopped",
			GetSessionID(), remainingIo);
	}

	SetClientSessionState(ClientSessionState::NONE);
	SetServerSessionState(ServerSessionState::NONE);

	if (m_clientSocket != INVALID_SOCKET)
	{
		ENGINE_VIOLATION("session %u finalized with a live socket %d. it was not detached",
			GetSessionID(), static_cast<int>(m_clientSocket));
	}

	::InterlockedExchange(&m_closing, 0);
	::InterlockedExchange(&m_ioCount, 0);
	::InterlockedExchange(&m_cancelIo, 0);

	if (m_ioCancelCompleteEvent)
	{
		::ResetEvent(m_ioCancelCompleteEvent);
		::CloseHandle(m_ioCancelCompleteEvent);
		m_ioCancelCompleteEvent = nullptr;
	}

	m_destroyFlag = true;
}

void BaseSession::ResetLifetimeState()
{
	Core::Sync::SRWWriteLockGuard lock(m_lifetimeLock);
	if (m_lifetimeReferences != 0)
	{
		ENGINE_VIOLATION("session %u cleaned up with outstanding lifetime references", GetSessionID());
	}
	m_releasePending = false;
}

void BaseSession::AddLifetimeReference()
{
	Core::Sync::SRWWriteLockGuard lock(m_lifetimeLock);
	++m_lifetimeReferences;
}

void BaseSession::ReleaseLifetimeReference()
{
	ReleaseReadyFunc callback = nullptr;
	void* context = nullptr;
	{
		Core::Sync::SRWWriteLockGuard lock(m_lifetimeLock);
		if (m_lifetimeReferences == 0)
		{
			ENGINE_VIOLATION("session %u released a lifetime reference with none outstanding", GetSessionID());
			return;
		}

		// 마지막 참조인지 확인하고 반환 예약을 소비하는 동안 새 참조가 끼어들 수 없다.
		if (--m_lifetimeReferences == 0 && m_releasePending)
		{
			m_releasePending = false;
			callback = m_releaseReadyFunc;
			context = m_releaseReadyContext;
		}
	}

	// 콜백에서 세션을 초기화하고 풀에 돌려주므로 수명 락을 먼저 해제한다.
	if (callback)
	{
		callback(context, this);
	}
}

void BaseSession::IncrementIO()
{
	AddLifetimeReference();
	::InterlockedIncrement(&m_ioCount);
}

void BaseSession::DecrementIO()
{
	const LONG ioCount = ::InterlockedDecrement(&m_ioCount);
	if (ioCount < 0)
	{
		ENGINE_VIOLATION("session %u IO count went negative (%ld)", GetSessionID(), ioCount);
		return;
	}

	// I/O 취소 완료와 세션 반환은 다릅니다. Job 워커가 남아 있어도 I/O 대기는 끝납니다.
	if (ioCount == 0 && ::ReadAcquire(&m_cancelIo) == 1 && m_ioCancelCompleteEvent)
	{
		::SetEvent(m_ioCancelCompleteEvent);
	}
	// 이 호출 뒤에는 세션을 참조하지 않습니다.
	ReleaseLifetimeReference();
}

void BaseSession::SetReleaseReadyFunc(ReleaseReadyFunc releaseReadyFunc, void* context)
{
	Core::Sync::SRWWriteLockGuard lock(m_lifetimeLock);
	m_releaseReadyFunc = releaseReadyFunc;
	m_releaseReadyContext = context;
}

bool BaseSession::RequestRelease()
{
	Core::Sync::SRWWriteLockGuard lock(m_lifetimeLock);
	m_releasePending = m_lifetimeReferences != 0;
	return !m_releasePending;
}

bool BaseSession::IsReleasePending() const
{
	Core::Sync::SRWReadLockGuard lock(m_lifetimeLock);
	return m_releasePending;
}

LONG BaseSession::GetOutstandingIOCount() const
{
	return ::ReadAcquire(&m_ioCount);
}

bool BaseSession::BeginIOCancellation()
{
	if (::InterlockedExchange(&m_cancelIo, 1) != 0)
		return false;

	if (m_ioCancelCompleteEvent)
	{
		::ResetEvent(m_ioCancelCompleteEvent);
	}

	return true;
}

bool BaseSession::CancelPendingIO()
{
	if (m_clientSocket != INVALID_SOCKET)
	{
		if (BeginIOCancellation())
		{
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

IOWaitResult BaseSession::WaitForIOCancelComplete(const uint32_t timeout_ms)
{
	constexpr DWORD cancelRetryMs = 50;
	const ULONGLONG deadline = ::GetTickCount64() + timeout_ms;

	while (GetOutstandingIOCount() != 0)
	{
		// 취소 직전에 발행된 I/O와 일시정지된 수신 참조까지 회수한다.
		ReissueCancelIo();
		ReleaseUnpostedIO();
		if (GetOutstandingIOCount() == 0)
			break;

		if (!m_ioCancelCompleteEvent)
			return { IOWaitStatus::Failed, GetSessionID(), ERROR_INVALID_HANDLE };

		const ULONGLONG now = ::GetTickCount64();
		if (now >= deadline)
			return { IOWaitStatus::TimedOut, GetSessionID() };

		const DWORD remaining = static_cast<DWORD>(deadline - now);
		const DWORD slice = remaining < cancelRetryMs ? remaining : cancelRetryMs;
		const DWORD result = ::WaitForSingleObject(m_ioCancelCompleteEvent, slice);
		if (result == WAIT_FAILED)
		{
			const DWORD error = ::GetLastError();
			return { IOWaitStatus::Failed, GetSessionID(), error };
		}
		// 신호와 마지막 완료가 경합할 수 있으므로 카운트로 다시 판정한다.
		if (result == WAIT_OBJECT_0 && GetOutstandingIOCount() != 0)
			::Sleep(1);
	}

	return { IOWaitStatus::Completed };
}

bool BaseSession::IsIOCancelRequested() const
{
	return (::ReadAcquire(&m_cancelIo) == 1);
}

bool BaseSession::BeginIO()
{
	// 증가가 먼저다. 이유는 헤더 주석 참고 —
	// 이 카운트가 곧 "대기가 통과하지 못하게 하는 장벽" 이다.
	IncrementIO();

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