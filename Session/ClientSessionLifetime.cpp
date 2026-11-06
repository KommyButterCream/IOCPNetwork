#include "ClientSession.h"

#include "SessionDefs.h"

#include "../Diagnostics/EngineAssert.h"

#include "../../Core/Sync/SRWLockGuard.h"
#include "../../Core/Util/Logger.h"

using namespace Core::Util;

void ClientSession::CheckCleanupPreconditions() const
{
	if (m_clientSocket != INVALID_SOCKET)
		ENGINE_VIOLATION("session %u cleaned up with a live socket", GetSessionID());

	const LONG outstanding = GetOutstandingIOCount();
	if (outstanding != 0)
		ENGINE_VIOLATION("session %u cleaned up with %ld outstanding IO operations", GetSessionID(), outstanding);

	Core::Sync::SRWReadLockGuard lock(m_lifetimeLock);
	if (m_lifetimeReferences != 0 || m_releasePending)
		ENGINE_VIOLATION("session %u cleaned up with outstanding lifetime references or pending release", GetSessionID());
}

void ClientSession::AddLifetimeReference()
{
	Core::Sync::SRWWriteLockGuard lock(m_lifetimeLock);
	++m_lifetimeReferences;
}

void ClientSession::ReleaseLifetimeReference()
{
	OwnerCallbacks callbacks{};
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
			callbacks = m_ownerCallbacks;
		}
	}

	// 콜백에서 세션을 초기화하고 풀에 돌려주므로 수명 락을 먼저 해제한다.
	if (callbacks.releaseReady)
	{
		callbacks.releaseReady(callbacks.owner, this);
	}
}

void ClientSession::IncrementIO()
{
	AddLifetimeReference();
	::InterlockedIncrement(&m_ioCount);
}

void ClientSession::DecrementIO()
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

void ClientSession::SetOwnerCallbacks(OwnerCallbacks callbacks)
{
	Core::Sync::SRWWriteLockGuard lock(m_lifetimeLock);
	m_ownerCallbacks = callbacks;
}

bool ClientSession::RequestRelease()
{
	Core::Sync::SRWWriteLockGuard lock(m_lifetimeLock);
	m_releasePending = m_lifetimeReferences != 0;
	return !m_releasePending;
}

bool ClientSession::IsReleasePending() const
{
	Core::Sync::SRWReadLockGuard lock(m_lifetimeLock);
	return m_releasePending;
}

LONG ClientSession::GetOutstandingIOCount() const
{
	return ::ReadAcquire(&m_ioCount);
}

bool ClientSession::BeginIOCancellation()
{
	if (::InterlockedExchange(&m_cancelIo, 1) != 0)
		return false;

	if (m_ioCancelCompleteEvent)
	{
		::ResetEvent(m_ioCancelCompleteEvent);
	}

	return true;
}

bool ClientSession::CancelPendingIO()
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

void ClientSession::ReissueCancelIo()
{
	if (m_clientSocket == INVALID_SOCKET)
		return;

	// 실패는 정상이다. 취소할 것이 없으면 ERROR_NOT_FOUND 가 돌아오고,
	// 그건 낙오가 없었다는 뜻이다. 여기서는 보고하지 않는다 —
	// 판정은 아래 대기가 카운트로 한다.
	::CancelIoEx(reinterpret_cast<HANDLE>(m_clientSocket), nullptr);
}

IOWaitResult ClientSession::WaitForIOCancelComplete(const uint32_t timeout_ms)
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

bool ClientSession::IsIOCancelRequested() const
{
	return (::ReadAcquire(&m_cancelIo) == 1);
}

bool ClientSession::BeginIO()
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

void ClientSession::AttachSocket(SOCKET socket)
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

SOCKET ClientSession::DetachSocket()
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