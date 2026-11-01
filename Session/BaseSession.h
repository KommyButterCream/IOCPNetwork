#pragma once

#include <stdint.h>

#include "ISession.h"
#include "SessionDefs.h"
#include "DisconnectReason.h"

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

// 세션의 공통 상태와 I/O 수명 관리.
class IOCP_ENGINE_API BaseSession : public ISession
{
public:
	BaseSession();
	~BaseSession() override;

	// 남은 I/O 가 0 이 되는 순간 부를 마무리 콜백.
	//
	// 세션은 자기를 담은 풀을 모른다(알면 풀 종류마다 세션이 갈라진다).
	// 그래서 풀이 자기 마무리 절차를 함수 포인터로 맡긴다.
	// ClientSessionPool 이 m_closeSocketFunc 를 넘기는 방식과 같다.
	using ReleaseReadyFunc = void (*)(void* context, BaseSession* session);

protected:
	bool m_destroyFlag = false;

	// 역할. Initialize 에서 한 번 정해지고 그 뒤로 바뀌지 않는다.
	// 세션이 어떤 I/O 에도 등록되기 전에 쓰이므로 원자 접근이 필요 없다.
	SESSION_ROLE m_sessionRole = SESSION_ROLE::NONE;

	// 세션 상태.
	//
	// 예전에는 평범한 enum 멤버였다. 그런데 쓰는 쪽과 읽는 쪽이 다른 스레드다.
	//   쓰기 : HeartbeatThread    -> MarkHeartbeatTimeout (HEARTBEAT_TIMEOUT)
	//          IOCP 워커          -> 접속/인증 시퀀스 (CONNECTED, AUTH_PENDING, ESTABLISHED)
	//          반납 경로          -> ResetSession / Finalize (CONNECT_READY)
	//   읽기 : IOCP 워커          -> HandleRecv 안의 IsEstablished() 로
	//                                서비스 패킷 디스패치 여부를 가른다
	//          앱 스레드          -> StopServer -> DisconnectAllSessions
	//
	// 지금까지 드러나지 않은 것은 두 가지 우연 덕분이었다. x64 MSVC 에서
	// 정렬된 4바이트 접근이 찢어지지 않는다는 것, 그리고 접근자가 다른 번역
	// 단위에 있어 인라인되지 않으므로 컴파일러가 값을 레지스터에 눌러
	// 담아두지 못한다는 것. 둘 다 설정 하나로(LTCG/WPO) 사라지는 전제다.
	//
	// AcceptSession::m_slotOwned 가 같은 부류의 문제를 먼저 맞았다 —
	// 상태가 원자적이지 않아 두 스레드가 같은 슬롯을 비었다고 읽었다.
	// 그쪽은 CAS 플래그를 따로 뒀지만 나머지 상태는 그대로 남아 있었다.
	//
	// 저장은 volatile LONG 이고 접근은 반드시 Interlocked 로만 한다.
	// 읽기 관용구는 IsReleasePending / GetOutstandingIOCount 와 같다.
	volatile LONG m_clientSessionState = static_cast<LONG>(ClientSessionState::NONE);
	volatile LONG m_serverSessionState = static_cast<LONG>(ServerSessionState::NONE);
	volatile LONG m_acceptSessionState = static_cast<LONG>(AcceptSessionState::NONE);

	// 이 세션이 왜 끝났는가. 서비스의 종료 훅으로 그대로 올라간다.
	// 먼저 쓴 값이 이긴다 (NoteDisconnectReason 주석).
	volatile LONG m_disconnectReason = static_cast<LONG>(DisconnectReason::Unknown);

	SOCKET m_clientSocket = INVALID_SOCKET;

	uint32_t m_sessionId = INVALID_SESSION_ID;

	volatile LONG m_closing = 0;
	volatile LONG m_ioCount = 0;
	volatile LONG m_cancelIo = 0;

	// 반납 예약. 1 이면 "남은 I/O 가 다 끝나는 대로 이 세션을 반납하라".
	//
	// 이 플래그는 예약이면서 동시에 소유권 토큰이다. 1 -> 0 전이에
	// 성공한 스레드 하나만 마무리를 수행한다. RequestRelease 를 부른
	// 스레드와 마지막 DecrementIO 를 하는 스레드가 동시에 "내가 마지막"
	// 이라고 판단할 수 있어, 둘 중 하나만 고르는 장치가 필요하다.
	volatile LONG m_releasePending = 0;

	ReleaseReadyFunc m_releaseReadyFunc = nullptr;
	void* m_releaseReadyContext = nullptr;

	HANDLE m_ioCancelCompleteEvent = nullptr;

public:
	// --- 확장점. 파생 클래스가 재정의한다 ---
	virtual bool Initialize(SESSION_ROLE sessionType, uint32_t sessionId);
	virtual void ResetSession();
	virtual void Finalize();

	virtual bool OnAccept() = 0;
	virtual bool OnConnect() = 0;
	virtual bool OnDisconnect();

	// --- 소켓 제어 ---
	//
	// AttachSocket 과 DetachSocket 은 짝이다. 붙일 때는 이미 들고 있지
	// 않은지 검사하므로(사용 중인 세션이 재배포되면 그 경로로 온다)
	// 본문이 .cpp 에 있다.
	void AttachSocket(SOCKET socket);
	SOCKET DetachSocket();

	void SetClientSocket(SOCKET socket) { m_clientSocket = socket; }
	SOCKET GetClientSocket() const { return m_clientSocket; }
	bool IsSocketInvalid() const { return (m_clientSocket == INVALID_SOCKET); }

	// --- 상태 관리 ---
	//
	// 역할별로 쓰는 것이 하나씩 정해져 있다. accept 세션은 Accept 상태만,
	// 서버 역할 세션은 Server 상태만 쓴다.
	//
	// 헤더에 남겨 인라인시킨다. Interlocked 는 내장 함수라 DLL 호출이 생기지
	// 않으므로, 접근자를 .cpp 로 내리면 호출 비용만 늘고 얻는 것이 없다.
	//
	// 주의: 값 하나로 여러 번 비교해야 하면 getter 를 여러 번 부르지 말고
	// 지역 변수에 한 번 받아서 쓴다. 호출마다 다시 읽으므로 비교 도중에
	// 값이 바뀌면 어느 분기에도 걸리지 않는 결과가 나온다.
	// (IsTransportConnected 가 그 예다)
	void SetClientSessionState(ClientSessionState sessionState)
	{
		::InterlockedExchange(&m_clientSessionState, static_cast<LONG>(sessionState));
	}
	ClientSessionState GetClientSessionState() const
	{
		return static_cast<ClientSessionState>(::ReadAcquire(&m_clientSessionState));
	}

	void SetServerSessionState(ServerSessionState sessionState)
	{
		::InterlockedExchange(&m_serverSessionState, static_cast<LONG>(sessionState));
	}
	ServerSessionState GetServerSessionState() const
	{
		return static_cast<ServerSessionState>(::ReadAcquire(&m_serverSessionState));
	}

	void SetAcceptSessionState(AcceptSessionState sessionState)
	{
		::InterlockedExchange(&m_acceptSessionState, static_cast<LONG>(sessionState));
	}
	AcceptSessionState GetAcceptSessionState() const
	{
		return static_cast<AcceptSessionState>(::ReadAcquire(&m_acceptSessionState));
	}

	// --- 타입 및 식별자 ---
	SESSION_ROLE GetSessionRole() const { return m_sessionRole; }
	void SetSessionID(uint32_t sessionId) { m_sessionId = sessionId; }

	// ISession 의 유일한 순수 가상. 서비스가 ISession* 로 부른다.
	uint32_t GetSessionID() const override { return m_sessionId; }

	// --- 종료 사유 ---
	//
	// 끊기로 결정한 자리에서 부른다. 실제로 종료가 일어나는 자리가 아니라
	// 이유를 아는 자리다. 그 둘은 대개 다르다.
	//
	// 먼저 쓴 값이 이긴다. 종료는 연쇄로 일어나기 때문이다 — 프로토콜
	// 위반으로 끊기로 하면 곧 소켓이 닫히고 걸려 있던 I/O 가 10054 로
	// 실패한다. 나중 것이 이기면 근본 원인이 매번 SocketError 로 덮인다.
	void NoteDisconnectReason(DisconnectReason reason)
	{
		::InterlockedCompareExchange(&m_disconnectReason,
			static_cast<LONG>(reason), static_cast<LONG>(DisconnectReason::Unknown));
	}

	DisconnectReason GetDisconnectReason() const
	{
		return static_cast<DisconnectReason>(::ReadAcquire(&m_disconnectReason));
	}

	// 세션을 재사용하기 전에 되돌린다. 남아 있으면 다음 접속의 종료가
	// 지난 접속의 사유를 물려받는다.
	void ClearDisconnectReason()
	{
		::InterlockedExchange(&m_disconnectReason, static_cast<LONG>(DisconnectReason::Unknown));
	}

	// --- I/O 수명 ---
	//
	// 엔진 내부 전용. ISession 에는 없다 (이유는 그쪽 주석).
	void IncrementIO();
	void DecrementIO();

	bool CancelPendingIO();
	bool WaitForIOCancelComplete(const uint32_t timeout_ms);

	// I/O 발행을 시도한다. 발행 지점(PostReceive / PostCurrentSend)이
	// IncrementIO 대신 부른다.
	//
	//   true  : 카운트를 하나 들고 나왔다. 부르는 쪽은 반드시 WSARecv/WSASend 를
	//           걸어야 하고, 그 완료가 DecrementIO 를 한다.
	//   false : 취소가 이미 걸렸다. 카운트는 이 함수가 되돌렸으므로 부르는
	//           쪽이 따로 할 일은 없다.
	//
	// 주의: false 를 받은 뒤에는 세션을 만지면 안 된다. 되돌린 그 감소가
	// 마지막이었다면 예약된 반납이 그 자리에서 끝나고 세션은 풀로 돌아간다.
	// 정리할 것이 있으면 이 함수를 부르기 전에 끝내야 한다.
	//
	// 왜 필요한가
	//   CancelIoEx 는 부르는 그 순간 걸려 있던 것만 취소하는데, 완료 핸들러는
	//   마지막에 늘 다음 I/O 를 건다. 막지 않으면 카운트가 0 으로 내려오지
	//   않아 WaitForIOCancelComplete 가 10초를 태우고, 미완료 I/O 를 안은
	//   세션이 리셋된다. (실측: bench Phase 4 반복 8회 전부)
	//
	// 왜 검사보다 증가가 먼저인가
	//   반대로 하면 검사를 통과한 채 멈춘 스레드가 있는 동안 카운트가 0 에
	//   도달할 수 있다. 그러면 대기가 통과해 세션이 리셋되고, 그 뒤에 그
	//   스레드가 I/O 를 걸어 죽은 세션에 미완료 I/O 를 남긴다.
	//   (실측: 검사를 먼저 두었을 때 Phase 4 가 4회 중 1회 위반 2건으로 실패)
	//   먼저 올려 두면 그 구간 내내 카운트가 0 이 아니므로 대기가 통과하지
	//   못하고, 실제로 걸린 I/O 는 재취소가 걷어 간다.
	bool BeginIO();

	// 취소가 걸렸는가. 값싼 사전 확인용이다.
	//
	// 이것만으로는 발행을 막을 수 없다 (검사와 발행 사이에 취소가 걸린다).
	// 발행 여부의 판단은 반드시 BeginIO 가 한다. 이 함수는 "카운트를 들고
	// 있는 동안 미리 정리해 둬야 하는" 경우에만 쓴다 —
	// PostCurrentSend 가 송신 엔트리를 되돌리는 자리가 그렇다.
	bool IsIOCancelRequested() const;

	// --- 지연 반납 ---
	//
	// 완료 핸들러 안에서 반납이 필요해지는 경로가 여럿이다. 그때 그 자리에서
	// 카운트가 0 이 되기를 기다리면, 핸들러 자신이 들고 있는 몫 때문에
	// 자기를 기다리게 된다(실측: bench 실행당 200회 이상의 10초 타임아웃).
	//
	// 그래서 기다리지 않는다. 반납을 예약해 두고, 마지막 완료가 카운트를
	// 0 으로 내리는 그 자리에서 마무리한다.
	void SetReleaseReadyFunc(ReleaseReadyFunc releaseReadyFunc, void* context);

	// 반납을 예약한다.
	//   true  : 남은 I/O 가 없고 마무리 권한을 이 스레드가 잡았다.
	//           부르는 쪽이 그 자리에서 마무리해야 한다.
	//   false : 아직 완료되지 않은 I/O 가 있거나, 다른 스레드가 이미
	//           권한을 잡았다. 부르는 쪽은 즉시 돌아간다.
	bool RequestRelease();

	// 반납이 예약되어 있는가. 핸들러가 더 진행할지 판단할 때 쓴다.
	bool IsReleasePending() const;

	// --- 진단 ---
	//
	// 지금 이 세션이 소유권을 주장 중인 I/O 수. 0 이면 어떤 스레드도
	// 완료 핸들러 안에서 이 세션을 만지고 있지 않다는 뜻이고, 반납이
	// 안전하다는 판정의 근거가 이 값 하나다.
	// 하네스가 시나리오마다 회계를 검사하려면 밖에서 읽을 수 있어야 한다.
	LONG GetOutstandingIOCount() const;

protected:
	// 취소가 걸린 뒤에도 남아 있는 "발행되지 않은" 카운트를 놓을 기회.
	//
	// BeginIO 는 발행 지점이 카운트를 먼저 올리게 해서 취소 대기가 통과하지
	// 못하게 만든다. 그 카운트는 곧 완료 통지가 내려 준다 — I/O 를 실제로
	// 걸었으니 통지가 반드시 온다.
	//
	// 그런데 완료 통지가 아니라 '다른 스레드의 행동' 이 내려 주기로 되어 있는
	// 카운트가 하나 있다. 수신 백프레셔의 일시정지다 (ClientSession::m_recvPaused).
	// 멈춘 세션에는 걸린 I/O 가 하나도 없고, 그 카운트를 내리는 것은 잡을
	// 드레인한 스레드뿐이다. 그 스레드가 이미 없으면 아무도 내리지 않는다.
	//
	// 실측(tools/cshutdown, 3/3 재현) : IOCPClient::StopClient 가 잡 스케줄러를
	// 먼저 없앤 뒤에 취소 완료를 기다리므로, 멈춰 있던 클라이언트 세션은
	// 10초를 꽉 채우고 "io count still 1" 로 타임아웃했다.
	//
	// 취소가 걸린 뒤로는 그 카운트를 붙들 이유가 없다. 재개해 봐야 BeginIO 가
	// 거절한다. 그래서 취소 요청 직후와 대기 슬라이스마다 파생 클래스에게
	// "지금 들고 있는 미발행 카운트를 놓아라" 고 알린다. 슬라이스마다 부르는
	// 것은 ReissueCancelIo 와 같은 이유다 — 게이트를 막 통과한 스레드가 취소
	// 직후에 멈춤을 표시할 수 있고, 그 낙오는 수가 유한하다.
	//
	// 주의: 이 안에서 마지막 카운트가 내려갈 수 있다. 그러면 예약된 반납이
	// 그 자리에서 마무리되므로, 돌아온 뒤 세션을 만지지 않는 자리에서만
	// 불러야 한다.
	virtual void ReleaseUnpostedIO() {}

private:
	// CancelIoEx 만 다시 부른다. 취소 플래그와 완료 이벤트는 건드리지 않는다.
	//
	// 게이트(IsIOCancelRequested)를 통과한 직후에 취소가 걸리면, 그 스레드는
	// 이미 검사를 지났으므로 I/O 하나를 더 발행한다. 그런 낙오는 게이트가
	// 닫힌 뒤로는 더 생기지 않으므로 수가 유한하고, 한 번 더 취소하면
	// 전부 걷힌다. WaitForIOCancelComplete 가 대기를 쪼개면서 부른다.
	void ReissueCancelIo();
};
