#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

#include "../../Core/Concurrency/ThreadBase.h"

class ClientSession;
class TlsMemoryPool;

// 세션 1개의 JobQueue 를 처리하는 전용 스레드.
// 스레드 1개 : 객체 1개 관계이므로 ThreadBase 를 직접 상속한다.
// 스레드 핸들, 정지 이벤트, 조인은 ThreadBase 가 관리한다.
class ClientJobWorker final : public Core::Concurrency::ThreadBase
{
public:
	ClientJobWorker();
	~ClientJobWorker() override;

public:
	// 외부 제어 스레드에서 직렬로 호출합니다. 콜백 안에서 종료하면 자기 스레드를 기다리게 됩니다.
	// 재시작 전에는 생산자·소비자가 멈춘 상태에서 세션 Job 큐를 Reset해야 합니다.
	// 세션과 풀은 Finalize가 끝날 때까지 살아 있어야 합니다. DB/System 데이터는 이 풀의 할당만 받습니다.
	[[nodiscard]] bool Initialize(ClientSession* clientSession, TlsMemoryPool* packetMemoryPool);
	void Finalize();

protected:
	void Run() override;

private:
	ClientSession* m_clientSession = nullptr;
	TlsMemoryPool* m_packetMemoryPool = nullptr;
};
