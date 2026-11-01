#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

#include "../../Core/Concurrency/ThreadBase.h"

class TlsMemoryPool;
class ClientSession;

// 세션 1개의 JobQueue 를 처리하는 전용 스레드.
// 스레드 1개 : 객체 1개 관계이므로 ThreadBase 를 직접 상속한다.
// 스레드 핸들, 정지 이벤트, 조인은 ThreadBase 가 관리한다.
class ClientSessionScheduler final : public Core::Concurrency::ThreadBase
{
public:
	ClientSessionScheduler();
	~ClientSessionScheduler() override;

public:
	bool Initialize(ClientSession* clientSession, TlsMemoryPool* jobMemoryPool, TlsMemoryPool* packetMemoryPool);
	void Finalize();

protected:
	void Run() override;

private:
	TlsMemoryPool* m_jobMemoryPool = nullptr;
	ClientSession* m_clientSession = nullptr;
	TlsMemoryPool* m_packetMemoryPool = nullptr;
};
