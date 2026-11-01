#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

class ISession;

class IOCP_ENGINE_API ReadySessionQueue
{
public:
	ReadySessionQueue();
	~ReadySessionQueue();

private:
	ISession** m_queue = nullptr;
	int32_t m_capacity = 0;
	int32_t m_head = 0;
	int32_t m_tail = 0;
	int32_t m_count = 0;

	mutable SRWLOCK m_srwLock = SRWLOCK_INIT;
	CONDITION_VARIABLE m_cv = CONDITION_VARIABLE_INIT;
	bool m_stopFlag = false;

public:
	bool Initialize(const uint32_t maxSessionCount);
	void Finalize();

	bool Push(ISession* session);
	ISession* Pop(const uint32_t timeout_ms);

	void WakeAll();

	bool IsEmpty() const;
	int32_t GetCount() const;
};
