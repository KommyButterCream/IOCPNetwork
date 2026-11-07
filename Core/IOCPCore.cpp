#include "EngineExecutionScope.h"
#include "IOCPCore.h"

#include <stdio.h> // for swprintf_s

#include "../Diagnostics/EngineAssert.h"
#include <memory> // for std::make_unique
#include <utility> // for std::move

#include "../../Core/Util/Logger.h"
#include "../../Core/Concurrency/ThreadBase.h"

#pragma comment(lib, "ws2_32.lib") // for WinSock2

using namespace Core::Util;

// GQCS 워커 스레드.
// ThreadBase 는 객체 1개당 스레드 1개이므로 워커 N개를 두려면 객체를 N개 만든다.
// 실제 루프는 IOCPCore 가 갖고 있고 이 클래스는 그 루프로 진입만 시킨다.
class IOCPWorkerThread final : public Core::Concurrency::ThreadBase
{
public:
	explicit IOCPWorkerThread(const wchar_t* name) : ThreadBase(name) {}

	void Bind(IOCPCore* owner, uint32_t workerIndex)
	{
		m_owner = owner;
		m_workerIndex = workerIndex;
	}

protected:
	void Run() override
	{
		if (m_owner)
		{
			m_owner->IOCPWorkerThreadLoop(*this, m_workerIndex);
		}
	}

private:
	IOCPCore* m_owner = nullptr;
	uint32_t m_workerIndex = 0;
};

IOCPCore::IOCPCore()
{
}

IOCPCore::~IOCPCore()
{
	Stop();
}

bool IOCPCore::Start()
{
	// 종료 게이트를 되돌린다.
	//
	// Stop 은 이 플래그를 1 로 올리고 다시는 내리지 않았다. 그래서
	// Start -> Stop -> Start 를 한 객체에서 하면 두 번째 Stop 이 통째로
	// 무시됐다 — GQCS 워커를 조인하지 않고, IOCP 핸들도 소켓도 닫지 않는다.
	// 기동이 실패해 다시 시도하는 서비스(포트가 잠깐 잡혀 있던 경우 등)가
	// 그대로 이 경로를 밟는다.
	//
	// 여기서 되돌리면 두 번째 주기가 자기 자원을 정상적으로 정리한다.
	m_destroyFlag.store(false);

	if (!InitializeWinsock())
	{
		return false;
	}

	if (!InitializeIOCPHandle())
	{
		return false;
	}

	if (!CreateCompletionWorkers())
	{
		return false;
	}

	return true;
}

void IOCPCore::Stop()
{
	if (m_destroyFlag.exchange(true))
		return;

	LOGI("IOCP core shutting down");

	StopCompletionWorkers();
	FinalizeIOCPHandle();
	FinalizeWinsock();
}

void IOCPCore::StopCompletionWorkers()
{
	RequestCompletionWorkersStop();
	JoinCompletionWorkers();
}

HANDLE IOCPCore::GetIOCPHandle() const noexcept
{
	return m_iocpHandle;
}

void IOCPCore::SetEngineLogLevel(int level)
{
	if (level < static_cast<int>(LogLevel::LOG_DEBUG)) level = static_cast<int>(LogLevel::LOG_DEBUG);
	if (level > static_cast<int>(LogLevel::LOG_NO_USE)) level = static_cast<int>(LogLevel::LOG_NO_USE);

	Logger::SetLogLevel(static_cast<LogLevel>(level));
}

int IOCPCore::GetEngineLogLevel()
{
	return static_cast<int>(Logger::GetLogLevel());
}

bool IOCPCore::SetEngineLogFile(const char* filePath)
{
	return Logger::OpenFile(filePath);
}

void IOCPCore::FlushEngineLog()
{
	Logger::Flush();
}

// 공개 헤더의 상수와 Core 의 정의가 어긋나면 여기서 컴파일이 멈춘다.
static_assert(static_cast<unsigned int>(ENGINE_LOG_SINK_CONSOLE) == static_cast<unsigned int>(Core::Util::LOG_SINK_CONSOLE), "EngineLogSink out of sync with Core::Util::LogSink");
static_assert(static_cast<unsigned int>(ENGINE_LOG_SINK_FILE) == static_cast<unsigned int>(Core::Util::LOG_SINK_FILE), "EngineLogSink out of sync with Core::Util::LogSink");

void IOCPCore::SetEngineLogSinks(unsigned int sinks)
{
	Logger::SetSinks(sinks);
}

unsigned int IOCPCore::GetEngineLogSinks()
{
	return Logger::GetSinks();
}

bool IOCPCore::InitializeWinsock()
{
	WSADATA wsaData;
	int result(-1);
	constexpr int sucesss(0);

	::ZeroMemory(&wsaData, sizeof(WSADATA));

	// WS2_32.dll 사용을 시작하기 위해 호출
	result = ::WSAStartup(MAKEWORD(2, 2), &wsaData);
	if (result != sucesss)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] WSAStartup failed\n", __FUNCTION__);

		return false;
	}

	m_winsockInitialized = true;

	//Log::log(LogLevel::LOG_INFO, "[%s] WSAStartup success\n", __FUNCTION__);

	return true;
}

void IOCPCore::FinalizeWinsock()
{
	if (m_winsockInitialized)
	{
		// WS2_32.dll 사용 해제를 위해 호출
		if (::WSACleanup() == SOCKET_ERROR)
		{
			LOGE("WSACleanup failed (error %d)", ::WSAGetLastError());
		}

		m_winsockInitialized = false;
	}
}

bool IOCPCore::InitializeIOCPHandle()
{
	HANDLE iocpHandle = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
	if (iocpHandle == nullptr)
	{
		// CreateIoCompletionPort 는 실패 시 NULL 을 반환한다. INVALID_HANDLE_VALUE 가 아니다.
		LOGE("CreateIoCompletionPort failed (error %lu)", ::GetLastError());
		return false;
	}

	// 결과를 지역 변수로 먼저 받는 이유는, 실패한 경우 m_iocpHandle 이
	// INVALID_HANDLE_VALUE 센티널을 그대로 유지하도록 하기 위함이다.
	// (FinalizeIOCPHandle, RequestCompletionWorkersStop 가 이 센티널을 전제로 동작한다)
	m_iocpHandle = iocpHandle;

	return true;
}

void IOCPCore::FinalizeIOCPHandle()
{
	if (m_iocpHandle != INVALID_HANDLE_VALUE)
	{
		if (!::CloseHandle(m_iocpHandle))
		{
			LOGE("failed to close the IOCP handle (error %lu)", ::GetLastError());
		}

		m_iocpHandle = INVALID_HANDLE_VALUE;
	}
}

void IOCPCore::CloseSocketHandle(SOCKET socket)
{
	// 소유자가 분리한 소켓을 닫고 결과를 기록한다.
	if (socket != INVALID_SOCKET)
	{
		if (::closesocket(socket) == SOCKET_ERROR)
		{
			LOGE("closesocket failed for socket %d (error %d)",
				static_cast<int>(socket), ::WSAGetLastError());
		}
		else
		{
			// 접속 1건당 최소 1회 호출되므로 추적 레벨로 둔다.
			LOGT("socket %d closed", static_cast<int>(socket));
		}
	}
}

void IOCPCore::SetIOCPThreadCount(DWORD threadCount)
{
	m_iocpThreadCount = threadCount;
}

bool IOCPCore::CreateCompletionWorkers()
{
	if (m_iocpThreadCount == 0 || !m_completionWorkers.empty())
	{
		LOGE("invalid IOCP worker state (requested %lu, existing %zu)", m_iocpThreadCount, m_completionWorkers.size());
		return false;
	}

	try
	{
		// 스레드를 띄우기 전에 컨테이너 공간을 확보하고, 시작 전부터 소유권을 보관한다.
		m_completionWorkers.reserve(m_iocpThreadCount);
		for (DWORD i = 0; i < m_iocpThreadCount; ++i)
		{
			wchar_t threadName[64]{};
			::swprintf_s(threadName, L"IOCP-GQCS-%lu", i);
			auto worker = std::make_unique<IOCPWorkerThread>(threadName);
			worker->Bind(this, i);
			m_completionWorkers.push_back(std::move(worker));
			if (!m_completionWorkers.back()->Start())
			{
				LOGE("failed to start IOCP worker %lu", i);
				StopCompletionWorkers();
				return false;
			}
		}
	}
	catch (...)
	{
		// 이미 시작한 워커도 깨워 조인한 뒤 상위 시작 실패 처리로 전달한다.
		StopCompletionWorkers();
		throw;
	}

	LOGI("IOCP GQCS workers started (count %zu)", m_completionWorkers.size());
	return true;
}

void IOCPCore::RequestCompletionWorkersStop()
{
	if (m_completionWorkers.empty())
		return;

	// 모든 정지 이벤트를 먼저 설정한다. GQCS에서 잠든 워커는 완료 패킷으로 깨운다.
	for (const auto& worker : m_completionWorkers)
		worker->RequestStop();

	if (m_iocpHandle == INVALID_HANDLE_VALUE)
		return;

	for (const auto& worker : m_completionWorkers)
	{
		// 네이티브 스레드 생성에 실패한 마지막 객체에는 깨울 스레드가 없다.
		if (worker->GetThreadId() == 0)
			continue;
		if (!::PostQueuedCompletionStatus(m_iocpHandle, 0, TERMINATE_CODE, nullptr))
		{
			LOGE("failed to wake IOCP worker %u for shutdown (error %lu)", worker->GetThreadId(), ::GetLastError());
			ENGINE_BREAK_IF_DEBUGGER();
		}
	}
}

void IOCPCore::JoinCompletionWorkers()
{
	if (m_completionWorkers.empty())
		return;

	// 파생 객체가 살아 있는 동안 전부 조인한다. 이후 소유 컨테이너를 비운다.
	for (const auto& worker : m_completionWorkers)
		worker->Join();
	m_completionWorkers.clear();
	LOGI("IOCP GQCS workers exited");
}

void IOCPCore::IOCPWorkerThreadLoop(IOCPWorkerThread& worker, uint32_t workerIndex)
{
	EngineExecutionScope execution;
	LOGI("GQCS worker %u entering loop", workerIndex);

	while (!worker.IsStopRequested())
	{
		BOOL completionStatus = FALSE;
		DWORD bytesTransferred = 0;
		ULONG_PTR completionKey = 0;
		LPOVERLAPPED overlapped = nullptr;

		// GetQueuedCompletionStatusEx 라는 확장형 함수도 있다. 나중에 사용해보자
		completionStatus = ::GetQueuedCompletionStatus(
			m_iocpHandle,
			&bytesTransferred,
			&completionKey,
			&overlapped,
			INFINITE);
		const DWORD completionError = completionStatus ? ERROR_SUCCESS : ::GetLastError();

		if (completionError == ERROR_SUCCESS && completionKey == TERMINATE_CODE && overlapped == nullptr)
		{
			// 종료 코드를 수신했으므로 이 스레드만 종료한다.
			// 이전 구현은 공유 bool 을 false 로 바꿔서 다른 워커까지 한꺼번에
			// 빠져나가게 만들었는데, 정지 판정이 워커별 이벤트로 바뀌었으므로
			// 각 워커는 자기 몫의 종료 코드를 받고 나간다.
			LOGI("GQCS worker %u received the terminate code", workerIndex);
			break;
		}

		// 여기 있던 "정지 요청이면 그냥 나간다" 검사를 뺐다.
		//
		// 이미 큐에서 꺼낸 완료 통지를 처리하지 않고 나가면, 그 I/O 의
		// 완료 가드가 I/O를 반환할 기회가 없다. 세션은 카운트가 0 이 아닌 채로
		// 정리되고 "finalized while N IO operations are still outstanding" 이 남는다.
		// 실측(예전 bench 로그) : 버려진 통지 수와 그 위반 수가 8:9, 9:10, 6:7,
		// 12:13 으로 나란히 움직였다 — 버린 통지가 그대로 그 위반이었다.
		//
		// 막 꺼낸 통지는 처리하는 편이 낫다. 그래야 회계가 맞고, 이 검사가
		// 없어도 루프 머리의 같은 검사가 다음 바퀴에서 내보낸다. GQCS 의 무한
		// 대기를 깨우는 것은 이 검사가 아니라 RequestCompletionWorkersStop 가 넣는
		// 종료 코드다.
		//
		// 이 워커가 종료 코드를 소비하지 않고 나가도 괜찮다. 코드는 워커 수만큼
		// 들어가 있고 한 워커는 많아야 하나를 소비하므로, 대기 중인 워커의 몫은
		// 언제나 남는다.
		HandleCompletion(completionKey, overlapped, bytesTransferred, completionError);
	}

	LOGI("GQCS worker %u leaving loop", workerIndex);
}

bool IOCPCore::RegisterSocketToIOCP(ULONG_PTR completionKey, SOCKET socket)
{
	if (::CreateIoCompletionPort(
		(HANDLE)socket,
		GetIOCPHandle(),
		completionKey,
		0) == NULL)
	{
		// 등록 실패한 소켓은 완료 통지를 받지 못하므로 그 세션은 쓸 수 없다.
		// INFO 가 아니라 ERROR 가 맞다.
		LOGE("failed to associate socket %d with the IOCP (error %lu)",
			static_cast<int>(socket), ::GetLastError());
		return false;
	}

	return true;
}

