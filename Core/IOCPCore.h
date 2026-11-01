#pragma once

// Windows.h 파일이 필요한 경우에는
// WIN32_LEAN_AND_MEAN 를 먼저 정의하고
// Windows.h 를 incldue 해야한다.
// 또한, 그 순서는 Windows.h 가 가장 먼저 include 되어야한다.
// Winsock.h 헤더 파일의 선언은 Windows sockets 2.0에 필요한
// Winsock2.h 헤더 파일의 선언과 충돌합니다.
// WIN32_LEAN_AND_MEAN 매크로를 사용하면 
// Winsock.h 가 Windows.h 헤더에 포함되지 않습니다. 
// 

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <Windows.h>

//#ifndef NOMINMAX
//#define NOMINMAX
//#endif

#include <WinSock2.h>
//#include <ws2tcpip.h>
//#include <iphlpapi.h>

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

// IOCP Server/Client 를 위한 공용 기능 구현
// Winsock 을 초기화 하고 GQCS 를 위한 스레드풀과 IOCP 핸들을 초기화 한다.
// Server 와 Client 는 IOCPCore 를 상속받아 구현

#include <stdint.h>

struct OverlappedEx;
enum class IO_OPERATION;

// GQCS 워커 스레드. Core::Concurrency::ThreadBase 파생이지만
// 전방 선언만 둔다.
//   - 엔진 내부 구현 세부사항이므로 소비자에게 노출할 필요가 없다
//   - ThreadBase 는 export 되지 않으므로, 이 클래스를 공개 헤더에서
//     dllexport 로 정의하면 C4275 (DLL 인터페이스가 아닌 기본 클래스) 가 난다
// 실제 정의는 IOCPCore.cpp 에 있다.
class IOCPWorkerThread;

// 엔진 로그 출력 대상. 비트 조합 가능.
// Core::Util::LogSink 와 값이 일치해야 하며 IOCPCore.cpp 에서 static_assert 로 검증한다.
// 소비자가 Core 헤더를 include 하지 않고도 로그를 제어할 수 있도록 여기에 둔다.
enum EngineLogSink : unsigned int
{
	ENGINE_LOG_SINK_NONE = 0,
	ENGINE_LOG_SINK_CONSOLE = 1 << 0,
	ENGINE_LOG_SINK_FILE = 1 << 1,
};

class IOCP_ENGINE_API IOCPCore
{
public:
	IOCPCore();
	virtual ~IOCPCore();

public:
	bool Start();
	void Stop();

	HANDLE GetIOCPHandle() const noexcept;

	// --- 로그 제어 ---
	//
	// 주의: Core 는 정적 라이브러리이고 Logger 의 레벨은 inline static 이므로
	// 엔진 DLL 안과 호스트 EXE 안에 각각 별도의 사본이 존재한다.
	// 호스트에서 Logger::SetLogLevel 을 호출해도 DLL 쪽 로그는 바뀌지 않는다.
	// 아래 함수들은 DLL 쪽 사본을 제어하기 위한 통로다.
	// (호스트 자신의 로그는 호스트가 Logger::SetLogLevel 로 따로 조절한다)
	static void SetEngineLogLevel(int level);
	static int GetEngineLogLevel();

	// 엔진 로그를 파일로 남긴다. 1MB 버퍼를 쓰고 ERROR 이상에서만 flush 하므로
	// 콘솔 출력보다 훨씬 빠르다.
	static bool SetEngineLogFile(const char* filePath);
	static void FlushEngineLog();

	// 출력 대상 지정. Core::Util::LOG_SINK_* 비트 조합을 넘긴다.
	// 콘솔 쓰기가 파일보다 훨씬 느리므로 부하 상황에서는 파일만 켜는 것이 좋다.
	static void SetEngineLogSinks(unsigned int sinks);
	static unsigned int GetEngineLogSinks();

protected:
	// 서버, 클라이언트에서 override 필수!
	// GQCS 에 통지 받은 IO 처리
	virtual void HandleCompletion(ULONG_PTR completionKey, LPOVERLAPPED overlapped, DWORD bytesTransferred, BOOL completionStatus) = 0;

	// HandleSocketError 는 여기에 두지 않는다.
	//
	// 순수 가상으로 선언돼 있었지만 이 기반 클래스는 한 번도 부르지 않았다.
	// 부르는 쪽은 파생 클래스의 HandleCompletion 뿐이다. 계약이 아니라
	// 주석 역할만 하면서, 세션 인자를 ISession* 로 고정시켜 파생 클래스가
	// 실제 타입(ClientSession)을 쓰지 못하게 막고 있었다.

	bool RegisterSocketToIOCP(ULONG_PTR completionKey, SOCKET socket);
	static constexpr ULONG_PTR TERMINATE_CODE = 0xCAFE;

	// socket Handle
	static void CloseSocketHandle(SOCKET socket);

	void SetIOCPThreadCount(DWORD threadCount);

private:
	// Destory Flag
	LONG m_destroyFlag = 0;

	// Winsock
	bool m_winsockInitialized = false; // Winsock 을 초기화 했는지, 안했는지에 대한 플래그 저장

	// IOCP Handle
	HANDLE m_iocpHandle = INVALID_HANDLE_VALUE; // IOCP 생성 핸들 저장

	// IOCP GQCS Threads
	// 스레드 핸들과 정지 이벤트, 조인은 ThreadBase 가 관리한다.
	DWORD m_iocpThreadCount = 0; // IOCP GQCS 스레드 수량
	IOCPWorkerThread** m_iocpWorkerThreads = nullptr;

private:
	// Winsock
	bool InitializeWinsock();
	void FinalizeWinsock();

	// IOCP Handle
	bool InitializeIOCPHandle();
	void FinalizeIOCPHandle();

	// IOCP GQCS Threads
	bool CreateIOCPWorkerthread();
	void DestroyIOCPWorkerthread();
	void RequestIOCPThreadTerminate();

public:
	// IOCPWorkerThread 가 호출한다.
	void IOCPWorkerThreadLoop(IOCPWorkerThread& worker, uint32_t workerIndex);
};



