#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

// WinSock2.h 를 Windows.h 보다 먼저 넣는다. 반대 순서면 Windows.h 가 끌어오는
// winsock.h 와 충돌한다.
//
// 예전에는 SOCKET 과 UINT_PTR 을 이 헤더에서 직접 typedef 했다. SDK 타입을
// 재정의하는 것이라 지금은 값이 같아 통과하지만, SDK 가 정의를 바꾸면 조용히
// 어긋난다. 필요한 타입은 원본에서 가져온다.
#include <WinSock2.h>
#include <Windows.h>

class SocketOption
{
public:
	// Reuse address
	static bool SetReuseAddress(SOCKET socket);

	// TCP No Delay (disable Nagle's Algorithm)
	static bool SetNoDelay(SOCKET socket);

	// KeepAlive
	static bool SetKeepAlive(SOCKET socket);
	static bool SetKeepAliveEx(SOCKET socket, DWORD keepAliveTime_ms, DWORD keepAliveInterval_ms);

	// Receive Buffer Size
	static bool SetReceiveBufferSize(SOCKET socket, int bufferSize);

	// Send Buffer Size
	static bool SetSendBufferSize(SOCKET socket, int bufferSize);

	// Linger
	static bool SetLinger(SOCKET socket, bool enable, int lingerTime = 0);

	// Update Accept Context
	static bool SetAcceptContext(SOCKET clientSocket, SOCKET listenSocket);

	// Update Connect Context
	static bool SetClientContext(SOCKET clientSocket);
};

