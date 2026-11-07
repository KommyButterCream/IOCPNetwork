#pragma once
#include <WinSock2.h> // for WSAECONNRESET and related socket errors

// 로컬 취소(ERROR_OPERATION_ABORTED)는 별도 경로로 처리한다.
[[nodiscard]] inline bool IsConnectionClosedError(int error) noexcept
{
    switch (error)
    {
    case WSAECONNRESET:
    case WSAECONNABORTED:
    case WSAENOTCONN:
    case WSAESHUTDOWN:
    case ERROR_NETNAME_DELETED:
    case ERROR_CONNECTION_ABORTED:
        return true;
    default:
        return false;
    }
}
