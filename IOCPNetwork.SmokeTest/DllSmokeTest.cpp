#include <IOCPNetwork/IOCPNetwork.h>

#include <iostream> // for std::cout, std::cerr

// 소비 프로젝트에도 동일한 언어 표준과 대상 플랫폼이 적용되는지 확인한다.
static_assert(__cplusplus >= 202002L, "C++20 or later is required.");
static_assert(sizeof(void*) == 8, "An x64 build is required.");

static_assert(!IOCPNetwork::ConnectionId{}.IsValid());

static_assert(IOCPNetwork::ConnectionId{ 0, 100 }.IsValid());

static_assert(
    IOCPNetwork::ConnectionId{ 3, 100 } ==
    IOCPNetwork::ConnectionId{ 3, 100 });

static_assert(
    IOCPNetwork::ConnectionId{ 3, 100 } !=
    IOCPNetwork::ConnectionId{ 3, 101 });

static_assert(
    IOCPNetwork::ConnectionId{ 3, 100 } !=
    IOCPNetwork::ConnectionId{ 4, 100 });

static_assert(
    sizeof(decltype(IOCPNetwork::ConnectionId::generation)) == 8);

int RunCommonTypesTests();

int main()
{
    const char* version = IOCPNetwork::GetLibraryVersion();

    if (version == nullptr || version[0] == '\0')
    {
        std::cerr << "FAIL: IOCPNetwork returned an empty library version.\n";

        return 1;
    }

    std::cout << "PASS: IOCPNetwork DLL call succeeded. Version: " << version << '\n';

    const int failureCount = RunCommonTypesTests();

    return failureCount == 0 ? 0 : 1;
}
