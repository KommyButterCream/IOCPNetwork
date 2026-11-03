# IOCPNetwork

Windows의 IOCP(I/O Completion Port)를 기반으로 구현한 C++ TCP 서버·클라이언트 통신 라이브러리입니다.
비동기 송수신, 세션 관리, 패킷 처리 스케줄링, 메모리 풀을 제공하며 DLL 형태로 빌드합니다.

## 주요 기능

- IOCP 기반 비동기 TCP 통신
- 서버의 연결 수락 및 클라이언트의 서버 접속
- 세션 풀과 세션 수명 관리
- 송수신 패킷 버퍼 및 송신 큐
- 세션별 작업 큐와 스케줄러를 통한 패킷 처리
- 시스템 인증 패킷과 하트비트를 통한 연결 상태 관리
- 메모리 풀을 이용한 패킷·작업 객체 할당 및 반납
- 공용 Core 라이브러리 연동

## 목표 개발 및 빌드 환경

다음 환경을 기준으로 개발합니다.

- Windows 10/11 x64
- C++20
- Visual Studio 2026
- CUDA Toolkit 13.4.3 (영상 코덱 모듈을 포함한 스트리밍 애플리케이션 빌드용)
- Windows SDK의 WinSock2 / MSWSock
- 공용 [Core](../Core) 프로젝트

Core와 이 라이브러리를 사용하는 프로젝트는 동일한 도구 집합으로 빌드해야 합니다.
특히 전체 프로그램 최적화를 사용하는 Release 구성에서는 컴파일러 버전이 다르면 링크 오류가 발생할 수 있습니다.
환경 전환 시 IOCPNetwork, Core 및 소비자 프로젝트의 플랫폼 도구 집합을 함께 맞추고 다시 빌드합니다.

현재 IOCPNetwork와 Core는 v145 도구 집합과 C++20을 사용합니다.
Visual Studio 2026에서 엔진, Echo 예제, Streaming 서버·클라이언트 서비스의 x64 Debug·Release 빌드를 확인했습니다.
CUDA를 사용하는 영상 코덱과 전체 스트리밍 GUI의 검증 범위는 아래에 별도로 적었습니다.

## 저장소 배치

`IOCPNetwork`와 `Core`를 같은 상위 폴더에 배치합니다.

```text
Module/
├── Core/
│   └── Core/
│       └── Core.vcxproj
└── IOCPNetwork/
    ├── IOCPNetwork.sln
    └── IOCPNetwork/
        └── IOCPNetwork.vcxproj
```

솔루션은 `../Core/Core/Core.vcxproj` 경로의 공용 Core 프로젝트를 참조합니다.
두 프로젝트는 별도 저장소로 관리할 수 있지만, 빌드할 때는 위 상대 경로를 유지해야 합니다.

## 빌드 방법

1. Visual Studio 2026에서 `IOCPNetwork.sln`을 엽니다.
2. IOCPNetwork와 Core의 플랫폼 도구 집합을 준비한 환경에 맞춰 통일하고, C++ 언어 표준을 C++20으로 설정합니다.
3. 솔루션 플랫폼을 `x64`로 선택합니다.
4. 구성을 `Debug` 또는 `Release`로 선택하고 솔루션을 다시 빌드합니다.

독립 솔루션의 기본 출력 경로는 다음과 같습니다.

| 구성 | 출력 폴더 | 주요 결과물 |
|---|---|---|
| Debug | `x64/Debug/` | `IOCPNetwork.dll`, `IOCPNetwork.lib` |
| Release | `x64/Release/` | `IOCPNetwork.dll`, `IOCPNetwork.lib` |

다른 애플리케이션의 솔루션에서 프로젝트 참조로 빌드하면 출력 경로는 해당 솔루션의 설정을 따릅니다.

### 스트리밍 예제 빌드 시 참고

IOCPNetwork 자체에는 CUDA가 필요하지 않습니다.
다만 이 라이브러리를 사용하는 DesktopStreamingApp 전체 솔루션은 영상 코덱 모듈에서 CUDA를 사용합니다.
스트리밍 애플리케이션의 목표 환경은 Visual Studio 2026과 CUDA Toolkit 13.4.3입니다.
환경 준비 후 CUDA 빌드 통합과 영상 코덱 프로젝트의 CUDA 참조 경로를 설치된 버전에 맞춰 갱신해야 합니다.
기존 CUDA 12.8 참조 설정을 그대로 사용하지 않도록 확인합니다.

## 프로젝트 구성

| 폴더 | 역할 |
|---|---|
| `Buffer/` | 수신 패킷 버퍼, 송신 큐, 공유 송신 패킷 및 버퍼 설정 |
| `Core/` | IOCP 실행 기반, 서버·클라이언트, 하트비트 및 연결 감시 |
| `Session/` | 세션 상태와 수명 관리, 세션 풀, 세션별 작업 큐 |
| `Scheduler/` | 서버의 준비된 세션 및 클라이언트 작업 처리 스케줄링 |
| `HandlerTable/` | 패킷 ID별 처리 함수 등록 및 조회 |
| `Memory/` | 메모리 풀 구현, 스레드별 캐시, 풀 설정 및 할당·반납 보조 함수 |
| `Network/` | 소켓 옵션과 Overlapped I/O 보조 구조체 |
| `Protocol/` | 패킷 헤더, 패킷 ID, 시스템 패킷 정의 |
| `Job/` | 세션 작업과 패킷 처리 함수·컨텍스트 정의 |
| `Diagnostics/` | 엔진 불변식 검사 및 진단 카운터 |
| `docs/` | 프로젝트 설명 및 구조 검토 문서 |

## 서비스에서 사용하는 API

공개 진입 헤더는 `IOCPNetwork.h`입니다. 필요한 경우 `Core/IOCPServer.h`,
`Core/IOCPClient.h` 등 개별 공개 헤더를 포함해도 됩니다.

- 서버·클라이언트 구현: `IOCPServer`, `IOCPClient` 상속 및 콜백 재정의
- 연결 사용: 콜백의 `ISession*`, 클라이언트의 `GetSession()`
- 핸들러 등록: `RegisterPacketHandler(packetId, handler)`
- 패킷 생성: 엔진 객체 또는 `HandlerContext`의 `CreatePacket(size)`
- 공유 송신용 변환: `CreateSharedPacket(std::move(packet))`
- 송신: `ISession::EnqueueSendPacket()`, `EnqueueSharedSendPacket()`

서비스는 `ClientSession`, `Job`, `JobHandle`, 큐, `TlsMemoryPool`, `PacketHandlerTable`을 직접 사용하지 않습니다.
기존의 풀·큐·핸들러 테이블 getter는 제거했습니다. 이 변경을 적용할 때는 엔진과 소비자 프로젝트를 함께 다시 빌드합니다.

아래는 핸들러 안에서 응답 패킷을 만드는 형태입니다. `ResponsePacket`은 서비스가 정의한 패킷 타입이며
생성자에서 패킷 ID와 전체 길이를 설정한다고 가정합니다.

```cpp
#include "IOCPNetwork.h"
#include <memory>  // for std::construct_at
#include <utility> // for std::move

bool SendResponse(ISession& session, const HandlerContext& context)
{
    auto packet = context.CreatePacket(sizeof(ResponsePacket));
    if (!packet)
        return false;

    auto* response = std::construct_at(
        reinterpret_cast<ResponsePacket*>(packet.MutableView().data()));
    // response의 서비스 데이터를 채웁니다.

    return session.EnqueueSendPacket(std::move(packet));
}
```

`CreatePacket()`은 풀에서 지정한 크기의 버퍼를 확보합니다. 패킷 ID·길이 설정이나 직렬화까지 수행하지 않습니다.
0바이트, 프로토콜 상한 초과, 시작 전 또는 풀 할당 실패 시에는 빈 핸들을 반환합니다.
패킷 타입은 별도의 소멸 처리가 필요 없는 전송용 데이터로 구성합니다. 핸들은 버퍼를 반환하며 사용자 타입의 소멸자를 호출하지 않습니다.

### 소유권과 수명

| 호출 | 소유권 규약 |
|---|---|
| `EnqueueSendPacket(std::move(packet))` | 성공하면 송신 큐로 이전, 실패하면 호출자의 핸들에 유지 |
| `CreateSharedPacket(std::move(packet))` | 성공·실패 모두 원래 핸들을 소비; 실패하면 자동 반환 |
| `EnqueueSharedSendPacket(shared)` | 값으로 복사된 참조 한 개를 소비; 원래 `shared`는 유지 |
| `SubmitPacketJob(session, id, std::move(packet))` | 성공하면 Job으로 이전, 실패하면 자동 반환 |
| `TakeCurrentPacket()` | 현재 실행 중인 클라이언트 패킷 핸들러에서만 소유권 회수 |

공유 핸들 복사는 패킷 바이트를 복사하지 않습니다. 공유 전환 후 기존 포인터나 `MutableView()`로 얻은 주소로 내용을 변경하지 않습니다.
`ISession*`과 `HandlerContext`는 빌린 참조입니다. 세션 포인터를 보관한다고 연결 수명이 연장되지 않으며,
`IsEstablished()` 확인만으로 이후 호출까지 수명이 고정되지도 않습니다. 서비스는 세션을 `delete`하지 않습니다.
서비스에 따로 보관한 패킷 핸들은 `StopServer()` / `StopClient()` 전에 정리하고,
엔진 종료와 패킷 생성·송신을 동시에 진행하지 않습니다. 엔진 파괴·재시작을 넘어 핸들이나 컨텍스트를 보관하지 않습니다.

## 빌드 확인 범위

2026-11-03 기준, Visual Studio 2026 / v145 / C++20 / x64에서 다음 항목을 확인했습니다.

- IOCPNetwork 및 IOCPEchoMessageApp 솔루션: Debug·Release 빌드
- StreamingServer·StreamingClient 서비스 프로젝트: Debug·Release 빌드
- 공개 헤더와 import 라이브러리만 사용하는 소비자: `/W4` 빌드, 경고 0
- 실제 DLL을 사용하는 TCP 에코 및 공유 송신: 정상 송신, 연결 종료, 미완료 송신 중 서버 종료
- 내부 객체 파일을 연결한 회귀 테스트: Job 큐/반환, 실패 경로, 공유 참조, DB/System의 기존 정리 경로
- 검증한 종료 시나리오: 미완료 I/O 0, 사용 중 세션 0, 풀 할당·반환 균형

CUDA Toolkit 13.4.3의 영상 인코딩·디코딩과 전체 스트리밍 GUI는 이번 검증에 포함하지 않았습니다.
공개 API 및 export 변경 전후, 세부 검증 결과와 남은 설계 과제는
[공개 API 정리 작업 보고서](docs/public-api-refactor.html)에 정리했습니다.

## 수신 버퍼 리팩토링

RecvPacketBuffer는 64바이트 정렬 C++ operator new/delete와 unique_ptr로 버퍼를 관리합니다.
PrepareForReceive() → GetWritableSpan() → WSARecv → CommitWrite() → ReadPacket() 순서로 사용합니다.
ReadPacket()은 PacketReadStatus와 PacketHandle, 패킷 ID를 담은 PacketReadResult를 반환합니다.
헤더·패킷 복사는 CopyReadableBytes()로 통합했습니다.

위치·크기는 std::size_t이며, 이는 메모리 크기 표현의 일관성을 위한 선택입니다. 성능 향상을 의미하지 않습니다.
m_readOffset / m_writeOffset은 바이트 위치, m_bufferedSize는 미추출 바이트 수,
m_wrapMask는 링 순환용 마스크입니다. 프로토콜 필드는 고정 폭 정수를 유지합니다.
초기화 시 WSABUF의 길이 범위를 검사하며, 비동기 수신 중에는 버퍼를 이동·초기화·해제하지 않습니다.

Debug·Release 빌드와 정렬·링 경계·실패·반환 회귀 검증을 완료했습니다.
변경 전후 이름, 소유권 규약과 성능 최적화 후속 과제는
[보고서의 수신 버퍼 정리](docs/public-api-refactor.html#recv-buffer)를 참고합니다.
