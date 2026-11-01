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

다음 환경을 기준으로 빌드 환경을 준비하고 있습니다.

- Windows 10/11 x64
- C++20
- Visual Studio 2026
- CUDA Toolkit 13.4.3 (영상 코덱 모듈을 포함한 스트리밍 애플리케이션 빌드용)
- Windows SDK의 WinSock2 / MSWSock
- 공용 [Core](../Core) 프로젝트

Core와 이 라이브러리를 사용하는 프로젝트는 동일한 도구 집합으로 빌드해야 합니다.
특히 전체 프로그램 최적화를 사용하는 Release 구성에서는 컴파일러 버전이 다르면 링크 오류가 발생할 수 있습니다.
환경 전환 시 IOCPNetwork, Core 및 소비자 프로젝트의 플랫폼 도구 집합을 함께 맞추고 다시 빌드합니다.

현재 저장소의 프로젝트 설정은 이전 빌드 검증에 사용한 v143 기준입니다.
위 목표 환경으로의 프로젝트 설정 전환과 빌드 검증은 아직 완료하지 않았습니다.

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

## 빌드 확인 범위

프로젝트 이름 변경 후 기존 환경에서 다음 솔루션의 x64 Debug·Release 컴파일과 링크를 확인했습니다.
당시 도구 집합은 v143이며, 스트리밍 전체 솔루션은 Visual Studio 2022와 CUDA 12.8 빌드 통합을 사용했습니다.

- IOCPNetwork
- IOCPEchoMessageApp
- DesktopStreamingApp

Visual Studio 2026·CUDA Toolkit 13.4.3 목표 환경의 빌드는 아직 검증하지 않았습니다.
실제 통신 및 부하 테스트는 위 빌드 확인 범위에 포함되지 않습니다.
