#pragma once
// 서비스용 공개 진입 헤더입니다. 내부 Job·큐·풀 헤더는 공개 API가 아닙니다.
#include "Core/IOCPServer.h"
#include "Core/IOCPClient.h"
#include "Session/ISession.h"
#include "Job/HandlerContext.h"
#include "Protocol/PacketHeader.h"
