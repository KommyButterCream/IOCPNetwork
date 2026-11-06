#pragma once

#include <stdint.h>

class ISession;
class IDBConnection;
class IOCPCore;

#include "HandlerContext.h"

// 도메인별 함수 규격
typedef bool (*DBHandlerFunc)(IDBConnection* dbConnection, const char* query, uint32_t queryLength, const HandlerContext& context);
typedef bool (*SystemHandlerFunc)(IOCPCore* iocpCore, const void* param, uint32_t paramSize, const HandlerContext& context);

// 엔진 내부 작업 분류입니다. 데이터는 Job의 핸들이 소유합니다.
enum class JobType : uint8_t
{
	NONE,
	PACKET,
	DB,
	SYSTEM
};
