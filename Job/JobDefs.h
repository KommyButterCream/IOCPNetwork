#pragma once

#include <stdint.h>

class ISession;
class IDBConnection;
class TlsMemoryPool;
class IOCPCore;

struct HandlerContext
{
	TlsMemoryPool* jobMemoryPool = nullptr;
	TlsMemoryPool* packetMemoryPool = nullptr;
	void* serviceContext = nullptr;
};

// 도메인별 함수 규격
typedef bool (*PacketHandlerFunc)(ISession* session, const char* packetData, uint32_t packetSize, const HandlerContext& context);
typedef bool (*DBHandlerFunc)(IDBConnection* dbConnection, const char* query, uint32_t queryLength, const HandlerContext& context);
typedef bool (*SystemHandlerFunc)(IOCPCore* iocpCore, const void* param, uint32_t paramSize, const HandlerContext& context);

// 단일 Job 구조체 사용하므로 내부에서 어떤 Job 을 처리하는지 구분하기 위한 타입
enum class JobType : uint8_t
{
	NONE,
	PACKET,
	DB,
	SYSTEM
};
