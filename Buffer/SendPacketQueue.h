#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <cstdint> // for uint32_t

#include "PacketHandle.h"
#include "SharedPacketHandle.h"

#include <cstddef> // for std::byte, std::size_t
#include <span>    // for std::span
#include <utility> // for std::move
#include <variant> // for std::variant, std::visit

class TlsMemoryPool;

// 송신할 패킷의 소유권을 보관합니다.
// 단독 소유 패킷과 공유 패킷 중 하나만 보유합니다.
// 연결 리스트의 노드 정보는 포함하지 않습니다.
class SendPacket final
{
public:
	SendPacket() noexcept = default;
	~SendPacket() = default;

	SendPacket(const SendPacket&) = delete;
	SendPacket& operator=(const SendPacket&) = delete;

	SendPacket(SendPacket&&) noexcept = default;
	SendPacket& operator=(SendPacket&&) noexcept = default;

	explicit SendPacket(PacketHandle&& packetHandle) noexcept
		: m_packetHandle(std::move(packetHandle))
	{
	}

	explicit SendPacket(SharedPacketHandle&& packetHandle) noexcept
		: m_packetHandle(std::move(packetHandle))
	{
	}

	[[nodiscard]]
	explicit operator bool() const noexcept
	{
		return std::visit(
			[](const auto& handle) noexcept
			{
				return static_cast<bool>(handle);
			},
			m_packetHandle);
	}

	[[nodiscard]]
	std::span<const std::byte> View() const noexcept
	{
		return std::visit(
			[](const auto& handle) noexcept
			{
				return handle.View();
			},
			m_packetHandle);
	}

	[[nodiscard]]
	std::size_t Size() const noexcept
	{
		return std::visit(
			[](const auto& handle) noexcept
			{
				return handle.Size();
			},
			m_packetHandle);
	}

	// 현재 패킷을 반환하고 빈 단독 소유 핸들로 초기화합니다.
	void Reset() noexcept
	{
		m_packetHandle.emplace<PacketHandle>();
	}

private:
	std::variant<PacketHandle, SharedPacketHandle> m_packetHandle;
};

// 엔진 내부 FIFO 송신 큐입니다.
// 노드는 SendPacket으로 단독 또는 공유 패킷을 소유합니다.
// 여러 생산자가 동시에 등록할 수 있으며, ClientSession이 소비를 직렬화합니다.
// Initialize/Finalize는 생산자와 소비자가 접근하지 않는 상태에서 호출합니다.
class SendPacketQueue
{
public:
	SendPacketQueue() noexcept = default;
	~SendPacketQueue();

	SendPacketQueue(const SendPacketQueue&) = delete;
	SendPacketQueue& operator=(const SendPacketQueue&) = delete;

	SendPacketQueue(SendPacketQueue&&) = delete;
	SendPacketQueue& operator=(SendPacketQueue&&) = delete;

	// maxCount 는 이 큐에 동시에 담을 수 있는 패킷 수의 상한이다.
	// 패킷 소유권은 각 노드의 SendPacket이 관리합니다.
	[[nodiscard]]
	bool Initialize(TlsMemoryPool* sendQueueMemoryPool, uint32_t maxCount);
	void Finalize();

	// 성공할 때만 소유권을 이동합니다. 실패하면 호출자의 핸들은 유지됩니다.
	[[nodiscard]]
	bool Enqueue(PacketHandle&& packetHandle);

	// 값으로 받은 참조 하나를 등록합니다. 실패하면 그 참조는 반환됩니다.
	[[nodiscard]]
	bool EnqueueShared(SharedPacketHandle packetHandle);

	// 대기 패킷의 소유권을 반환합니다. 큐가 비어 있으면 빈 값을 반환합니다.
	// 패킷을 보관하던 노드는 함수 안에서 풀로 반환합니다.
	[[nodiscard]]
	SendPacket Dequeue();

	void Reset();

	// 락을 획득해서 확인한다. 락을 보유하지 않은 외부 경로에서 사용한다.
	[[nodiscard]]
	bool IsEmpty() const;

	// 대기 중인 패킷 수 / 대기 큐의 상한입니다. 이미 꺼낸 송신 중 패킷은 제외합니다.
	[[nodiscard]]
	uint32_t GetCount() const;

	[[nodiscard]]
	uint32_t GetMaxCount() const noexcept { return m_maxCount; }

	// 엔진 초기화 시 송신 큐 풀의 블록 크기를 결정하는 데 사용합니다.
	[[nodiscard]]
	static std::size_t GetNodeSize() noexcept;

private:
	struct Node;

	template<typename Handle>
	[[nodiscard]]
	bool EnqueueHandle(Handle&& packetHandle);

	[[nodiscard]]
	Node* CreateNode();

	// 큐 내부 노드를 소멸시키고 풀에 반환합니다.
	void ReleaseNode(Node* entry);

	mutable SRWLOCK m_srwLock = SRWLOCK_INIT;

	TlsMemoryPool* m_sendQueueMemoryPool = nullptr;

	Node* m_head = nullptr;
	Node* m_tail = nullptr;

	uint32_t m_count = 0;
	uint32_t m_maxCount = 0;
};
