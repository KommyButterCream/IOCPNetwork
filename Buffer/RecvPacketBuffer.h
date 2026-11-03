#pragma once

#include <cstdint> // for uint16_t
#include <memory> // for std::unique_ptr
#include <span> // for std::span, std::as_writable_bytes
#include <cstddef> // for std::byte, std::size_t

#include "../Protocol/PacketHeader.h"
#include "PacketHandle.h"

class TlsMemoryPool;

// 링에서 패킷 하나를 꺼낸 결과
//
//   Ok           : 결과의 packet이 추출한 패킷을 소유한다
//   NeedMoreData : 아직 다 안 왔다. 다음 수신을 기다리면 된다
//   Invalid      : 헤더가 규칙을 어겼다. 피어 잘못이므로 세션을 끊어야 한다
//   OutOfMemory  : 패킷 풀이 고갈됐다. 우리 잘못이지만 역시 세션을 끊는다
enum class PacketReadStatus
{
	Ok,
	NeedMoreData,
	Invalid,
	OutOfMemory,
};

// 패킷 추출 상태와 추출한 패킷의 소유권을 함께 반환
struct [[nodiscard]] PacketReadResult
{
	PacketReadStatus status = PacketReadStatus::NeedMoreData;
	PacketHandle packet;
	uint16_t packetId = ToInteger(PACKET_ID::NONE);
};

class RecvPacketBuffer
{
public:
	RecvPacketBuffer() = default;
	~RecvPacketBuffer();

	RecvPacketBuffer(const RecvPacketBuffer&) = delete;
	RecvPacketBuffer& operator=(const RecvPacketBuffer&) = delete;
	RecvPacketBuffer(RecvPacketBuffer&&) = delete;
	RecvPacketBuffer& operator=(RecvPacketBuffer&&) = delete;

private:
	TlsMemoryPool* m_packetMemoryPool = nullptr;

	static constexpr std::size_t BUFFER_ALIGNMENT = 64;

	struct AlignedBufferDeleter
	{
		void operator()(char* buffer) const noexcept;
	};

	std::unique_ptr<char[], AlignedBufferDeleter> m_buffer;

	std::size_t m_writeOffset = 0;
	std::size_t m_readOffset = 0;
	std::size_t m_bufferedSize = 0;

	std::size_t m_capacity = 0;
	std::size_t m_wrapMask = 0;
	std::size_t m_maxPacketSize = 0;

public:
	[[nodiscard]] bool Initialize(TlsMemoryPool* packetMemoryPool, std::size_t bufferSize, std::size_t maxPacketSize);
	void Finalize();
	void Reset();

	// 다음 수신에 사용할 연속된 쓰기 영역을 반환
	// 반환된 영역은 버퍼를 소유하지 않음
	[[nodiscard]]
	std::span<char> GetWritableSpan() noexcept;

	// 수신 완료 후 처리
	[[nodiscard]] bool CommitWrite(const std::size_t bytesReceived);

	// 성공하면 패킷 소유권을 반환하며, 실패하면 읽기 위치를 유지
	[[nodiscard]]
	PacketReadResult ReadPacket();

	// WSARecv 를 걸기 직전에 호출. 링 끝에 붙어 있으면 앞으로 당겨서
	// 한 번에 최대 패킷을 받을 수 있는 연속 공간을 만듬.
	void PrepareForReceive();

	// 상태 확인
	[[nodiscard]] std::size_t GetBufferedSize() const noexcept { return m_bufferedSize; }
	[[nodiscard]] std::size_t GetCapacity() const noexcept { return m_capacity; }
	[[nodiscard]] std::size_t GetMaxPacketSize() const noexcept { return m_maxPacketSize; }
	[[nodiscard]] bool IsFull() const noexcept { return m_bufferedSize == m_capacity; }
	[[nodiscard]] bool IsEmpty() const noexcept { return m_bufferedSize == 0; }

private:
	[[nodiscard]] bool PeekPacketHeader(PACKET_HEADER& header) const noexcept;

	// 현재 읽기 위치부터 목적지 크기만큼 복사
	// 목적지 크기는 저장된 데이터 크기 이하여야 한다.
	// 읽기 위치와 저장된 데이터 크기는 변경하지 않음.
	void CopyReadableBytes(std::span<std::byte> destination) const noexcept;

	// 남아 있는 조각을 오프셋 0 으로 모은다. 조각은 최대 패킷보다 작다.
	void CompactUnreadData();
};

