#include "RecvPacketBuffer.h"

#include "PacketHandle.h"

#include "../Diagnostics/EngineAssert.h"
#include "../Memory/TlsMemoryPool.h"
#include "../Memory/EngineMemoryPoolHelper.h"

#include <new>		// for std::align_val_t, std::nothrow, operator new, operator delete
#include <limits>	// for std::numeric_limits
#include <cstring>	// for std::memcpy, std::memmove
#include <utility>	// for std::move
#include <cassert>	// for assert
#include <bit>		// for std::has_single_bit

using namespace Core::Util;

void RecvPacketBuffer::AlignedBufferDeleter::operator()(char* buffer) const noexcept
{
	::operator delete(buffer, std::align_val_t{ BUFFER_ALIGNMENT });
}

bool RecvPacketBuffer::Initialize(TlsMemoryPool* packetMemoryPool, std::size_t bufferSize, std::size_t maxPacketSize)
{
	if (packetMemoryPool == nullptr)
	{
		return false;
	}

	// 2의 거듭제곱(Power of 2) 확인
	if (!std::has_single_bit(bufferSize))
	{
		ENGINE_VIOLATION("recv ring size %zu is not a power of two", bufferSize);
		return false;
	}

	if (maxPacketSize < sizeof(PACKET_HEADER) || maxPacketSize > PACKET_SIZE_LIMIT)
	{
		ENGINE_VIOLATION("max recv packet size %zu is outside [%zu, %u]",
			maxPacketSize, sizeof(PACKET_HEADER), PACKET_SIZE_LIMIT);
		return false;
	}

	if (bufferSize < maxPacketSize * 2)
	{
		ENGINE_VIOLATION("recv ring %zu cannot hold a fragment plus a %zu byte packet", bufferSize, maxPacketSize);
		return false;
	}

	// WSABUF::len은 32비트이므로 호출부에서 길이가 잘리지 않도록 제한
	if (bufferSize > (std::numeric_limits<std::uint32_t>::max)())
	{
		ENGINE_VIOLATION("recv ring size %zu exceeds the WSABUF length range", bufferSize);
		return false;
	}

	Finalize();

	m_packetMemoryPool = packetMemoryPool;
	m_capacity = bufferSize;
	m_wrapMask = bufferSize - 1;
	m_maxPacketSize = maxPacketSize;

	// BUFFER_ALIGNMENT 크기에 얼라인된 버퍼 할당
	m_buffer.reset(static_cast<char*>(::operator new(m_capacity, std::align_val_t{ BUFFER_ALIGNMENT }, std::nothrow)));

	if (!m_buffer)
	{
		Finalize();
		return false;
	}

	Reset();
	return true;
}

RecvPacketBuffer::~RecvPacketBuffer()
{
	Finalize();
}

void RecvPacketBuffer::Finalize()
{
	m_packetMemoryPool = nullptr;

	m_writeOffset = 0;
	m_readOffset = 0;
	m_bufferedSize = 0;

	m_capacity = 0;
	m_wrapMask = 0;
	m_maxPacketSize = 0;

	m_buffer.reset();
}

void RecvPacketBuffer::Reset()
{
	m_writeOffset = 0;
	m_readOffset = 0;
	m_bufferedSize = 0;
}

std::span<char> RecvPacketBuffer::GetWritableSpan() noexcept
{
	if (!m_buffer || IsFull())
	{
		return {};
	}

	std::size_t writableSize = 0;

	// 쓰기 위치가 읽기 위치와 같거나 뒤에 있다.
	// 빈 공간이 링 뒤쪽과 앞쪽으로 나뉠 수 있지만, 이번에는 뒤쪽의 연속 영역만 제공한다.
	// 두 위치가 같은 경우는 위에서 가득 찬 상태를 제외했으므로 빈 버퍼다.
	if (m_writeOffset >= m_readOffset)
	{
		// 가득 찬 상태는 위에서 제외했으므로 쓰기 위치부터 링 끝까지는 모두 빈 공간입니다.
		writableSize = m_capacity - m_writeOffset;
	}
	else
	{
		// 쓰기 위치가 링 앞쪽으로 돌아온 상태다.
		// [쓰기 위치, 읽기 위치)만 비어 있으며, 읽기 위치부터는 미처리 데이터이므로 제외한다.
		writableSize = m_readOffset - m_writeOffset;
	}

	return { m_buffer.get() + m_writeOffset, writableSize };
}

bool RecvPacketBuffer::CommitWrite(const std::size_t bytesReceived)
{
	// TCP의 0바이트 수신 완료는 호출부에서 연결 종료로 처리하며, 여기서는 위치를 바꾸지 않는다.
	if (!m_buffer || bytesReceived == 0)
		return false;

	const std::size_t writableSize = GetWritableSpan().size();

	// 수신을 걸 때 제공한 연속 영역보다 많은 바이트를 반영하려는 경우다.
	// 수신 대기 중 버퍼 상태가 유지된다는 전제에서 발생하면 안 되는 크기 불일치이며,
	// 읽지 않은 데이터를 침범하지 않도록 위치와 데이터 크기를 그대로 두고 실패한다.
	if (bytesReceived > writableSize) [[unlikely]]
	{
		ENGINE_VIOLATION("recv ring overflow : committing %zu bytes but only %zu writeable (stored %zu)",
			bytesReceived, writableSize, m_bufferedSize);

		return false;
	}

	m_writeOffset = (m_writeOffset + bytesReceived) & m_wrapMask;
	m_bufferedSize += bytesReceived;

	return true;
}

bool RecvPacketBuffer::PeekPacketHeader(PACKET_HEADER& header) const noexcept
{
	if (!m_buffer || m_bufferedSize < sizeof(PACKET_HEADER))
	{
		return false;
	}

	// 연속된 헤더는 고정 4바이트 복사로 처리해 컴파일러가 단순 로드로 최적화할 수 있게 합니다.
	if (m_capacity - m_readOffset >= sizeof(PACKET_HEADER))
	{
		std::memcpy(&header, m_buffer.get() + m_readOffset, sizeof(PACKET_HEADER));
	}
	else
	{
		// 헤더가 링 끝과 시작에 걸친 경우에만 두 구간을 합칩니다.
		CopyReadableBytes(std::as_writable_bytes(std::span<PACKET_HEADER>{ &header, 1 }));
	}

	return true;
}

void RecvPacketBuffer::CopyReadableBytes(std::span<std::byte> destination) const noexcept
{
	// 목적지 길이가 이번에 복사할 바이트 수다. 호출부는 그만큼의 데이터가
	// 링에 도착했는지 먼저 확인해야 한다. 이 함수는 읽기 위치를 전진시키지 않는다.
	assert(destination.size() <= m_bufferedSize);

	// 요청한 복사 길이가 0이면 버퍼 주소에 접근할 필요가 없다.
	if (destination.empty())
	{
		return;
	}

	// 실제로 복사할 데이터가 있다면 원본 버퍼도 존재해야 한다.
	assert(m_buffer != nullptr);

	// sizeToEnd: 현재 읽기 위치부터 링의 물리적인 끝까지의 바이트 수.
	// 도착한 데이터 양이 아니라, 뒤쪽에서 연속으로 접근할 수 있는 경계까지의 거리다.
	const std::size_t sizeToEnd = m_capacity - m_readOffset;
	// firstSize: 첫 번째 memcpy에서 복사할 바이트 수.
	// 요청한 전체 길이와 링 끝까지의 거리 중 작은 값을 선택해 링 경계를 넘지 않게 한다.
	const std::size_t firstSize = destination.size() < sizeToEnd ? destination.size() : sizeToEnd;

	// 링의 현재 읽기 위치에서 목적지 시작으로 첫 구간을 복사한다.
	std::memcpy(destination.data(), m_buffer.get() + m_readOffset, firstSize);

	// 첫 구간만으로 요청 길이를 채우지 못했다면 나머지 데이터는 링 앞쪽에 있다.
	// 링 시작에서 읽어 목적지의 첫 복사 구간 바로 뒤에 이어 붙인다.
	// 예: 요청 100바이트, 링 끝까지 60바이트라면 뒤쪽 60 + 앞쪽 40을 복사한다.
	if (firstSize < destination.size())
	{
		std::memcpy(destination.data() + firstSize, m_buffer.get(), destination.size() - firstSize);
	}
}

void RecvPacketBuffer::PrepareForReceive()
{
	if (!m_buffer)
		return;

	// 이전 데이터를 모두 추출했으므로 보존할 내용이 없다.
	// 두 위치를 0으로 되돌려 다음 수신에서 버퍼 전체를 연속 영역으로 사용할 수 있게 한다.
	if (m_bufferedSize == 0)
	{
		m_readOffset = 0;
		m_writeOffset = 0;

		return;
	}

	// 미처리 데이터 길이가 읽기 위치부터 링 끝까지의 길이보다 크면,
	// 데이터가 링 뒤쪽과 앞쪽에 나뉘어 있다. 한 번의 memmove로 옮기는
	// CompactUnreadData를 사용할 수 없으므로 현재 배치를 유지한다.
	// 이후 GetWritableSpan이 쓰기 위치부터 읽기 위치 직전까지의 빈 공간을 제공한다.
	// 가득 찬 경우에는 GetWritableSpan이 빈 영역을 반환한다.
	if (m_bufferedSize > m_capacity - m_readOffset)
		return;

	// 쓰기 위치부터 링 끝까지 최대 패킷 크기 이상의 공간이 있으면
	// 꼬리 공간이 작아서 수신 길이가 줄어드는 상황이 아니므로 복사를 생략한다.
	// 실제로 쓸 수 있는 길이는 GetWritableSpan이 미처리 데이터까지 고려해 제한한다.
	if (m_capacity - m_writeOffset >= m_maxPacketSize)
		return;

	// 미처리 데이터는 한 덩어리지만 링 뒤쪽의 쓰기 공간이 작다.
	// 데이터를 버퍼 시작으로 옮겨 그 뒤에 큰 연속 쓰기 공간을 확보한다.
	// 정상 수신 흐름에서는 완성된 패킷을 모두 추출하고 남은 조각만 이곳에 도달한다.
	CompactUnreadData();
}

void RecvPacketBuffer::CompactUnreadData()
{
	// 여기 오는 조각은 연속이고 최대 패킷보다 작다. 완성된 패킷은 이미 다
	// 꺼내갔고 남은 건 헤더가 주장한 크기에 못 미치는 꼬리뿐이기 때문이다.
	// 그래서 복사량이 최대 패킷을 넘지 않고, 링 끝에 닿았을 때만 일어난다.
	std::memmove(m_buffer.get(), m_buffer.get() + m_readOffset, m_bufferedSize);

	m_readOffset = 0;
	m_writeOffset = m_bufferedSize;
}

PacketReadResult RecvPacketBuffer::ReadPacket()
{
	PACKET_HEADER header = {};

	if (!PeekPacketHeader(header))
	{
		return { PacketReadStatus::NeedMoreData };
	}

	if (header.packetSize < sizeof(PACKET_HEADER) || header.packetSize > m_maxPacketSize) [[unlikely]]
	{
		return { PacketReadStatus::Invalid };
	}

	if (m_bufferedSize < header.packetSize)
	{
		return { PacketReadStatus::NeedMoreData };
	}

	if (!m_packetMemoryPool) [[unlikely]]
	{
		return { PacketReadStatus::OutOfMemory };
	}

	auto packet = MEMORY_POOL::CreatePacketHandle(*m_packetMemoryPool, header.packetSize);

	if (!packet) [[unlikely]]
	{
		LOGE("failed to acquire %u bytes for an incoming packet, "
			"dropping the session",
			header.packetSize);

		return { PacketReadStatus::OutOfMemory };
	}

	CopyReadableBytes(packet.MutableView());

	// 할당과 복사가 끝난 뒤에만 읽기 위치를 전진시킵니다.
	m_readOffset = (m_readOffset + header.packetSize) & m_wrapMask;
	m_bufferedSize -= header.packetSize;

	return { PacketReadStatus::Ok, std::move(packet), header.packetId };
}
