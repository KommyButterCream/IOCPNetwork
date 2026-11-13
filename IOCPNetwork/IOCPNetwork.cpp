#include "IOCPNetwork.h"

#include "Detail/OptionsValidation.h"
#include "Detail/PacketHeader.h"
#include "Detail/PacketBlock.h"
#include "Detail/IOOperation.h"
#include "Detail/IOWorkerPool.h"
#include "Detail/SocketHandle.h"

namespace IOCPNetwork
{
    const char* GetLibraryVersion() noexcept
    {
        return "0.1.0-dev";
    }
}
