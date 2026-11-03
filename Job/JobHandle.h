#pragma once
#include <memory> // for std::unique_ptr

#ifndef IOCP_ENGINE_API
#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif
#endif

class Job;
class TlsMemoryPool;

class JobDeleter final
{
public:
    explicit JobDeleter(TlsMemoryPool& pool) noexcept : m_pool(&pool) {}
    IOCP_ENGINE_API void operator()(Job* job) const noexcept;
    [[nodiscard]] TlsMemoryPool* GetPool() const noexcept { return m_pool; }

private:
    // The pool must outlive every JobHandle that refers to it.
    TlsMemoryPool* m_pool;
};

using JobHandle = std::unique_ptr<Job, JobDeleter>;

namespace MEMORY_POOL
{
    // The pool must support alignof(Job). Failure returns an empty handle.
    [[nodiscard]] IOCP_ENGINE_API JobHandle CreateJobHandle(TlsMemoryPool& pool);
}
