/** @file
 * @brief Defines per-connection admission policies for complete
 * protocol frames.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

#include "net/ConnectionContext.h"

namespace service::gateway
{
struct PacketView
{
    /// Outer wire type, inspected before protobuf decoding.
    std::uint16_t type;
    /// Borrowed frame body, valid only during inspect().
    std::span<const std::byte> payload;
};

enum class PacketDecision
{
    Continue,
    Drop,
    Close
};

/** @brief Inspects a complete frame before protobuf decoding. */
class PacketPolicy
{
public:
    virtual ~PacketPolicy() = default;
    virtual PacketDecision
    inspect(const ConnectionContext&, PacketView) = 0;
};

/** @brief Owns fresh ordered packet policies for one protocol
 * instance. */
class PacketPolicyChain final
{
public:
    template <class T, class... Args> T& add(Args&&... args)
    {
        if (m_sealed)
            throw std::logic_error("packet policies are sealed");
        auto policy = std::make_unique<T>(
            std::forward<Args>(args)...
        );
        auto& result = *policy;
        m_policies.push_back(std::move(policy));
        return result;
    }

    void seal() noexcept
    {
        m_sealed = true;
    }
    PacketDecision
    inspect(const ConnectionContext& context, PacketView packet)
    {
        for (auto& policy : m_policies)
        {
            const auto decision = policy->inspect(context, packet);
            if (decision != PacketDecision::Continue)
                return decision;
        }
        return PacketDecision::Continue;
    }

private:
    /// Prevents policy mutation once input delivery begins.
    bool m_sealed{false};
    /// Owner-thread policy instances belonging to one decoder.
    std::vector<std::unique_ptr<PacketPolicy>> m_policies;
};

using PacketPolicyFactory = std::function<
    void(PacketPolicyChain&, const ConnectionContext&)>;
} // namespace service::gateway
