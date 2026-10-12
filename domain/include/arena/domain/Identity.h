#pragma once

#include <cstdint>
#include <stdexcept>

namespace arena::domain
{
// Stable, nonzero business identity. Tags prevent mixing entity IDs.
template<typename Tag>
class Identity final
{
public:
    explicit Identity(std::uint64_t value) : m_value(value)
    {
        if (!value)
            throw std::invalid_argument("domain identity must be nonzero");
    }
    std::uint64_t value() const noexcept { return m_value; }
    bool operator==(const Identity&) const = default;

private:
    std::uint64_t m_value;
};

struct PlayerTag;
struct SessionTag;
struct TicketTag;
struct RoomTag;
struct BattleTag;
struct SettlementTag;
using PlayerId = Identity<PlayerTag>;
using SessionId = Identity<SessionTag>;
using TicketId = Identity<TicketTag>;
using RoomId = Identity<RoomTag>;
using BattleId = Identity<BattleTag>;
using SettlementId = Identity<SettlementTag>;
} // namespace arena::domain
