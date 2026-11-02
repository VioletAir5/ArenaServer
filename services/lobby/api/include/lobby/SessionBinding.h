/** @file
 * @brief Shares immutable transport authorization values without
 * Lobby ownership.
 */
#pragma once

#include <cstdint>

#include "arena/domain/Identity.h"

namespace service::lobby
{
/** @brief Identifies the exact transport generation authorized for
 * one player. */
struct SessionBinding
{
    /// Authenticated account identity.
    arena::domain::PlayerId playerId;
    /// Physical connection permitted to use this binding.
    std::uint64_t connectionId;
    /// Monotonic authority generation fencing older connections.
    std::uint64_t epoch;
};
} // namespace service::lobby
