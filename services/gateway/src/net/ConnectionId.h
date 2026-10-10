/** @file
 * @brief Allocates process-wide connection identifiers.
 */
#pragma once
#include <cstdint>

namespace service::gateway::detail
{
// Process-wide identity shared by all transport backends.
std::uint64_t nextConnectionId();
} // namespace service::gateway::detail
