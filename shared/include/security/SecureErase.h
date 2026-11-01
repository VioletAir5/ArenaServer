/** @file
 * @brief Erases owned credential buffers before releasing or
 * retaining storage.
 */
#pragma once

#include <cstddef>
#include <string>

namespace arena::security
{
/** @brief Prevents dead-store elimination of writes to the current
 * string bytes. */
inline void eraseSecret(std::string& value) noexcept
{
    volatile char* bytes = value.data();
    for (std::size_t index = 0; index < value.size(); ++index)
        bytes[index] = 0;
    value.clear();
}
} // namespace arena::security