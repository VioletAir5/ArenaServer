/** @file
 * @brief Shares complete query authorization without linking the
 * simulation kernel.
 */
#include "room/RoomQuery.h"

#include <algorithm>
#include <limits>
namespace service::room
{
namespace
{
using Error = RuntimeError;
using Kind = RuntimeCommandKind;
using Phase = RuntimePhase;
bool currentTransport(
    const RuntimeAuthorization& caller,
    const RuntimeAuthorization& current
)
{
    return caller.member.playerId == current.member.playerId &&
           caller.member.sessionId == current.member.sessionId &&
           caller.sessionEpoch &&
           caller.sessionEpoch == current.sessionEpoch &&
           caller.connectionHandle &&
           caller.connectionHandle == current.connectionHandle &&
           current.online;
}
bool validAuthorizations(
    const std::vector<RuntimeAuthorization>& values, bool roster
)
{
    for (std::size_t i = 0; i < values.size(); ++i)
    {
        const auto& value = values[i];
        if (!value.sessionEpoch ||
            (value.online ? !value.connectionHandle
                          : value.connectionHandle != 0) ||
            (roster &&
             (!value.member.epoch ||
              value.member.epoch ==
                  std::numeric_limits<std::uint64_t>::max())))
            return false;
        for (std::size_t j = 0; j < i; ++j)
            if (value.member.playerId == values[j].member.playerId ||
                value.member.sessionId == values[j].member.sessionId)
                return false;
    }
    return true;
}
bool authorized(
    const std::vector<RuntimeAuthorization>& values,
    const RuntimeAuthorization& caller
)
{
    const auto current = std::find_if(
        values.begin(),
        values.end(),
        [&](const auto& value)
        { return value.member.playerId == caller.member.playerId; }
    );
    return current != values.end() &&
           currentTransport(caller, *current);
}
} // namespace
std::expected<void, RuntimeError> authorizeRoomQuery(
    const RuntimeImage& image,
    const RuntimeCommand& command,
    const RuntimeCutoff& context
)
{
    if (command.kind != Kind::Snapshot &&
        command.kind != Kind::Status &&
        command.kind != Kind::Settlement)
        return std::unexpected(Error::Invalid);
    if (context.nowNanos < 0 || context.roster.size() > 65536 ||
        context.currentAuthorizations.size() >
            image.policy.maxCommands + image.policy.actor.maxMembers)
        return std::unexpected(Error::Capacity);
    if (!validAuthorizations(context.roster, true) ||
        !validAuthorizations(context.currentAuthorizations, false))
        return std::unexpected(Error::Invalid);
    const auto& managed = image.managed;
    if (managed)
        for (const auto& current : context.roster)
            if (std::find(
                    managed->allocation.members.begin(),
                    managed->allocation.members.end(),
                    current.member
                ) == managed->allocation.members.end())
                return std::unexpected(Error::Invalid);
    if (!command.caller ||
        !authorized(context.currentAuthorizations, *command.caller))
        return std::unexpected(Error::Unauthenticated);
    if (managed && managed->expiresAtNanos &&
        context.nowNanos >= *managed->expiresAtNanos)
        return std::unexpected(Error::BusinessFailure);
    if (managed && (managed->phase == Phase::Settling ||
                    managed->phase == Phase::Finished))
    {
        const bool participant = std::any_of(
            managed->allocation.members.begin(),
            managed->allocation.members.end(),
            [&](const auto& member)
            {
                return member.playerId ==
                           command.caller->member.playerId &&
                       member.sessionId ==
                           command.caller->member.sessionId;
            }
        );
        if (!participant)
            return std::unexpected(Error::BusinessFailure);
        if (command.kind == Kind::Status ||
            (command.kind == Kind::Snapshot &&
             managed->phase == Phase::Settling))
            return {};
        if (command.kind == Kind::Settlement &&
            command.matchId == managed->allocation.matchId)
        {
            if (managed->phase != Phase::Finished)
                return std::unexpected(Error::Unavailable);
            return {};
        }
        return std::unexpected(
            managed->phase == Phase::Settling ? Error::Unavailable
                                              : Error::BusinessFailure
        );
    }
    if (command.kind == Kind::Settlement ||
        (!managed && command.kind == Kind::Status))
        return std::unexpected(Error::BusinessFailure);
    if (!authorized(context.roster, *command.caller))
        return std::unexpected(Error::BusinessFailure);
    if (!managed &&
        context.roster.size() > image.policy.actor.maxMembers)
        return std::unexpected(Error::Unavailable);
    return {};
}
} // namespace service::room
