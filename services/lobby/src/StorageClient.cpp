/** @file
 * @brief Exchanges owned Storage DTOs over the independent internal
 * RPC transport.
 */
#include "lobby/StorageClient.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "security/SecureErase.h"

namespace service::lobby
{
namespace
{
using Status = arena::rpc::Status;
using namespace std::chrono_literals;
constexpr std::size_t maxPayload = 8192;
constexpr std::uint64_t managedBit = std::uint64_t{1} << 63;
constexpr std::uint64_t maxTimestamp = 253402300799999;

/** @brief Rejects invalid finite bindings before executor admission.
 */
StorageClient::Config validated(StorageClient::Config config)
{
    const auto address = config.endpoint.address();
    if (!config.endpoint.port() ||
        !(address == boost::asio::ip::address_v4::loopback() ||
          address == boost::asio::ip::address_v6::loopback()) ||
        config.token.size() < 32 || config.token.size() > 256 ||
        !config.maxInflight || config.maxInflight > 4096 ||
        config.timeout < 1ms || config.timeout > 1000ms)
    {
        arena::security::eraseSecret(config.token);
        throw std::invalid_argument(
            "invalid Lobby Storage client configuration"
        );
    }
    return config;
}

/** @brief Matches Storage's bounded UTF-8 display-name and control
 * policy. */
bool displayName(std::string_view value)
{
    if (value.empty() || value.size() > 128)
        return false;
    bool nonspace = false;
    for (std::size_t i = 0; i < value.size();)
    {
        const auto first = static_cast<unsigned char>(value[i++]);
        std::uint32_t codepoint = first;
        unsigned extra = 0;
        std::uint32_t minimum = 0;
        if (first >= 0xc2 && first <= 0xdf)
        {
            codepoint = first & 0x1f;
            extra = 1;
            minimum = 0x80;
        }
        else if (first >= 0xe0 && first <= 0xef)
        {
            codepoint = first & 0x0f;
            extra = 2;
            minimum = 0x800;
        }
        else if (first >= 0xf0 && first <= 0xf4)
        {
            codepoint = first & 7;
            extra = 3;
            minimum = 0x10000;
        }
        else if (first >= 0x80)
            return false;
        if (extra > value.size() - i)
            return false;
        for (unsigned n = 0; n != extra; ++n)
        {
            const auto next = static_cast<unsigned char>(value[i++]);
            if ((next & 0xc0) != 0x80)
                return false;
            codepoint = (codepoint << 6) | (next & 0x3f);
        }
        if (codepoint < minimum || codepoint > 0x10ffff ||
            (codepoint >= 0xd800 && codepoint <= 0xdfff) ||
            codepoint < 32 || (codepoint >= 127 && codepoint <= 159))
            return false;
        nonspace = nonspace || codepoint != 32;
    }
    return nonspace;
}

bool playerRecord(const arena::storage::PlayerRecord& value)
{
    return value.player_id() && value.account_id() &&
           displayName(value.display_name()) &&
           value.mmr() <= 10000 &&
           value.revision() <=
               static_cast<std::uint64_t>(INT64_MAX) &&
           (value.state() == "active" ||
            value.state() == "suspended" ||
            value.state() == "retired");
}

/** @brief Preserves all integer bits while checking the managed match
 * DTO. */
bool matchRecord(
    const arena::storage::MatchRecord& value,
    bool terminal,
    bool receipt
)
{
    if (!value.match_id() || value.match_id() >= managedBit ||
        value.room_id() != (managedBit | value.match_id()) ||
        !value.result_version() ||
        value.result_version() >
            static_cast<std::uint32_t>(INT32_MAX) ||
        value.participants_size() != 2 ||
        value.created_unix_ms() > maxTimestamp ||
        value.started_unix_ms() > maxTimestamp ||
        value.finished_unix_ms() > maxTimestamp ||
        (value.started_unix_ms() && value.created_unix_ms() &&
         value.started_unix_ms() < value.created_unix_ms()) ||
        (value.finished_unix_ms() &&
         value.finished_unix_ms() < std::max(
                                        value.created_unix_ms(),
                                        value.started_unix_ms()
                                    )) ||
        (receipt && !value.created_unix_ms()))
        return false;
    const auto& a = value.participants(0);
    const auto& b = value.participants(1);
    for (const auto* p : {&a, &b})
        if (!p->player_id() || !p->session_id() ||
            !p->membership_epoch() || p->slot() < 1 ||
            p->slot() > 2 || p->initial_mmr() > 10000 ||
            p->last_dx() < -1 || p->last_dx() > 1 ||
            p->last_dy() < -1 || p->last_dy() > 1)
            return false;
    if (a.player_id() == b.player_id() || a.slot() == b.slot() ||
        a.session_id() == b.session_id() ||
        (value.winner_player_id() &&
         value.winner_player_id() != a.player_id() &&
         value.winner_player_id() != b.player_id()))
        return false;
    if (value.state() == "forming" || value.state() == "running")
        return !terminal && value.finish_reason().empty() &&
               !value.winner_player_id() && !value.final_tick() &&
               !value.final_checksum() && !value.finished_unix_ms() &&
               (value.state() == "running"
                    ? value.started_unix_ms() != 0
                    : value.started_unix_ms() == 0);
    if (value.state() != "completed" && value.state() != "aborted")
        return false;
    if ((value.state() == "completed") !=
            (value.finish_reason() == "completed") ||
        (value.finish_reason() != "completed" &&
         value.finish_reason() != "member_unavailable" &&
         value.finish_reason() != "ready_timeout") ||
        (value.state() == "aborted" && value.winner_player_id()) ||
        (value.finish_reason() == "ready_timeout" &&
         value.started_unix_ms()) ||
        (receipt && (!value.finished_unix_ms() ||
                     (value.state() == "completed" &&
                      !value.started_unix_ms()))))
        return false;
    return true;
}

bool eventId(std::string_view value)
{
    if (value.size() != 36)
        return false;
    bool nonzero = false;
    for (std::size_t i = 0; i != value.size(); ++i)
    {
        if (i == 8 || i == 13 || i == 18 || i == 23)
        {
            if (value[i] != '-')
                return false;
        }
        else
        {
            if (!((value[i] >= '0' && value[i] <= '9') ||
                  (value[i] >= 'a' && value[i] <= 'f')))
                return false;
            nonzero = nonzero || value[i] != '0';
        }
    }
    return nonzero;
}

bool settlementRecord(const arena::storage::SettlementRecord& value)
{
    return value.settlement_id() && value.match_id() &&
           value.match_id() < managedBit && eventId(value.event_id());
}

/** @brief Compares immutable facts while allowing Storage to fill
 * zero receipt times. */
bool sameMatch(
    const arena::storage::MatchRecord& actual,
    const arena::storage::MatchRecord& expected
)
{
    if (actual.match_id() != expected.match_id() ||
        actual.room_id() != expected.room_id() ||
        actual.state() != expected.state() ||
        actual.result_version() != expected.result_version() ||
        actual.winner_player_id() != expected.winner_player_id() ||
        actual.final_tick() != expected.final_tick() ||
        actual.final_checksum() != expected.final_checksum() ||
        actual.finish_reason() != expected.finish_reason() ||
        (expected.created_unix_ms() &&
         actual.created_unix_ms() != expected.created_unix_ms()) ||
        (expected.started_unix_ms() &&
         actual.started_unix_ms() != expected.started_unix_ms()) ||
        (expected.finished_unix_ms() &&
         actual.finished_unix_ms() != expected.finished_unix_ms()))
        return false;
    for (const auto& wanted : expected.participants())
    {
        const auto found = std::find_if(
            actual.participants().begin(),
            actual.participants().end(),
            [&](const auto& p) { return p.slot() == wanted.slot(); }
        );
        if (found == actual.participants().end())
            return false;
        const auto& p = *found;
        if (p.player_id() != wanted.player_id() ||
            p.session_id() != wanted.session_id() ||
            p.membership_epoch() != wanted.membership_epoch() ||
            p.initial_mmr() != wanted.initial_mmr() ||
            p.last_input_seq() != wanted.last_input_seq() ||
            p.final_x() != wanted.final_x() ||
            p.final_y() != wanted.final_y() ||
            p.last_dx() != wanted.last_dx() ||
            p.last_dy() != wanted.last_dy())
            return false;
    }
    return true;
}

/** @brief Converts only bounded successful payloads; public transport
 * errors remain exact. */
template <class Reply, class Validate>
arena::internal_rpc::Client::Completion
decode(StorageClient::Completion<Reply> completion, Validate validate)
{
    return [complete = std::move(completion),
            validate = std::move(validate)](
               arena::rpc::Response response
           ) mutable
    {
        if (response.status() != arena::rpc::OK)
        {
            arena::security::eraseSecret(*response.mutable_payload());
            complete(std::unexpected(response.status()));
            return;
        }
        Reply reply;
        const bool parsed = response.payload().size() <= maxPayload &&
                            reply.ParseFromString(response.payload());
        arena::security::eraseSecret(*response.mutable_payload());
        const auto status = parsed ? validate(reply)
                                   : arena::rpc::INTERNAL;
        if (status != arena::rpc::OK)
        {
            complete(std::unexpected(status));
            return;
        }
        reply.DiscardUnknownFields();
        complete(StorageClient::Result<Reply>(std::move(reply)));
    };
}
} // namespace

StorageClient::StorageClient(
    boost::asio::any_io_executor executor, Config config
)
    : m_config(validated(std::move(config)))
    , m_client(
          std::move(executor), m_config.endpoint, m_config.maxInflight
      )
{
}
StorageClient::~StorageClient() noexcept
{
    try
    {
        stop();
    }
    catch (...)
    {
    }
    arena::security::eraseSecret(m_config.token);
}

StorageClient::Admission StorageClient::exchange(
    arena::rpc::Request request,
    arena::internal_rpc::Context context,
    arena::internal_rpc::Client::Completion complete
)
{
    if (context.deadline <= std::chrono::steady_clock::now())
        return std::unexpected(arena::rpc::DEADLINE_EXCEEDED);
    auto endpoint = m_config.endpoint;
    if (m_config.runtime)
    {
        const auto snapshot = m_config.runtime->snapshot();
        const auto selected = snapshot->resolve("storage");
        if (!selected)
            return std::unexpected(arena::rpc::UNAVAILABLE);
        endpoint = {
            boost::asio::ip::make_address(selected->host),
            selected->port
        };
    }
    request.set_caller_service("lobby");
    request.set_target_service("storage");
    request.set_service_token(m_config.token);
    arena::internal_rpc::Options options;
    options.timeout = m_config.timeout;
    options.context = std::move(context);
    options.endpoint = endpoint;
    return m_client.call(
        std::move(request),
        std::move(options),
        std::move(complete)
    );
}

StorageClient::Admission StorageClient::player(
    arena::storage::PlayerGetRequest payload,
    arena::internal_rpc::Context context,
    Completion<arena::storage::PlayerGetReply> complete
)
{
    if (!complete || !payload.player_id() ||
        payload.ByteSizeLong() > maxPayload)
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    arena::rpc::Request request;
    request.set_method("player.get");
    if (!payload.SerializeToString(request.mutable_payload()))
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    const auto id = payload.player_id();
    return exchange(
        std::move(request),
        std::move(context),
        decode<arena::storage::PlayerGetReply>(
            std::move(complete),
            [id](const auto& reply)
            {
                if (!reply.has_player() ||
                    reply.player().player_id() != id ||
                    !playerRecord(reply.player()))
                    return arena::rpc::INTERNAL;
                return reply.player().state() == "active"
                           ? arena::rpc::OK
                           : arena::rpc::UNAUTHENTICATED;
            }
        )
    );
}

StorageClient::Admission StorageClient::allocateIds(
    arena::storage::AllocateIdsRequest payload,
    arena::internal_rpc::Context context,
    Completion<arena::storage::AllocateIdsReply> complete
)
{
    if (!complete || !payload.count() || payload.count() > 1024 ||
        payload.ByteSizeLong() > maxPayload)
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    arena::rpc::Request request;
    request.set_method("match.allocate_ids");
    if (!payload.SerializeToString(request.mutable_payload()))
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    const auto count = payload.count();
    return exchange(
        std::move(request),
        std::move(context),
        decode<arena::storage::AllocateIdsReply>(
            std::move(complete),
            [count](const auto& reply)
            {
                return reply.first() && reply.last() < managedBit &&
                               reply.last() >= reply.first() &&
                               reply.last() - reply.first() + 1 ==
                                   count
                           ? arena::rpc::OK
                           : arena::rpc::INTERNAL;
            }
        )
    );
}

StorageClient::Admission StorageClient::saveResult(
    arena::storage::SaveResultRequest payload,
    arena::internal_rpc::Context context,
    std::string stableIdempotencyKey,
    Completion<arena::storage::SaveResultReply> complete
)
{
    if (!complete || !payload.has_match() ||
        payload.ByteSizeLong() > maxPayload ||
        !matchRecord(payload.match(), true, false) ||
        stableIdempotencyKey.empty() ||
        stableIdempotencyKey.size() > 128 ||
        !std::all_of(
            stableIdempotencyKey.begin(),
            stableIdempotencyKey.end(),
            [](unsigned char c) { return c >= 32 && c <= 126; }
        ))
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    arena::rpc::Request request;
    request.set_method("match.save_result");
    request.set_idempotency_key(std::move(stableIdempotencyKey));
    if (!payload.SerializeToString(request.mutable_payload()))
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    auto expected = std::move(*payload.mutable_match());
    return exchange(
        std::move(request),
        std::move(context),
        decode<arena::storage::SaveResultReply>(
            std::move(complete),
            [expected = std::move(expected)](const auto& reply)
            {
                return reply.has_match() &&
                               matchRecord(
                                   reply.match(),
                                   true,
                                   true
                               ) &&
                               sameMatch(reply.match(), expected) &&
                               reply.has_settlement() &&
                               settlementRecord(reply.settlement()) &&
                               reply.settlement().settlement_id() ==
                                   expected.match_id() &&
                               reply.settlement().match_id() ==
                                   expected.match_id()
                           ? arena::rpc::OK
                           : arena::rpc::INTERNAL;
            }
        )
    );
}

StorageClient::Admission StorageClient::match(
    arena::storage::MatchGetRequest payload,
    arena::internal_rpc::Context context,
    Completion<arena::storage::MatchGetReply> complete
)
{
    if (!complete || !payload.match_id() ||
        payload.match_id() >= managedBit ||
        payload.ByteSizeLong() > maxPayload)
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    arena::rpc::Request request;
    request.set_method("match.get");
    if (!payload.SerializeToString(request.mutable_payload()))
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    const auto id = payload.match_id();
    return exchange(
        std::move(request),
        std::move(context),
        decode<arena::storage::MatchGetReply>(
            std::move(complete),
            [id](const auto& reply)
            {
                return reply.has_match() &&
                               reply.match().match_id() == id &&
                               matchRecord(reply.match(), false, true)
                           ? arena::rpc::OK
                           : arena::rpc::INTERNAL;
            }
        )
    );
}

StorageClient::Admission StorageClient::getSettlement(
    arena::storage::SettlementGetRequest payload,
    arena::internal_rpc::Context context,
    Completion<arena::storage::SettlementGetReply> complete
)
{
    if (!complete || !payload.settlement_id() ||
        payload.ByteSizeLong() > maxPayload)
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    arena::rpc::Request request;
    request.set_method("settlement.get");
    if (!payload.SerializeToString(request.mutable_payload()))
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    const auto id = payload.settlement_id();
    return exchange(
        std::move(request),
        std::move(context),
        decode<arena::storage::SettlementGetReply>(
            std::move(complete),
            [id](const auto& reply)
            {
                return reply.has_settlement() &&
                               settlementRecord(reply.settlement()) &&
                               reply.settlement().settlement_id() ==
                                   id
                           ? arena::rpc::OK
                           : arena::rpc::INTERNAL;
            }
        )
    );
}

StorageClient::Admission StorageClient::probe(
    arena::internal_rpc::Context context, Completion<void> complete
)
{
    if (!complete)
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    arena::rpc::Request request;
    request.set_method("health.ready");
    return exchange(
        std::move(request),
        std::move(context),
        [complete = std::move(complete)](
            arena::rpc::Response response
        ) mutable
        {
            const auto status = response.status() != arena::rpc::OK
                                    ? response.status()
                                : response.payload().empty()
                                    ? arena::rpc::OK
                                    : arena::rpc::INTERNAL;
            arena::security::eraseSecret(*response.mutable_payload());
            if (status == arena::rpc::OK)
                complete(Result<void>{});
            else
                complete(std::unexpected(status));
        }
    );
}
void StorageClient::stop()
{
    m_client.stop();
}
std::size_t StorageClient::inflight() const
{
    return m_client.inflight();
}
} // namespace service::lobby