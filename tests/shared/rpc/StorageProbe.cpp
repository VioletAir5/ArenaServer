#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

#include <boost/asio/io_context.hpp>

#include "rpc/Client.h"
#include "storage.pb.h"

namespace
{
bool decimal(
    std::string_view text, std::uint64_t maximum, std::uint64_t& value
)
{
    if (text.empty() || text.size() > 20 || text.front() == '0')
        return false;
    const auto result = std::from_chars(
        text.data(),
        text.data() + text.size(),
        value
    );
    return result.ec == std::errc{} &&
           result.ptr == text.data() + text.size() && value > 0 &&
           value <= maximum;
}

bool accountName(std::string_view value)
{
    if (value.empty() || value.size() > 128)
        return false;
    for (const unsigned char byte : value)
        if (byte < 32 || byte > 126)
            return false;
    return true;
}

bool eventId(std::string_view value)
{
    if (value.size() != 36)
        return false;
    for (std::size_t index = 0; index != value.size(); ++index)
    {
        if (index == 8 || index == 13 || index == 18 || index == 23)
        {
            if (value[index] != '-')
                return false;
        }
        else if (!((value[index] >= '0' && value[index] <= '9') ||
                   (value[index] >= 'a' && value[index] <= 'f')))
            return false;
    }
    return true;
}

template <class Request, class Reply>
bool invoke(
    arena::internal_rpc::Client& client,
    boost::asio::io_context& context,
    std::string_view token,
    std::string_view method,
    const Request& payload,
    Reply& reply
)
{
    arena::rpc::Request request;
    request.set_method(method.data(), method.size());
    request.set_payload(payload.SerializeAsString());
    request.set_service_token(token.data(), token.size());
    request.set_correlation_id("native-storage-test");
    std::optional<arena::rpc::Response> response;
    auto accepted = client.call(
        std::move(request),
        arena::internal_rpc::Options{},
        [&](auto result) { response = std::move(result); }
    );
    if (!accepted)
        return false;
    context.restart();
    context.run();
    return response && response->status() == arena::rpc::OK &&
           reply.ParseFromString(response->payload());
}

bool probe(
    boost::asio::io_context& context,
    arena::internal_rpc::Client& client,
    std::string_view token,
    std::string_view name,
    std::uint64_t matchId
)
{
    arena::storage::FindAccountRequest accountRequest;
    accountRequest.set_account_name(name.data(), name.size());
    arena::storage::FindAccountReply accountReply;
    if (!invoke(
            client,
            context,
            token,
            "account.find",
            accountRequest,
            accountReply
        ) ||
        !accountReply.has_account() || !accountReply.has_player())
        return false;
    const auto& account = accountReply.account();
    const auto& player = accountReply.player();
    if (!account.account_id() || account.account_name() != name ||
        account.state() != "active" ||
        !account.password_digest().starts_with(
            "$arena-pbkdf2-sha256$"
        ) ||
        !player.player_id() ||
        player.account_id() != account.account_id() ||
        player.state() != "active" || player.display_name().empty() ||
        player.display_name().size() > 128 || player.mmr() > 10000)
        return false;

    arena::storage::MatchGetRequest matchRequest;
    matchRequest.set_match_id(matchId);
    arena::storage::MatchGetReply matchReply;
    if (!invoke(
            client,
            context,
            token,
            "match.get",
            matchRequest,
            matchReply
        ) ||
        !matchReply.has_match())
        return false;
    const auto& match = matchReply.match();
    if (match.match_id() != matchId ||
        match.room_id() != ((std::uint64_t{1} << 63) | matchId) ||
        (match.state() != "completed" &&
         match.state() != "aborted") ||
        !match.result_version() || match.participants_size() != 2 ||
        !match.created_unix_ms() || !match.finished_unix_ms() ||
        match.finished_unix_ms() < match.created_unix_ms())
        return false;
    if (match.state() == "completed")
    {
        if (match.finish_reason() != "completed" ||
            !match.started_unix_ms() || !match.final_tick() ||
            match.finished_unix_ms() < match.started_unix_ms())
            return false;
    }
    else if (
        (match.finish_reason() != "member_unavailable" &&
         match.finish_reason() != "ready_timeout") ||
        match.winner_player_id()
    )
        return false;

    bool member = false;
    bool winner = match.winner_player_id() == 0;
    const auto& first = match.participants(0);
    const auto& second = match.participants(1);
    if (first.player_id() == second.player_id() ||
        first.session_id() == second.session_id() ||
        first.slot() != 1 || second.slot() != 2)
        return false;
    for (const auto& participant : match.participants())
    {
        if (!participant.player_id() || !participant.session_id() ||
            !participant.membership_epoch() ||
            participant.initial_mmr() > 10000)
            return false;
        member |= participant.player_id() == player.player_id();
        winner |= participant.player_id() == match.winner_player_id();
    }
    if (!member || !winner)
        return false;

    arena::storage::SettlementGetRequest settlementRequest;
    settlementRequest.set_settlement_id(matchId);
    arena::storage::SettlementGetReply settlementReply;
    if (!invoke(
            client,
            context,
            token,
            "settlement.get",
            settlementRequest,
            settlementReply
        ) ||
        !settlementReply.has_settlement())
        return false;
    const auto& settlement = settlementReply.settlement();
    if (settlement.settlement_id() != matchId ||
        settlement.match_id() != matchId ||
        !eventId(settlement.event_id()))
        return false;
    std::cout << "{\"player_id\":\"" << player.player_id()
              << "\",\"match_id\":\"" << matchId
              << "\",\"settlement_id\":\""
              << settlement.settlement_id() << "\"}\n";
    return true;
}
} // namespace

int main(int argc, char** argv)
{
    if (argc != 4)
        return 2;
    std::uint64_t port = 0, matchId = 0;
    if (!decimal(argv[1], 65535, port) || !accountName(argv[2]) ||
        !decimal(argv[3], (std::uint64_t{1} << 63) - 1, matchId))
        return 2;
    const auto* configured = std::getenv("ARENA_INTERNAL_TOKEN");
    if (!configured)
        return 2;
    std::size_t length = 0;
    while (length != 257 && configured[length])
        ++length;
    if (length < 32 || length > 256)
        return 2;
    try
    {
        boost::asio::io_context context;
        arena::internal_rpc::Client client(
            context.get_executor(),
            boost::asio::ip::tcp::endpoint(
                boost::asio::ip::address_v4::loopback(),
                static_cast<std::uint16_t>(port)
            ),
            1
        );
        const bool valid = probe(
            context,
            client,
            std::string_view(configured, length),
            argv[2],
            matchId
        );
        client.stop();
        context.restart();
        context.run();
        return valid ? 0 : 1;
    }
    catch (...)
    {
        return 1;
    }
}
