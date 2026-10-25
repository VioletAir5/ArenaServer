#include "protocol/EnvelopeCodec.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/unknown_field_set.h>
#include <gtest/gtest.h>

#include "legacy_envelope.pb.h"

namespace
{
using service::protocol::EnvelopeCodec;
using New = arena::protocol::Envelope;
using Old = arena::protocol::legacy::Envelope;
bool decode(std::string_view s, New& e)
{
    return EnvelopeCodec::decode(std::as_bytes(std::span(s)), e);
}
void stable(
    const google::protobuf::Descriptor* newer,
    const google::protobuf::Descriptor* older
)
{
    for (int i = 0; i < older->field_count(); ++i)
    {
        const auto* before = older->field(i);
        if (newer == New::descriptor() && before->number() == 2)
            continue;
        const auto* after = newer->FindFieldByName(before->name());
        ASSERT_NE(after, nullptr) << before->full_name();
        EXPECT_EQ(after->number(), before->number());
        EXPECT_EQ(after->type(), before->type());
    }
}
} // namespace

TEST(
    EnvelopeCodecTest, OldClientReadsNewLoginAndPreservesUnknownFields
)
{
    New newer;
    newer.set_request_id(42);
    newer.set_sent_at_unix_ms(123456);
    newer.set_msg_id(arena::protocol::MSG_LOGIN_REQUEST);
    auto* login = newer.mutable_login_request();
    login->set_account("alice");
    login->set_credential("secret");
    login->set_client_version("2.0");
    const auto wire = EnvelopeCodec::encode(newer);
    ASSERT_TRUE(wire);
    Old older;
    ASSERT_TRUE(older.ParseFromString(*wire));
    EXPECT_EQ(older.request_id(), 42);
    ASSERT_TRUE(older.has_login_request());
    EXPECT_EQ(older.login_request().account(), "alice");
    EXPECT_EQ(older.login_request().credential(), "secret");
    EXPECT_GT(
        older.GetReflection()->GetUnknownFields(older).field_count(),
        0
    );
    EXPECT_GT(
        older.login_request()
            .GetReflection()
            ->GetUnknownFields(older.login_request())
            .field_count(),
        0
    );
    std::string forwarded;
    ASSERT_TRUE(older.SerializeToString(&forwarded));
    New restored;
    ASSERT_TRUE(decode(forwarded, restored));
    EXPECT_EQ(restored.sent_at_unix_ms(), 123456);
    EXPECT_EQ(restored.msg_id(), 10);
    EXPECT_EQ(restored.login_request().client_version(), "2.0");
}

TEST(EnvelopeCodecTest, NewClientReadsOldPingAndPreservesRetiredTag)
{
    Old older;
    older.set_request_id(7);
    older.set_legacy_client_tag("v1");
    older.mutable_ping()->set_nonce(99);
    std::string wire;
    ASSERT_TRUE(older.SerializeToString(&wire));
    New newer;
    ASSERT_TRUE(decode(wire, newer));
    EXPECT_EQ(newer.request_id(), 7);
    EXPECT_EQ(newer.sent_at_unix_ms(), 0);
    EXPECT_EQ(newer.msg_id(), 0);
    ASSERT_TRUE(newer.has_ping());
    EXPECT_EQ(newer.ping().nonce(), 99);
    EXPECT_GT(
        newer.GetReflection()->GetUnknownFields(newer).field_count(),
        0
    );
    const auto forwarded = EnvelopeCodec::encode(newer);
    ASSERT_TRUE(forwarded);
    Old restored;
    ASSERT_TRUE(restored.ParseFromString(*forwarded));
    EXPECT_EQ(restored.legacy_client_tag(), "v1");
}

TEST(EnvelopeCodecTest, OtherPayloadsUseStableTags)
{
    New login;
    login.mutable_login_response()->set_accepted(true);
    login.mutable_login_response()->set_session_token("token");
    const auto wire = EnvelopeCodec::encode(login);
    ASSERT_TRUE(wire);
    Old oldLogin;
    ASSERT_TRUE(oldLogin.ParseFromString(*wire));
    ASSERT_TRUE(oldLogin.has_login_response());
    EXPECT_TRUE(oldLogin.login_response().accepted());
    EXPECT_EQ(oldLogin.login_response().session_token(), "token");
    Old oldPong;
    oldPong.mutable_pong()->set_nonce(88);
    std::string serialized;
    ASSERT_TRUE(oldPong.SerializeToString(&serialized));
    New pong;
    ASSERT_TRUE(decode(serialized, pong));
    ASSERT_TRUE(pong.has_pong());
    EXPECT_EQ(pong.pong().nonce(), 88);
    Old oldError;
    oldError.mutable_error()->set_code(401);
    oldError.mutable_error()->set_message("denied");
    ASSERT_TRUE(oldError.SerializeToString(&serialized));
    New error;
    ASSERT_TRUE(decode(serialized, error));
    ASSERT_TRUE(error.has_error());
    EXPECT_EQ(error.error().code(), 401);
    EXPECT_EQ(error.error().message(), "denied");
}

TEST(EnvelopeCodecTest, FieldNumbersAreStableAndRetiredTagIsReserved)
{
    stable(New::descriptor(), Old::descriptor());
    stable(
        arena::protocol::LoginRequest::descriptor(),
        arena::protocol::legacy::LoginRequest::descriptor()
    );
    stable(
        arena::protocol::LoginResponse::descriptor(),
        arena::protocol::legacy::LoginResponse::descriptor()
    );
    stable(
        arena::protocol::Ping::descriptor(),
        arena::protocol::legacy::Ping::descriptor()
    );
    stable(
        arena::protocol::Pong::descriptor(),
        arena::protocol::legacy::Pong::descriptor()
    );
    stable(
        arena::protocol::Error::descriptor(),
        arena::protocol::legacy::Error::descriptor()
    );
    const auto* descriptor = New::descriptor();
    EXPECT_TRUE(descriptor->IsReservedNumber(2));
    EXPECT_TRUE(descriptor->IsReservedName("legacy_client_tag"));
    EXPECT_EQ(descriptor->FindFieldByNumber(2), nullptr);
    EXPECT_EQ(
        descriptor->FindFieldByName("sent_at_unix_ms")->number(),
        3
    );
    EXPECT_EQ(descriptor->FindFieldByName("msg_id")->number(), 4);
    EXPECT_EQ(
        arena::protocol::LoginRequest::descriptor()
            ->FindFieldByName("client_version")
            ->number(),
        3
    );
    EXPECT_EQ(
        arena::protocol::Error::descriptor()
            ->FindFieldByName("causes")
            ->number(),
        3
    );
}

TEST(
    EnvelopeCodecTest, RejectsOversizedPayloadAndLeavesOutputUnchanged
)
{
    New huge;
    huge.mutable_login_request()->set_credential(
        std::string(EnvelopeCodec::kMaxBytes, 'x')
    );
    EXPECT_FALSE(EnvelopeCodec::encode(huge));
    std::string wire;
    ASSERT_TRUE(huge.SerializeToString(&wire));
    ASSERT_GT(wire.size(), EnvelopeCodec::kMaxBytes);
    New output;
    output.set_request_id(9);
    EXPECT_FALSE(decode(wire, output));
    EXPECT_EQ(output.request_id(), 9);
}

TEST(EnvelopeCodecTest, RejectsDeeplyNestedError)
{
    New deep;
    auto* error = deep.mutable_error();
    for (int i = 0; i < EnvelopeCodec::kMaxDepth + 1; ++i)
        error = error->add_causes();
    error->set_code(1);
    EXPECT_FALSE(EnvelopeCodec::encode(deep));
    std::string wire;
    ASSERT_TRUE(deep.SerializeToString(&wire));
    New parsed;
    EXPECT_FALSE(decode(wire, parsed));
    New shallow;
    shallow.mutable_error()->set_code(1);
    const auto valid = EnvelopeCodec::encode(shallow);
    ASSERT_TRUE(valid);
    EXPECT_TRUE(decode(*valid, parsed));
}

TEST(EnvelopeCodecTest, RejectsMalformedAndPayloadlessMessages)
{
    New output;
    output.set_request_id(17);
    const std::string malformed{"\x52\x80", 2};
    const std::string unknownOnly{"\xA0\x06\x07", 3};
    const std::string trailingJunk{"\x62\x00\xFF", 3};
    EXPECT_FALSE(decode("", output));
    EXPECT_FALSE(decode(malformed, output));
    EXPECT_FALSE(decode(unknownOnly, output));
    EXPECT_FALSE(decode(trailingJunk, output));
    EXPECT_EQ(output.request_id(), 17);
}
TEST(
    EnvelopeCodecTest,
    LoginCredentialModeAndSessionBindingRemainWireCompatible
)
{
    Old oldRequest;
    oldRequest.mutable_login_request()->set_account("alice");
    oldRequest.mutable_login_request()->set_credential("password");
    std::string bytes;
    ASSERT_TRUE(oldRequest.SerializeToString(&bytes));
    New request;
    ASSERT_TRUE(decode(bytes, request));
    EXPECT_EQ(
        request.login_request().credential_kind(),
        arena::protocol::CREDENTIAL_PASSWORD
    );

    New response;
    response.mutable_login_response()->set_accepted(true);
    response.mutable_login_response()->set_session_token(
        "signed-token"
    );
    response.mutable_login_response()->set_player_id(42);
    response.mutable_login_response()->set_session_epoch(123);
    response.mutable_login_response()->set_session_id(456);
    response.mutable_login_response()->set_room_id(7);
    response.mutable_login_response()->set_resumed(true);
    ASSERT_TRUE(response.SerializeToString(&bytes));
    Old older;
    ASSERT_TRUE(older.ParseFromString(bytes));
    EXPECT_TRUE(older.login_response().accepted());
    EXPECT_EQ(older.login_response().session_token(), "signed-token");
    ASSERT_TRUE(older.SerializeToString(&bytes));
    New forwarded;
    ASSERT_TRUE(decode(bytes, forwarded));
    EXPECT_EQ(forwarded.login_response().player_id(), 42u);
    EXPECT_EQ(forwarded.login_response().session_epoch(), 123u);
    EXPECT_EQ(forwarded.login_response().session_id(), 456u);
    EXPECT_EQ(forwarded.login_response().room_id(), 7u);
    EXPECT_TRUE(forwarded.login_response().resumed());
    EXPECT_EQ(
        arena::protocol::LoginRequest::descriptor()
            ->FindFieldByName("credential_kind")
            ->number(),
        4
    );
    EXPECT_EQ(
        arena::protocol::LoginResponse::descriptor()
            ->FindFieldByName("player_id")
            ->number(),
        4
    );
    EXPECT_EQ(
        arena::protocol::LoginResponse::descriptor()
            ->FindFieldByName("session_epoch")
            ->number(),
        5
    );
    EXPECT_EQ(
        arena::protocol::LoginResponse::descriptor()
            ->FindFieldByName("session_id")
            ->number(),
        6
    );
    EXPECT_EQ(
        arena::protocol::LoginResponse::descriptor()
            ->FindFieldByName("room_id")
            ->number(),
        7
    );
    EXPECT_EQ(
        arena::protocol::LoginResponse::descriptor()
            ->FindFieldByName("resumed")
            ->number(),
        8
    );
}

TEST(
    EnvelopeCodecTest,
    OnlineStateAndRoomMessagesRoundTripWithStableAdditiveTags
)
{
    namespace wire = arena::protocol;
    New stateRequest;
    stateRequest.set_msg_id(wire::MSG_SESSION_STATE_REQUEST);
    stateRequest.mutable_session_state_request();
    New stateResponse;
    stateResponse.set_msg_id(wire::MSG_SESSION_STATE_RESPONSE);
    auto* state = stateResponse.mutable_session_state_response();
    state->set_session_id(101);
    state->set_player_id(42);
    state->set_room_id(7);
    state->set_presence(wire::PRESENCE_ONLINE);
    auto* member = state->add_members();
    member->set_session_id(202);
    member->set_player_id(43);
    member->set_presence(wire::PRESENCE_DETACHED);
    New joinRequest;
    joinRequest.set_msg_id(wire::MSG_ROOM_JOIN_REQUEST);
    joinRequest.mutable_room_join_request()->set_room_id(7);
    New joinResponse;
    joinResponse.set_msg_id(wire::MSG_ROOM_JOIN_RESPONSE);
    *joinResponse.mutable_room_join_response()
         ->mutable_state() = *state;
    New leaveRequest;
    leaveRequest.set_msg_id(wire::MSG_ROOM_LEAVE_REQUEST);
    leaveRequest.mutable_room_leave_request();
    New leaveResponse;
    leaveResponse.set_msg_id(wire::MSG_ROOM_LEAVE_RESPONSE);
    leaveResponse.mutable_room_leave_response()
        ->mutable_state()
        ->set_session_id(101);
    for (const auto& request :
         {stateRequest,
          stateResponse,
          joinRequest,
          joinResponse,
          leaveRequest,
          leaveResponse})
    {
        const auto encoded = EnvelopeCodec::encode(request);
        ASSERT_TRUE(encoded);
        New decoded;
        ASSERT_TRUE(decode(*encoded, decoded));
        EXPECT_EQ(decoded.msg_id(), request.msg_id());
        EXPECT_EQ(decoded.payload_case(), request.payload_case());
        EXPECT_EQ(
            decoded.SerializeAsString(),
            request.SerializeAsString()
        );
    }
    EXPECT_EQ(
        New::descriptor()
            ->FindFieldByName("session_state_request")
            ->number(),
        20
    );
    EXPECT_EQ(
        New::descriptor()
            ->FindFieldByName("session_state_response")
            ->number(),
        21
    );
    EXPECT_EQ(
        New::descriptor()
            ->FindFieldByName("room_join_request")
            ->number(),
        30
    );
    EXPECT_EQ(
        New::descriptor()
            ->FindFieldByName("room_join_response")
            ->number(),
        31
    );
    EXPECT_EQ(
        New::descriptor()
            ->FindFieldByName("room_leave_request")
            ->number(),
        32
    );
    EXPECT_EQ(
        New::descriptor()
            ->FindFieldByName("room_leave_response")
            ->number(),
        33
    );
    EXPECT_EQ(
        wire::SessionStateResponse::descriptor()
            ->FindFieldByName("members")
            ->number(),
        5
    );
    EXPECT_EQ(
        wire::RoomMember::descriptor()
            ->FindFieldByName("presence")
            ->number(),
        3
    );
}

TEST(
    EnvelopeCodecTest,
    LegacyForwarderPreservesNewRoomPayloadWithoutUnderstandingIt
)
{
    New request;
    request.set_msg_id(arena::protocol::MSG_ROOM_JOIN_REQUEST);
    request.set_request_id(1234567890123ULL);
    request.mutable_room_join_request()->set_room_id(77);
    const auto encoded = EnvelopeCodec::encode(request);
    ASSERT_TRUE(encoded);
    Old legacy;
    ASSERT_TRUE(legacy.ParseFromString(*encoded));
    EXPECT_GT(
        legacy.GetReflection()
            ->GetUnknownFields(legacy)
            .field_count(),
        0
    );
    std::string forwarded;
    ASSERT_TRUE(legacy.SerializeToString(&forwarded));
    New restored;
    ASSERT_TRUE(decode(forwarded, restored));
    EXPECT_EQ(restored.request_id(), request.request_id());
    EXPECT_EQ(
        restored.msg_id(),
        arena::protocol::MSG_ROOM_JOIN_REQUEST
    );
    EXPECT_EQ(restored.room_join_request().room_id(), 77);
}

TEST(
    EnvelopeCodecTest,
    MatchMessagesKeepAllTicketValuesAndSurviveLegacyForwarding
)
{
    namespace wire = arena::protocol;
    New join;
    join.set_msg_id(wire::MSG_MATCH_JOIN_REQUEST);
    join.mutable_match_join_request()->set_join_key(UINT64_MAX);
    New joined;
    joined.set_msg_id(wire::MSG_MATCH_JOIN_RESPONSE);
    auto* ticket = joined.mutable_match_join_response()
                       ->mutable_state();
    ticket->set_player_id(42);
    ticket->set_ticket_id(91);
    ticket->set_join_key(UINT64_MAX);
    ticket->set_state(wire::MATCH_TICKET_ASSIGNED);
    ticket->set_mmr(1500);
    ticket->set_window(175);
    ticket->set_waited_ms(5010);
    ticket->set_remaining_ms(24990);
    ticket->set_match_id(7);
    ticket->set_opponent_player_id(43);
    ticket->set_opponent_ticket_id(92);
    ticket->set_active(true);
    ticket->set_room_id(501);
    New cancel;
    cancel.set_msg_id(wire::MSG_MATCH_CANCEL_REQUEST);
    cancel.mutable_match_cancel_request()->set_ticket_id(91);
    New cancelled;
    cancelled.set_msg_id(wire::MSG_MATCH_CANCEL_RESPONSE);
    *cancelled.mutable_match_cancel_response()
         ->mutable_state() = *ticket;
    cancelled.mutable_match_cancel_response()
        ->mutable_state()
        ->set_state(wire::MATCH_TICKET_CANCELLED);
    cancelled.mutable_match_cancel_response()
        ->mutable_state()
        ->set_active(false);
    New status;
    status.set_msg_id(wire::MSG_MATCH_STATUS_REQUEST);
    status.mutable_match_status_request()->set_ticket_id(91);
    New queried;
    queried.set_msg_id(wire::MSG_MATCH_STATUS_RESPONSE);
    *queried.mutable_match_status_response()
         ->mutable_state() = *ticket;
    for (auto value :
         {join, joined, cancel, cancelled, status, queried})
    {
        value.set_request_id(1234567890123ULL);
        const auto encoded = EnvelopeCodec::encode(value);
        ASSERT_TRUE(encoded);
        New decoded;
        ASSERT_TRUE(decode(*encoded, decoded));
        EXPECT_EQ(
            decoded.SerializeAsString(),
            value.SerializeAsString()
        );
        Old legacy;
        ASSERT_TRUE(legacy.ParseFromString(*encoded));
        EXPECT_EQ(legacy.payload_case(), Old::PAYLOAD_NOT_SET);
        EXPECT_GT(
            legacy.GetReflection()
                ->GetUnknownFields(legacy)
                .field_count(),
            0
        );
        std::string forwarded;
        ASSERT_TRUE(legacy.SerializeToString(&forwarded));
        New restored;
        ASSERT_TRUE(decode(forwarded, restored));
        EXPECT_EQ(
            restored.SerializeAsString(),
            value.SerializeAsString()
        );
    }
}

TEST(
    EnvelopeCodecTest,
    ManagedLifecycleMessagesPreserveRecoveryStateThroughLegacyForwarding
)
{
    namespace wire = arena::protocol;
    New ready;
    ready.set_msg_id(wire::MSG_ROOM_READY_REQUEST);
    auto* readyRequest = ready.mutable_room_ready_request();
    readyRequest->set_ready(true);
    readyRequest->set_actor_incarnation(UINT64_MAX);
    readyRequest->set_membership_epoch(UINT64_MAX - 1);
    New readied;
    readied.set_msg_id(wire::MSG_ROOM_READY_RESPONSE);
    auto* state = readied.mutable_room_ready_response()
                      ->mutable_state();
    state->set_match_id(71);
    state->set_phase(wire::ROOM_PHASE_PLAYING);
    state->set_started_tick(3);
    state->set_played_ticks(7);
    state->set_duration_ticks(100);
    auto* snapshot = state->mutable_snapshot();
    snapshot->set_room_id(501);
    snapshot->set_incarnation(UINT64_MAX);
    snapshot->set_tick(10);
    snapshot->set_checksum(UINT64_MAX);
    snapshot->set_checksum_version(1);
    snapshot->set_position_limit(1000000);
    auto* player = snapshot->add_players();
    player->set_player_id(42);
    player->set_session_id(UINT64_MAX - 2);
    player->set_member_epoch(UINT64_MAX - 1);
    player->set_x(-5);
    player->set_y(6);
    player->set_last_seq(UINT64_MAX - 3);
    player->set_last_dx(-1);
    player->set_last_dy(1);
    auto* member = state->add_members();
    member->set_player_id(player->player_id());
    member->set_session_id(player->session_id());
    member->set_member_epoch(player->member_epoch());
    member->set_ready(true);
    member->set_presence(wire::PRESENCE_DETACHED);
    New status;
    status.set_msg_id(wire::MSG_ROOM_STATUS_REQUEST);
    status.mutable_room_status_request();
    New queried;
    queried.set_msg_id(wire::MSG_ROOM_STATUS_RESPONSE);
    *queried.mutable_room_status_response()->mutable_state() = *state;
    New settlement;
    settlement.set_msg_id(wire::MSG_SETTLEMENT_REQUEST);
    settlement.mutable_settlement_request()->set_match_id(71);
    New settled;
    settled.set_msg_id(wire::MSG_SETTLEMENT_RESPONSE);
    auto* result = settled.mutable_settlement_response()
                       ->mutable_state();
    result->set_result_id(71);
    result->set_match_id(71);
    result->set_room_id(501);
    result->set_final_tick(103);
    result->set_final_checksum(UINT64_MAX);
    result->set_reason(wire::SETTLEMENT_REASON_COMPLETED);
    result->set_winner_player_id(0);
    *result->add_players() = *player;
    *queried.mutable_room_status_response()
         ->mutable_state()
         ->mutable_settlement() = *result;
    queried.mutable_room_status_response()
        ->mutable_state()
        ->set_finished_tick(103);
    queried.mutable_room_status_response()
        ->mutable_state()
        ->set_phase(wire::ROOM_PHASE_FINISHED);
    for (auto value :
         {ready, readied, status, queried, settlement, settled})
    {
        value.set_request_id(UINT64_MAX - 4);
        const auto encoded = EnvelopeCodec::encode(value);
        ASSERT_TRUE(encoded);
        Old legacy;
        ASSERT_TRUE(legacy.ParseFromString(*encoded));
        EXPECT_EQ(legacy.payload_case(), Old::PAYLOAD_NOT_SET);
        std::string forwarded;
        ASSERT_TRUE(legacy.SerializeToString(&forwarded));
        New restored;
        ASSERT_TRUE(decode(forwarded, restored));
        EXPECT_EQ(
            restored.SerializeAsString(),
            value.SerializeAsString()
        );
        value.set_msg_id(0);
        const auto inferred = EnvelopeCodec::encode(value);
        ASSERT_TRUE(inferred);
        ASSERT_TRUE(decode(*inferred, restored));
        EXPECT_EQ(restored.payload_case(), value.payload_case());
    }
}

TEST(
    EnvelopeCodecTest,
    ManagedLifecycleTagsAreAdditiveAndIdentityIsServerOwned
)
{
    namespace wire = arena::protocol;
    const std::array<std::pair<std::string_view, int>, 6> payloads{
        {{"room_ready_request", 54},
         {"room_ready_response", 55},
         {"room_status_request", 56},
         {"room_status_response", 57},
         {"settlement_request", 58},
         {"settlement_response", 59}}
    };
    for (const auto& [name, number] : payloads)
    {
        const auto* field = New::descriptor()->FindFieldByName(
            std::string(name)
        );
        ASSERT_NE(field, nullptr);
        EXPECT_EQ(field->number(), number);
        EXPECT_EQ(field->containing_oneof()->name(), "payload");
    }
    const auto* ticket = wire::MatchTicket::descriptor()
                             ->FindFieldByName("room_id");
    ASSERT_NE(ticket, nullptr);
    EXPECT_EQ(ticket->number(), 13);
    EXPECT_EQ(
        ticket->type(),
        google::protobuf::FieldDescriptor::TYPE_UINT64
    );
    EXPECT_EQ(wire::RoomReadyRequest::descriptor()->field_count(), 3);
    EXPECT_EQ(
        wire::RoomStatusRequest::descriptor()->field_count(),
        0
    );
    EXPECT_EQ(
        wire::SettlementRequest::descriptor()->field_count(),
        1
    );
    for (const auto* request :
         {wire::RoomReadyRequest::descriptor(),
          wire::RoomStatusRequest::descriptor(),
          wire::SettlementRequest::descriptor()})
    {
        EXPECT_EQ(request->FindFieldByName("player_id"), nullptr);
        EXPECT_EQ(request->FindFieldByName("session_id"), nullptr);
        EXPECT_EQ(request->FindFieldByName("room_id"), nullptr);
        EXPECT_EQ(
            request->FindFieldByName("winner_player_id"),
            nullptr
        );
    }
    EXPECT_EQ(
        wire::RoomReadyRequest::descriptor()
            ->FindFieldByName("ready")
            ->number(),
        1
    );
    EXPECT_EQ(
        wire::RoomReadyRequest::descriptor()
            ->FindFieldByName("actor_incarnation")
            ->number(),
        2
    );
    EXPECT_EQ(
        wire::RoomReadyRequest::descriptor()
            ->FindFieldByName("membership_epoch")
            ->number(),
        3
    );
    EXPECT_EQ(
        wire::SettlementRequest::descriptor()
            ->FindFieldByName("match_id")
            ->number(),
        1
    );
    EXPECT_EQ(
        wire::RoomStatus::descriptor()
            ->FindFieldByName("snapshot")
            ->number(),
        1
    );
    EXPECT_EQ(
        wire::RoomStatus::descriptor()
            ->FindFieldByName("settlement")
            ->number(),
        9
    );
    EXPECT_EQ(
        wire::SettlementResult::descriptor()
            ->FindFieldByName("final_checksum")
            ->type(),
        google::protobuf::FieldDescriptor::TYPE_FIXED64
    );
    EXPECT_EQ(wire::ROOM_PHASE_FORMING, 1);
    EXPECT_EQ(wire::ROOM_PHASE_READY, 2);
    EXPECT_EQ(wire::ROOM_PHASE_PLAYING, 3);
    EXPECT_EQ(wire::ROOM_PHASE_SETTLING, 4);
    EXPECT_EQ(wire::ROOM_PHASE_FINISHED, 5);
    EXPECT_EQ(wire::SETTLEMENT_REASON_COMPLETED, 1);
    EXPECT_EQ(wire::SETTLEMENT_REASON_MEMBER_UNAVAILABLE, 2);
    EXPECT_EQ(wire::SETTLEMENT_REASON_READY_TIMEOUT, 3);
}

TEST(
    EnvelopeCodecTest,
    MatchTagsAreAdditiveAndRequestsNeverCarryRatings
)
{
    namespace wire = arena::protocol;
    const std::array<std::pair<std::string_view, int>, 6> payloads{
        {{"match_join_request", 40},
         {"match_join_response", 41},
         {"match_cancel_request", 42},
         {"match_cancel_response", 43},
         {"match_status_request", 44},
         {"match_status_response", 45}}
    };
    for (const auto& [name, number] : payloads)
    {
        const auto* field = New::descriptor()->FindFieldByName(
            std::string(name)
        );
        ASSERT_NE(field, nullptr);
        EXPECT_EQ(field->number(), number);
        EXPECT_EQ(field->containing_oneof()->name(), "payload");
    }
    const std::array<std::pair<std::string_view, int>, 12> fields{
        {{"player_id", 1},
         {"ticket_id", 2},
         {"join_key", 3},
         {"state", 4},
         {"mmr", 5},
         {"window", 6},
         {"waited_ms", 7},
         {"remaining_ms", 8},
         {"match_id", 9},
         {"opponent_player_id", 10},
         {"opponent_ticket_id", 11},
         {"active", 12}}
    };
    for (const auto& [name, number] : fields)
    {
        const auto* field = wire::MatchTicket::descriptor()
                                ->FindFieldByName(std::string(name));
        ASSERT_NE(field, nullptr);
        EXPECT_EQ(field->number(), number);
    }
    for (const auto* request :
         {wire::MatchJoinRequest::descriptor(),
          wire::MatchCancelRequest::descriptor(),
          wire::MatchStatusRequest::descriptor()})
    {
        EXPECT_EQ(request->field_count(), 1);
        EXPECT_EQ(request->field(0)->number(), 1);
        EXPECT_EQ(request->FindFieldByName("mmr"), nullptr);
        EXPECT_EQ(request->FindFieldByName("player_id"), nullptr);
        EXPECT_EQ(request->FindFieldByName("session_id"), nullptr);
    }
    for (const auto* response :
         {wire::MatchJoinResponse::descriptor(),
          wire::MatchCancelResponse::descriptor(),
          wire::MatchStatusResponse::descriptor()})
    {
        const auto* state = response->FindFieldByName("state");
        ASSERT_NE(state, nullptr);
        EXPECT_EQ(state->number(), 1);
        EXPECT_EQ(
            state->message_type(),
            wire::MatchTicket::descriptor()
        );
    }
    EXPECT_EQ(
        wire::MatchTicket::descriptor()->FindFieldByName(
            "session_id"
        ),
        nullptr
    );
    EXPECT_EQ(wire::MATCH_TICKET_QUEUED, 1);
    EXPECT_EQ(wire::MATCH_TICKET_RESERVED, 2);
    EXPECT_EQ(wire::MATCH_TICKET_ASSIGNED, 3);
    EXPECT_EQ(wire::MATCH_TICKET_CANCELLED, 4);
    EXPECT_EQ(wire::MATCH_TICKET_EXPIRED, 5);
    EXPECT_EQ(wire::MATCH_TICKET_FAILED, 6);
}

TEST(
    EnvelopeCodecTest,
    RoomTicksRoundTripSignedStateAndFixedChecksumThroughLegacy
)
{
    namespace wire = arena::protocol;
    New input;
    input.set_msg_id(wire::MSG_ROOM_INPUT_REQUEST);
    auto* control = input.mutable_room_input_request();
    control->set_actor_incarnation(71);
    control->set_membership_epoch(4);
    control->set_seq(10);
    control->set_dx(-1);
    control->set_dy(1);
    New applied;
    applied.set_msg_id(wire::MSG_ROOM_INPUT_RESPONSE);
    auto* result = applied.mutable_room_input_response();
    result->set_result(wire::ROOM_INPUT_APPLIED);
    auto* snapshot = result->mutable_state();
    snapshot->set_room_id(7);
    snapshot->set_incarnation(71);
    snapshot->set_tick(123);
    snapshot->set_checksum(UINT64_MAX);
    snapshot->set_checksum_version(1);
    snapshot->set_position_limit(1000000);
    auto* player = snapshot->add_players();
    player->set_player_id(42);
    player->set_session_id(81);
    player->set_member_epoch(4);
    player->set_x(-999);
    player->set_y(999);
    player->set_last_seq(10);
    player->set_last_dx(-1);
    player->set_last_dy(1);
    New query;
    query.set_msg_id(wire::MSG_ROOM_SNAPSHOT_REQUEST);
    query.mutable_room_snapshot_request();
    New queried;
    queried.set_msg_id(wire::MSG_ROOM_SNAPSHOT_RESPONSE);
    *queried.mutable_room_snapshot_response()
         ->mutable_state() = *snapshot;
    for (auto message : {input, applied, query, queried})
    {
        message.set_request_id(1ULL << 40);
        const auto encoded = EnvelopeCodec::encode(message);
        ASSERT_TRUE(encoded);
        New decoded;
        ASSERT_TRUE(decode(*encoded, decoded));
        EXPECT_EQ(
            decoded.SerializeAsString(),
            message.SerializeAsString()
        );
        Old legacy;
        ASSERT_TRUE(legacy.ParseFromString(*encoded));
        EXPECT_EQ(legacy.payload_case(), Old::PAYLOAD_NOT_SET);
        std::string forwarded;
        ASSERT_TRUE(legacy.SerializeToString(&forwarded));
        New restored;
        ASSERT_TRUE(decode(forwarded, restored));
        EXPECT_EQ(
            restored.SerializeAsString(),
            message.SerializeAsString()
        );
    }
}

TEST(
    EnvelopeCodecTest,
    RoomTickSchemaKeepsStableFencesAndCanonicalStateFields
)
{
    namespace wire = arena::protocol;
    const auto checkFields =
        [](const auto* descriptor, const auto& fields)
    {
        for (const auto& [name, number] : fields)
        {
            const auto* field = descriptor->FindFieldByName(
                std::string(name)
            );
            ASSERT_NE(field, nullptr);
            EXPECT_EQ(field->number(), number);
        }
    };
    checkFields(
        New::descriptor(),
        std::array<std::pair<std::string_view, int>, 4>{
            {{"room_input_request", 50},
             {"room_input_response", 51},
             {"room_snapshot_request", 52},
             {"room_snapshot_response", 53}}
        }
    );
    checkFields(
        wire::RoomInputRequest::descriptor(),
        std::array<std::pair<std::string_view, int>, 5>{
            {{"actor_incarnation", 1},
             {"membership_epoch", 2},
             {"seq", 3},
             {"dx", 4},
             {"dy", 5}}
        }
    );
    checkFields(
        wire::RoomSnapshot::descriptor(),
        std::array<std::pair<std::string_view, int>, 7>{
            {{"room_id", 1},
             {"incarnation", 2},
             {"tick", 3},
             {"checksum", 4},
             {"players", 5},
             {"checksum_version", 6},
             {"position_limit", 7}}
        }
    );
    checkFields(
        wire::RoomPlayerState::descriptor(),
        std::array<std::pair<std::string_view, int>, 8>{
            {{"player_id", 1},
             {"session_id", 2},
             {"member_epoch", 3},
             {"x", 4},
             {"y", 5},
             {"last_seq", 6},
             {"last_dx", 7},
             {"last_dy", 8}}
        }
    );
    EXPECT_EQ(wire::RoomInputRequest::descriptor()->field_count(), 5);
    EXPECT_EQ(
        wire::RoomInputRequest::descriptor()->FindFieldByName(
            "player_id"
        ),
        nullptr
    );
    EXPECT_EQ(
        wire::RoomInputRequest::descriptor()->FindFieldByName(
            "session_id"
        ),
        nullptr
    );
    EXPECT_EQ(
        wire::RoomInputRequest::descriptor()->FindFieldByName(
            "room_id"
        ),
        nullptr
    );
    EXPECT_EQ(
        wire::RoomSnapshot::descriptor()
            ->FindFieldByName("checksum")
            ->type(),
        google::protobuf::FieldDescriptor::TYPE_FIXED64
    );
    EXPECT_EQ(
        wire::RoomPlayerState::descriptor()
            ->FindFieldByName("x")
            ->type(),
        google::protobuf::FieldDescriptor::TYPE_SINT32
    );
    EXPECT_EQ(
        wire::RoomSnapshotRequest::descriptor()->field_count(),
        0
    );
    EXPECT_EQ(
        wire::RoomInputResponse::descriptor()
            ->FindFieldByName("state")
            ->number(),
        1
    );
    EXPECT_EQ(
        wire::RoomInputResponse::descriptor()
            ->FindFieldByName("result")
            ->number(),
        2
    );
    EXPECT_EQ(
        wire::RoomSnapshotResponse::descriptor()
            ->FindFieldByName("state")
            ->number(),
        1
    );
    EXPECT_EQ(wire::ROOM_INPUT_APPLIED, 1);
    EXPECT_EQ(wire::ROOM_INPUT_DUPLICATE, 2);
    EXPECT_EQ(wire::ROOM_INPUT_GAP, 3);
    EXPECT_EQ(wire::ROOM_INPUT_STALE, 4);
    EXPECT_EQ(wire::ROOM_INPUT_CONFLICT, 5);
    EXPECT_EQ(wire::ROOM_INPUT_NOT_MEMBER, 6);
    EXPECT_EQ(wire::ROOM_INPUT_INVALID, 7);
}
