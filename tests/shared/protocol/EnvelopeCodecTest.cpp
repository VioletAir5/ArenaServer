#include "protocol/EnvelopeCodec.hpp"

#include <string>
#include <string_view>

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
