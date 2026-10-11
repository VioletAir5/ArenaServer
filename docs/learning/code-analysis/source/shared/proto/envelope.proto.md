# shared/proto/envelope.proto

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；74行。

[对应分析](../../../03-protocol.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~protobuf
syntax = "proto3";

// Application messages carried inside the length-prefixed packet frame.

package arena.protocol;

// Stable payload schema inside the existing length-prefixed PacketCodec frame.
// Never renumber a field or reuse a retired tag. Tag 2 was legacy_client_tag
// in the frozen v1 compatibility fixture.
enum MsgId {
  MSG_UNSPECIFIED = 0;
  MSG_LOGIN_REQUEST = 10;
  MSG_LOGIN_RESPONSE = 11;
  MSG_PING = 12;
  MSG_PONG = 13;
  MSG_ERROR = 14;
}

// Stable wire codes. Error.code stays uint32 for v1 descriptor compatibility.
enum ErrorCode {
  ERR_OK = 0;
  ERR_UNKNOWN_MESSAGE = 1001;
  ERR_INVALID_REQUEST = 1002;
  ERR_UNAUTHENTICATED = 1003;
  ERR_TIMEOUT = 1004;
  ERR_TOO_MANY_REQUESTS = 1005;
  ERR_DUPLICATE_REQUEST = 1006;
  ERR_BUSINESS_FAILURE = 2001;
  ERR_UNAVAILABLE = 2002;
  ERR_INTERNAL = 9000;
}

message Envelope {
  uint64 request_id = 1;
  reserved 2;
  reserved "legacy_client_tag";
  uint64 sent_at_unix_ms = 3;
  // Zero means a v1 peer: infer from the known payload. Never reuse this tag.
  uint32 msg_id = 4;

  oneof payload {
    LoginRequest login_request = 10;
    LoginResponse login_response = 11;
    Ping ping = 12;
    Pong pong = 13;
    Error error = 14;
  }
}

message LoginRequest {
  string account = 1;
  bytes credential = 2;
  string client_version = 3;
}

message LoginResponse {
  bool accepted = 1;
  string session_token = 2;
  Error error = 3;
}

message Ping {
  uint64 nonce = 1;
}

message Pong {
  uint64 nonce = 1;
}

message Error {
  uint32 code = 1;
  string message = 2;
  repeated Error causes = 3;
}
~~~
