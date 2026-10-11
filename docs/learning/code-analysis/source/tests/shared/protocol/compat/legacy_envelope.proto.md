# tests/shared/protocol/compat/legacy_envelope.proto

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；41行。

[对应分析](../../../../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~protobuf
syntax = "proto3";

package arena.protocol.legacy;

// Frozen v1 client fixture. Keep its tags and wire types unchanged.
message Envelope {
  uint64 request_id = 1;
  string legacy_client_tag = 2;

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
}
~~~
