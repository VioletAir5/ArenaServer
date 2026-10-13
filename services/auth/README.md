# Auth library

`arena_auth` now implements the authentication interface used by Gateway:

- `PasswordDigest`: replaceable `hash/verify` interface; the supplied stub returns
  `Unavailable`. No plaintext or fast-hash fallback is provided.
- `PasswordAccounts`: replaceable account/digest lookup; the supplied stub returns
  `Unavailable`. Database account storage remains planned.
- `LoginAuthenticator`: bounded password verification through these adapters.
- `JwtTokens`: OpenSSL 3 HMAC-SHA256 issuing and verification with a closed JWT profile.

There is no standalone Auth executable or internal RPC. Gateway consumes this library
in process. Adapters and the injected clock must be safe for concurrent calls.

## JWT profile

HS256 is fixed by server configuration; `alg=HS256` and `typ=JWT` are required.
The header has exactly those two fields; claims contain exactly
`iss/aud/sub/iat/nbf/exp/epoch`. Issuer and audience default to
`arena-auth` and `arena-gateway`. `sub` and `epoch` are nonzero canonical decimal
strings, preserving the full uint64 range. Times are nonnegative JSON integer numbers.
The verifier requires `iat <= now`, `iat <= nbf <= now < exp`, and a lifetime no longer
than the configured limit (default five minutes). There is no clock leeway.

Tokens are bounded to 4096 bytes, decoded header/payload to 512/2048 bytes and
JSON depth to one object level. Duplicate keys, unsupported claims/headers, malformed
JSON/types, noncanonical base64url, bad signatures and weak key lengths are rejected.
HMAC uses `EVP_Q_mac`; signature comparison uses `CRYPTO_memcmp`.

This is an application profile; audience arrays, extensions, key rotation and other
JWT algorithms are not supported. Algorithm/issuer/audience checks follow
[RFC 8725](https://www.rfc-editor.org/rfc/rfc8725.html); time claims are defined in
[RFC 7519](https://www.rfc-editor.org/rfc/rfc7519.html).
OpenSSL primitives: [EVP_MAC](https://docs.openssl.org/3.0/man3/EVP_MAC/) and
[CRYPTO_memcmp](https://docs.openssl.org/3.0/man3/CRYPTO_memcmp/).

## Gateway integration

`ArenaServer asio handshake` reads `ARENA_JWT_KEY` once. It requires at least 32
random key bytes and has no default key. Missing configuration retains the previous
login-unavailable behavior; invalid configured keys fail startup.
The in-process credential adapters remain stubs, so password login cannot succeed in
the application until real account/KDF adapters are supplied. Test doubles are
confined to tests and are never selected by the application.

`makeLoginMessageRegistry` accepts actual adapters and one shared `AccountSessions`
directory for all workers/listeners. Protobuf `LoginRequest.credential_kind` tag 4
defaults to PASSWORD=0 for old clients; JWT=1 requests must leave `account` empty and
use the signed subject. Accepted responses add `player_id` tag 4 and
`session_epoch` tag 5 without changing existing tags.

A password login replaces the player's active binding. JWT login must match the
directory's current epoch and expiry; it rotates the epoch on successful resume.
Disconnect clears only the matching player/connection/epoch active binding and
retains resume authorization until token expiry. A bounded directory defaults to
100000 account records; expired entries are reclaimed when capacity is needed.
Epochs start from an OpenSSL random boot seed and monotonically increase without wrap.
This mitigates restart ABA; it is not a durable or distributed epoch authority.

Revocation precedes the deferred socket close. Message admission and response
completion recheck lease validity, including expiry, so old sockets cannot resume
or route packets after revocation. Work admitted before replacement may already
have run; the library cannot undo prior business side effects. Future asynchronous
business owners must carry/check the epoch before applying side effects.
The directory does not instantiate the separate owner-thread domain Session model.

## Verification

The dedicated `arena_auth_tests` target covers crypto, password stubs, concurrent
replacement, stale detach, resume, old packet rejection and late completion.
`AccountLoginTransport_*` covers epoll/Asio 0/4 workers and 50 TCP + 50 WS clients.
The network fixture uses a deterministic test-only key/clock and strict test doubles.
New login fields are also covered by frozen-client Envelope compatibility tests.
