# Auth library

`arena_auth` now implements the authentication interface used by Gateway:

- `PasswordDigest`: replaceable hash/verify interface. `Pbkdf2PasswordDigest`
  uses OpenSSL PBKDF2-HMAC-SHA256, a random 16-byte salt, a 32-byte key and
  600000 rounds when creating digests. Verification accepts 100000..2000000
  stored rounds and uses constant-time key equality. Malformed digests fail closed.
- `PasswordAccounts`: replaceable account/digest lookup. `FilePasswordAccounts`
  loads a read-only startup snapshot of tab-separated account, nonzero player ID
  and versioned digest. Duplicate accounts/players and malformed rows reject
  startup. Bounds are 8 MiB, 10000 accounts, 128 ASCII account bytes and 512 bytes
  per row. Database storage and live reload remain planned.
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
Set `ARENA_ACCOUNTS_FILE=/safe/local/accounts.tsv` to enable password login.
Without this setting the account adapter remains unavailable. Configured unreadable
or invalid account files reject startup. Test doubles are never selected by main.
The file is immutable after loading; no file I/O occurs during lookup. Password
KDF work is synchronous on the connection owner, so this local adapter is intended
for the current bounded development deployment; an executor-backed credential
service is needed before handling large login bursts.

Create two real account rows without putting passwords in shell arguments:

```sh
umask 077
python3 tools/password_account.py alice 42 > /safe/local/accounts.tsv
python3 tools/password_account.py bob 43 >> /safe/local/accounts.tsv
export ARENA_ACCOUNTS_FILE=/safe/local/accounts.tsv
export ARENA_JWT_KEY="$(openssl rand -hex 32)"
./build/services/gateway/app/ArenaServer asio handshake
```

The helper prompts twice per password. Its output contains salted digests, not
plaintext passwords. The file and JWT key must stay outside the repository.
Transport encryption is not implemented by these loopback TCP/WS listeners.

`makeLoginMessageRegistry` accepts actual adapters and one shared `OnlineRegistry`
directory for all workers/listeners. Protobuf `LoginRequest.credential_kind` tag 4
defaults to PASSWORD=0 for old clients; JWT=1 requests must leave `account` empty and
use the signed subject. Accepted responses add `player_id` tag 4 and
`session_epoch` tag 5, then stable `session_id` tag 6, `room_id` tag 7 and
`resumed` tag 8 without changing existing tags.

A password login starts a fresh logical session and removes its old room membership.
JWT login must match the registry's current epoch and expiry; it rotates the epoch
while preserving stable SessionId/RoomId on successful recovery. First matching
disconnect starts monotonic grace (default 30 seconds), retaining the logical
session and room slot without retaining a transport. Repeated or stale detach
cannot extend the deadline or affect the replacement. At the grace boundary,
recovery fails even when the JWT itself has not expired.

The registry defaults to 100000 retained players and bounded gateway room rosters.
Management-owned upkeep sweeps expired records/members every 100 ms independently
of new traffic, prunes empty rooms, and is cancelled before executor shutdown.
Player/room operations use one synchronized authority; domain Session/Room remain
separate owner-affine lifecycle models. See gateway README and docs/protocol.md
for authenticated state/join/leave messages and ARENA_DISCONNECT_GRACE_MS.
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
It also checks grace boundaries, complete roster reclamation, maintenance lifetime,
and the production state/join/leave routes.
`AccountLoginTransport_*` covers epoll/Asio 0/4 workers and 50 TCP + 50 WS clients.
The network fixture uses a deterministic test-only key/clock and strict test doubles.
New login fields are also covered by frozen-client Envelope compatibility tests.
`OnlinePresenceTransport_*` uses a real JWT clock and short grace to verify
TCP/WS recovery and timer reclamation without further requests.
