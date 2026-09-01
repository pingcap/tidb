# `pkg/util/httputil` — Go-master parity boundary receipt

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).
This package is the shared HTTP client/response helper used by BR,
Lightning, object storage, and other Go tooling.

## Complete inventory

All three Go-master artifacts were read in full. There are no package docs,
fixtures, generated outputs, platform variants, benchmarks, fuzz targets, or
nested packages.

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 18 | `4f4f4ab8f9b0cf4db322dbc7754c30bb9c7230f6` | `d04b8b6a272c193e869e4a1c5b28c4d493617d28eb61a606bd3d2d0ba0af6bf3` | public library/flaky test target and errors dependency inventoried |
| `http.go` | 97 | `34b6c69ec35622582f3616ac2159556938aec3cc` | `c5cbbf2214e16694bc19090d09866a87ae8943526cae88baf53e33a4e199e392` | 30-second client construction, context GET, JSON decode, text read, and non-200 error body inventoried |
| `http_test.go` | 84 | `06a1e8fafa23e3601c933684d68ab93ba493c8af` | `027da28e4f20f9a83888d301b7bab450a85ab208ade95a65542d41f8d80229c7` | httptest success, transport failure, JSON, text, and non-200 cases inventoried |

Total: 199 textual lines. Production has four functions: `NewClient`,
`GetJSON`, `GetText`, and private `doGet`; tests have two source identities.
TLS clients clone the default transport, install the caller's TLS config and
30-second idle timeout, and all clients default to a 30-second request
timeout. `doGet` binds cancellation to a context, closes non-200 response
bodies after reading the body for its error, and returns only status-200
bodies; JSON and text helpers propagate read/decode errors with stack traces.

## Rust ownership and integration decision

Rust has isolated HTTP/PD/status-server transports, but no shared client with
Go's `NewClient` timeout/TLS clone policy, context cancellation, response-body
ownership, JSON/text helpers, and exact non-200 body errors. The package is
consumed by Go BR/Lightning/object-storage roots that are not dependency-closed
Rust crates. Adding a second generic HTTP client or adapting one PD transport
would create Rust-only behavior and could diverge in TLS/timeout semantics.
The package is explicitly unclaimed; no source change is justified.

## Validation

Profile: **WIP**. This is a complete three-artifact inventory and explicit
boundary audit with no code change, so `make bazel_prepare` and the Ready lint
gate are not triggered.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/httputil -count=1
# ok
```

## Risks and unverified behavior

- Correctness: timeout, context, TLS transport, body-closing, JSON/text, and
  non-200 error contracts remain Go-owned.
- Compatibility: BR/Lightning/object-store callers rely on the exact shared
  client behavior; a future Rust owner must move those composition roots with
  the helper.
- Performance: no runtime code changed; Go's client reuses the cloned
  transport's connection pool.
- Not verified locally: Bazel execution, TLS handshake variants, cancellation
  during body reads, and all downstream BR/Lightning/object-storage callers.
