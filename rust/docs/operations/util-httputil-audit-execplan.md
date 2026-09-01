# `pkg/util/httputil` parity audit ExecPlan

## Purpose

Walk the complete Go `pkg/util/httputil` package at the current Go-master pin,
including its shared HTTP client behavior, tests, and Bazel target, then decide
whether Rust has a dependency-closed owner.

## Progress

- Read all three artifacts: `http.go` (97 lines), `http_test.go` (84 lines),
  and `BUILD.bazel` (18 lines), 199 lines total.
- Enumerated all four production functions (`NewClient`, `GetJSON`, `GetText`,
  and `doGet`) and both source test identities. The tests cover transport
  errors, JSON success, non-200 responses, and text success; there are no
  fixtures, generated/platform variants, benchmarks, fuzzers, examples, or
  nested packages.
- Compared the package with Go master
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`; there is no source delta.
- Searched Rust transports and consumers. Rust has isolated HTTP clients but no
  shared owner matching Go's timeout/TLS clone policy, context cancellation,
  response-body ownership, and exact non-200 diagnostics. BR/Lightning/object
  storage composition roots are not dependency-closed Rust packages.

## Decision

Keep `pkg/util/httputil` explicitly unclaimed. Adding a generic Rust client or
adapting one isolated transport would create Rust-only behavior and risk
observable timeout, TLS, or error differences. No production change or new
regression test is justified at this boundary.

## Validation

- Active checkout: `go test ./pkg/util/httputil -count=1` — passed.
- Detached Go-master checkout: the same focused suite — passed.
- Rust owner/consumer search completed; no dependency-closed owner suite
  exists.
- Ready gates: Rust fmt check, pinned `make lint` in the clean detached
  checkout, and `git diff --check`.

## Risks and follow-up

Timeout, TLS handshake, cancellation during body reads, and all downstream
BR/Lightning/object-storage callers remain outside local verification. A
future Rust claim must move those callers and preserve Go's body/error
contracts together.
