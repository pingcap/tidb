# `pkg/session/sessionapi` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains two tracked artifacts and 111 lines. Every production
source and Bazel target was read in full before comparing the Rust workspace.
There is no `doc.go`, test file, fixture directory, generated output,
benchmark, fuzz target, or platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 21 | `a6806e098295b64d92d6e6fa4a69472aed780292` | `443e508c2053d6e301ec57bcd1a4c131a107129b28721a4974bc30718c38f5af` | public session API library target and dependencies |
| `session.go` | 90 | `9df9b57da4dd3340b20a1530a8ba80376e77fc6a` | `9ec9d9e931d3424f0e487e6be2dbdb42700f9723b3fa48ed57a1bc2041c88ef5` | exported client-session interface and identity error |

`session.go` defines the `ErrIdentityNotFound` sentinel and one exported
`Session` interface embedding `sessionctx.Context` plus 34 explicit methods.
Those methods cover status and affected-row reporting, text/parsed/internal
execution, transaction boundaries, prepared statements, session-state and
protocol configuration, TLS/collation, authentication and identity matching,
transaction diagnostics, field listing, ports, and extension registration.
There are no function declarations, package-local tests, fixtures, generated
outputs, benchmarks, fuzz inputs, or platform variants. All interface
methods, the sentinel, and both build artifacts were checked individually.

## Rust ownership and explicit boundary

Rust has several adjacent owners: `tidb-session::Session` carries SQL,
transaction, variable, and prepared-state behavior; `tidb-server` owns
connection/session context, protocol configuration, authentication, and
field/result routing; and `tidb-exec`/`tidb-planner` provide execution and
result metadata. These concrete components do not expose one
dependency-closed public interface equivalent to Go's plugin-facing
`sessionapi.Session`, nor its full authentication, session-manager,
transaction-info, extension, and deprecated compatibility methods.

No Rust-only behavior was found to remove, and no safe missing behavior can
be added by defining a thin trait or adapter: doing so would either duplicate
the server's concrete session state or silently omit one of the embedded
`sessionctx.Context`, authentication, prepared-statement, or plugin contracts.
The existing Rust APIs remain native owners with their documented scope. This
complete Go API package is therefore recorded as an explicit SEED/boundary;
future parity requires a coordinated public-session and plugin integration
decision rather than a leaf-only port.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, Rust compilation gates, and the Ready lint gate were
not required for this batch.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 go test ./pkg/session/sessionapi -count=1)
# passed: pkg/session/sessionapi [no test files]
```

The package was compiled from an exact detached Go-master worktree. No Rust
code changed, so no Rust owner test was applicable. Not verified here: Bazel
execution, full Go repository tests, plugin ABI compatibility, or a future
dependency-closed Rust replacement for the public Go session API.

This receipt certifies the bounded `pkg/session/sessionapi` inventory and
ownership decision; it is not a repository-wide transcreation claim.
