# `pkg/session/test/privileges` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package is test-only: three tracked artifacts and 138 lines. Every test
source and BUILD target was read in full before comparing Rust. There is no
production source, `doc.go`, fixture directory, generated output, benchmark,
fuzz target, or platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 23 | `06de6c79970723deb98cfe02a09172544b5a1230` | `569bfc45315ff19eafa097a7b2f64bc7aa3d9329648d03a28317c6366c3a5157` | two-shard flaky test target |
| `main_test.go` | 62 | `53f54cf3747fb72bb951bfe24a35103d38551781` | `262359995ad943b081cecf6872135423a0c5ac8cd3ee633b306367beed6eee48` | common setup, failpoint enablement, and goleak harness |
| `privileges_test.go` | 53 | `77d62dd8cba2f0b792d74f893e65e0f10806d8d8` | `cb43c4aff17fc8924dc33aa59da91d7dd9e6c6a0669c0e9375494fa3fe1a597b` | `SkipWithGrant` and unknown-user authentication tests |

The test surface defines three functions and two top-level tests. `TestMain`
configures common mock-store state, TiKV failpoints, async-commit safety, and
the goleak allowlist. `TestSkipWithGrant` checks denied authentication when
the global bypass is off, unrestricted authentication and role DDL/grants
when it is on, and restores the global. `TestSessionAuth` checks that an
unknown user with an empty password is rejected.

## Rust ownership and explicit boundary

Rust's `tidb-session::privilege` and `tidb-server::configured_user_store`
provide account rows, password verification, privilege checks, and a
session-level `enable_privilege_bypass` equivalent to Go's
`privileges.SkipWithGrant`. Executable Rust coverage exists in
`tidb-server::cluster_session_node::tests::accounts` for bypassed account
DDL and in `tidb-session::tests_deadlock_history` for bypassed privilege
checks. The source-carrier entries for these two Go tests remain ignored
because the exact Go TestKit/Auth wiring and process-global variable are not
a standalone session-test package.

No Rust-only behavior was found to remove, and no safe missing behavior can
be added in this test-only package. Replacing the ignored carriers with a
second miniature authentication harness would duplicate the server's
configured-store authority and risk diverging on host matching, password
plugins, role activation, and bypass scope. This package is therefore an
explicit SEED/boundary; its production owners are already covered by their
own focused Rust tests and receipts.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, the Ready lint gate, and a new regression test were not
required for this batch.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 ./tools/check/failpoint-go-test.sh ./pkg/session/test/privileges -count=1)
# passed: pkg/session/test/privileges (4.220s); failpoints enabled and disabled
```

The package was tested from an exact detached Go-master worktree. Not
verified here: Bazel execution, full Go repository tests, external LDAP/JWT
authentication, or the complete server/session integration under a live
network connection. Compatibility and performance risk are unchanged because
this batch modified documentation only.

This receipt certifies the bounded test-package inventory and ownership
decision; it is not a repository-wide transcreation claim.
