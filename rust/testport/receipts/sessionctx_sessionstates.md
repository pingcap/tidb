# `pkg/sessionctx/sessionstates` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains five tracked artifacts and 2,578 lines. Every session
state/token production declaration, eighteen test functions, binary-protocol
helpers, certificate fixtures generated in temporary directories, failpoint
hooks, and eighteen-shard flaky Bazel target was read before this receipt was
written. There is no checked-in fixture or `testdata` directory, generated
output, platform-specific variant, benchmark, fuzz target, or generator input.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 55 | `0e2bc8e98db9d5f6e2d4fe3f79259a6366575686` | `017c6166ad8e859b037c8443d1daaaec9c4a4e36017eae2648503f5b8172a474` | public library/test targets, eighteen-shard flaky harness, and dependency closure |
| `session_states.go` | 90 | `3ebebbddd330fda21c5c3b2c721b86088a6d1a33` | `e8e6833fce91fbe68583254d49c87b56efc97e99411bcdab6a743cc10e66b8f2` | migratable session-state types and `ErrCannotMigrateSession` |
| `session_states_test.go` | 1,762 | `0180da72a13b9e142baaf52122081ecacc83e6d3` | `6b2d7b30b167e1528346fa18a8f955b7e68bf3cbffed90a0d6e5635cd9034f61` | twelve session-state, prepared-protocol, binding, and migration tests plus wire helpers |
| `session_token.go` | 366 | `bdadcbed431d06a1fac50fcc5e10aca7645e05ce` | `57a25600b7406d68bf388baa8091237ab57563c302e88fff24bc81370e03d9fd` | token timing, certificate rotation, RSA/ECDSA/Ed25519 signing, verification, and failpoint clock |
| `session_token_test.go` | 305 | `c3a5c0af59de6defb6d1024bbda775bd28de66cb` | `bc552cc604dd32f81825ef0140a2345669af11ffcf8c66ace94d59e423f973b1` | six token/certificate tests and temporary-certificate helpers |

`session_states_test.go` declares `TestGrammar`, `TestUserVars`,
`TestSystemVars`, `TestInvisibleVars`, `TestIssue47665`, `TestSessionCtx`,
`TestStatementCtx`, `TestPreparedStatements`, `TestSQLBinding`,
`TestSQLBindingCompatibility`, `TestShowStateFail`, and `TestInvalidSysVar`,
plus state serialization, error, and COM_STMT wire helpers. The tests cover
session-state JSON round trips, scoped/hidden/no-op variables, prepared text
and binary protocols, SQL bindings, transaction/warning/cursor/lock
migration guards, SEM/starter security gates, and invalid-variable handling.
`session_token_test.go` declares `TestSetCertAndKey`, `TestSignAlgo`,
`TestVerifyToken`, `TestStarterSessionTokenLifetime`, `TestCertExpire`, and
`TestLoadAndReadConcurrently`, plus certificate/token helpers; these cover
missing/mismatched keys, RSA/ECDSA/Ed25519 algorithms, expiry/username and
forgery checks, Starter timing, certificate grace rotation, and concurrent
load/sign/verify.

The Go master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is empty for all five artifacts.

## Rust ownership and explicit boundary

Rust has session-state fields threaded through selected session/executor
contexts and a `tidb-exec::session_token_timing` owner for the classic and
Starter timing constants. It does not yet own Go's dependency-closed
`SessionStates` serializer/restorer, Domain/TestKit migration protocol,
prepared-statement and SQL-binding transfer, certificate loading/rotation,
RSA/ECDSA/Ed25519 signing, token verification, or failpoint clock seam. The
Go package's temporary certificate generation is test support, not a checked-in
fixture. Existing Rust timing tests preserve only the constant-level contract.

No Rust-only behavior was found to remove, and no safe package-local
implementation can be added without duplicating session/server authentication,
TLS, prepared-protocol, and migration ownership. The ignored source carriers
remain explicit evidence of these gaps.

## Validation and risk

Profile: **WIP** for this documentation-only boundary audit; no production,
test, or Bazel file changed, so no regression test or package-complete Ready
claim is made.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/sessionctx/sessionstates \
  -count=1                                                          # PASS 29.543s
```

The command ran from the exact detached Go-master worktree; the failpoint
wrapper enabled and disabled failpoints around the complete suite. Rust source,
Bazel, and module files were unchanged; `make bazel_prepare` and Ready lint
were not required. Not verified: all eighteen Bazel shards, live TLS rotation,
external session migration, or full repository tests. Correctness risk is
concentrated in the untranscreated session-state wire format and
cryptographic/authentication lifecycle; runtime behavior is unchanged because
this batch modifies documentation only.

This receipt certifies the bounded sessionstates package inventory and explicit
ownership boundary; it is not a repository-wide parity claim.
