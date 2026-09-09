# `pkg/sessionctx/slowlogrule` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains two tracked artifacts and 73 lines. The complete
inventory includes the public BUILD library target and every condition/rule,
session wrapper, global wrapper, constructor, field, and comment in
`rules.go`. It has no Go tests, fixtures or `testdata`, generated output,
platform-specific variant, benchmark, fuzz target, or generator input.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 8 | `684bdfbac02c9741d03252c3141e930f631f8523` | `50da5328ba016fc46b5a486f6708008a8ffb0747009fdaa859fd28fce760412e` | public Go library target |
| `rules.go` | 65 | `40790ede5ac83f4f9faec0ffadc50b6be3540eb0` | `e9c802841ee2beb055ff8d193ce7a8a48d3e5731db57e4ca9f8b3ff387d017bd` | `SlowLogCondition`, `SlowLogRule`, `SlowLogRules`, session/global wrappers, and constructor |

The source declares four data types and one function:
`SlowLogCondition` stores a field and typed threshold;
`SlowLogRule` groups conditions with logical AND; `SlowLogRules` stores raw
text, a unique field map, and rules with logical OR; `SessionSlowLogRules`
embeds a rule pointer and tracks effective fields/global hash/invalidation;
`GlobalSlowLogRules` stores raw text/hash and a connection-ID→rule-pointer map;
and `NewSessionSlowLogRules` initializes the session wrapper with an empty
effective-field map and `NeedUpdateEffectiveFields=true`.

The Go master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is empty for both artifacts.

## Rust ownership and explicit boundary

Rust's `tidb-exec::slow_log_rules` owns equivalent typed condition/rule,
session-effective, and global connection-index metadata, while
`tidb-exec::slow_log_parse` implements the parser/encoder and CRC64 hash
behavior consumed by the variable layer. The Rust representation uses owned
values and ordered sets/maps rather than Go pointers/maps; no live Rust
SessionVars integration currently depends on pointer identity, and the
source package itself defines no mutation methods. Existing executable
metadata/parser tests cover the preserved contracts. The remaining boundary
is wiring these values into a dependency-closed session slow-log evaluator;
that belongs to the larger `pkg/sessionctx/variable` package, not this
two-artifact data package.

No Rust-only behavior was found to remove, and no safe package-local behavior
is missing from the data-model surface. Adding pointer emulation or a second
evaluator here would be speculative and would duplicate the variable/session
owner.

## Validation and risk

Profile: **WIP** for this documentation-only boundary audit; no production,
test, or Bazel file changed, so no regression test or package-complete Ready
claim is made.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/sessionctx/slowlogrule -count=1                         # passed (no test files)
```

The exact detached Go-master worktree was used. Rust source, Bazel, and
module files were unchanged; `make bazel_prepare` and Ready lint were not
required. Not verified: the larger variable-package slow-log integration,
live session evaluator, or external slow-log consumers. Compatibility and
performance risk are unchanged because this batch modifies documentation
only.

This receipt certifies the bounded slowlogrule data-package inventory and
explicit ownership boundary; it is not a repository-wide parity claim.
