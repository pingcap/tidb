# The warning channel: three stacks, not one fragmented channel

Measured at `e48b552bd0`. This is a measurement record, not a refactor plan.

## Verdict: DO NOT COLLAPSE

The nine warning emission sites are **not nine doors onto one channel**. They
are three independent stacks plus one dead decision point. Only one stack
reaches a client. Unifying them would fuse a live accumulator with two
bounded-tier ports, which is the mistake `#122` avoided with the five
statement classifiers.

What the measurement *did* find is worse than fragmentation: two of the three
stacks emit into buffers that **nothing on the live path drains**, and the one
piece Go actually centralizes — the error-vs-warning decision — is the piece
with zero production callers.

## Stack A — LIVE. The only route to a client.

| Site | Role |
| --- | --- |
| `tidb-executor/src/stmt_context.rs:40,562,661,757` | per-statement **evaluation accumulator**, `Rc<RefCell<Vec<(u16, String)>>>` |
| `tidb-session/src/warnings.rs:142` `drain_eval_warnings` | **transfer**, executor accumulator -> session buffer |
| `tidb-session/src/warnings.rs:98` `warning_output` | **publisher**, renders `SHOW WARNINGS` / `SHOW ERRORS` / `SHOW COUNT(*) WARNINGS` |
| `tidb-session/src/variables.rs:380` `warn_truncated_var` | direct push into the session buffer |
| `tidb-session/src/variables.rs:314` `warn_removed_feature_var` | direct push into the session buffer |

`drain_eval_warnings` is called from six dispatch arms
(`tidb-session/src/dispatch.rs:263,372,385,395,429,443`) and
`explain_arm.rs:119`.

The two `variables.rs` sites bypass the executor accumulator because the sysvar
assignment path never builds a `StmtContext`. That is not a fragmentation bug;
it is the correct shape for a statement that has no evaluation phase. Go does
the same thing — `SetSessionSystemVar` reaches `StmtCtx.AppendWarning` directly,
not through a type-conversion context.

## Stack B — BOUNDED. `tidb-exec`, source-faithful `WarnHandler` port.

| Site | Role |
| --- | --- |
| `tidb-exec/src/warning_publication.rs:53` `StaticWarningHandler` | source `StaticWarnHandler`: mutable sink with the `MaxUint16` cap |
| `tidb-exec/src/warning_publication.rs:175` `IgnoreWarnings` | source `ignoreWarn`: no-op sink |
| `tidb-exec/src/warning_publication.rs:286` `WarningPublication` | **read-only borrowed view**, no storage of its own |
| `tidb-exec/src/statement_status.rs:114,262` | per-statement **status owner**; warnings are one field beside affected-rows and counters |

`tidb-session/Cargo.toml` does **not** depend on `tidb-exec`. This stack is
reachable only from `tidb-exec`, its own tests, and the `ReadOnlyScanPlan`
bounded proof tier documented in `read-tier-boundary.md`.

## Stack C — BOUNDED, and unwired past its own crate.

| Site | Role |
| --- | --- |
| `tidb-distsql/src/warning.rs:70` `WarningCollector` | `Arc<Mutex<Vec<Warning>>>` sink shared across `Detach` |
| `tidb-distsql/src/context.rs:89,184` | `DistSQLContext.warning_handler`, the field that survives detach |
| `tidb-distsql/src/response_channel.rs:794,905` | **transport decoder**: TiKV `SelectResponse` error values -> `Warning` |

`tidb-executor` depends on `tidb-distsql`, but every import is a range helper
(`handle_range.rs:82`, `kv_table/table_scan.rs:216`, `remote_scan.rs:272`
comment only). The live executor never constructs a `DistSQLContext` and never
reads a `WarningCollector`.

**Consequence:** a warning TiKV reports in a coprocessor `SelectResponse` on the
live `cluster_session` read path has nowhere to go. `response_channel.rs`
decodes it correctly into a buffer that the live path does not own.

## The dead decision point — `tidb-datatype` `ConversionContext`

`ConversionContext::handle_truncate` (`truncate.rs:77`) is the Rust image of
Go's error-vs-warning decision: `TypeFlags.TruncateAsWarning` deciding whether a
truncation is returned as an error or diverted to the `WarnAppender`. It is the
single most load-bearing piece of Go's design — the reason one channel works in
Go is that the *decision*, not just the storage, is centralized.

Production callers of `handle_truncate`: **one**, `binary_literal.rs:149`, inside
`BinaryLiteral::to_int_with_context`.

Production callers of `to_int_with_context`: **zero**. Every caller is a test
(`tidb-datatype/tests/value_context_format_source.rs:61`,
`tests/conversion_context_source.rs:137,153,162`).

Production implementors of `ConversionWarningAppender`: `StaticWarningHandler`
and `IgnoreWarnings` in the bounded Stack B, plus the no-op
`IgnoreConversionWarnings`. No live-path type implements it.

So the decision point is wired only to a bounded sink, and reached only by
tests. This is the same shape as the seven earlier "present but unwired" cases,
including `Validated{truncated}` — the precursor that forced
`warn_truncated_var` into existence.

## What a correct unification would be, and why it is not this unit

Collapsing the sinks is the wrong axis. The sinks have genuinely different
roles — accumulator, publisher, borrowed view, transport decoder, detach-shared
`Arc` — and they live in crates with no dependency edge between them.

The axis that would actually pay is the one Go centralizes: give the live path
(Stack A) the `TruncateAsWarning`-style decision, so a conversion that is an
error under strict mode and a warning otherwise is decided in one place rather
than at each call site. That is a **behaviour-bearing** change and belongs in
its own unit with a capture, not in a refactor commit.
