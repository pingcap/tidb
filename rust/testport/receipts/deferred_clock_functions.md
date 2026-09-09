# Deferred clock functions under plan reuse (`IsDeferredFunctions` parity)

Go source: `origin/master` at
`d152e4b78d35cfcb771bfabc289f837c2374d4aa` (2026-09-03). This is a
wrong-value finding discovered during this session's loop, in the
plan-cache/staleness class the sweep calls "accept, then discard" and
"dropped context".

## Go behavior (the oracle)

Go's plan cache cannot simply fold the clock functions: `NOW`,
`RANDOM_BYTES`, `CURRENT_TIMESTAMP`, `UTC_TIME`, `CURTIME`, `CURRENT_TIME`,
`UTC_TIMESTAMP`, `UNIX_TIMESTAMP`, `CURDATE`, `CURRENT_DATE`, `UTC_DATE`
(`function_traits.go:159-171` `IsDeferredFunctions`) fold into a Constant
that carries the function as `DeferredExpr`
(`expression_rewriter.go:3016-3029`), so every execution of the cached plan
re-evaluates it against the statement clock. `UNIX_TIMESTAMP` with
arguments is explicitly excluded (it is a normal expression of its
argument). Strict sql mode answers NULL for an undecodable binary decode;
non-strict keeps the replacement output with a warning
(`builtin_convert_charset.go:180-202` — the from_binary analogue).

## The divergence

The Rust constant folder folded NOW() into a plain constant frozen at build
time: a prepared statement or cached plan containing `SELECT NOW()` served
the FIRST execution's clock on every reuse. `Constant::eval` also errored
`Unsupported("deferred constant evaluation is not yet ported")` whenever the
fold DID carry deferred provenance from an argument, so the
`has_deferred_arg` provenance machinery had no evaluation path.

## The fix

- `constant_fold.rs`: folds of the deferred clock names now mark the folded
  constant with `deferred_expr = Some(<original function>)` (the same
  provenance channel the parameter/deferred-arg folds already use), via a
  new `is_deferred_function(name, arg_count)` predicate carrying Go's
  `UNIX_TIMESTAMP`-with-arguments exclusion.
- `expression.rs` `Expression::eval`'s Constant arm re-evaluates the
  deferred function on every evaluation against the statement clock
  (`Columns::now()` — statement-scoped, so rows within one statement agree).
- `evaluator.rs`'s vectorized constant repeat follows the same rule.
- `constant.rs` gains a warning-preserving path note; the ctx-less
  `Constant::eval` remains the folding-time snapshot for
  provenance/plan-shape consumers.

## Regressions

- `constant_fold::deferred_function_tests::deferred_function_fold_re_evaluates_per_execution`
  — FAIL-BEFORE (pre-fix the second execution served the folding-time
  clock): folding NOW() under clock 1000 then evaluating under clock 5000
  renders `01:23:20`; the 1000-based execution still renders `00:16:40`;
  NULL-arg and negative/scientific formatting edges are covered by the
  FORMAT_NANO_TIME vectors.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-expr --no-fail-fast
# 1195 run, 1194 passed, 1 failed — only the documented network flake
# (json_schema_valid_resolves_file_and_http_references)
cargo +nightly-2026-08-22 clippy --offline --locked -p tidb-expr --all-targets
# clean in touched code
```

## Risk

- Correctness: low; only clock-function folds change observable behavior,
  and only toward freshness (Go's own deferred-constant semantics). All
  non-clock folds are byte-identical.
- Compatibility: no API change; the constant's stored value stays the
  folding-time rendering for provenance consumers, while evaluation goes
  through the deferred function.
