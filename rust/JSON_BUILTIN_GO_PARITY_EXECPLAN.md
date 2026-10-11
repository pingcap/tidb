# Re-port the JSON builtins onto the BinaryJSON port of Go's pkg/types

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.


## Purpose / Big Picture

TiDB's JSON functions (`JSON_SET`, `JSON_EXTRACT`, `JSON_CONTAINS`, `JSON_OBJECTAGG`, ...) are implemented in Go in `pkg/expression/builtin_json.go` on top of `types.BinaryJSON` (`pkg/types/json_binary*.go`). The Rust port has two unconnected JSON stacks. `rust/crates/tidb-datatype` ports `pkg/types` faithfully (`BinaryJSON`, `JSONPathExpression`, `modify`, `array_insert`, `remove`, `extract`, `search`, merges). The builtins in `rust/crates/tidb-expr/src/builtin_ext/json/` do not use it: they convert every document to `serde_json::Value` (by printing it and re-parsing the text), run their own path parser (`path.rs`) and their own tree operations (`modify.rs`), and print the result back into a `BinaryJSON`.

`serde_json::Value` cannot represent TiDB's opaque, date, datetime, timestamp or duration JSON values, and the text round trip loses the error identity Go reports. After this change a user sees exactly Go's answers and error codes for the JSON functions. Observable today, captured from Go on this branch (`$SP/zz_b89_go.txt`):

    select json_set(1, '$', 2)                     Go: 2            Rust: error 3146
    select json_set(b, '$', 1) from t  (varbinary) Go: 1            Rust: error 3140
    select json_unquote(1)                         Go: error 3064   Rust: internal error
    select json_contains_path(1, 'one', '$.a')     Go: error 3146   Rust: 0
    select json_keys(b) from t         (varbinary) Go: error 3144   Rust: error 3140
    select json_contains('{}', 123)                Go: error 3146   Rust: 0
    select json_object(b, a) from t    (varbinary) Go: error 3144   Rust: {"x": 1}
    select cast(binary 'aa' as json)               Go: "base64:type254:YWE="  Rust: type253
    select json_objectagg('a', b'010101')          Go: {"a": "base64:type253:FQ=="}  Rust: {"a": "\u0015"}
    json_array_insert/json_set past depth 100      Go: error 3157   Rust: error 3140
    json_schema_valid on a 1000-deep document      Go: error 3157   Rust: error 3140

The integration topic `expression/json` (`tests/integrationtest/t/expression/json.test`) is the end-to-end check: today 9 of its 344 compared statements diverge.


## Progress

- [x] (2026-10-10) Exponential decode fixed separately (`decode_binary_entry` no longer re-validates each element); `expression/json` replays in 0.8s instead of never finishing.
- [x] (2026-10-10) Go oracle matrix captured for argument verification, binary charsets, opaque values, depth errors, path positions and evaluation-route error codes.
- [x] (2026-10-10) Milestone 1: `verify_json_function_args` (Go `verifyJSONArgsType` table plus `json_object`'s binary key) runs in the rewriter after the arguments are rewritten; the generic function path applies Go's `typeInferForNull` first.
- [x] (2026-10-10) Milestone 2: JSON_SET/INSERT/REPLACE/REMOVE/ARRAY_APPEND/ARRAY_INSERT on `BinaryJSON` with Go's document cast; path caches hold `tidb_datatype::JSONPathExpression`.
- [x] (2026-10-10) Milestone 3: 3157 for built and parsed documents; `json_scanner` (Go `encoding/json` scanner) decides deep-text validity and backs `JSON_VALID`.
- [x] (2026-10-10) Milestone 4: `BINARY expr` is `TypeString`; `JSON_ARRAYAGG`/`JSON_OBJECTAGG` follow `getRealJSONValue`; opaque JSON casts warn as Go (follows from the opaque value surviving `JSON_EXTRACT`).
- [x] (2026-10-10) Milestone 5, part 1: JSON_EXTRACT/TYPE/LENGTH/KEYS/DEPTH/STORAGE_SIZE/STORAGE_FREE/PRETTY on `BinaryJSON` (`document.rs`, Go `json.Indent` port for PRETTY); `tidb-expr`'s own path parser deleted, `parse_path` delegates to the `tidb-datatype` parser, which now reproduces Go's error positions; `text.rs` deleted.
- [x] (2026-10-10) Path errors carry 3143 at the root and 1105 when the function runs in a coprocessor projection or predicate (`ProjectionExec::in_coprocessor`).
- [ ] Milestone 5, part 2: JSON_CONTAINS/CONTAINS_PATH/OVERLAPS/MEMBER OF, JSON_SEARCH, JSON_MERGE*, JSON_QUOTE/UNQUOTE, JSON_SCHEMA_VALID and JSON_SUM_CRC32 still walk the serde tree (parsed through `BinaryJSON`); move them and delete the serde tree code.
- [ ] Remove the rewriter's build-time constant-path check (`rewriter.rs`, the block after `verify_json_function_args`): Go never refuses a malformed constant path while planning.
- [ ] Follow-up outside this module: Go's build-time argument casts (`WrapWithCastAsJSON`, `isnull`'s JSON-to-string cast) are visible in EXPLAIN (`json_extract(cast(test.t.c, json BINARY), $[)`, `isnull(cast(json_extract(..), var_string(16777216)))`); the Rust expression layer still applies them per value at evaluation.

## Surprises & Discoveries

- Observation: decoding a nested `BinaryJSON` element validated it by decoding its whole subtree and then decoded it again, doubling per level.
  Evidence: `length(cast('<20-deep>' as json))` took 680ms, 18-deep 178ms; fixed in `rust/crates/tidb-datatype/src/binary_json.rs::decode_binary_entry`.
- Observation: the replay harness counts a statement both sides reject as `BothRejected` without comparing error codes, so wrong codes (3140 for 3157) do not show up as divergences.
  Evidence: `rust/difftests/result-tests/tests/integration_diff.rs`, the `(Err(_), true)` arm.
- Observation: Go verifies argument TYPES only for 13 function classes; `JSON_SET`, `JSON_INSERT`, `JSON_REPLACE`, `JSON_REMOVE`, `JSON_LENGTH`, `JSON_DEPTH`, `JSON_VALID` accept an integer document (it is cast to a JSON number), while the Rust rewriter refused the modify family.
  Evidence: `select json_set(1, '$', 2)` answers 2 on Go.


- Observation: Go's path error positions come from its stream's rewinds (`tryReadIndexNumber`, `tryParseArrayIndex`, `tryReadString`) and from `read()` consuming the rune it compares; neither Rust parser matched (`$[1` is 2 in Go, 4 and 3 in the two Rust parsers).
  Evidence: 20 malformed paths answered by Go, now `json_path::go_position_tests`.
- Observation: the executor mapped every evaluation-time path error to 1105, matching only the coprocessor route; a root-evaluated function answers 3143 in Go.
  Evidence: `select json_extract(a, '$[') from t` (JSON column, pushed projection) 1105; `json_extract(c, '$[')` (varchar, root) 3143.
- Observation: the AST rewriter never applied Go's `typeInferForNull`, so a NULL argument kept `TypeNull` where Go gives it the last typed argument's type.
  Evidence: `json_schema_valid(NULL, 1 / 0)` names argument 1 in Go.

- Observation: Go evaluates a malformed constant path only when the function runs: `EXPLAIN select json_extract('[1,2,3]', '$[1')` succeeds, the same call over an empty table returns no rows, and an evaluated one fails with 3143.
  Evidence: Go oracle on this branch, 2026-10-10.

## Decision Log

- Decision: re-port the evaluation onto `tidb_datatype::BinaryJSON` and `tidb_datatype::json_path` rather than teaching the serde tree about opaque values.
  Rationale: Go's builtins call `pkg/types`; the Rust `pkg/types` port already exists and is tested against Go vectors (`rust/crates/tidb-datatype/tests/json_ops_go_source.rs`). Two stacks keep drifting.
  Date/Author: 2026-10-10 / agent.
- Decision: argument verification runs in the AST rewriter right after the arguments are rewritten (where their static types are known), replacing the literal-only checks before it.
  Rationale: Go's `verifyJSONArgsType` reads `arg.GetType()`; the literal checks could not see column or expression types and named the wrong functions.
  Date/Author: 2026-10-10 / agent.


- Decision: apply `typeInferForNull` in the rewriter's generic function path rather than only to a copy of the arguments for the JSON check.
  Rationale: Go applies it to every function's arguments before `getFunction`; the argument types it fixes are the ones the function then evaluates with.
  Date/Author: 2026-10-10 / agent.
- Decision: a projection in a reader's coprocessor plan wraps its evaluation errors as coprocessor errors and runs without TiDB projection workers.
  Rationale: Go runs that projection in TiKV; the pushed predicate path already did the same.
  Date/Author: 2026-10-10 / agent.

## Outcomes & Retrospective

(2026-10-10) Milestones 1-4 and the first part of milestone 5 landed in one batch; every statement captured from the Go oracle for this batch answers as Go. The remaining families keep a serde tree but parse and print through `BinaryJSON`, so they see the same validity, depth and error identity.


## Context and Orientation

Go sources (this branch): `pkg/expression/builtin_json.go` (function classes and `evalJSON` bodies), `pkg/expression/builtin_cast.go` (`builtinCastStringAsJSONSig`), `pkg/types/json_binary.go` and `pkg/types/json_binary_functions.go` (`Modify`, `ArrayInsert`, `Remove`, depth check `GetElemDepth()-1 > maxJSONDepth`), `pkg/executor/aggfuncs/func_json_objectagg.go` (`getRealJSONValue`), `pkg/parser/expr_prefix_parser.go` (`BINARY expr` is a cast to `TypeString`).

Rust sources: `rust/crates/tidb-expr/src/builtin_ext/json/` (builtins), `rust/crates/tidb-expr/src/rewriter.rs` (function-call rewrite; the literal JSON argument checks sit after `verify_args_by_count`), `rust/crates/tidb-expr/src/scalar_function.rs` (calls `json_dispatch_typed`), `rust/crates/tidb-datatype/src/binary_json.rs`, `binary_json_ops.rs`, `json_path.rs` (the `pkg/types` port), `rust/crates/tidb-executor/src/hash_agg.rs` (`json_value` for `JSON_OBJECTAGG`), `rust/crates/tidb-expr/src/rewriter/result_type.rs` (`cast_target`).

Terms. A document argument is a JSON function argument Go wraps with `WrapWithCastAsJSON` and leaves `ParseToJSONFlag` on, so a non-binary string is parsed as JSON text. A value argument has the flag disabled (`DisableParseJSONFlag4Expr`), so a string becomes a JSON string. A binary string (binary charset) always becomes an opaque JSON value carrying its MySQL type code. Error codes: 3140 invalid JSON text, 3143 invalid path, 3144 binary charset, 3146 invalid type for JSON, 3064 incorrect type (JSON_UNQUOTE), 3157 document too deep.


## Plan of Work

Milestone 1 ports `verifyJSONArgsType(ctx, funcName, useJSONErr, args, idx...)` as a function in `rust/crates/tidb-expr/src/builtin_ext/json/` and calls it from the rewriter's generic function path after the arguments are rewritten, with Go's table: json_type(0), json_extract(0), json_unquote(0, ErrIncorrectType), json_merge/json_merge_preserve/json_merge_patch(all), json_contains_path(0), member of(1), json_contains(0,1), json_overlaps(0,1), json_search(0), json_keys(0), json_schema_valid(0,1). A NULL-typed argument passes; a string argument with binary charset fails with 3144; JSON and other strings pass; anything else fails with 3146 (3064 for json_unquote). `json_object` refuses a binary-charset key with 3144. The literal checks for json_set/insert/replace/remove are deleted; the constant-path check moves after the type check so Go's error order holds.

Milestone 2 converts document arguments with Go's cast semantics into `BinaryJSON` and evaluates JSON_SET/INSERT/REPLACE/REMOVE/ARRAY_INSERT/ARRAY_APPEND with `BinaryJSON::modify`, `array_insert`, `remove`, parsing paths with `tidb_datatype::parse_json_path_expr`.

Milestone 3 maps `BinaryJSONError::TooDeep` to 3157 everywhere a document is built, and makes `BinaryJSON::parse` report 3157 for valid text deeper than serde's recursion limit (Go decodes it, then refuses its depth).

Milestone 4 fixes the three value-domain gaps listed in Purpose.

Milestone 5 moves the remaining builtins and deletes the dead serde code.


## Concrete Steps

From `rust/`:

    cargo test -p tidb-expr --lib builtin_ext::json
    cargo test -p tidb-session --lib tests_json
    cargo test -p difftest-result-tests --test integration_diff --no-run
    INTEGRATION_SHOW_DIVERGENCES=1 INTEGRATION_TOPIC=expression/json <integration_diff binary> --ignored --nocapture replay_one_topic

Expect the divergence count to fall milestone by milestone, ending at 0 for the rows listed in Purpose.


## Validation and Acceptance

Each milestone adds tests built from the Go oracle captures (Go worktree probe, `pkg/executor/test/writetest/zz_*_test.go` run with `go test -tags=intest,deadlock`). The repository gate (planner, executor, session, expr, datatype nextest plus the integration corpus) must stay at its known failures. `expression/json` reaches 0 divergences once all milestones land.


## Idempotence and Recovery

Each milestone is a separate commit; reverting one restores the previous behavior. No data formats change: `BinaryJSON` bytes are already Go's.


## Artifacts and Notes

Go oracle matrix: 46 statements, captured 2026-10-10, recorded in the Purpose table and in the milestone tests.


## Interfaces and Dependencies

`tidb_datatype::{BinaryJSON, BinaryJSONError, JSONModifyType, JSONPathExpression, parse_json_path_expr}` are the interfaces the builtins call. `tidb_expr::context::JsonError` keeps the wire error codes.
