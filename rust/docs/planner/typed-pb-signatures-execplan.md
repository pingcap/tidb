# Preserve Go protobuf builtin signatures

This living ExecPlan follows PLANS.md. Work extends the existing pkg/expression
translation; it does not claim that the whole Go package has been transcreated.

## Purpose / Big Picture


A pushed expression must execute the builtin selected by its protobuf signature,
not select another overload from a SQL name. Preserve the wire result type and
request settings so root and coprocessor execution agree.

## Progress


- [x] Inspect Go master 8936d7bdcb13a4fc767de42489aace2711c2c6fd and the existing
  Rust decoder, ScalarFunction and coprocessor context.
- [x] Add failing tests for signature identity and preserved wire metadata.
- [x] Implement typed protobuf builtin selection and shared evaluation kernels.
- [x] Preserve explicitly supplied zero division precision.
- [x] Run scoped regression, integration and lint gates; review and prepare
  the checkpoint for publication. Git history records the publication.

## Context and Orientation


Go pkg/expression/distsql_builtin.go selects concrete builtin implementations
with getSignatureByPB, retains the signature with setPbCode, and puts the
implementation into ScalarFunction.Function. Rust currently reverses the encoder
catalog into SQL names and uses the SQL cast builder during protobuf decoding.
That loses overload identity and can rewrite return metadata or argument trees.
The owning Rust crate is rust/crates/tidb-expr; tidb-unistore consumes its decoder.

## Plan of Work


First prove the failures at the decoder boundary. Then give ScalarFunction a
validated protobuf implementation whose enum signature selects typed evaluation
kernels directly. Reuse existing arithmetic, cast, string, math, temporal and
JSON implementations rather than create a second scalar interpreter. The encoder
catalog remains admission policy; decoding has an explicit implementation match.
Test that every admitted signature and implicit argument cast has a decoder.
Keep legacy SQL-name construction for existing root expressions outside this
checkpoint, with no protobuf fallback through that construction.

The second milestone threads exact request division precision through both
existing request fields: absence defaults to four, while zero remains zero.
Finally run affected tests and the repository lint gate, record any failed or
unavailable validation without claiming package completion, and commit/push to
hparser-integration after fetching concurrent changes.

## Validation and Acceptance


From repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib distsql_builtin -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib cophandler -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --lib unistore_cop -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --lib pushed_ -- --test-threads=1
    cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --all-targets
    GOTOOLCHAIN=go1.25.12 make lint
    git diff --check

The protobuf tests must fail before the fix, then pass afterward. Changing a
function's display name must not change a decoded signature's execution. Distinct
binary/UTF-8 overloads must remain distinct. Wire metadata must survive decoding
without invoking the SQL rewriter. Go reference sources are available in the
pinned archive /private/tmp/tidb-go-structural-20260927.

## Decision Log


- Choose typed protobuf implementations, not a signature-to-name adapter: the
  latter leaves runtime overload selection unchanged. 2026-09-27.
- Rust-only changes do not trigger bazel_prepare. Whole-package inventories,
  unported variants and benchmark gates remain outside any completion claim.

## Surprises & Discoveries


The prior checkpoint still defaults explicit precision zero to four. Go checks
presence only. Existing expression unit tests also fail to compile because seven
call sites omit the new Columns parameter; repair those test call sites to make
validation executable, preserving their original expected values.

## Idempotence and Recovery


Preserve concurrent changes. Never force push. Keep fail-before evidence in
local logs and restore any temporary baseline changes before rebuilding. No
production Go source or generated protobuf outputs should be edited.

## Outcomes & Retrospective


Implemented a validated PbBuiltin on ScalarFunction. getSignatureByPB's role is
an explicit Rust enum match selecting typed kernels or function pointers. The
protobuf path no longer reverses the encoder catalog into SQL names and no
longer invokes the SQL cast builder. ScalarFunction preserves the selected
implementation across Clone, and eval dispatch ignores its display name.
Binary/UTF-8 string overloads and MOD signedness are retained. Shared regexp
compilation and JSON constant-path caching remain attached to the function.
Absent division precision defaults to four; explicit zero remains zero.

The signature-identity and binary-length tests failed on the old decoder; the
zero-precision test failed with 4 versus 0 before the request fix. Final targeted
results are seven decoder tests and 64 coprocessor unit tests passing. The three
cluster pushdown tests passed. All-target server checking and make lint passed.

The broader expression suite ran 1225 passing tests, four failing and 93 ignored
(before adding the final JSON reuse test, which passed in the targeted suite).
A baseline run restored all production files to HEAD, retaining only the seven
missing test-context arguments: 1219 passed with the same four failures and
93 ignored. The failures are:

- pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation
- tests::builtin_info_json_math_source::exp
- tests::builtin_math_misc_op_source::vectorized_builtin_op_func
- time_fn::tests::str_to_date_partial_formats_follow_no_zero_date

The full cluster coprocessor suite again ran 112 passing and the same three
previously recorded failures: cluster_info_reports_this_node,
stats_notifier_uses_a_real_internal_transaction_like_go, and
unchanged_updates_lock_only_matched_rows. These are unresolved, not parity wins.
The JSON schema HTTP test initially failed under the filesystem/network sandbox;
it passed with local HTTP access on the baseline and final broader runs.

Go's matching signature-identity and binary-length assertions passed using an
external test overlay in the pinned archive. From that archive:

    GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/typed-pb-overlay.json' ./tools/check/failpoint-go-test.sh pkg/expression -run '^TestRustTypedPBSignatures$' -count=1

Failpoint use was detected in the package, so the wrapper enabled and disabled
it; final refcount was zero. Its git diagnostics reflect an archive without .git.
The Go test modifies UpperUTF8's FuncName to lower and still expects ABC, then
checks CharLength versus CharLengthUTF8 for é (two bytes versus one character).

Remaining scope: SQL-built functions still use the existing name-based builder
and evaluation dispatch;
unported protobuf signatures and the legacy coprocessor evaluator outside the
admitted catalog remain. No claim covers the complete upstream expression or
cophandler packages, their generated/platform variants, or all original tests.
Per-expression MutRow materialization remains; no sysbench/TPC-C/TPC-H/YCSB
performance claim is made.

## Changed Files and Interfaces


The decoder and its boundary tests are in
rust/crates/tidb-expr/src/distsql_builtin.rs. ScalarFunction's selected builtin
and shared regexp entrypoint are in scalar_function.rs, with the enum dispatch
and cast signature matrix in scalar_function/pb_builtin.rs. The existing math,
time and JSON kernels are reused through crate-visible entrypoints in
math_fn/mod.rs, time_fn/mod.rs, time_fn/session_tz.rs and builtin_ext/json/mod.rs.
Only missing context arguments were repaired in builtin_ext/string2.rs and
time_fn/tests.rs. Request precision and decoder admission changed in
rust/crates/tidb-unistore/src/cophandler.rs; its regression is in
cophandler/eval_context.rs. This plan records the design, evidence and limits.

Revision note: 2026-09-27, replaced the proposed name adapter with retained typed
implementations, recorded verified baseline failures, and preserved the existing
JSON and regexp caches rather than introducing per-row recompilation.
