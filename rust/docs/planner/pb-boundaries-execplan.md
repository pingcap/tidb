# Follow Go at protobuf expression boundaries

This living plan follows PLANS.md. It extends the pkg/expression and unistore
ports without claiming completion of either upstream package.

## Purpose / Big Picture


Pushed timestamp constants must represent the same instant across timezones.
Literal admission must match Go, and strict evaluation failures must remain
errors rather than becoming SQL NULL when a legacy parent consumes a shared
expression.

## Progress


- [x] Reproduce timestamp, JSON admission and swallowed-child-error failures.
- [x] Pull integration branch e3ff2f4dd7 and compare current Go master 46df29c960.
- [x] Thread timezone through literal encoding and shared decoding.
- [x] Align literal admission and decoding with Go.
- [x] Preserve errors across the legacy typed evaluation boundary.
- [x] Run regressions, affected integration tests, lint and self-review.
- [x] Prepare the verified checkpoint for commit/push; Git history records publication.

## Context and Orientation


The encoder is rust/crates/tidb-expr/src/pushdown_catalog.rs. Its consumers are
in tidb-exec. distsql_builtin.rs decodes protobuf into shared expression trees.
tidb-unistore/src/cophandler.rs retains a legacy evaluator for signatures not yet
ported; its six nullable typed helpers currently discard errors with .ok().
Go pkg/expression/expr_to_pb.go and distsql_builtin.go are the source of truth.

## Plan of Work


First retain failing tests at each boundary. Require an explicit timezone for
production encoder and decoder calls, recursively preserving it. Use the existing
codec EncodeMySQLTime port. Follow Go literal admission, including rejecting JSON
constants while retaining codec-framed JSON decoding. Then propagate typed errors
through legacy callers without eagerly evaluating unused branches. Finish with
scoped tests and the repository lint gate. Do not add signature-name fallbacks.

## Decision Log


- Use explicit context and fallible interfaces rather than function-specific
  patches or a side-channel error slot. These preserve ownership and laziness.
  2026-09-28.
- Rust-only changes do not require bazel_prepare. Existing failing broad tests
  remain recorded in typed-pb-signatures-execplan.md, not silently reclassified.

## Surprises & Discoveries


The encoder admitted raw binary JSON, but Go does not admit JSON constants.
The decoder expects datum-codec framing. UTC timestamp decoding in Shanghai
returned epoch 1704135845 instead of 1704164645. A GtReal parent converted a
strict CastStringAsReal failure into Ok(None). Temporary failing probes are in
/private/tmp/similar-parity-probe.log and /private/tmp/similar-error-probe.log.

## Validation and Acceptance


From repository root run the focused expression, coprocessor and encoding suites:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib distsql_builtin -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib cophandler -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-exec --lib -- --test-threads=1
    cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --all-targets
    GOTOOLCHAIN=go1.25.12 make lint
    git diff --check

Tests must prove UTC/session conversions in both directions, unchanged DATETIME,
JSON rejection at admission, correct supported literal decoding, and propagated
strict errors with lazy unused branches.

## Idempotence and Recovery


Do not edit generated protobuf files. Preserve concurrent changes and never force
push. Capture failed-before/passed-after evidence before publication. Tests may
be rerun without changes to data or external services.

## Outcomes & Retrospective


Implemented the three audited boundaries and verified them against Go. This is
an integration checkpoint in the ongoing package translation, not a completion
claim for pkg/expression or unistore. No benchmarks were run and no performance
improvement is claimed. Correctness/compatibility changes are intentional: invalid
strict casts now raise their error, JSON constants stay at root, and timestamps
retain their instant across session and UTC wire representations.

## Interfaces and implementation evidence


`pb_to_expr_in`, `expression_to_pb_in` and `to_pb_in` carry the session timezone
recursively. UTC convenience wrappers remain for tests and callers encoding only
columns or COUNT's integer constant. Production Selection admission/encoding and
aggregate-expression lowering use the request zone. `encode_mysql_time` owns
TIMESTAMP conversion; the decoder performs the matching UTC-to-session conversion.
DATETIME is unchanged. No generated files or protocol identifiers were edited.

JSON literals are refused in both description and encoding. Decoder JSON input
uses DecodeOne semantics and checks the decoded kind. BIT, ENUM and vector leaves
use existing datatype decoders, FLOAT32 retains its source datum kind, and
DURATION uses Go's maximum fractional precision independently of wire metadata.

The six legacy typed helpers now return Result<Option<T>, String>. The NULL
extraction macro propagates NULL only; Result's question-mark propagates errors.
Integer-child adapters use transpose instead of discarding errors. AND/OR retain
short-circuit evaluation. Existing parser/conversion behavior inside legacy
builtins outside this boundary is not claimed as translated or fully audited.

The incoming branch added ConfiguredWritePlan.warnings without updating one unit
test initializer and fifteen existing test destructuring patterns. Those fixtures
now supply an empty warnings vector or ignore unused fields. No expectations were
changed. The existing string-comparison coprocessor fixture now carries the real
column/literal type and collation required by the shared decoder introduced by
that incoming branch.

Go reference tests use the existing archive at
/private/tmp/tidb-go-structural-20260927 with an external overlay. The relevant
expr_to_pb.go, distsql_builtin.go and builtin_compare.go files are byte-identical
between archived master 8936d7bdcb and current master 46df29c960. From that archive:

    GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/pb-boundaries-overlay.json' ./tools/check/failpoint-go-test.sh pkg/expression -run '^TestRustPBBoundaries$' -count=1

This passes timestamp encode/decode, JSON admission/framing/kind checks, and strict
child-error propagation. Failpoints were enabled and disabled by the wrapper;
final reference count is zero. The initial reference run caught an inconsistent
mock session/statement timezone; correcting both mock fields made it pass.

Rust decoder tests: 11 pass. Coprocessor tests: 66 pass. Executor unit tests:
347 pass, one mock-PD bind failed under the sandbox; that exact test passes with
local network access. Wide Selection integration tests: 16 pass, including the
nested timestamp predicate and existing TPC-H Q1/Q6 wire assertions. make lint
passes after allowing access to its pinned Go tool download. All-target server checking and all three server pushdown tests pass. The server
runtime tests required system access because the sandbox blocks sysctl hw.memsize.
Publication is recorded in Git history.

Revision note: 2026-09-28, implemented context flow and fallible child evaluation,
recorded literal-family coverage and test fixture repairs for incoming changes.

## Final validation and remaining limits


The legacy decoder's duplicate literal arms have been removed. It calls the shared
decoder for every literal, then retains cheap legacy values where representable.
This also removes its raw-IEEE FLOAT64 decoding bug. The old FLOAT64 fixture was
corrected to encode with codec::encode_float, as Go does. A standalone timestamp
regression failed at 03:04:05 versus 11:04:05 before this route was unified.

Additional baseline restores proved the FLOAT32 metadata test and corrected
FLOAT64 wire test fail before the fix. Each restore saved/restored exact bytes in
a finally block. After restoration, all 11 decoder and 66 coprocessor tests pass.
No temporary baseline edits remain. Evidence is in /private/tmp/pb-red-tidb-expr.log,
/private/tmp/pb-red-tidb-unistore.log and /private/tmp/pb-restored-*.log.

Final commands from repository root (all pass except the catalog suite noted below):

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib distsql_builtin -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib cophandler -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-exec --lib -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-exec --lib label_delivery::tests::get_and_patch_use_pds_exact_region_label_api -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-exec --test all wide_scan_selection_source -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib pushdown_catalog -- --test-threads=1
    cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --all-targets
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --lib pushed_ -- --test-threads=1
    GOTOOLCHAIN=go1.25.12 make lint
    git diff --check

The full executor unit command passed 347 tests and failed only the sandboxed
mock-PD bind; the subsequent targeted command passed that remaining test.
The catalog suite passed 22 and retained the previously recorded failure
ifnull_string_column_literal_uses_go_signature_and_column_collation. Its known
baseline result is documented in typed-pb-signatures-execplan.md. It is unresolved,
not waived as a parity success. Full server/expression suites were not repeated;
prior failures remain recorded there. No sysbench/TPC-C/TPC-H/YCSB benchmark run,
complete package inventory, or complete upstream test translation is claimed.

Changed files cover tidb-expr's decoder/catalog, tidb-unistore's literal and
fallible typed evaluation boundaries, tidb-exec's Selection/aggregate encoding,
and tidb-executor's exhaustive scalar match. Tests exercise the changed paths;
the warnings-field fixture repairs enable compilation of the pulled branch.

Revision note: final review removed duplicate legacy literal decoding; all
production literal paths now share Go's codec contract. Final master reference:
46df29c960f60869f842165415644b29e172287f.
