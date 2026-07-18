# Source-backed Datum and expression evaluation context

This ExecPlan is a living document. Keep `Progress`, `Discoveries`, and
`Validation evidence` current while the migration proceeds.

## Purpose

Replace the seed evaluator's UTF-8-only `Value::Str(String)` and scattered
session inputs with the same stable seams used by Go TiDB: a datatype-owned
`Datum`/`FieldType` domain, a build context that selects typed expression
signatures, and an explicit expression evaluation context. This is a
structural prerequisite for directly porting the remaining string, binary,
charset, collation, temporal, JSON, warning, and SQL-mode behavior from Go.

The end state has no `Value` compatibility alias, no lossy conversion of raw
bytes through Rust `String`, and no builtin-specific substitute for missing
context. Expressions are built once from typed inputs, then evaluated with a
`Datum` plus one statement-scoped `EvalContext`; unsupported domains are
rejected at the typed boundary before evaluation.

## Source boundary

- `pkg/types/datum.go` is the value-domain truth. In particular,
  `KindString` and `KindBytes` are distinct, both retain byte payloads, and a
  string datum retains collation metadata.
- `pkg/expression/exprctx/context.go` is the expression-context truth. Its
  `EvalContext` owns SQL mode, type conversion flags, warnings/errors, current
  time, and optional statement properties; `BuildContext` separately owns
  charset/collation and selects typed builtin signatures while constructing
  expressions.
- `pkg/expression/builtin_string.go` is the first behavior slice. `LENGTH`,
  `CHAR_LENGTH`, `HEX`, `UNHEX`, and `CHAR` expose the byte-vs-character and
  invalid-UTF-8 distinctions that the current Rust value domain cannot model.
- `components/tidb_query_datatype` in TiKV remains the intended extraction
  source for the production-complete datatype implementation. The seed crate
  ports the Go contracts now without introducing an API that conflicts with
  that later extraction.

## Target ownership

`tidb-datatype` owns the SQL scalar domain and all representation invariants:

```text
Datum
  Null | Int | UInt | Decimal | Real
  Bytes(Vec<u8>)
  String(StringDatum { bytes, collation })
  Time | Duration | Json                 (later source-backed milestones)

Collation -> Charset
```

The string payload is bytes-first. Decoding is an operation requiring a known
charset and explicit error/warning policy, never an invariant of storage.
`Bytes` always has binary semantics; `String` always carries a validated,
registered collation whose registry determines its charset. This makes the
invalid state “string with unrelated charset and collation” unrepresentable.

`tidb-expr` owns:

```text
BuildContext
  default collation (charset derived from its registry entry)
  explicit FieldType input to typed builtin builders
  typed builtin-signature construction

EvalContext<'a>
  statement/type flags
  sql_mode
  fixed statement time and timezone
  warning sink
  session variable and column resolver
```

`ExprContext` groups build and evaluation contexts without collapsing their
responsibilities. There is one context-aware evaluation entry point.
Context-free constant evaluation is a test/convenience constructor around
deterministic default build/eval contexts, not a second semantics path.
Existing explicit parameters such as `div_precision_increment` move into the
evaluation context as their source domains are ported.

## Parallel ownership rule

- Root owns `tidb-datatype`, the `Datum`/`FieldType` and build/evaluation
  context public seams, workspace compilation, and deletion of the old
  `Value` path.
- Result workers own disjoint Go builtin families after the seam lands. They
  must not add another scalar enum, charset registry, warning channel, or
  context-free semantic path.
- Executor workers adapt row/session resolvers to construct `EvalContext`; they
  must not move datatype semantics into `tidb-exec`.
- Difftest workers own Go-oracle selectors and executable evidence, not result
  implementations.

## Progress

- [x] Inventory the current seam: `Value::Str` appears 247 times across 27
  Rust source/test files; explicit UTF-8 limitations already exist in binary
  literal, crypto, misc, and `CHAR` code.
- [x] Identify Go `Datum`, expression context, and string builtin source
  boundaries.
- [x] Add datatype-owned `Charset`, `Collation`, string payload, and `Datum`
  with source-backed constructors/accessors for the currently supported scalar
  kinds. The payload preserves invalid UTF-8 and embedded NUL without a
  compatibility alias or speculative temporal/JSON variants.
- [x] Port the minimal source-backed `FieldType` metadata that distinguishes a
  binary string from a character string using Go's SQL-type-plus-collation
  rule.
- [x] Add the first build phase that consumes `FieldType` and selects binary
  versus character builtin signatures. Do not infer every string operation
  from the runtime datum when Go decides it from argument type during
  construction.
  `BuildContext::build_string_length` now selects immutable `LENGTH`, binary
  `CHAR_LENGTH`, or UTF-8 `CHAR_LENGTH` signatures. The normal AST evaluator
  resolves source-visible `FieldType` facts before datum evaluation and rejects
  unresolved types rather than guessing from a default or runtime value.
- [x] Mechanically move every expression and executor scalar consumer to the
  datatype-owned `Datum`, delete `tidb-expr/src/value.rs`, and retain neither
  a duplicate enum nor a `Value` alias. `tidb-exec` now depends on
  `tidb-datatype` directly rather than receiving its row type through
  `tidb-expr`.
- [x] Remove `tidb-expr`'s public `Datum`/`Decimal` reexports. Expression code
  retains only crate-private names, and every external scalar consumer imports
  the datatype authority directly.
- [ ] Replace `Columns` plus explicit semantic parameters with one borrowed
  `EvalContext`, while retaining the resolver as a narrow field/trait owned by
  that context.
- [x] Port the core Go byte-vs-character seam for `LENGTH` and `CHAR_LENGTH`,
  including build-time signature selection, every currently representable
  source-table scalar row, arbitrary invalid UTF-8, embedded NUL, and
  multibyte inputs.
- [ ] Complete `TestLengthAndOctetLength`'s deferred time, set, duration,
  injected-error, and GBK rows only when those source-backed value/charset
  domains land; do not silently omit them from final coverage.
- [ ] Complete the remaining source milestone for `HEX`, `UNHEX`, and `CHAR`
  and its Go-oracle evidence before changing ledger coverage.
- [x] Make the Go scalar oracle's byte label contract exact: valid UTF-8 uses
  raw `STR:` bytes (including embedded NUL), while invalid UTF-8 uses
  `STR_HEX:` plus uppercase hexadecimal. Add an oracle-generated `UNHEX`
  corpus topic covering both branches.
- [x] Make the separate Go/Rust result-cell transport byte-safe: preserve
  ordinary valid UTF-8, encode invalid cells as `BYTES_HEX:` plus uppercase
  hexadecimal, and escape valid marker-prefixed text with `TEXT:`. Add an
  oracle-generated query topic plus Go helper and Rust executor regressions.
- [ ] Move the corresponding ledger entries from `PARTIAL` only when every
  cited Go case has executable Rust evidence.
- [ ] Add temporal and JSON variants only alongside their bounded Go source
  domains and tests; never add placeholder variants whose semantics are not
  implemented.

## Execution order

First, land the datatype representation as a dependency-leaf foundation, then
perform the exhaustive workspace rename as the next integration increment.
Until that rename completes, no semantic code may consume `Datum` and no
coverage claim may cite it: that keeps `Value` as the sole active authority
without adding a compatibility alias. The integration increment must be
atomic and must delete `tidb_expr::value::Value` when its consumers compile on
`Datum`.

Second, introduce the minimal typed build phase plus `BuildContext`, then add
`EvalContext` and route every evaluator call through it. Preserve results while
moving the existing fixed clock, timezone, system/user variables, column
lookup, and `div_precision_increment` inputs into the evaluation context.
Delete superseded entry points in that same increment.

Third, port the five source builtin families against raw bytes and registered
collations. Add selector rows that fail on the pre-migration representation,
then pass against both Go and Rust. Only this milestone changes coverage
claims.

Fourth, let parallel result workers consume the stable seam for disjoint
builtin families. Temporal and JSON domains remain separate source-first
milestones so they bring their Go representations and tests with them rather
than growing speculative enum cases.

## Invariants and failure policy

- A `Datum::Bytes` value can contain any octet sequence, including invalid
  UTF-8 and embedded NUL.
- A string's collation determines its charset through one registry; callers
  cannot supply contradictory metadata.
- Typed signature selection follows the argument `FieldType`: binary
  `CHAR_LENGTH` counts bytes, while its UTF-8 signature counts Go runes.
  Runtime datum metadata is retained but does not replace Go's build-time
  decision. `LENGTH` never decodes.
- Go `CHAR_LENGTH` is an explicit exception to checked decoding:
  `len([]rune(val))` counts one `RuneError` of width one for each invalid
  encoding byte or incomplete-suffix byte. Other conversion errors and
  truncation warnings flow through `EvalContext`; no builtin silently replaces
  or drops invalid bytes unless its Go source does.
- Statement time is read once per statement and reused by every evaluation.
- Scalar difftest labels encode arbitrary bytes losslessly: valid UTF-8 keeps
  its exact `STR:` payload, including embedded NUL, while invalid UTF-8 is
  `STR_HEX:` followed by uppercase hexadecimal. The Go `goeval` oracle and
  Rust `Datum::label` share this contract, so malformed text cannot turn a
  representation mismatch into a harness failure.
- `gorun`/`ResultSet::label` are a separate SQL result-cell contract. Go first
  applies `Datum.ToString`; Rust preserves semantic `Datum::sql_string` as a
  checked conversion. The differential transport then keeps ordinary valid
  UTF-8 unchanged, writes invalid cells as `BYTES_HEX:<UPPERCASE HEX>`, and
  prefixes valid text beginning with `BYTES_HEX:` or `TEXT:` with `TEXT:` so
  marker-shaped valid values cannot collide with encoded byte payloads.

## Validation evidence

Before implementation, capture focused tests demonstrating that the current
domain cannot express invalid-UTF-8 binary results. Each behavior increment
must run its exact Rust unit test and Go-oracle selector first.

The merged WIP gate is:

```text
cargo fmt --all -- --check
cargo test -j 12 -p tidb-datatype -q
cargo test -j 12 -p tidb-expr -q
cargo test -j 12 -p tidb-exec --lib -q
cargo test -j 12 -p difftest-result-tests --test <source_selector> -q
cargo clippy -j 12 -p tidb-datatype -p tidb-expr -p tidb-exec --all-targets -- -D warnings
```

Datatype foundation evidence (2026-07-15):

```text
cargo fmt --package tidb-datatype --
cargo test -j 12 -p tidb-datatype -q
  10 passed; 0 failed
cargo clippy -j 12 -p tidb-datatype --all-targets -- -D warnings
  passed
RUSTDOCFLAGS='-D warnings' cargo doc -j 12 -p tidb-datatype --no-deps
  passed
```

The focused tests cite `pkg/types/datum.go`, `pkg/types/datum_test.go`,
`pkg/parser/charset/charset.go`, `pkg/parser/charset/charset_test.go`,
`pkg/types/field_type.go`, `pkg/types/field_type_test.go`, and
`pkg/types/etc.go`. They prove arbitrary invalid UTF-8 and embedded NUL survive
both string and bytes payloads, bytes always derive the binary charset, a
string derives its charset from its registered collation, and build-time
binary-string selection requires both a string SQL type and the `binary`
collation. This foundation increment intentionally did not run expression,
executor, difftest, or workspace gates because it has no consumers yet.

Atomic workspace migration evidence (2026-07-15):

```text
cargo check -j 12 -p tidb-expr --all-targets
  passed
cargo check -j 12 -p tidb-exec --all-targets
  passed
cargo test -j 12 -p tidb-datatype -q
  11 passed; 0 failed
cargo test -j 12 -p tidb-expr -q
  88 passed; 0 failed
cargo test -j 12 -p tidb-exec --lib -q
  226 passed; 0 failed
cargo fmt --all -- --check
  passed
cargo clippy -j 12 -p tidb-datatype -p tidb-expr -p tidb-exec --all-targets -- -D warnings
  passed
```

This increment also moved hex/bit literals, `UNHEX`, and no-`USING` `CHAR`
to `Datum::Bytes`, and made `HEX` consume raw payload bytes. Focused tests now
prove `0xff`, `b'111111111'`, `UNHEX('FF00')`, and `CHAR(-1)` round-trip exact
octets. Ordinary valid UTF-8 labels remain unchanged, including embedded NUL
and other control bytes required by the Go differential oracle; only invalid
UTF-8 uses a lossless hexadecimal diagnostic label. Semantic stringification
remains checked; the later result-ring increment adds its own explicit
byte-safe transport rather than changing SQL coercion. The next focused increment below
implements `LENGTH`/`CHAR_LENGTH` signature selection; the full five-builtin
source milestone and ledger transition remain open.

No difftest selector was run by this lane. Evidence callers initially adapted
to checked result labeling; the later evidence-steward increment replaced that
temporary boundary with the explicit byte-safe result-cell transport.

Typed string-length build seam evidence (2026-07-15):

```text
cargo test -j 12 -p tidb-datatype -q
  11 passed; 0 failed
cargo test -j 12 -p tidb-expr -q
  93 passed; 0 failed
cargo fmt --all -- --check
  passed
cargo clippy -j 12 -p tidb-datatype -p tidb-expr --all-targets -- -D warnings
  passed
```

The new focused tests pass the identical raw multibyte payload through binary
and character `FieldType`s and prove that runtime `Datum` metadata cannot
re-select the signature. They also cover embedded NUL, an isolated invalid
byte, an invalid four-byte sequence, a truncated multibyte suffix, and a valid
multibyte rune adjacent to an invalid byte. `LENGTH` and binary `CHAR_LENGTH`
remain byte-exact; UTF-8 `CHAR_LENGTH` ports Go's one-byte-at-a-time
`RuneError` counting instead of using Rust's checked or lossy decoding. Both
`LENGTH` names cover every source-table scalar domain representable today;
the plan explicitly retains the deferred time, set, duration, injected-error,
and GBK rows rather than treating them as covered.

Scalar differential-label evidence (2026-07-15):

```text
go build -p 12 -o /tmp/tidb-goeval ./rust/difftests/goeval
  passed
go test -p 12 ./rust/difftests/goeval
  passed; package has no standalone Go test files
make -j12 bazel_prepare
  passed; required because the new Rust workspace contains Go oracle source
bazel build //rust/difftests/goeval:goeval
  passed
/tmp/tidb-goeval < rust/difftests/corpus/expr/byte_lossless_labels.txt \
  > rust/difftests/corpus/expr/byte_lossless_labels.golden.txt
  generated by the Go TiDB expression engine
tmp_golden="$(mktemp)"
/tmp/tidb-goeval < rust/difftests/corpus/expr/byte_lossless_labels.txt \
  > "$tmp_golden"
cmp "$tmp_golden" rust/difftests/corpus/expr/byte_lossless_labels.golden.txt
rm "$tmp_golden"
  passed; checked-in bytes exactly reproduce from the Go oracle
xxd -g 1 rust/difftests/corpus/expr/byte_lossless_labels.golden.txt
  STR:a<00>b
  STR_HEX:FF0041
  STR_HEX:C328
  STR:<F0 9F 92 A9>
cargo test -j 12 -p tidb-datatype \
  diagnostic_labels_are_lossless_but_sql_stringification_is_checked
  passed; 1 test
cargo test -j 12 -p difftest --lib
  passed; 7 tests
cargo test -j 12 -p difftest-result-tests --test expr_diff
  passed; 1 test
cargo clippy -j 12 -p difftest-result-tests --test expr_diff -- -D warnings
  passed
cargo fmt --all -- --check
  passed
```

The new topic uses `UNHEX`, sourced from
`pkg/expression/builtin_string_test.go:TestUnhexFunc`, to make Go itself
produce the raw bytes. Its golden file was emitted by the built `goeval`
binary rather than handwritten. Inspection of `gorun` confirmed that it uses
`session.ResultSetToStringSlice`, whose cells use Go `Datum.ToString`; this is
why the result-cell transport remains separate from the scalar diagnostic
label.

Result-cell differential evidence (2026-07-15):

```text
cargo test -j 12 -p tidb-exec --lib \
  tests::expr::hex_bit_literal_eval -- --exact
  failed before the transport change: invalid UTF-8 returned Utf8Error
  passed after the transport change
go test -p 12 ./rust/difftests/gorun
  passed
go build -p 12 -o /tmp/tidb-gorun ./rust/difftests/gorun
  passed
make -j12 bazel_prepare
  passed and regenerated the Go helper BUILD target
bazel test --jobs=12 //rust/difftests/gorun:gorun_test
  passed
/tmp/tidb-gorun < rust/difftests/corpus/query/byte_lossless_results.txt \
  2>/dev/null | grep -E '^(RS:|ERR)'
  RS:a<00>b
  RS:BYTES_HEX:FF0041
  RS:BYTES_HEX:C328
  RS:<F0 9F 92 A9>
  RS:TEXT:BYTES_HEX:FF
  RS:TEXT:TEXT:value
cargo test -j 12 -p difftest-result-tests --test query_diff -q
  passed; 1 test
cargo test -j 12 -p difftest-result-tests --test table_diff -q
  passed; 1 test
cargo clippy -j 12 -p tidb-exec -p difftest-result-tests \
  --all-targets -- -D warnings
  passed
```

The checked query golden has SHA-256
`22db999606770825b141c1ab5eea86a38d5a54464906d90066c2fde3bbc78123`
and was emitted by the built Go session oracle rather than handwritten.

AST `FieldType` integration and datatype-authority evidence (2026-07-15):

```text
go build -p 12 -o /tmp/tidb-gorun ./rust/difftests/gorun
  passed; wrapper rebuilt from the current checkout before the probes below
/tmp/tidb-gorun stdin queries:
  CHAR_LENGTH(0xE4BDA0)                         -> 3
  CHAR_LENGTH(b'111001001011110110100000')      -> 3
  CHAR_LENGTH('你')                              -> 1
  CHAR_LENGTH(CAST('你' AS BINARY))               -> 3
  CHAR_LENGTH(UNHEX('E4BDA0'))                  -> 3
  CHAR_LENGTH(CHAR(228,189,160))                -> 3
  CHAR_LENGTH(FROM_BASE64('5L2g'))              -> 3
  CHAR_LENGTH(0xF0288C28)                       -> 4
  CHAR_LENGTH(CAST(0xE4BDA0 AS CHAR))           -> 1
cargo test -j 12 -p tidb-datatype -q
  11 passed; 0 failed
cargo test -j 12 -p tidb-expr -q
  95 passed; 0 failed
cargo test -j 12 -p tidb-exec --lib -q
  229 passed; 0 failed
cargo test -j 12 -p difftest-result-tests --test expr_diff -q
  1 passed; 0 failed
cargo fmt --package tidb-datatype --package tidb-expr --package tidb-exec -- --check
  passed
cargo fmt --all -- --check
  passed after the parser-owned concurrent import wrap settled
cargo clippy -j 12 -p tidb-datatype -p tidb-expr -p tidb-exec --all-targets -- -D warnings
  passed
cargo check -j 12 --workspace --all-targets
  passed
```

The public AST evaluator now chooses `CHAR_LENGTH`'s signature from explicit
source type facts for string, hex/bit, binary/character cast, `UNHEX`,
no-`USING` `CHAR`, `FROM_BASE64`, and source-propagating `ELT` forms before it
evaluates the argument. An unresolved column, variable, or unlisted function
returns `Unsupported("unresolved CHAR_LENGTH argument FieldType")`; even a
resolver that would return `Datum::Bytes` cannot influence signature choice.
`LENGTH` remains type-independent. The executor query suite exercises the same
path end to end.

The workspace audit found one remaining external scalar import through
`tidb-expr`, in `tidb-exec/src/aggregate.rs`; it now imports `Datum` directly
from `tidb-datatype`. `tidb-expr` keeps only crate-private scalar names and no
longer publicly reexports `Datum` or `Decimal`. The full workspace check proves
all external consumers compile against the datatype-owned public authority.

Before a Ready claim, run the repository-required Ready profile, the full Rust
workspace tests/clippy, the static parser oracle, the result differential
suite, and ledger `--check`. The final evidence must name every Go test/source
row moved to covered and must report any Go wrapper that could not run locally.

## Discoveries

- The current `Columns` trait already accumulated statement time and session
  variable access, while decimal division is threaded as a separate integer.
  This is an early form of Go's `EvalContext`, split across unrelated APIs.
- Binary literal code currently documents lossy UTF-8 conversion as a known
  boundary. Crypto and `CHAR` repeat the same boundary independently. A
  datatype fix removes all of these special cases together.
- Go stores a string datum's payload as bytes and records collation; charset is
  derived. Mirroring that relationship eliminates contradictory metadata
  instead of adding runtime checks for every function.
- Go's `charLengthFunctionClass.getFunction` selects
  `builtinCharLengthBinarySig` or `builtinCharLengthUTF8Sig` from argument
  `FieldType` during construction. A bytes-capable datum alone is necessary but
  insufficient; the Rust evaluator now exposes that typed build seam and keeps
  its selected signature immutable during evaluation.
- Go `builtinCharLengthUTF8Sig.evalInt` uses `len([]rune(val))`; malformed UTF-8
  is not an `EvalContext` warning/error boundary. Go's decoder emits one
  replacement rune and advances exactly one byte for each invalid encoding or
  incomplete suffix, which differs from relying on Rust lossy-decoder grouping.
- The AST carries enough source type identity to close the literal/cast/simple
  function gap without inspecting a runtime datum. It does not carry schema or
  parameter `FieldType`; silently applying the connection collation there would
  recreate the original phase error, so unresolved forms must remain an honest
  build failure until a typed resolver seam lands.
- A public reexport from `tidb-expr` made the datatype move look complete while
  still allowing downstream code to treat the expression crate as scalar
  authority. Making the internal names crate-private exposed the sole remaining
  executor import and let a workspace check prove `tidb-datatype` is the only
  public authority.
- `pkg/types/etc.go::IsBinaryStr` does not select on the `BinaryFlag` or the
  `BLOB` type name alone. It requires a string SQL type whose collation is
  exactly `binary`; a blob carrying a character collation is therefore a
  character string for signature selection.
- `DefaultTypeForValue` and `InferParamTypeFromDatum` intentionally disagree
  about NULL charset metadata. The datatype foundation therefore does not add
  a generic `Datum -> FieldType` inference helper; the build integration must
  port the exact Go call path for constants versus prepared parameters.
- Result-set labeling used to double as SQL string coercion. Once arbitrary
  bytes became representable, that forced either lossy decoding or a semantic
  hexadecimal substitution. Semantic `Datum::sql_string` is now checked;
  scalar and result-ring diagnostics each have an explicit reversible byte
  transport outside SQL coercion.
- Go `fmt.Sprintf("%X", []byte(value))` is already the required uppercase,
  reversible invalid-byte encoding. `strings.ToValidUTF8(value, "") == value`
  detects exactly the valid-UTF-8 branch while leaving valid control bytes,
  including NUL, unchanged and avoids adding a second String/Bytes formatter.
- Raw byte values cannot be reconstructed as `Expr::String` during subquery
  substitution. The executor now emits an `Expr::Hex` for `Datum::Bytes` (and
  for an otherwise-unrepresentable invalid-UTF-8 string payload), preserving
  every octet through the existing literal evaluator.
