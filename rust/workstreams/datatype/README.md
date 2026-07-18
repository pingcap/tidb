# Datatype and evaluation-context workstream

Owns the shared scalar representation, typed expression-build metadata, and
statement-scoped expression context.
The source boundaries are `pkg/types/**`, `pkg/expression/exprctx/**`,
`pkg/sessionctx/stmtctx/**`, and the production TiKV query-datatype crates
identified by the rewrite design.

This is a stewarded integration seam, not an open feature lane. One owner lands
changes to `tidb-datatype`, `Datum`, `FieldType`, charset/collation
registration, the single parser/runtime `EvalType` identity,
`BuildContext`/`EvalContext`, public re-exports, and the exhaustive workspace
migration.
Parallel result workers consume that API through disjoint Go builtin families;
they do not add competing value types, warning sinks, or semantic parameters.

The active migration contract is
[`../../execplans/2026-07-15-datum-eval-context.md`](../../execplans/2026-07-15-datum-eval-context.md).
It requires an atomic move away from `tidb_expr::Value`, bytes-first string
storage, one collation-to-charset registry, and deletion of the old path rather
than a lasting compatibility alias.

## Parser FieldType metadata wave

`tidb-datatype::FieldType` now owns the dependency-closed metadata portion of
`pkg/parser/types/field_type.go`: source flag masks with immutable bit
operations, `flen`/`decimal` sentinels, normal and CAST default length/decimal
tables, DECIMAL validity bounds, and variable-length predicates. SQL type
formatting, enum/set element storage, mutable charset propagation, and full
session FieldType construction remain partial and must not be inferred from
this metadata leaf.

## Enum, SET, and collation source wave

`tidb-datatype` now owns the byte-preserving collation authority consumed by
ENUM and SET. The wave directly translates `pkg/types/enum.go` and `set.go`,
including every row in `TestEnum` and `TestSet`. `utf8_general_ci` uses TiDB's
exact general-CI planes; `utf8_unicode_ci` uses the exact UCA 4.0 table and all
long-rune expansions. The images are deterministic outputs of
`scripts/generate_collation_data.py`, compared to TiDB's retained original UCA
fixture, and pinned by byte length and SHA-256.

The API accepts bytes because Go strings and Rust `StringDatum` preserve
arbitrary octets. General-CI and Unicode-CI therefore retain the source rule
that compare returns equality at the first invalid UTF-8 sequence and key
returns the valid prefix. No Unicode library approximation or `str`-only
authority exists. Enum/set and UCA 4.0 data are complete; wildcard patterns,
0900, GBK, and pinyin collations remain outside this wave.

## Checked overflow arithmetic

`tidb-datatype::overflow` is the first arithmetic leaf from `pkg/types`: it
ports all checked signed, unsigned, mixed-integer, duration, and division
operations from `overflow.go`, with the four original overflow test tables in
`overflow_tests.rs`. It deliberately exposes only a source-shaped
`OverflowError`; the full TiDB `dbterror` hierarchy is still stewarded work.
Use this module for exact arithmetic, not as a second warning or error channel.

## ASCII encoding leaf

`ascii_encoding.rs` ports the bounded `encoding_ascii.go` contract as a
byte-first leaf: `peek`, seven-bit validation, Go-compatible UTF-8 lead-byte
grouping, operation flags, replacement/truncation, and the source's
bytes-plus-optional-error transform result. It deliberately does not grow the
shared charset registry or claim the UTF-8/GBK/GB18030 encoding families;
those remain stewarded partial work until their source APIs and tests move
together.

## UTF-8 encoding leaf

`utf8_encoding.rs` ports the byte-level `encoding_utf8.go` contract for both
the normal four-byte UTF-8 encoding and strict legacy `utf8mb3`: lead-byte
`Peek`, decoder-width `MbLen`, malformed-sequence grouping, three-byte
validation, and source `Transform` replacement/truncation/error behavior.
The API intentionally stays below the shared charset registry and does not
pretend to cover `encodingBase`, GBK/GB18030, collations, or session warning
channels; those remain explicit `PARTIAL` boundaries in the evidence ledger.

## Shared encoding transform policy

`encoding_base.rs` owns the byte-preserving policy shared by charset leaves:
the source operation bits, source-before-converted collection precedence,
first invalid-group error retention, replacement with `?`, and trim-at-first
invalid behavior. ASCII and UTF-8 expose source-named aliases of this one
policy instead of maintaining competing flag/result implementations. Charset
decoder wiring, the error hierarchy, and registry dispatch remain outside the
leaf until their source families move together.
