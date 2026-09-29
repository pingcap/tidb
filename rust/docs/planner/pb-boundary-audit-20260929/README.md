# Charset, regexp, and timestamp boundary audit, 2026-09-29

This audit demonstrates additional expression-integration defects beyond the
[previous audit](../pb-expanded-audit-20260928/README.md). No production code is
changed and no finding is closed. The strongest evidence is a bypass of existing
Go-compatible Rust primitives: all 5,064 direct encoding/collation checks match
Go, while the expression routes disagree.

This is seed evidence, not a transcreated-package receipt. The inventories are
complete for the specified tracked subtrees and special-case tables; the runtime
input space and TiDB implementation are not exhaustively verified.

## Reference and coverage

- Rust: `730504b02d`, `hparser-integration`.
- Fetched Go master: `3425d7860550d38b43c6faa9f3078647b2796abb`.
- Go execution archive: `8936d7bdcb` at
  `/private/tmp/tidb-go-structural-20260927`, using `GOTOOLCHAIN=go1.25.14`.
  `git diff --name-only 8936d7bdcb origin/master -- pkg/expression pkg/types
  pkg/util/collate pkg/parser/charset` was empty. This establishes equality of
  those source subtrees, not equality of the entire archive to master.
- `package-inventory.json` records Git blob identities for 208 tracked artifacts
  under `pkg/expression`, 14 under `pkg/parser/charset`, and 35 under
  `pkg/util/collate`. Subpackages remain separate package completion units.
  Inventory does not prove all production paths, fixtures, generated inputs,
  build/platform variants, or validation gates have been ported.
- **23,568 differential rows**, each serialized by Go and replayed through Rust.
  Modes are strict, warning, and ignore truncation. This is a new matrix; it does
  not rerun every previous audit case.
- An initial 10,626-row subset matched in compared results/states/diagnostics.
  That subset covers seven collations, string operations, integer signedness,
  rounding and arithmetic extremes. Later additions cover GBK/GB18030, regexp
  engine boundaries, timestamp precision and negative fractions.
- Every code point in both Go special-case tables is exercised in both UPPER and
  LOWER: GBK has 23 ranges/30 code points; GB18030 has 58 ranges/754 code points.
  Their 4,704 rows plus 360 wildcard rows are also evaluated directly through
  Rust datatype primitives.

## Results

**4,032 differing rows across six signatures**, all result/state differences.
There are no Go or Rust panics. Rows repeat inputs across context modes and
metadata shapes; they are not independent bugs. Raw observations are in
`go.tsv.gz`, `rust.tsv.gz`, and `differences.json`.

| Signature | Differing rows |
| --- | ---: |
| UpperUTF8 | 1,266 |
| LowerUTF8 | 1,137 |
| RegexpLikeSig | 1,482 |
| LikeSig | 24 |
| FromUnixTime1Arg | 51 |
| FromUnixTime2Arg | 72 |

### 1. Charset-specific case mapping is bypassed

`UpperUTF8/special_case/gbk/00e9`: Go keeps `é`; Rust produces `É`.
`LowerUTF8/special_case/gb18030/03c2`: Go produces `σ`; Rust keeps `ς`.
These use individual characters, not an unsupported character mixed into a
larger GBK string.

Go's `builtinUpperUTF8Sig.evalString` and `builtinLowerUTF8Sig.evalString` select
`charset.FindEncoding` from the argument FieldType and call that encoding's
case operation. Rust's `scalar_function/pb_builtin.rs` turns the argument into a
new generic string, and `string_fn.rs::case_convert` uses generic Go Unicode
simple-case mapping. It never selects the source encoding.

Rust already has the special tables and their implementation in
`tidb-datatype/src/multibyte_encoding.rs::Encoding::{to_upper,to_lower}`. The direct
primitive replay matches **all 4,704 Go table rows**. The whole special-table
subset has 2,385 expression differences: GBK upper/lower 84/6 rows, GB18030
upper/lower 1,170/1,125. Remaining case differences are multi-character samples.

The repair belongs at the typed expression/encoding boundary. Replacing or
patching individual case-table entries would target the wrong layer. Native
UPPER/UCASE/LOWER/LCASE also call `case_convert`; those complete SQL routes were
not separately executed here.

### 2. LIKE duplicates and diverges from the collator's wildcard implementation

`LikeSig/wildcard/gb18030_bin/1/0`: matching `中` against `_` returns false in Go
and true in Rust. Go's `gb18030BinPattern` embeds the byte-oriented `binPattern`;
its behavior differs from GBK's rune-oriented matcher.

Rust's datatype `Collation::pattern` already makes that distinction. All **360
wildcard rows** match Go when evaluated directly through it. But both
`like.rs::CompiledLikePattern` and `like_match_with_collation` implement their
own selection, treating GB18030 as a rune matcher. The scalar expression cache
uses that duplicate implementation.

Follow Go's collator-owned pattern lifecycle and cache the selected collator's
compiled matcher. Another character-specific shortcut would retain two
independent semantics. Direct primitive results are in `primitive.tsv.gz`.

### 3. Regexp engine contracts are not equivalent

The shared Rust `regexp.rs::build_regexp` directly uses `regex::RegexBuilder`;
Go's `regexpBaseFuncSig.buildRegexp` uses Go's `regexp.Compile`.

- `RegexpLikeSig/regex/0/0/0`: `\d` on Arabic-Indic digit `١` is false in Go,
  true in Rust. `\w`, `\s`, their complements and word boundaries also differ:
  Go's Perl character classes are ASCII-based, Rust's defaults are Unicode-based.
- `RegexpLikeSig/regex/9/3/0`: `(?u)a` on `a` is a Go 1139 compile error but matches
  in Rust. Other probes expose differing repetition limits, Unicode escape
  syntax, repeated quantifiers, and bracket-class set operations.
- `RegexpLikeSig/regex/0/10/0`: a malformed-byte text input is accepted by Go's
  matcher and returns false for `\d`; Rust rejects it as invalid UTF-8 before
  compiling. `ScalarFunction::eval_regexp_like` requires a Rust UTF-8 String,
  whereas Go accepts byte strings and its regexp engine interprets invalid
  sequences using RuneError.

A shared regexp compatibility boundary must account for accepted syntax,
character classes, byte-string interpretation, diagnostics and caching together.
Changing a single flag cannot establish compatibility. `REGEXP`, `REGEXP_LIKE`,
`REGEXP_SUBSTR`, `REGEXP_INSTR`, and `REGEXP_REPLACE` reference the shared compiler
in source; only the protobuf REGEXP_LIKE route is replayed here. The findings
must not be reported as separately tested SQL regressions in all those routes.

### 4. Negative fractional epochs lose their sign

`FromUnixTime1Arg/precision/8/6`: input decimal `-0.5` returns NULL in Go, but Rust
returns `1970-01-01 00:00:00.500000`. The two-argument form has the same problem.
Both result precision and input domain are ordinary valid values here.

Go's `evalFromUnixTime` tests `MyDecimal.IsNegative` before decomposing the value.
Rust's `time_fn/session_tz.rs::unix_arg_nanos` renders it, parses the integer
substring `-0` to numeric zero, tests only that integer for negativity, then adds
a positive fractional part. It also affects `-0.1` and very small negatives.
Preserve the typed decimal sign and Go's ordering of validation and conversion.

### 5. FROM_UNIXTIME precision is applied at a different layer

`FromUnixTime1Arg/precision/1/3`: decimal `0.1234999`, wire result precision 3:
Go gives `.123`; Rust gives `.124`. Rust first rounds to six digits from the
value, then parses/rounds the produced time again at the result precision.
Go rounds once at the builtin result precision.

The two-argument Rust path formats before applying that result precision:
`FromUnixTime2Arg/precision/1/3` gives `.123000` in Go and `.123500` in Rust.
The root is the value-only helper's production contract and its composition
with typed result construction. Use a single typed conversion/rounding boundary
for both signatures, as Go's `evalFromUnixTime` does.

**Reachability qualification:** precision probes deliberately vary wire result
precision independently of the decimal input's declared scale 7. They demonstrate
protobuf decoder behavior; this audit does not prove that Go's planner emits all
those combinations. The negative-fraction defect also occurs at the normal
six-digit result precision and does not depend on that qualification.

## Reproduction and validation

Harnesses are evidence text, not installed tests or production changes. Copy:
`go-oracle.txt` to `/private/tmp/pb-boundary_test.go`, `go-overlay.json` to
`/private/tmp/pb-boundary-overlay.json`, `rust-replay.txt` to
`/private/tmp/pb-boundary-rust-probe.rs`, `registry-probe.txt` to
`/private/tmp/pb-systematic-inventory-probe.rs`, `run-rust.txt` to
`/private/tmp/run-pb-boundary-rust.py`, and `compare.txt` to
`/private/tmp/compare-pb-boundary.py`. For Rust-only replay, decompress the captured input:
`gzip -dc rust/docs/planner/pb-boundary-audit-20260929/go.tsv.gz > /private/tmp/pb-boundary-go.tsv`. Use the audited checkout and archive paths;
adjust the overlay on another host. Do not run the temporary source-insertion
runner alongside other source writers; its finally block restores both files.

Exact collection commands:

```sh
# Go archive working directory:
GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/pb-boundary-overlay.json' ./tools/check/failpoint-go-test.sh pkg/expression -run '^TestRustBoundaryPB$' -count=1 -v

# TiDB repository working directory:
python3 /private/tmp/run-pb-boundary-rust.py
# Runner invokes:
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib audit_pb_registry_inventory -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib audit_boundary_pb_matrix -- --test-threads=1
python3 /private/tmp/compare-pb-boundary.py
```

The Go package uses failpoints; the wrapper enabled them and cleanup returned
refcount zero. Both Rust collector tests passed. Passing means the observations
were collected, not that parity holds. Final log excerpts are in `validation.txt`;
missing Git metadata warnings come from the source archive. All 23,568 keys were
unique and matched between collectors; all 5,064 primitive results were checked
against the corresponding Go rows. No production source changes remained.

The three TSV artifacts are losslessly gzip-compressed to reduce checkout size.
TSV/comparator formats follow the previous audit: exact real bits, SQL bytes,
error states, warning counts and normalized warning identities. The comparator
only compares error text when Rust exposes a bracketed typed SQL identity.
Matching error states can therefore still hide diagnostic differences. The
special-case inventory counts table entries, not all Unicode code points.
Some stress cases intentionally pair a signature and metadata differently from
normal planner construction; the concrete case, wildcard, and regexp examples
above use the relevant ordinary signatures. Malformed-byte inputs are identified
explicitly, not described as ordinary valid text.

This documentation-only change does not require a new production lint run.
`git diff --check` and
`python3 rust/docs/planner/pb-boundary-audit-20260929/verify.txt` passed,
covering checksums, row identities, difference membership, and primitive results. Repository policy
also requires `cd rust && cargo build --locked -p tidb-server` in the pre-commit
hook and again immediately before push. Build success does not close findings.

Not verified: every expression/decoder input, full SQL/cluster execution,
non-protobuf routes, complete error diagnostics, exhaustive collator behavior,
whole-package transcreation, or sysbench/TPCC/TPCH/YCSB performance. Earlier
conversion, temporal, decoder-registration and protocol gaps remain open. This
commit has no runtime impact; correctness and compatibility risks remain in the
implementation. No performance improvement is claimed.
