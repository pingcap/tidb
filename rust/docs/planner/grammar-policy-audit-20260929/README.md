# Shared grammar and statement-policy structural audit

This audit confirms more structural mismatches after the charset/regexp/time
repair in `cec2e3f475`. It changes no production code. It is seed evidence,
**not a complete package transcreation receipt or an exhaustive repository audit**.
The earlier repair fixed its captured cases; it did not establish complete Go
regexp or temporal-context compatibility.

## Reference and coverage

Rust was updated to `70d6f796f75962a656055d682edcffdde5914252` on
`hparser-integration`. Before publication, the audit commit was rebased onto
`3f3a2d2541` (concurrent table-reader changes in executor access-path/table-scan
files). Those changes do not touch the audited grammar, request-policy,
expression or SQL error-conversion sources. The locked server build was rerun
after integration. Go master was fetched at
`cce8ac7d6feb073325909ffaf344630bd24cca1d`.
The execution archive `/private/tmp/tidb-go-structural-20260927` is at
`8936d7bdcb`. Its expression, types, statement-context and mock-context sources,
and `pkg/store/mockstore/unistore/cophandler/cop_handler.go`, have no diff
against that master. This does not establish equivalence of the entire archive.

The regexp oracle uses the installed Go 1.25.14 standard library. It evaluates
all 236 entries in eight upstream `regexp/syntax/parse_test.go` tables, including
constructed deep/large patterns, plus 12 focused cases. Table names are
`parseTests`, `foldcaseTests`, `literalTests`, `matchnlTests`, `nomatchnlTests`,
`invalidRegexps`, `onlyPerl`, and `onlyPOSIX`. Each pattern goes through actual
`regexp.Compile` with its normal flags, not the table's expected parse dump or
its original test-specific flags. Each accepted pattern is matched against 16
strings. This is not every upstream regexp test or every Unicode scalar value.

The policy oracle constructs Go protobuf expressions and evaluates them using
`mock.NewContextDeprecated()` and `StmtCtx.InitFromPBFlagAndTz`, the same
initializer used by Go Unistore. Rust consumes the identical protobuf bytes
through its `RequestEvalContext`. The 854 cases cover 61 expression/input shapes
under 14 flag combinations. There are no panics. They exercise temporal casts
and consumers, unsigned conversions, and division/modulo by zero. Request
flags do not expose every session setting; these results concern this request
path, not every native SQL session configuration.

`source-inventory.json` records Go Git blob identities for the owning subtrees
and hashes of the standard-library regexp source/test/support files. Subpackages
remain separate completion units. Inventory is not evidence that every listed
file, platform variant, fixture, integration decision, or validation gate has
been implemented or reviewed.

## Confirmed structural mismatches

| Boundary | Reproduction | Go | Rust | Cause and design direction |
| --- | --- | --- | --- | --- |
| Regexp grammar ownership | `focused/0`: match `a` against `a{01}` | No match: malformed count is literal text | Matches: interpreted as repetition count one | Rewriting selected syntax before Rust's AST parser leaves Rust in charge of language acceptance and meaning. Go's grammar must be authoritative before lowering to a backend. |
| Regexp accepted language | `parseTests/22`, `parseTests/74`, `focused/1`, `focused/3` | Accepts `x{1001`, `\p{^Braille}`, duplicate inline flags and duplicate capture names | Rejects these | The same parser mismatch also rejects literal braces and Go property negation. These are families of syntax rules, not isolated spelling exceptions. |
| Regexp Rust-only language | `focused/8`, `focused/9` | Rejects `\p{XID_Start}` and `\p{gc=L}` | Accepts both | Rust property lookup accepts a broader vocabulary. Validate against Go's property vocabulary and semantics. |
| Regexp backend limits | `focused/7`: 300 nested capture groups | Compiles | Rejects at backend nesting limit 250 | The adapter permits AST nesting up to 1000, then uses the backend builder's separate default. Grammar/complexity decisions must remain consistent through compilation. |
| Temporal policy bypass | `CastStringAsTime/2024-00-02`, flags 0 | Error 1292 | Returns `2024-00-02 00:00:00.000` | `cast.rs::parse_time_by_source` passes permissive zero-date arguments; parser errors are erased to `()` and several paths append warnings directly. Go passes its type context and sends the original typed error through `handleInvalidTimeError`. |
| Conflicting temporal context defaults | `Date/2024-00-02`, flags 128 | Returns `2024-00-02` | Error 1292 | RequestEvalContext supplies conversion flags but inherits `Columns::date_modes()`'s default SQL mode. DATE uses those defaults while other paths use type flags or permissive parser arguments. Carry the actual Go-equivalent type context, SQL mode and error context separately and consistently. Do not blindly derive all SQL modes from request flags. |
| Regexp error identity lost | Pattern `[` or match type `z` | Typed error 1139 | SQL client receives 1105 | `regexp.rs::build_regexp` drops compiler details into `EvalError::Unsupported`; executor error conversion maps that to unknown error. Preserve the regexp error class, message and SQL identity through caching, evaluation and transport. |

The grammar matrix has **24 differing patterns**: 21 Go-accepted patterns
rejected by Rust, two Rust-only acceptances, and one differing match result.
Sixteen differences come directly from upstream tables. This shared adapter
also serves utility filter, table-filter, table-router and regexp-router users;
their callers were source-traced, not each exercised end to end.

The policy matrix has **370 differing rows across eight signatures**. Overlapping
categories are 272 result/state differences, 240 warning-count differences,
254 warning-content differences and 28 typed-error differences. These are not
370 independent bugs. Many extend the previously reported temporal and numeric
conversion root causes. All seven division/modulo signatures agree across the
14 tested flag combinations. Negative int/real/decimal unsigned conversion
cases agree; string and JSON diagnostics still have differences.

The temporal contradiction is especially important: setting flag 128 makes Go's
cast accept the zero component, while Rust's subsequent DATE still rejects it.
Removing one hardcoded check would not repair the different policy sources or
the discarded conversion errors. Flags 4 and 64 must not be declared bugs merely
because Rust does not use them: Go's initializer does not consume them either.

`sql-rust.tsv` additionally proves native SQL reachability: `regexp_like('a',
'a{01}')` returns 1, `regexp_like('a', '(?ii)a')` errors, and invalid pattern/flag
errors have code 1105. The corresponding Go expression oracle returns 0, 1 and
1139. The Go side uses PBToExpr, not a full SQL parser/server connection. The
harness checks the real Rust Session and its client error conversion.

## Previously identified structural issues still open

The complete scalar registration scan was rerun: Rust still accepts 234 of the
565 Go decoder registrations, leaving **331 missing**. Decoder registration
does not establish planner pushdown eligibility; these are capability gaps,
not 331 proven SQL regressions.

The previous 8,298-row matrix was also rerun. Its **392-row difference list is
identical** to the saved expanded audit: 21 malformed-input Go panics and three
ASIN one-ULP differences remain separately classified; the other 368 rows cover
51 signatures. See [expanded audit](../pb-expanded-audit-20260928/README.md) and
[first structural audit](../pb-systematic-audit-20260928/README.md) for the
reproducers and owning source boundaries. Remaining categories include:

- Byte-string numeric conversion and lost source-specific diagnostics.
- JSON/numeric child evaluation order and NULL/error exits.
- JSON-to-binary padding applied at the wrong production boundary.
- Temporal/interval error policy, result scale, and missing request-owned clock.
- SQL error identity erased before coprocessor response construction.
- Incomplete decoder/schema capability inventories and master dependency drift.

The schema omission inventory and coprocessor response transport path were not
rerun dynamically in this audit. They remain prior findings, not new counts.
The charset, LIKE and negative-epoch findings repaired by `cec2e3f475` are not
being reopened based on these results. No new optimizer candidate-lifecycle or
merge-planning audit was performed here.

## Reproduction and evidence

Compressed TSVs retain all observations. `grammar-differences.json` and
`policy-differences.json` contain every difference. All patterns are hex-encoded
in grammar TSVs; columns are ID, pattern, compile state, 16 match bits, error.
Policy TSV columns follow the expanded audit's format, retaining protobuf input,
result, warnings and typed diagnostics. Grammar error text is retained but only
compile state and matches are compared. Policy comparison checks error details
only when the Rust replay exposes a typed SQL error; untyped errors may conceal
additional differences. Both compare warning contents, not just counts.

Run `python3 rust/docs/planner/grammar-policy-audit-20260929/verify.txt` to verify
checksums, unique/equal case keys, counts and the prior matrix comparison.
The executable text harnesses are audit artifacts, not production tests.
Copy them to the temporary filenames mapped in `verify.txt --prepare`, then run:

```sh
python3 rust/docs/planner/grammar-policy-audit-20260929/verify.txt --prepare
GOTOOLCHAIN=go1.25.14 go build -o /private/tmp/regexp-grammar-oracle /private/tmp/regexp-grammar-oracle.go
/private/tmp/regexp-grammar-oracle /Users/qiliu/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.25.14.darwin-arm64/src/regexp/syntax/parse_test.go > /private/tmp/regexp-grammar-go.tsv
python3 /private/tmp/run-regexp-grammar.py
python3 /private/tmp/run-pb-policy-rust.py
python3 /private/tmp/compare-pb-policy.py
python3 /private/tmp/run-regexp-sql.py
```

For fresh Go expression observations, run from the archive directory:

```sh
GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/pb-policy-overlay.json' ./tools/check/failpoint-go-test.sh pkg/expression -run '^TestRustPolicyPB$' -count=1 -v
GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/regexp-sql-overlay.json' ./tools/check/failpoint-go-test.sh pkg/expression -run '^TestRustRegexpErrorPB$' -count=1 -v
```

The Go failpoint wrapper returned its refcount to zero after each run. Adjust
absolute reference/toolchain paths on other hosts. Rust runners temporarily
insert probes and restore source in `finally`; run with no concurrent edits to
those files. Capture tests passing means observations were collected, not parity
passed. The runners invoke these exact Rust checks:

```sh
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib audit_go_grammar_tables -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-unistore --lib audit_policy_pb_matrix -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --test all audit_regexp_sql_boundary -- --test-threads=1
```

Previous-matrix revalidation used `python3 /private/tmp/run-pb-expanded-rust.py`
and `python3 /private/tmp/compare-pb-expanded.py`, whose preserved harnesses and
exact Cargo commands are in the expanded audit. The nonexistent standalone
`regexp_family_source` Cargo test target was initially attempted, then corrected
to the repository's `all` target. An initial Go `go run` invocation incorrectly
treated the corpus's `_test.go` argument as source; the successful commands above
build and run the oracle separately.

No production code changed, so no new `make lint` run is claimed. Both mandatory
Rust server gates still apply to these rust/docs artifacts:
`cd rust && cargo build --locked -p tidb-server` in the pre-commit hook and again
immediately before push. Validation logs retain successful collection excerpts.

Not verified: exhaustive regexp inputs, every package/platform/generated variant,
all session modes, distributed transport, planner reachability of every PB case,
end-to-end Go SQL server execution, full package parity, or
sysbench/TPCC/TPCH/YCSB performance. The documentation has no runtime effect;
reported correctness, protocol compatibility and performance risks remain open.
