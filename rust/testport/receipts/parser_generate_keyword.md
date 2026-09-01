# `pkg/parser/generate_keyword` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

Go master adds exactly four tracked artifacts and 211 lines. All production,
test, and Bazel lines were read before comparing the existing Rust generator.
The package has two production functions (`parseLine`, `main`) and one test
entry point (`TestParseLine`). It has no fixtures, `testdata`, generated output,
platform variants, or binary artifacts of its own; `parser.y` and the root
`keywords.go` are its external grammar/output inputs.

| Go-master artifact | Lines | Blob | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 26 | `e605907d0ff7029ec845ce09dd202d364acf1ff9` | library, binary, and test targets |
| `filter_mariadb.go` | 21 | `e066365f220fc669cec95a9f2e7300193beab469` | MariaDB-only keyword filter |
| `genkeyword.go` | 149 | `b6d746f7b948099cbaa2994be273dc1b13e849ea` | parser.y scanner and Go catalog writer |
| `genkeyword_test.go` | 15 | `8e3f49fac4a1a616a844e2a65e952066ab76e985` | `parseLine` regression test |

`genkeyword.go` uses the exact Go regexp
`^\s+\w+\s+"(\w+)"$`, resets sections at empty lines, excludes the
MariaDB-only `MONITOR` token, and truncates/writes the generated Go catalog.

## Rust ownership and fixes

`rust/crates/tidb-lexer/src/generate_keyword.rs` is the native Rust generator
and `keyword_catalog` is its generated static output. Before this batch its
catalog contained 685 entries while Go master produces 689; the missing
unreserved entries were `ALERT`, `FAST`, `IMMEDIATE`, and `MATERIALIZED`.
They are now present, `KEYWORD_COUNT` is 689, and the source-derived tests
assert their section and reserved flags.

The Rust generator also now uses `split('\\n')`, matching Go's
`strings.Split(string(parserData), "\\n")`. Rust's `str::lines()` stripped
CRLF carriage returns and accepted a catalog line Go rejects. Focused tests
cover Go's ASCII whitespace/word boundary and CRLF behavior. The explicit
Rust path/`--check` mode and stdout catalog are tooling conveniences for the
native static output; they do not alter SQL runtime behavior or the Go
generator's contract.

## Validation

Profile: Ready for this code batch.

```text
cargo +nightly-2026-08-22 test -p tidb-lexer --test all parser_keywords_source -- --test-threads=1: PASS; 4 tests
cargo +nightly-2026-08-22 test -p tidb-lexer --bin generate_keyword -- --test-threads=1: PASS; 3 tests
cargo +nightly-2026-08-22 run -p tidb-lexer --bin generate_keyword -- /dev/stdin --check < <(git show origin/master:pkg/parser/parser.y): PASS
Exact Go-master detached worktree: go test ./generate_keyword -count=1: PASS; 0.275s
cargo +nightly-2026-08-22 fmt --all -- --check: PASS
Pinned-Go make lint: PASS
git diff --check: PASS
```

The pre-fix source-catalog run failed with `left: 685, right: 689` and the
pre-fix CRLF regression failed because `parse_catalog` returned an entry.
No Go/Bazel file changed in this batch, so `make bazel_prepare` is not
required.

## Risks and next boundary

- Correctness: keyword count, ordering, section, and MariaDB filtering feed
  `information_schema.keywords` and scanner classification.
- Compatibility: changing generated catalog entries changes whether words are
  accepted as identifiers or keywords; the catalog must remain regenerated
  from the pinned grammar.
- Performance: the static catalog remains compile-time data; only the
  development generator parses source text.

