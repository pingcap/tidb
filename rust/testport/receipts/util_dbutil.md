# `pkg/util/dbutil` — Go-master package boundary receipt

Go source: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package and its
nested test helper have no source delta from extraction pin
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

All seventeen artifacts were read in full before deciding ownership,
including the nested `dbutiltest` package and every SQL-mock/table/index/
retry/variable test. There is no package `doc.go`.

| Artifact | Lines | Git blob | SHA-256 | Inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 62 | `c6880117717d4f5f6d635b90f0615c090deed09d` | `7cb2c1cba99d0f8aad4341338de14ff1ee438c311ff69063d0acd9546571ac34` | public library and flaky SQL-mock test target |
| `README.md` | 2 | `0026cc61c0dcc8f1f85a7a57367f62c35050e1a7` | `55b7e04d18b9147c4641a59815ce0d9fee5e290407ab5cb2362675986b214398` | package description |
| `common.go` | 889 | `53a5e495ed4d9758080557a811fe4db00d2165df` | `a77ea4fa04239b22aba050b0873be7c1732815b5202a9f8e43a7b1b55e707c03` | DB config/open/close, table/schema/version/time-zone/statistics queries, SQL retry/transaction helpers, escaping and row deletion |
| `common_test.go` | 257 | `0187059d7e49d9bced14aaf7dc3a420c0c016ab0` | `e2e886b3b178b5364d9f6fee7b146b68fa5cdff0b39058c116101ab30c5b4410` | placeholder/name/error/delete/parser/bucket/time-zone tests |
| `dbutiltest/BUILD.bazel` | 18 | `455cfcd851df9db11874bdd9304c8d74faead34b` | `3d4a19bc0d125948329db31111ea38b06be89366d2a4cd87e7b0ca1ef64cf4d7` | nested test-helper target |
| `dbutiltest/utils.go` | 67 | `d1ad5e1376aada7bc47b8190fc9230eb4d3ecf89` | `2a0ebd4db10d750e34e0f3356bbf6676efb8b0c4cbc2f5181a0b734a704ab4e0` | AST-to-`TableInfo` test helper with synthetic primary index |
| `index.go` | 209 | `1155ea3551c99483a967f953b6370b993fccf59b` | `93d1cf78e34690b6d1467e823dbeae372634127835badbdfce6992b6a2a5dfee` | SHOW INDEX loader and primary/unique/cardinality index/column selection |
| `index_test.go` | 103 | `16bc85d7ba5b7dcf40db276e2a0b9f3818ee516c` | `83f8bbc2eb4ab59bcd3aafeb8a10f33b425a342ca99df872503760352dc2cc30` | index ordering and indexed-column cases |
| `interface.go` | 43 | `999393b5a6d42142468bc824e354d165cbab29a0` | `b711bbc29cbb6fd3435e75b7e17df16591e6785ee5fdd3d519ea3ab7313f24e7` | `QueryExecutor`/`DBExecutor` interfaces and `database/sql` compatibility assertions |
| `query.go` | 78 | `3d6f790c1ab3d58cd3fee90884a8ae55de87e4ad` | `5b7589ee5234bc2369346b825b605129eeb4d1f8bc594e33d4f3fac4cb5eae29` | `ScanRowsToInterfaces`, `ColumnData`, and named `ScanRow` conversion |
| `retry.go` | 73 | `d37e8f32fde0c35fa132d72f6f17fc49c82ac292` | `e62d7da09629745c7bfa0db896844d500609f0c299aad772766218a18c55f1c7` | TiDB/MySQL retryable error allowlist and legacy 1105 message matching |
| `retry_test.go` | 132 | `04228f9f6ade7117f1908200b850e2b1bfca46d3` | `6a8fb3e8a2fff5bd85206c883c75d5a990f53b3bb5ef31b54d9f05568617d678` | retryable/non-retryable error matrix |
| `table.go` | 53 | `b0660c5fe24e68e0a1a759c7777bab07538e613e` | `190ba8f54fca06484fe6dbec58c97fedcefd10df7ce8df7ca6b7b5218b575c14` | case-insensitive column lookup and normal-table-mode guard |
| `table_test.go` | 214 | `ae3d5671879ca6a1bbeb7ca2da3f41e850d50399` | `5592d0b359527da5d2db13c96be2637804853790dfbdcedb4be4a20a0a66022d` | table/index shape, structural equality, and schema-cmp encoding cases |
| `types.go` | 47 | `e129a63faad28d9a598e4575b505c02f1f98b3da` | `b38fb56f55530c7f44091252023c38020fefc05599976d1d2141e0165d7bf55b` | numeric/float/time-type classifiers |
| `variable.go` | 155 | `ffe8947eeebb242522c459409d733a23b387afa2` | `87956c0320b0619d2d1748e8ebb1846193334e2058687dc1e53605538098e4f0` | SHOW global variables, server ID, and role-aware/masked SHOW GRANTS |
| `variable_test.go` | 116 | `1b640cd172554828239704d3f8ffd7613d7d6c42` | `83301ebcd4ae05598b5b66b67a8003308e5bd6102cde8d6fb257f1a72895a174` | current-user/role/password-masking SHOW GRANTS cases |

The boundary has 2,518 Go lines (including nested helper/build files), 55
production functions in `pkg/util/dbutil` plus one nested helper, 16
top-level Go test functions, and no fixture/testdata, generated output,
platform-specific variant, benchmark, fuzz target, or additional package.

## Go behavior and consumers

This is a broad auxiliary layer consumed by BR, Lightning, TiDB tooling,
statistics, schema comparison, and tests. It owns MySQL connector setup with
snapshot/session parameters; context-aware SHOW/SELECT/DDL/statistics helpers;
SQL identifier escaping; transaction execution and retry classification;
bucket/time decoding; index ordering and suitable-column selection; role-aware
SHOW GRANTS normalization; and table-mode/type predicates. The package's
contracts include exact SQL text, null handling, retryable error codes and
legacy 1105 messages, synthetic primary-index handling in `dbutiltest`, and
stable PK→UK→normal-index ordering.

## Rust ownership and decision

Rust contains independent owners for SQL parsing, privilege/SHOW GRANTS
execution, statistics-bucket presentation, table metadata/modes, transaction
retry classification, and TiKV/PD transport. Those fragments do not form one
dependency-closed `dbutil` utility layer and do not expose the Go package's
`database/sql` interfaces, MySQL connector, SQL string/error contracts,
schema-comparison test helper, or cross-tool call graph. A new Rust facade
would either duplicate server behavior or force an unsafe broad migration of
BR/Lightning/tooling consumers. No Rust-only behavior was found and no safe
missing Go behavior can be implemented in isolation. The complete package
remains explicitly unclaimed; no production Rust change or duplicate
regression carrier was added in this boundary batch.

## Validation

Profile: WIP for the continuing repository audit; no source or build artifact
changed.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/dbutil/... -count=1` — passed (`pkg/util/dbutil` and nested `dbutiltest` compile; all 16 tests pass).
- `git diff --stat e2788410d8d696605e8cb002585877a063ccc909..origin/master -- pkg/util/dbutil` — empty; source is unchanged at Go master.
- Rust search across session, executor, model, stats, transaction, and BR crates found only independent fragments, not a dependency-closed owner.

No Go or Bazel file changed, so `make bazel_prepare` is not required. Live
MySQL/BR/Lightning consumers, SQL retry under injected transport failures,
and every SHOW/DDL version matrix were not run locally.

## Risks and unverified scope

- Correctness: SQL strings, NULL scans, time bucket decoding, retry allowlists,
  and index ordering are externally observable and must remain source-shaped.
- Compatibility: preserve backtick escaping, role/password normalization,
  exact error text, legacy 1105 compatibility, snapshot DSN parameters, and
  `dbutiltest`'s synthetic primary index when this boundary is eventually
  ported.
- Performance: no runtime path changed.
- Not verified locally: external BR/Lightning callers, live database behavior,
  network retry timing, and database-version-specific SHOW result shapes.
