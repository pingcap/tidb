# `cmd/importer` — Go-master command boundary receipt

Go source: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The command has no
source delta from extraction pin `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

All thirteen command artifacts were read in full before deciding ownership.
The statistics file was parsed structurally as JSON in addition to being
byte-accounted; it is a checked-in fixture, not generated Rust output.

| Artifact | Lines / bytes | Git blob | SHA-256 | Inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 49 lines | `a7f688613f32c275c9609dae95e6a3f4eb42bfb2` | `8b8d3592e6baeba9d74aacb51823546acf9ecb953e79b233d2d4e6fabdd0edb1` | private library, public binary, flaky short test target, and complete dependency graph |
| `README.md` | 177 lines | `1e0f44e1512c7b0947cfaf7ba84b2a173c796bae` | `eea75e82a833359e550df279fa51fbc06769b18b12ec19f15b9962fd0311175d` | CLI flags, examples, range/step/set/incremental generation rules, supported SQL types |
| `config.go` | 151 lines | `4bbc94217ed2aed73143f1d9651f4822ac6f6a28` | `47d4a61ff9ea123fc9029a98d6eff66f325e0df95c0c6e6859b90640f9dad0be` | flag defaults, TOML overlay, DB/DDL/stats/system config, parse and format methods |
| `config.toml` | 38 lines | `8f3fb3a459eeda50323212203926fb54f7abd955` | `3908c511e03ff6c98b88e29051e078a48a1585cd5545767bb87d65b999075902` | example DDL/index, statistics path, worker/job/batch and TiDB connection settings |
| `data.go` | 153 lines | `4848a4086b6b6a3ac49d8ead0673b08337345d77` | `53448682993317cb2567c05599d74ed221d0d930f0d45eab1023cd85ccff5340` | synchronized incremental datum with repeats/probability and typed next-value formatters |
| `db.go` | 360 lines | `1954529b59b06444d9dff188aaf9b80713043757` | `958963aec0a9d6e6e2e9e2a3cf62667e104c7789d0df63dd0849bc19d27f162f` | histogram-aware/random/unique SQL literals, decimal formatting, MySQL connector and DB lifecycle |
| `db_test.go` | 43 lines | `1d1722a74de4b7d9938132455601415a7d15fde7` | `3219875b5040c2b42344028ae8c10fe2184858c8f372fdcb2b68849edef382c9` | table-driven `intToDecimalString` regression cases |
| `job.go` | 111 lines | `828bd8d6be224dddbfd55a1730f30c57c7e44a4f` | `f8583ab0857129cd9dc491ac3f561a6d2bb75002d87f267f355b8dd20f2e2427` | job channel, transactional batches, incremental single-worker constraint, completion/TPS reporting |
| `main.go` | 89 lines | `84cf16593b4a31cfb756f74bc242679369ca6348` | `6dc2e78cb5bd66d870b7a27f565565ca8555f9305f35c25be7ea304d64824319` | CLI entrypoint, DDL/index setup, optional stats loading, and worker orchestration |
| `parser.go` | 313 lines | `9ef2a538b28d373dc1afe1911fab0b010c46575c` | `888a5b12c2dd4988d346028553011a9ae904b41bf079fd8fb7f6a6b749147fad` | CREATE TABLE/INDEX AST-to-TableInfo parser, comments/rules, unique/index maps, column list |
| `rand.go` | 177 lines | `67d4c9b79c4fa3268ebe0d5bd73d14262c4084b4` | `ca1698ae62dbceb00d9d9be66ccce49e4e6a08bfd0bae009541dd54e5ff0582f` | PRNG, string, date/time/timestamp/year generation with histogram fallback |
| `stats.go` | 151 lines | `51bad8f6f22819ccb667a2c2506d5fd0c6e605d5` | `d399ec9aeefd0a1dd2f369afbaa0059ab4f154c065232ff9e1799c3768d61b10` | JSON stats loader and histogram bound/string/date sampling helpers |
| `stats.json` | 155,728 bytes, one line | `7fda74fe7c8757aa152ff8b3e473c2a65cf7cb50` | `1bde2ecb15bfa7495b8f158e37ede2732cbcca3f2910b417e7b287c22a579e7f` | JSON fixture for `test.t`, ten column entries, six index histograms, 10,000 rows, zero modifications |

The command contains 1,812 Go lines plus the 155,728-byte JSON fixture, 62
production functions/methods (including `main` and config/parser/data/db/job/
stats helpers), and one top-level Go test function. There is no `doc.go`,
nested package, generated output, platform-specific variant, benchmark,
fuzz target, or additional fixture directory. `stats.json` is the sole
non-source fixture and is consumed by the sample `config.toml` path.

## Go behavior and consumers

This is a standalone legacy data generator binary. It parses CREATE TABLE and
CREATE INDEX statements into TiDB `TableInfo`, optionally loads histogram
statistics, generates random or incremental values for supported MySQL types,
and writes INSERT statements through MySQL transactions and worker batches.
The command-line configuration deliberately parses flags twice around a TOML
file so explicit flags override file values. Incremental columns force a
single worker; random generation can use histogram bucket boundaries and the
fixture's table/index statistics. Setup and malformed rule failures use the
source's fatal logging policy.

## Rust ownership and decision

Rust has independent SQL `IMPORT INTO` parser/executor support and BR
restore/SST import machinery, but no Rust binary or dependency-closed owner
for this command's CLI/TOML contract, AST-to-`TableInfo` setup, histogram
sampling, INSERT text generation, MySQL driver lifecycle, or worker policy.
Those paths cannot substitute for one another without changing the command's
user-visible behavior. No Rust-only behavior was found and no safe missing Go
behavior can be implemented in the Rust SQL server without first porting this
standalone command and its Go-specific statistics/DDL dependencies. The
complete command boundary is therefore explicitly unclaimed; no production
Rust change or additional regression test was added in this batch.

## Validation

Profile: WIP for the continuing repository audit; no source or build artifact
changed.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./cmd/importer -count=1` — passed (the decimal-format regression suite).
- `git diff --stat e2788410d8d696605e8cb002585877a063ccc909..origin/master -- cmd/importer` — empty; all command artifacts are unchanged at Go master.
- `jq` structural check of `stats.json` — passed; keys are `database_name`, `table_name`, `columns`, `indices`, `count`, `modify_count`, and `version`; columns `a`–`i`, indices `d`, `e`, `f`, `g`, `h`, and `u_b`.
- Rust search across parser, executor, BR, and workspace binaries found only independent import/restore implementations and no command owner.

No Go or Bazel file changed, so `make bazel_prepare` is not required. A live
MySQL/TiDB run, histogram-distribution assertions, and Bazel binary execution
were not run locally.

## Risks and unverified scope

- Correctness: SQL generation depends on parser `TableInfo`, histogram row
  encoding, and MySQL connector/transaction behavior; malformed range and
  probability inputs intentionally retain fatal logging semantics.
- Compatibility: preserve two-pass flag precedence, decimal formatting,
  histogram bucket selection, incremental single-worker behavior, and the
  checked-in statistics fixture if a native command is introduced.
- Performance: no runtime path changed.
- Not verified locally: end-to-end database insertion, all random/type/rule
  combinations, stale or malformed stats JSON, and Bazel's flaky test target.
