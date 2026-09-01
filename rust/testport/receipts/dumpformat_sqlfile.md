# `pkg/dumpformat/sqlfile` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly four tracked artifacts and 318 lines. Every file
was read in full in a detached worktree at the pinned Go commit before any
editing. There is no fixture/testdata directory, generated or platform-specific
variant, fuzz or benchmark input, or generator source.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 25 | `1f33b4dfd50b1e79470093c9d3c8298995b1115a` | `92a1282d8349b4cba6d21b5f463802c20b6e9ea9459e424cce8a89cee86848dc` | public library and four-shard flaky test target |
| `sql.go` | 94 | `ab8c50b07f776ed4d242b2d4319a7d48d6eda9d4` | `e674c9aff66c5c9eb126a2cd782d2cec93fb689c1db52421ddb3d6f0d4870eab` | NULL/number/string/hex-value framing and SQL escaping |
| `writer.go` | 115 | `65e7f38e862a279361a9a8724558580b24887870` | `dcb42c98ad6806871baa3b1e6b1b365f9ef450b2e8ddd00fe6abf65c2c93ab02` | INSERT tuple streaming, statement splitting, size accounting, and close |
| `writer_test.go` | 84 | `df3671b811ff838457eefb82b8c0423d9ce0c4e4` | `0f0cbf6ea44f1d3e238ec16de9dbfc648f25523cba65a98201eb604affcf7cd1` | four framing, escaping, split, and empty-tuple tests |

The production surface was audited function by function: `AppendValue`, the
single-quote and backslash escaping helpers, `NewWriter`, `Write`,
`EstimateFileSize`, and `Close`; the SQL NULL, numeric, string, and hex-byte
contracts were checked. The four tests cover tuple framing/NULL/hex values and
byte counts, all MySQL backslash escape bytes versus quote doubling,
statement-size splitting, and empty tuples.

## Rust ownership and parity decision

Rust's `tidb-util::sqlescape` formats SQL arguments, but it is not a
dependency-closed dump writer: it has no `FieldKind` row stream,
`StatementSize` splitting, prefix/file-size accounting, or dump-format caller.
Parser support for `FORMAT 'sql file'` likewise does not own output behavior.
No Rust-only behavior was found to remove, and no speculative SQL dump writer
or ignored carrier was added.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The exact
Go-master package suite passes in a detached worktree:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dumpformat/sqlfile -count=1
# ok github.com/pingcap/tidb/pkg/dumpformat/sqlfile

git diff --check
```

No Go source, import section, test, Bazel target, or module dependency
changed; `make bazel_prepare` is not required. Rust tests and a full workspace
build were not run because this package has no dependency-closed Rust owner or
changed Rust source. SQL dump output compatibility remains unverified on the
Rust side; that is a compatibility risk explicitly retained by this boundary
receipt.
