# `pkg/ddl/notifier` parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly eight tracked artifacts and 1,999 lines: four
production files, three test files, and one Bazel build file. All artifacts were
read in full before editing. There is no package `doc.go`, fixture directory,
`testdata`, generated source/input, platform variant, benchmark, fuzz target,
or `OWNERS` file.

| artifact | lines | Go-master blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 59 | `ca17f30d6a6015d0049e45bac11dd0591bfd3d08` | `773af2aac4840aa83566e513fb1af9e9fd41c19c2ccd29e234c53b7b798f8393` | public notifier library and 12-shard flaky test target |
| `events.go` | 530 | `96686b2e3f655092066531f93ef2341af1c18022` | `944c77659e4135d1ada46e3e69ec6b0183c728284d6b3aac9a3e3ec91b65c518` | schema-change event model, constructors, getters, and JSON wire representation |
| `events_test.go` | 70 | `fd412723a3aee25e7326ba3c0653743e4815d05a` | `40162ec77db5b5be18720edaafb6f4e2cbabfbcd3fd97529b590a9733c7b4d28` | event string rendering regression |
| `publish.go` | 51 | `88289c0be1cfd688fdb7844da67dac43f4ce4145` | `ca73a038b719a0a5b267a93681f78b861407a84a5b5eb6588c8e4e6eec4027a0` | transactional schema-change publication |
| `store.go` | 235 | `047919a23222fe4d6ea360cef99dc90d5e5a323f` | `934524d841b9be3d3073e2a3e3fdf3657d815815b16a64f8dbcc0732983fac53` | SQL-backed persistence, pagination, decoding, and deletion |
| `store_test.go` | 80 | `3aee8879ebc70c13b0a9cd0e337c0e3d0cd51483` | `b62e7caa42530177649f60b4ccee246b0af36019df8d03d557be05b50c5fc8fb` | reused-row JSON decode regression |
| `subscribe.go` | 357 | `6f5e76ff75e4b832eaaed9b34238a7a33fe98c6f` | `80f899a4765d7f7c6d5f8750751bdf77001c4518e55a459077c9a0150c6dc992` | owner listener, handler delivery order, retries, and cleanup |
| `testkit_test.go` | 617 | `2758d23f898afaa50528eda5699e95f02518f2ef` | `454de08be8d5ac7836679ef288f08c7748cd21311044445f7848343f7ff79358` | end-to-end publication/subscription, pagination, ownership, and transaction tests |

The production inventory contains 71 declarations; the test inventory has 12
top-level tests. The current package now matches Go master byte-for-byte: the
only delta was the cleanup eventual-wait timeout in `TestDeliverOrderAndCleanup`,
changed from one second to five seconds to match the upstream reliability
contract.

## Native integration decision

The notifier is Go-native infrastructure coupled to TiDB SQL sessions,
transaction semantics, owner election, persistent JSON schema-change rows,
failpoints, and handler registration. Rust has no dependency-closed DDL
notifier, SQL-backed event store, or owner-listener equivalent. No Rust-only
behavior was found to remove and no speculative Rust notifier was introduced.

## Validation and risk

Profile: **Ready** for this test reliability fix. The focused regression passed
with failpoints enabled and disabled by the repository wrapper:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/ddl/notifier \
  -run '^TestDeliverOrderAndCleanup$' -count=1
# PASS; ok github.com/pingcap/tidb/pkg/ddl/notifier 2.746s
```

The complete failpoint-aware package suite also passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/ddl/notifier -count=1
# PASS; ok github.com/pingcap/tidb/pkg/ddl/notifier 14.269s
```

The shared Ready gates (`make lint`, Rust formatting, and `git diff --check`)
also passed for this batch. No import section, new Go file, Bazel target, or
module dependency changed, so `make bazel_prepare` is not required.

## Outcome

The notifier's Go-master timeout contract is restored with focused regression
coverage. The complete package inventory and Go-only ownership boundary remain
recorded here while the rolling audit continues.
