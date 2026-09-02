# `pkg/objstore/ossstore` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`94eb995357f34b7bab4889a82f0405797046447d` (2026-09-02).

## Complete inventory

The root package contains ten tracked artifacts and 1,902 lines. Every
production source, test source, and BUILD target was read in full before this
receipt was written. There are no package fixtures, platform variants, or
additional generated outputs in the root directory. The nested generated mock
package is a separate Go package and is covered by its own receipt.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 66 | `4d648c5003bddc1306acf706b8424aa094671125` | `e8d730cdc7ef29bf0e58867938e1a5564307f73740393f90f4a99323be3fb1de` | OSS library and 14-shard flaky test target |
| `client.go` | 373 | `841b77b53704f14c374bd46722eb12086b5e5417` | `9dc1e1496e4bae20b28744e08bc88d6c4509def123726539ba825fb5845787b9` | PrefixClient adapter, CRUD/list/copy, presigning, and multipart writer/uploader |
| `client_test.go` | 389 | `577093fb628dabcd0a6025e72522b24d0cbd6322` | `20c22a961719bb3d8dc425b18b5279dbfba42707df7d65f4c4e9b8507f43cd1d` | mocked permissions, ranged reads, CRUD/list/copy, and presign tests |
| `credential.go` | 121 | `f453f9320a810d55adb757b923e6e4ba389df6b3` | `69e29ae434d3eb04ebcdcfe0160a049e86fcbc36ba344f3b04ff095995a7f74a` | synchronized temporary-credential refresher |
| `credential_test.go` | 63 | `4939b6b38f8344aa5b3b73a62bb9f963e47fa92e` | `420e76b40b34496017cdd5382573515054fb197555d3ac856c184ddd65ae5423` | synctest refresh lifecycle and close coverage |
| `interface.go` | 39 | `6248bf9d02020ad87a8a469a7218e65ea5101444` | `a348835840e0900790393ba420e647e56a993cdc8f4ceedf25af0b47641a8d17` | SDK API subset, including presign |
| `logger.go` | 91 | `4fc2b5fbb7bfdd37d665df03527529ca218c5aa7` | `c2c3ef05497674c0788c599df2956c51d18a74a6444ef1db7353aebee5417b42` | OSS log-printer bridge and level mapping |
| `retry.go` | 67 | `3d52270b08ab70e3b50f12d6fc717c007f0f8b9a` | `258f71368f929cf8c5f31a869f7573607016c3faba5fcdfb2bd193378c18483c` | OSS SDK retry configuration and ECS metadata classifier |
| `store.go` | 282 | `7904cb71210b4e4eab0d82d22a87ea51f9ff582c` | `bed569609c4d1de1f8341fa87f7c63c2412a0ab467d24e17dc5b22c317994f62` | OSS storage construction, region discovery, credential forwarding, and presign client |
| `store_test.go` | 410 | `964ae821e04ff8fba21ef92e15f53f248a471ddf` | `29faa972388989ed59c7efd57449d88b6cf212a91f8ba1fc361810800be896b4` | skipped live OSS CRUD/walk tests and local endpoint/credential helper regressions |

The production sources contain 41 functions/methods. The three test sources
contain 14 top-level tests plus three local test helpers. Tests cover all
permission branches and service errors, object ranges and key normalization,
batch deletion, list/copy behavior, presign endpoint and expiration routing,
virtual/inner endpoint selection, temporary-credential refresh, credential
forwarding, and the live OSS CRUD/walk workflow (explicitly skipped without
real credentials).

The current Go-master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` was read in full. The BUILD target
increases from ten to fourteen shards. The SDK API and client gain presigning;
the store constructs an isolated public-endpoint, logging-disabled presign
client, validates positive expiration through `s3like`, and forwards current
credentials only when `SendCredentials` is requested while retaining the
legacy scrub otherwise. Focused tests cover the presign request and public
endpoint, credential forwarding, no-forward scrubbing, and provider errors;
the generated API mock gains the corresponding method. No platform or fixture
delta accompanied these changes.

## Rust ownership and explicit boundary

Rust has no dependency-closed Alibaba OSS owner, OSS SDK adapter, temporary
credential refresher, multipart protocol, or provider-specific endpoint and
permission implementation. Rust object-storage references are limited to
unrelated plan-replayer contracts and cannot replace this package's cloud
consumer surface.

No Rust-only behavior was found to remove, and implementing an OSS backend
without the Go object-store interfaces and SDK lifecycle would be speculative.
This package remains an explicit parity boundary.

## Validation and risk

Profile: **Ready** for this bounded Go compatibility batch. The package uses
no failpoints.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/objstore/ossstore ./pkg/objstore/ossstore/mock -count=1 -vet=off
# passed (live-service tests skipped)

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/objstore/ossstore -run '^(TestClientPresignObject|TestSetBackendCredentials|TestSendCredentialsIsSupported)$' -count=1 -vet=off
# focused regressions passed; the pre-fix presign probe failed to build because
# client had no presign service field

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
make lint
# passed

make bazel_prepare
# attempted and blocked: bazel executable is unavailable

git diff --check
# passed
```

Not verified here: live Alibaba OSS credentials, cloud endpoint behavior,
Bazel's 14-shard target, Windows execution, or full-workspace tests. No Rust
validation was applicable because no dependency-closed Rust OSS backend exists
to exercise.
