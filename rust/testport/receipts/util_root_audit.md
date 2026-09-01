# Root `pkg/util` — Go-master inventory and parity boundary receipt

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The root package is
cross-cutting and is not dependency-closed in one Rust crate. This audit read
every root-level production file, test, platform variant, and build input
before the scoped mock-PD URL fix below.

## Complete inventory

The inventory is 33 tracked artifacts: one Bazel target, 20 production Go
files (including four platform variants), and 11 Go test files. There are no
root-level generated files, examples, benchmarks, fuzz targets, nested Go
packages, or checked-in `tls_test/**` fixture files despite the BUILD glob.

| Artifact | Lines | Git blob | SHA-256 |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 107 | `e678d92f69640d215366760be2b4d7b2aab420ea` | `02ac5a784140040abae33aceea26cf3fc847e93ef6a618a27dcb1a4453fdfc3c` |
| `cpu_posix.go` | 39 | `76c74021e21f684a6b162d89d695a4637cea94ef` | `5aef6a39254c93621f3f527e9238b62fbac76fa2692784074a6e59b59bc49031` |
| `cpu_windows.go` | 45 | `9d2bea0babac161e6477667c8f1074b9f0640690` | `29c91cb69800828a4a9e905b5cc7d80dd75fd1e746b10ecb8bea695ca3f4b163` |
| `errors.go` | 31 | `c85828be0b0bc5c214130599ee75f59cbf33ef91` | `3d9286cdffbcc4dca8fde03a876581ec6212ec3a3fb7eb3b3f3733d01d0a508a` |
| `errors_test.go` | 36 | `d75e41855a6ef07d8c9eaf8a9933f601b2184d04` | `5bc713f4eb364c1aa39cb67d5e52475095bf02e80d310289f0ebed7ed80bc011` |
| `etcd.go` | 110 | `480ee668292886d2c5731ae67de7c2ebd920549a` | `c61b1dd5bf63f22172ded928403e27e2723fd915704bfe3c7f52c40835f2c269` |
| `gogc.go` | 50 | `85e34dc96c0aaee6798873ffeefbe221509da7a0` | `43d31638d50e6da7e985f93c5415087310dd01fd1f09c882915c9f8a8a0dd2b2` |
| `id_generator.go` | 27 | `c89d9d88ec2b3ff9bf45a1f2ed47164e2505f7f7` | `9a53695c8936739032ea8e3530913b73400af0e240ba944c5d8abd064da0dc57` |
| `main_test.go` | 34 | `bbc8dd073a68ea4fa9bf1bd97b8dc4006f1133f2` | `b6f6394b5de06f4a1a095e0c070636184ce1b0ae918c7ab15287680ec32aa205` |
| `misc.go` | 651 | `88665ebb167a8ea949710608c1ac4c1340a8983c` | `0da134d5a166296f15aa8171e2de02e9457e606c0de44689d1e9024df59fd461` |
| `misc_test.go` | 190 | `fde66ae7575a93f85b5ef28bfd74c99b8f4f1285` | `78968a4db4a27077efe861b0a1fdc3b8436d374420dcf2a3c62b52badc180b72` |
| `prefix_helper.go` | 88 | `2e9e3db5924b6a07d85bc12d27f0bd04dfd99c8b` | `8728c0ff3354b7fab4b2ceb0d870016c13210b1789467715728c7a4e9b7bdd2d` |
| `prefix_helper_test.go` | 141 | `809046701ac507609a8d32754212b548e0f5cfb7` | `d32ae8ec4d7a1ebf4f08afc7174c271ed0664a82946a36d2ef9880c01f607c38` |
| `printer.go` | 56 | `56724bc1ff664a7fc5fdf74281e2fe07308d73b7` | `a78ce99fbe8915005f16023b61bd9c859465fdb12f4ca6a184f14de97d8370f5` |
| `rlimit_other.go` | 38 | `4a6ef7e6d2713861d41223978cb33dabd73a06e9` | `bc875d264338b13040239453e48a6cb66a390cad419fa4b55e04d06989d092e3` |
| `rlimit_windows.go` | 21 | `6a628a60a4a941eb54bceeb7eab8f1cb2b979f8a` | `b4d65e6e2a73b187e9bf0f0c72e445f4abff4b48d88e33d423d5aca175795412` |
| `security.go` | 370 | `28511cfdbe9507d8c003c299cee305e7345022d7` | `179844746ef3f7d6eb4c1db50d15ec9921e447bcc0eb618810baad8a56ec9ec6` |
| `security_test.go` | 387 | `036bb7ea9f22469700e905fac181e41855b9d83d` | `b1621aba86f6365cfc5b825bcd10ec8001e06af8d5fb0f77a616fb7ab833a71a` |
| `service_url.go` | 141 | `2d4c88816d1e27f6360641885db0fd6a96c766f9` | `5cb39ec6177dfd4cc37d7bd05d514651a6ae148b1034e95a589d60b799bc6b35` |
| `service_url_test.go` | 120 | `3c97ed21dae74d2ac055cf03661d472938130b55` | `96fc0038999edce288dade0f94ba1cdb53f0238d218214857af267ff9e934687` |
| `session_pool.go` | 131 | `7cca0b74c36e03f56e126fb2c9e9a9365e356de6` | `253764bd6f4bb5c620ff05382adac713726a57ac0835ec6830abce005039898d` |
| `session_pool_test.go` | 70 | `99de5d0cf7e0bf9f7093448e60dad4648b037c29` | `579df2750855ab21cfeb59d540197d749cae10487d0de28f81c78ed6d2d7f677` |
| `split.go` | 73 | `fef402d4fda9fc950a430f5b36af98f30bb97f7a` | `ce8861a0cb4397db6e18056f1c6a597bccd0badfa426064872dc4021e8e1e9b5` |
| `split_test.go` | 69 | `050e7be38a8e4136326328d2f3630578e501eed8` | `c5790e54d34ec713100491d59a5b6e1d5f04558b0deb7e04e58ab8c9f13338fe` |
| `tokenlimiter.go` | 45 | `d82737de9860a235fcbea9c50ed55673c0d5af60` | `5919efc039808639354d2d104fd741b97548f1d00257480b81098a42d4e744f2` |
| `urls.go` | 38 | `48a4d2e2a60d9e13888c669dc4b7494a86da6911` | `072b19cda3d33f258ff0d5ce6b06faf71ac53dcbcd20abbd2b3a6be1dbb28cee` |
| `urls_test.go` | 70 | `8be2d84de32f713b15ca100b5c46141bf860596b` | `4bdcfa3f8e26cb88d8f995f39335617f686f69c93e69023e650fbe7dfbeddd30` |
| `util.go` | 371 | `25d8b3b66c966014acb5f7193b0af3f4c2fcaeed` | `04146723a3df9b0711ef6d1e2da5e5d9451bfac31a8371564525354b80c0e61a` |
| `util_test.go` | 150 | `24cb7f7d94b32c92a7b0330a2c943bb4158ab2a1` | `c44446dbcdf2934126196e133ffd6dd84b329bd28ce2c4b419a35a87519b5f52` |
| `wait_group_wrapper.go` | 265 | `3fccd66810bd095ea2fb90d2b5412fb62d270318` | `b37f630fd1af804330931780996b4478299fce7a99f6658d474987146b8b095f` |
| `wait_group_wrapper_test.go` | 145 | `561fe7e6d0e931e36b7d8ff0dc3c34e52ae63440` | `e38a3ac26bc93c3f544fd537b27467651db4ea163faa3abae72dd764a23f6066` |
| `worker_pool.go` | 117 | `4ff46ea1a5f630488cfdfc2d6d9cd32aa52884fe` | `34c95259970d1404b34853332991c4e7b65fdbce552698e7e49824ff080a138e` |

The Go sources total 3,978 lines and 118 production function/method
declarations. The tests cover retry/error unwrapping, X.509/TLS rotation and
versions, metadata prefix scanning/deletion, service URL parsing, session and
worker pools, key splitting, URL lists, log formatting/proto cloning, and
wait-group panic/recovery behavior. `BUILD.bazel` carries the 50-shard flaky
test target and its `tls_test/**` data glob; `cpu_*` and `rlimit_*` are the
platform-specific variants.

## Go behavior and Rust boundaries

The root package combines process CPU/RLIMIT and GOGC metrics, etcd session
retry/lease formatting, panic/recovery and retry helpers, X.509/TLS creation
and certificate rotation, metadata prefix operations over `kv.Retriever`,
session/token/worker pools, key-split arithmetic, URL normalization, logging
and protobuf helpers, SQL type flags, and wait-group/error-group lifecycle
wrappers. These functions are consumed by server, domain, DDL, executor,
Lightning/BR, PD, session, and storage code.

Rust has supporting slices (`tidb-pd-client::security`,
`tidb-server::mysql_tls`/`node_config`, `tidb-txnkv::iteration`,
`tidb-timer`, and the existing `tidb-util` utility modules), but no single
dependency-closed root-package owner. In particular, Rust has no equivalent
for Go's runtime GOGC/CPU/RLIMIT globals, etcd session lifecycle, TLS
certificate rotation, SQL log-field assembly, or the full goroutine/wait-group
instrumentation. A detached replacement would create Rust-only behavior and
violate the package-atomic rule.

The current hparser worktree predates Go-master's `service_url.go` and
`service_url_test.go` addition and has the older `urls.go` implementation; the
baseline inventory above intentionally uses `origin/master` as requested.

## Scoped fix and regression

The existing Rust mock-PD test helper in `tidb-txnkv::unistore` was a concrete
Rust-only behavior at this boundary. It accepted `ftp`, `tcp`, `udp`, `ws`, and
`wss`; rejected Go's opaque `unix://`/`unixs://` endpoints; did not trim input;
and stored bare addresses unnormalized. The helper now follows the Go
`NormalizeServiceURL(addr, "http")` contract for HTTP/HTTPS host:port and
opaque Unix-family addresses, and stores normalized URLs. The new focused
regression exercises whitespace, both Unix forms, invalid Rust-only schemes,
and normalized service/client lists. This is a test-harness alignment only;
the root production package remains unclaimed until its runtime and server
consumers can be ported atomically.

## Validation

Profile: **Ready** for the scoped Rust behavior fix (targeted owner test,
formatting, repository lint); the broader root package remains a WIP boundary
audit and is not claimed complete. No Go/Bazel source changed, so
`make bazel_prepare` is not required.

Baseline regression (before the fix):

```text
OPENSSL_DIR=... DYLD_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --offline --locked -p tidb-txnkv --lib \
  mock_pd_service_discovery -- --test-threads=1
# test_mock_pd_service_discovery_matches_service_url_parser FAILED:
# old helper returned ["ftp://127.0.0.1:2379", "ws://127.0.0.1:2379"]
```

Passed after the fix:

```text
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --offline --locked -p tidb-txnkv --lib \
  mock_pd_service_discovery -- --test-threads=1
# 2 passed

cargo +nightly-2026-08-22 fmt --check --all --manifest-path rust/Cargo.toml
# passed

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
# passed
```

## Risks and unverified behavior

- Correctness: the scoped URL regression passes and removes the extra Rust
  schemes; HTTP host/port validation is intentionally limited to the mock
  helper and does not claim a production URL parser.
- Compatibility: Unix-family addresses are preserved as opaque strings, as in
  Go; the larger root package's TLS, etcd, pools, runtime, and SQL consumers
  remain outside Rust ownership.
- Performance: no production runtime path changed; the helper only affects
  test-only mock service discovery.
- Not verified locally: full root `pkg/util` Go tests, Bazel's 50-shard/race
  target, Windows platform execution, live etcd/TLS certificate rotation,
  remote/server integration, and a package-complete Rust root-util owner.
