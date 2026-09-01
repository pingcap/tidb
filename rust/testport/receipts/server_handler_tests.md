# `pkg/server/handler/tests` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

This test-only package contains exactly five tracked artifacts and 3,630
lines. The complete BUILD file, four test files, fixtures, and harness were
read before editing. It has no production files, generated/platform variants,
benchmark/fuzz targets, or package-local testdata.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 82 | `2e81b679319b62b0a6f5b446f083679e371214c4` | `20d55ed62d3163e78f51e48795d43f31f8d99f90db45619c68ba6bd801f6f63` | 46-shard HTTP handler test target |
| `dxf_test.go` | 673 | `ee247e9af790dc22c510bdbe9e7c7fd94460efea` | `ad0c533ca258c3232b1a0fbe1632202b2b437387113c602d096ea37bcee83e01` | DXF API integration tests, including history redaction |
| `http_handler_serial_test.go` | 879 | `11ae57a9adf8603ab8fa7a00771504ab40902503` | `4abebffce2d813879e0b422fbd8f90f5282525ecaad96fa122ab50d4365036f2` | serial HTTP handler tests |
| `http_handler_test.go` | 1,924 | `c4bfc2b649fdc924c04ad4d187b5442e2da0b153` | `836824f0609f854b11762de921dafd30ddb16b64e6d0180383afc68af356018b` | concurrent HTTP handler tests |
| `main_test.go` | 72 | `e08f866d7768544ec060da92a736a651c2ed884a` | `f92729c7c92ba3e13c7605ada5926e11bd738504e1eb4bbb5c08f84bda621940` | test-server setup and cleanup |

The package has 46 top-level tests in the Go-master source. This batch updates
the DXF history consumer to decode `ErrorCode`/`ErrorCategory` and assert that
raw sensitive task errors never cross the HTTP boundary.

## Rust ownership and parity decision

Rust has no dependency-closed HTTP handler test server or Go session-backed
DXF history API. No Rust-only handler behavior was found to remove and no
disconnected Rust HTTP harness was added. This package remains Go-native
consumer coverage for the storage redaction contract.

## Validation

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/server/handler/tests \
  -run '^TestDXFAPI$/^task_history_api$/^success$' -count=1
# PASS; ok github.com/pingcap/tidb/pkg/server/handler/tests 1.489s
```

The failpoint wrapper enabled and disabled failpoints around the test. This
test import changes a Go test file, so `make bazel_prepare` remains required;
the local gate is blocked because `bazel` is not installed. Ready formatting,
lint, and diff checks are shared with the storage/proto batch.
