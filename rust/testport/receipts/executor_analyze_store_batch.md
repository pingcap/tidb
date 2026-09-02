# `pkg/executor` Analyze store-batch follow-up

Comparison source: fetched Go master
`78cac443a4f46c13bfe27eb247b5c80657952547` (2026-09-02).

## Inventory and boundary

The complete root `pkg/executor` inventory is the 173-artifact, 101,740-line
boundary recorded in `receipts/executor_user_attributes.md`; its production,
test, fixture, generated, platform, and Bazel inputs were read before this
follow-up. This batch is limited to the five directly affected production/test
files: `analyze.go`, `analyze_col.go`, `analyze_idx.go`, `builder.go`, and
`analyze_test.go`. No fixture or generated input changed.

## Restored Go behavior

Analyze column requests now carry the Analyze plan ID, honor
`AnalyzeStoreBatchSize`, enable serial batch-task merging only when the
Analyze-specific size is non-zero, and avoid forcing handle order for
full-sampling scans that restore order before correlation. Unsigned handles
crossing the signed boundary are split with the Go-master range policy. Index
requests also carry the plan ID and close an already-open result if the null
range request fails. The request-built failpoint and
`TestAnalyzeBuildsRequest` regression pin the request flags, concurrency,
batch size, unsigned boundary buckets, and zero-disables-batching behavior.

## Validation (Ready profile)

The focused failpoint-wrapped regression passes in the detached Go-master
worktree:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex \
./tools/check/failpoint-go-test.sh ./pkg/executor -tags=intest \
  -run '^TestAnalyzeBuildsRequest$' -count=1 -vet=off
# passed: github.com/pingcap/tidb/pkg/executor 2.520s
```

`make lint`, Rust formatting, and `git diff --check` pass for the shared
worktree. `make bazel_prepare` was attempted and is blocked because the local
`bazel` executable is unavailable.

## Risks and remaining boundaries

The request flags now depend on the Analyze-specific session value and plan ID;
incorrect propagation would affect TiKV batching or runtime statistics. The
broader executor package and real TiKV Analyze lifecycle remain outside this
focused source batch, and no full repository or Bazel build was run locally.
