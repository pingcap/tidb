# `pkg/util/profile` — Go-master parity boundary receipt

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package is
runtime/pprof- and server-integration-bound; no Rust crate is currently a
dependency-closed owner. This receipt records the complete inventory before
any porting decision.

## Complete inventory

All seven Go-master artifacts were read in full, including the binary profile
fixture and the Bazel target:

| Artifact | Lines/size | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 49 lines | `37ec7a716a1f344892eecce1bf86fe9c44f71bd5` | `6d8f82b7ffdbe6a358c1040dd491ab28e2b40b5d75078c09c99901aab0422afa` | library/test deps, fixture glob, flaky short target, and logging/profiler test deps inventoried |
| `flamegraph.go` | 189 lines | `d4de6c33d8d48ad869e3a4f8bec7400ddffb4975` | `cbffc9b2d38c2c2c2732e7082f776ff3d10a94edb5a69f9b5010d94af89c8bff` | production flamegraph DAG and row conversion inventoried |
| `flamegraph_test.go` | 104 lines | `71ad775b75fda55453a66674f60005655f2cdae5` | `e5faf55fc2ff5936541cca8016c9d77b69098fc9eb140bf1c389fce39c8471c6` | pprof fixture conversion test inventoried |
| `main_test.go` | 33 lines | `350b9aa02484eb1140fbc656d9f060f114a3641b` | `608ea9bcd1364f660c923bd68ea51db8b0c422d1acc960e992c4118dcca15cff9` | common test setup and leak exclusions inventoried |
| `profile.go` | 154 lines | `e11235446e99e59edb31ac3b38503ef1877ce633` | `3d896aeff593c705ef16c70e6c785120979cace4ac39d7c9436d28a4fe955717` | collector, CPU/pprof dispatch, and goroutine parser inventoried |
| `profile_test.go` | 82 lines | `fce92f3ac6720d106112fe14098518ee661a0792` | `0c0e61604071df0d105432f2e03eb1e42674d64c646300e18b760fc42b06b0b9` | six SQL profile-table integration requests and one-log-event assertions inventoried |
| `testdata/test.pprof` | 1,206 bytes | `118d3f3faecf2eee890d4cd0e0719c68e197373` | `892f2aef3b5e42ab0b82d8fac8be5c3063858f3c054f08e02aea5c68fec5680b` | gzip pprof fixture (uncompressed size 2,133 bytes) inventoried |

The textual artifacts total 611 lines (plus the 1,206-byte binary fixture),
with 15 production functions/methods, one production type, one production
variable, two source tests, and one `TestMain` harness. There is no `doc.go`,
README, generated output, platform/build-tag source variant, example,
benchmark, fuzz target, nested package, or additional fixture.

## Go behavior

`Collector.ProfileReaderToDatums` parses a Google pprof stream, converts the
sample stack into a flamegraph DAG, and emits `types.Datum` rows. The
flamegraph collector aggregates the final sample value along each stack,
sorts children by descending cumulative value (then location ID), formats
percentages with the source's 99.95–100.05 normalization window, and renders
function/file-line labels through `texttree`. `profileToFlamegraphNode` rejects
invalid profiles and empty samples.

`ProfileGraph` dispatches `cpu` through the shared `cpuprofile` collector and
uses `runtime/pprof.Lookup` for heap, mutex, allocs, block, and goroutine
profiles. CPU collection starts/stops the process profiler and sleeps for the
30-second `CPUProfileInterval` (two seconds in the integration test). The
goroutine path requests debug level 2 and `ParseGoroutines` splits the runtime
text dump into stack rows with goroutine headers and frame lines. The
infoschema performance-schema owner calls these methods for local and remote
TiDB/TiKV/PD profile tables; the Go-master test also verifies exactly one
`profiling request received` log event per table request.

## Rust ownership and integration decision

No Rust crate provides the dependency-closed equivalent of this package.
`tidb-parser`/`tidb-session` preserve `SHOW PROFILE` syntax and variables, but
they do not implement runtime pprof collection or SQL profile-table execution.
`tidb-server::http_status` explicitly leaves pprof handlers unported (404),
and `tidb-util::memoryusagealarm` only documents runtime/pprof profile-writing
as a Go boundary. The Rust workspace has no shared `cpuprofile` collector,
Google pprof decoder/flamegraph renderer, goroutine text parser, infoschema
profile-table provider, remote profile fetcher, or equivalent test fixture.

Adding a detached parser or flamegraph helper would therefore create
Rust-only behavior without the server/infoschema consumers and would violate
the package-atomic rule. The package remains explicitly unclaimed until the
runtime profiler, profile-table integration, remote endpoints, logging, and
fixture-backed tests can land as one dependency-closed owner. No production or
Rust source change is justified by this audit.

## Validation

Profile: **WIP**. This is a complete inventory and ownership-boundary audit;
there is no source fix and no package-completion claim, so `make
bazel_prepare` and the Ready lint gate are not triggered.

Passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/profile -run '^TestProfileToDatum$' -count=1
# ok

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test -tags=intest,deadlock ./pkg/util/profile -run '^TestProfiles$' -count=1
# ok
```

The untagged `TestProfiles` diagnostic failed only with the repository's
expected `you should add --tags=intest` guard; the tagged command above passes.
No failpoint wrapper is required because this package has no failpoint
injection.

## Risks and unverified behavior

- Correctness: source flamegraph ordering, pprof dispatch, goroutine parsing,
  fixture conversion, and six profile-table requests are covered by the
  focused tests; no Rust owner is claimed.
- Compatibility: runtime/pprof output formats, CPU profiler ownership, remote
  TiKV/PD endpoint behavior, and the exact SQL/logging integration remain
  unported Rust boundaries.
- Performance: CPU collection intentionally blocks for `CPUProfileInterval`;
  any future native owner must preserve profiler exclusivity and row ordering.
- Not verified locally: Bazel execution, race/flaky target behavior, Linux
  runtime pprof output, live remote TiKV/PD endpoints, and an end-to-end Rust
  infoschema/server implementation.
