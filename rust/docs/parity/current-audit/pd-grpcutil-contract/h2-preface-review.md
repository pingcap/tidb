# PD transport preface ownership experiment

This living ExecPlan follows `PLANS.md` and continues the complete PD grpcutil
package plan in `README.md`. It is an isolated feasibility milestone, not a
production transcreation or acceptance of a partial package.

## Purpose and context


Resolve the two known failures of the previous h2 candidate without adding a
second HTTP/2 parser in the PD client. Go's pinned grpc transport requires its
first parsed server frame to be SETTINGS, including SETTINGS ACK. h2's existing
non-ACK settings flag has a different meaning: it may become true after earlier
PING or unknown frames. The shared protocol owner must expose the right event
before a PD connection can truthfully publish readiness.

Starting integration is d77a941f466d43e858869003d94b140c6d483645; fetched Go
master remains 93a01d31f6da205ae4bf376825293903a6899fdb. Native client-rust remains
6163ecfc587b248dcbf0e30c1c9d905b4bc5a665. PD source, support, consumers and build
inputs are inventoried in inventory.json; h2 0.4.19's complete published crate
is inventoried in h2-inputs.json. No production dependency is changed here.

## Progress


- [x] Refresh both branches, read the prior rejection and compare codec owners.
- [x] Reproduce the existing eight-pass/two-fail candidate with the Go oracle.
- [x] Implement opt-in first-frame validation in the scratch h2 decoder, retaining
  h2's normal frame parser and default behavior.
- [x] Pass unchanged source expectations and additional fragmented/malformed/
  post-preface/default-policy cases, checked against Go where applicable.
- [x] Record generated observations, remaining package gates, register consistency
  and passing root lint. Re-run the default harness to preserve the rejected baseline.
- [x] Pass the actual locked-build commit hook and fresh post-commit locked build.
  Repeat both gates for the final receipt/patch-artifact amendment; final
  commit/remote verification belongs in the task result.

## Milestones and implementation


First run the existing probe_h2.py unchanged into a fresh scratch directory. It
must reproduce the two documented assertion failures while its Go race/goleak
tests pass. Keep that evidence and the old patch/probe inputs reproducible.

Next add a separate h2-preface.patch against the same pristine h2 crate. The
client builder opts into the source's preface requirement. Its codec validates
the first decoded frame before unknown frames can be discarded, and records
receipt only after normal SETTINGS parsing succeeds. Expose that receipt through
the protocol connection. Terminal transport failure still takes precedence over
readiness. Do not infer readiness from stream capacity or send a probe RPC.

Extend the existing harness with an explicit preface mode that uses the new
patch and an incremental patch to the old isolated probe. The default mode must
still reproduce the rejected candidate. Extend Go comparison tests separately
so original source support and historical test inputs stay intact. Require every
new candidate test to pass, strict Clippy and formatting, unchanged lock inputs,
and both source dependency selections with failpoint restoration and goleak.

## Validation and publication


From TiDB root, run the baseline and then the same command with --preface:

    PYTHONDONTWRITEBYTECODE=1 python3 rust/docs/parity/current-audit/pd-grpcutil-contract/probe_h2.py --h2-source /Users/qiliu/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/h2-0.4.19 --pd-source /Users/qiliu/go/pkg/mod/github.com/tikv/pd/client@v0.0.0-20260805103528-afa43111d149 --output <new-scratch-directory>

The harness runs cargo test --locked --lib, cargo clippy --locked --all-targets
-- -D warnings, cargo fmt --all --check and go test -mod=readonly -race
./pkg/utils/grpcutil -count=1 under both dependency selections. No Go production
source/import/module/Bazel edit occurs; Bazel preparation is not needed.

Run git diff --check and make lint at root. Commit with TERM=xterm git
-c core.hooksPath=hooks commit; its required cd rust && cargo build --locked
-p tidb-server must pass. Run that exact locked build again after the final
commit before pushing origin HEAD:hparser-integration.

## Surprises & Discoveries


The normal decoder discards unknown frames before the settings state observes
them. A settings-only accessor cannot distinguish valid prefaces from invalid
frames followed by SETTINGS. The fix belongs before that discard in the protocol
owner, not in a new raw-byte parser above it.

Both fragmented SETTINGS variants must wait for their last byte, even when the
complete header has arrived. A valid initial ACK makes Go ready, but an ACK with
a payload fails frame validation. The new decoder records receipt only after
the existing frame parser succeeds. Unknown frames following a valid preface
remain ignored; h2's default policy still acknowledges a preceding PING.

## Decision Log


Use an opt-in codec policy rather than globally changing h2 behavior. Preserve
the prior rejected candidate and its assertions. A passing preface experiment
does not certify grpcutil's TLS, options, backoff, cache, interceptor or generated
RPC contracts. Complete package ownership remains the production gate.

## Outcomes & Retrospective


The original two regressions fail on the prior accessor and pass with opt-in
protocol ownership. All 14 candidate tests pass, none ignored. They cover the
original ten lifecycle cases plus fragmented SETTINGS/ACK, malformed frames,
unknown frames after valid SETTINGS and default decoder behavior. The added Go
table has six cases. Original package tests and source-oracle cases pass with
race/goleak under both PD-owned and TiDB-selected dependencies. Candidate strict
Clippy/formatting and formatting of all four patched protocol files pass.

The input patch changes only scratch h2 client configuration, codec framing and
connection-state access. h2-preface-owner.patch adapts the prior isolated probe
and adds its four tests. preface_test.go.txt extends the copied Go oracle.
probe_h2.py selects the candidate explicitly and validates every recorded input
and the unchanged lock. preface-observations.json is copied from the final run,
not hand-edited. The old patch, probe, Go cases and observations remain intact.

The baseline run is /private/tmp/pd-h2-preface-baseline; the final preface run is
/private/tmp/pd-h2-preface-publish. /private/tmp/pd-h2-preface-legacy-check proves
that the updated harness still reproduces eight passes and the same two failures
in default mode. Each directory retains Rust tests, strict Clippy, formatting,
Go race/goleak and failpoint cleanup logs. Root make lint passed with output in
/private/tmp/pd-h2-preface-lint.log. The final patch artifacts use zero context
to avoid trailing-space context lines in committed patches; their applied
protocol/probe sources are byte-identical to the prior passing experiment.

The actual hook's locked server build passed in 0.40 seconds, recorded in
/private/tmp/pd-h2-preface-commit.log. The separate post-commit locked server
build passed in 12.68 seconds, recorded in /private/tmp/pd-h2-preface-prepush.log.
The final amendment repeats the hook, followed by a fresh post-commit build;
corresponding logs use -commit-final.log and -prepush-final.log. The task result
records those gates and the published commit. Optional tools/bin/goword is
absent; no spelling-check success is claimed.

Compatibility and acceptance limits: no native update, production dependency,
SQL behavior change or structural closure occurs. The whole grpcutil package
still requires TLS/reload, backoff/option ordering, GOAWAY draining, connection
cache, interception and generated unary/stream RPC integration gates. The fixed
100 ms reconnect delay in the isolated probe remains test scaffolding, not Go's
backoff. The published h2 archive omits upstream test/CI fixtures, so this does
not certify the complete h2 repository. Linux, ThreadSanitizer, live PD/TiKV
failover and sysbench/TPC-C/TPC-H/YCSB benchmarks were not run. All 77 unresolved
IDs retain their statuses; no performance improvement is claimed.
