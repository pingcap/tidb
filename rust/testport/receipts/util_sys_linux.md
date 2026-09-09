# `pkg/util/sys/linux` — current Go-master package parity receipt

Go source: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package is
byte-for-byte unchanged from extraction pin
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

All six artifacts were read in full, including every platform/build variant:

| Artifact | Lines | Blob | SHA-256 | Inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 78 | `ae4ce4522e7a4f93b2014d1cbcc8967cb03d8e1e` | `76c3e508070ad4ad9599c9a537fb2df7f6334dfe98006742bd99dffc8be07bc4` | library/test targets and the complete OS dependency select |
| `main_test.go` | 33 | `9f013b7c303e68853d24399a20f70d2c5141f9e3` | `467bad1f4a5229e3623901dff392ace1ccb870fc9786077b2bff0bed96b637c9` | common testkit/goleak harness |
| `sys_linux.go` | 78 | `8a2d7ee9165c5a4d42afc72742ca331a53128f2b` | `a4b6bc1faa9819bdc710364a2245a479d36e28b1121ec087023f6e849659381d` | Linux uname string, CPU affinity, and Unix peer UID implementations |
| `sys_other.go` | 55 | `49305e40dc30adaa01605e426437d6dd4250845f` | `7224b359ffccb5ad60e7284f401916eab492378b78da218cb754df9eb4b50013` | non-Linux/non-Windows OS/arch identity, affinity no-op, and Xucred peer UID |
| `sys_test.go` | 27 | `2b492bc560ae0efe4147223fcdf58c0b765dbdd2` | `9a797648b19236c194d25d842d368517a45e25b789159b7c3e7920f154e40f90` | one OS-version assertion |
| `sys_windows.go` | 39 | `428be298a10ebefd0a3cfcd3a9f5bd53137af662` | `0d0cc20277391eae0f1cbf4c7320959df083e89f247ac68ebe50bf0b5853d18e` | Windows OS/arch identity, affinity no-op, and explicit unsupported peer UID |

The package has 310 Go lines and no doc, fixture/testdata, generated output,
benchmark/fuzz target, or nested package. `tidb-server` consumes
`SetAffinity` during server startup; the other helpers are public utility
boundaries without additional in-repository consumers.

## Rust ownership and behavior

`rust/crates/tidb-util/src/sys/linux.rs` is the single native owner and
`tidb-util::sys::mod` exposes it. Linux `uname`, `sched_setaffinity`, and
`SO_PEERCRED` map to `rustix`/`nix`; macOS/BSD peer credentials use
`LOCAL_PEERCRED`; Windows and unsupported targets preserve the Go fallback
strings/no-op/error. The server binary forwards its configured CPU list to
the native affinity helper.

Rust's two additional in-module checks (peer UID and off-Linux affinity) are
platform corroboration of existing production methods, not alternate policy or
another implementation. No Rust-only production behavior was found and no
missing Go behavior required a source edit in this audit.

## Validation

Profile: Ready for this package audit; the repository-wide loop remains in
progress.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/sys/linux -count=1` — passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-util sys::linux --lib` — passed (3 tests).
- Same command with `cargo ... check --offline --locked -p tidb-server --bin tidb-server` — passed (warnings only).
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --stat e2788410d8d696605e8cb002585877a063ccc909..origin/master -- pkg/util/sys/linux` — empty; source is unchanged at Go master.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risks and unverified scope

- Correctness: Linux behavior is exercised on the host; CPU affinity errors
  and peer credentials remain OS-kernel dependent.
- Compatibility: preserve Go's `GOOS.GOARCH` spelling, Unix peer-credential
  error text, and platform-selected no-op behavior.
- Performance: no new allocation-heavy path; affinity and uname remain direct
  syscalls.
- Not verified locally: Windows, unsupported-target builds, and negative CPU
  indexes under the exact Go `x/sys` implementation (the source test does not
  cover `SetAffinity`).
