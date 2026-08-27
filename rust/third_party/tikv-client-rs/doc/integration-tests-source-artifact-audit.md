# `integration_tests` source-artifact audit

This is the atomic completion receipt for the root client-go test package
`integration_tests`, pinned at client-go commit
`52c1e76cec993571493c81de442bcbef90cdc106`. The nested
`integration_tests/raw` directory is a separate Go package and remains covered
by the `rawkv` receipt. The package under this receipt has no production Go
source: its complete inventory is 31 test/support files, two Go module files,
and three cluster configuration fixtures.

## Immutable source inventory

`git rev-parse HEAD:integration_tests` is
`33bbed2a6133b8e5ae555d7359e3d7810b204324`. The 36 root artifacts contain
15,938 lines. The SHA-256 of the sorted relative-path SHA-256 manifest is
`d26e1a9f789b92bc78ae07a1ab0c546611bd931beb5a6d81ea1771da1e55b86a`.

| Artifact | Lines | SHA-256 |
| --- | ---: | --- |
| `1pc_test.go` | 331 | `3cf0eae3a212d2cfea1dcdeedf28f4494eab95723c5b4289782d5af760a1e734` |
| `2pc_test.go` | 2,932 | `6cd9c21669b3ec98966d9dcab60b477836fed88dbd960fc899fed8ec5ba90eeb` |
| `assertion_test.go` | 300 | `53273faa21dcf73d716de2f6e0433f1e5425262177e293a50a94be34a793d983` |
| `async_commit_fail_test.go` | 288 | `74538cb9633bdf9efbc6dcb03147c0d29dc7feeaef2dd461945d800bf352d3ca` |
| `async_commit_test.go` | 676 | `b280851b4a1cac808c41595faf4f667af0c834de4d1d6f6beb0e347312e46af7` |
| `client_fp_test.go` | 116 | `c259961554e5b57b5cdfdcc8b89ac3d496a8b48e17c3c764d94e559835e64fe3` |
| `delete_range_test.go` | 167 | `6399c63252dd0d568885cc2edb558cb4ecf73d90a1574c15a9ce4aa8fa24833c` |
| `gc_test.go` | 345 | `afdfdd8e97fdc93bfbd0eaec21c1d55694611ed46434badfdba13d00dea94750` |
| `go.mod` | 181 | `4fa6425d78775f2a14d6599ee58cc8486b2ada9abdee81e5f3446c8d7c8b49e3` |
| `go.sum` | 2,629 | `e680350c2c13ae6abb0097c1790282120d2e0ccc8b060550499a9a5c5d842720` |
| `health_feedback_test.go` | 64 | `02f37505ed92532ca2e8eec727dfeb0bd1f8c74d25cb3212e156b900654857e2` |
| `interceptor_test.go` | 58 | `aee3a3841a4d4ebfacc315516554e27a0d6a90943adc488e3e20d16394b81d5d` |
| `isolation_test.go` | 230 | `a96d73cd9b08f759c896b6800fba50e7f1a538cb09b06bbb35f1d4fbe4b3f9fc` |
| `lock_test.go` | 2,310 | `30ba11975a921c53678c22c73b0369edf362b466425ff3a163df70d6ea63d6b6` |
| `main_test.go` | 35 | `42da637db1caf1195c06d9098d31ade681250f0204bcb46ff2db7f147157501c` |
| `option_test.go` | 279 | `09ce8ef2edb891c742eaa375e8aa3527448e1b0b0e7b70ca9ffc64866d7b4a93` |
| `pd_api_test.go` | 214 | `925bd3b60a3d9b3ea337283c615c0bb1b84236f97f4a3942b64cd484e61ac10a` |
| `pd_next_gen.toml` | 1 | `43af8281143c608231129ec5b44abee52228cf228408b1d331c152230d6d63d8` |
| `pipelined_memdb_test.go` | 519 | `634d60e5529cb5dbd596260d991b3e84d318c18bbe3250bae26c8bf6c4e6069f` |
| `prewrite_test.go` | 147 | `1bd468b17b324cad6c606ad71e7788ddfa29741488ef0256b8ade516cb71f9e3` |
| `range_task_test.go` | 265 | `2d1eaaa41c420168363a1e3a66dfb1226c21d07f5f8744e1d865942615ca3be8` |
| `resource_group_test.go` | 64 | `7810e0fb357feb8d4644c5155a69172f511397f76a8cdea3501f78ad247e9342` |
| `resource_tag_test.go` | 146 | `1379a39f825fb9af3ede565ba676e74534a940c52fa3d19e2c5795d344ac0415` |
| `safepoint_test.go` | 151 | `7782837935b87a33a040b1478db019de38df2582b65af813521d93e7df0f67f9` |
| `scan_mock_test.go` | 123 | `706a7162dfcfaf80fe704ec9c3547dfb8b370b4e32926c079c10f30caf3ff530` |
| `scan_test.go` | 200 | `dce231e22e7f80eda257d7c5b8d647d43d1048501926f3e37c209793f5d34f70` |
| `shared_lock_test.go` | 570 | `224476fbd5f1b9699bee5116d2e317c86efe27698179b03db5a9fd6c3dcf61d8` |
| `snapshot_fail_test.go` | 547 | `2dddd5e93d11ee5882f3ac249c85c2e68f3261c935986dda6d4c9f25e535c8a9` |
| `snapshot_test.go` | 592 | `037e3406bb569ef8e6fc7357e3e4722c4004126bd27167e6cf1cc104a0fd5177` |
| `split_test.go` | 504 | `441965cfd2b702d00f2e58627cf211a9e48414215c9b4c571e87e4714a5310ba` |
| `store_test.go` | 200 | `34b0045178d1932eb4094732ca782e18da0409363e2b27afc3b8dab3a92bbb3a` |
| `ticlient_test.go` | 229 | `a5e5613e07d83574473be6355f0d73ac61abf3a2085969ede82daf8df3674cb3` |
| `tikv.toml` | 10 | `c915f9747347e5de6c6bc7a7eb1a36d137eb71f4d98cddf00727c6795b79df4c` |
| `tikv_next_gen.toml` | 18 | `6f5f6909109b3d6a8459b9e7354ea3008b6edd6eed3d022037a68973dda85924` |
| `txn_file_test.go` | 288 | `cd98d6a82db4e998a53e2c7955e3afd768d5a903c799222a7c38e66deda3f64c` |
| `util_test.go` | 209 | `3e7475d6539880085d7d997cebc1bfecdb7aeb7435c3d1a9d83e3d9cb41fb369` |

There is no package `doc.go`, production source, generated source/input,
platform-specific filename, benchmark, example, or other root fixture. The
three build constraints are retained explicitly: health feedback and
pipelined-MemDB tests are excluded under Rust `nextgen`, matching source
`!nextgen`; Go's `!race` isolation file is covered in the ordinary Rust matrix
and omitted by the pinned Go race baseline.

## Executable test inventory

Mechanical enumeration finds 181 receiver test methods and 33 top-level
`Test*` declarations. Twenty-three top-level declarations only invoke
`testify/suite` and map to the Rust harness; `TestMain` enables failpoints and
goleak checking and maps to Rust failpoint scenarios plus test-task shutdown
gates. The remaining nine top-level declarations are behavioral. Therefore the
package owns exactly 190 behavioral tests: 181 suite methods plus nine
standalone tests.

Every behavioral declaration has one direct, independently selectable Rust
owner. No source identity forwards to another registered test. Rust ownership
follows the already-complete production package when the Go test is an external
integration test for that package; it is not duplicated under a second test
name.

| Rust gate/owner | Source files and executable declarations | Count |
| --- | --- | ---: |
| `source_go_integration_tests_` | `1pc` 7; `2pc` 63; assertion 4; async-commit-fail 6; async-commit 8; delete-range 1; GC 2; isolation 2; lock 29 methods plus `TestResolveLockWithTiKVSideAsync`; option 2; PD API 3; safepoint 1; shared-lock 9; store 3; ticlient 5; health 1; interceptor 1; prewrite 2; resource-group name 1; resource-group tag 1; txn-file 2 | 154 |
| `source_go_txnkv_txnsnapshot_` | pipelined-MemDB 10; scan-mock 2; scan 1; snapshot-fail 7; snapshot 11; split 3 | 34 |
| `source_go_txnkv_rangetask_` | range-task success and error matrices | 2 |
| **Total** | 181 receiver methods plus nine standalone tests | **190** |

The two `TestBatchResolveLocks` methods in `lock_test.go` belong to different
suites. Rust preserves both independently as `lock_test_...` and
`lock_with_tikv_test_...`; the synthetic suite segment prevents a name
collision without merging behavior.

The 23 suite runners are `TestOnePC`, `TestCommitter`, `TestAssertion`,
`TestAsyncCommitFail`, `TestAsyncCommit`, `TestDeleteRange`,
`TestGCWithTiKVSuite`, `TestIsolation`, `TestLock`, `TestLockWithTiKV`,
`TestOption`, `TestPDAPI`, `TestPipelinedMemDB`, `TestRangeTask`,
`TestSafepoint`, `TestScanMock`, `TestScan`, `TestSharedLock`,
`TestSnapshotFail`, `TestSnapshot`, `TestSplit`, `TestStore`, and
`TestTiclient`. They add setup/configuration, not a second behavioral case.

## Support, module, and fixture decisions

- `client_fp_test.go` has no test declaration. Its `fpClient` injects
  prewrite/commit ambiguity, lock responses, transport loss, and region retry
  outcomes. Rust uses typed `MockKvClient` dispatch hooks, `FailScenario`, and
  in-process mocktikv controls; the affected async-commit, 2PC, lock, snapshot,
  and retry source tests drive every observable branch.
- `util_test.go` has no test declaration. `NewTestStore`, `NewTestUniStore`,
  the injected store constructor, API/config accessors, storage cleanup,
  encoded-key/value helpers, TiDB transaction conversion, and key conversion
  map to `source_integration_store`, public ordinary-build injected clients,
  `testutils`, native keyspace codecs, and typed transaction helpers. No
  single-use Go panic helper is transcreated as a public Rust API.
- `main_test.go` enables source failpoints and checks goroutine leaks. Rust
  failpoint tests use scoped setup/teardown, and all in-process client,
  heartbeat, resolver, flush, and batch workers have explicit close/cancel/join
  gates. The harness does not add a behavioral test identity.
- `go.mod` and `go.sum` pin the standalone Go test module. Rust uses the
  workspace Cargo graph and the exact public kvproto crate; dependency behavior
  remains assigned to each completed owning package receipt.
- `tikv.toml`, `tikv_next_gen.toml`, and `pd_next_gen.toml` are real-cluster
  fixtures. They are retained by the pinned Go real-cluster workflow and map to
  Rust's API-version/keyspace configuration and completed live differential
  matrix; deterministic unit ports use the reusable in-process mock instead of
  copying server TOML into a unit-test-only path.

## Parity corrections exposed by the port

The complete package gate exposed production differences that narrower owner
tests did not catch:

- ForceLock against a shared holder returned a stale failed per-key result
  after the holder woke. The mock server now reissues ForceLock after wakeup,
  while terminal shared-lock and deadlock outcomes retain their source paths.
- Transaction-file prewrite preserved `txn_size` while regrouping but reset
  `is_retry_request` for secondary batches after a primary region error. The
  retry state now enters the secondary slice, producing the source history
  `[false, true, true, true]` across lost response, stale region, and two
  post-split requests.
- Prewrite minimum-commit timestamp calculation now uses the source branch
  order and Go-compatible wrapping increment for start/for-update timestamp
  overflow. The direct integration test covers start TS, for-update TS, and an
  explicit managed minimum.
- Scan-lock responses now merge ordinary and shared locks into one ascending
  key stream. Client-go's region lock resolver requires that ordering; the
  mixed-lock regression prevents shared-lock groups from being appended after
  later ordinary keys.
- Pessimistic max-execution arbitration now uses the effective wait timeout
  stored in `LockContext`, not the helper's placeholder argument. When
  whole-millisecond request truncation returns `LockWaitTimeout` just before a
  tighter client-side execution deadline, the translated source test now
  deterministically reports the source max-execution interruption.

The new standalone ports also directly inspect all three read request types for
resource-group names; all nine static/dynamic/static-precedence resource-tag
cases; interceptor begin/end/log counts; three health-feedback messages with
sequence, store, and slow-score fields; and both transaction-file size/regroup
scenarios.

## Validation

Pinned source baselines with Go 1.25.12:

```text
/private/tmp/go1.25.12-full/bin/go test . -count=1
ok integration_tests 84.784s

/private/tmp/go1.25.12-full/bin/go test -race . -count=1
ok integration_tests 91.239s
```

The race linker emitted the already-recorded macOS malformed `LC_DYSYMTAB`
warning and the test binary passed.

Focused Rust gates on `nightly-2026-08-22`:

```text
# no default features
source_go_integration_tests_    154 passed
source_go_txnkv_txnsnapshot_     33 passed, 1 source skip
source_go_txnkv_rangetask_        2 passed

# all features / NextGen
source_go_integration_tests_    153 passed; health is source-gated out
source_go_txnkv_txnsnapshot_     19 passed, 5 source skips; ten pipelined tests are source-gated out
source_go_txnkv_rangetask_        2 passed
```

Complete workspace gates:

```text
make check
workspace/all-target/all-feature check, rustfmt, and Clippy passed

make unit-test
no default features: 1393 passed, 2 skipped
all features/lib:     1357 passed, 6 skipped

make doc
strict private rustdoc passed; 51 doctests passed
```

Mechanical declaration reconciliation reports 190 expected unique behavioral
identities and no missing Rust owner. The source's unconditional `TestRCRead`
skip and four conditional NextGen snapshot skips remain explicit rather than
being reported as passes.
