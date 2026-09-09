# `pkg/testkit` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains 38 tracked artifacts and 4,202 lines. Every production,
test, nested support package, Bazel target, and build-tagged source was read in
full before comparing the Rust workspace. There is no `doc.go`, data fixture,
benchmark, fuzz target, example, generated Go output, or platform-specific
variant; `testdata/` is a Go support package, not a fixture directory. All
`!codes`-gated sources in the async, DB-test, mock-store, result, test-kit,
testdata, testmain, testsetup, and testutil support surfaces are included below.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 83 | `5748b1123bedd5f7edf073435c025e4040dcea34` | `8ec1d64ba4b4931709c24ee5249b5acb82f1eb58bbedf6cb125639fe255e4658` | public testkit library and root tests |
| `analyzehelper/BUILD.bazel` | 14 | `cdfedb52c491da0c74e2d8639a5d46a20fc6c14e` | `2178384f2d4fd84105a9e7ef41884eeebc19aa22db69a11f52bbae4f80ff99a2` | analyze helper target |
| `analyzehelper/helper.go` | 41 | `ed8fd8a1d6d6cfd72cdd313b72a8ba68e63fca36` | `f001a6952634a9b3417e14312d958d7a1d1d5182bb77d422d3713138ebe39b31` | predicate-column collection helper |
| `asynctestkit.go` | 250 | `8e7b8c765e1afbbf22e87c4f1ebe297810c2f223` | `2e8e0725d744dce0985cc6e72d266ad043937f9881ce3138fc5cc52894d7a40a` | concurrent session/test execution |
| `db_driver.go` | 308 | `a8ac755e19e04972a2e8078fb6b4cef862ad56d6` | `56cc8ccb5a340544294e597184a963fdab5365a727cda114953f5e160c80c283` | database/sql driver bridge |
| `db_driver_test.go` | 79 | `96fa00a4c73cd7b315cdfb10781a06cc663a46b3` | `dea1b62bb06358c765121e86f9185db446aafddea4650ee82798071b7a155f9b` | driver integration test |
| `dbtestkit.go` | 96 | `285f7dcfc1f449d875f730a5c39a7d233c00aec0` | `dda41bba54ee0ab1c31af104d1ad3da925f86a81e263e549c7db85c3434d4f9d` | database/sql assertion wrapper |
| `ddlhelper/BUILD.bazel` | 14 | `179863f2d88c066f61b1859bbc3314d0740da1ee` | `efd253c460125a97ca524271972932c41e78f74c59e8bd9ff04acdf08be7011e` | DDL helper target |
| `ddlhelper/helper.go` | 28 | `3edf9c14bf1c5ef367d31feeb1617453ca44eb7e` | `15b331a1fc6861316bcf099c7c790754bab98019f0b05e62219e5477a9da9b29` | table-info construction helper |
| `external/BUILD.bazel` | 16 | `0e4ebfc7b68ea6a66127d259d8441d0750d08fb8` | `6bbd50c8b1520d7d81d1e5ae0cb3a1b078b325c745d50bf999211446b916d3c9` | external test helper target |
| `external/util.go` | 73 | `acc01eae5e29dcad9e37d46f50f318d27a196ad5` | `ddb9628ea06b34ae948979d4f24ee6340f884d23696c1f30af5d0d0c8ac859ef` | table/column/index lookup helpers |
| `mocksessionmanager.go` | 214 | `1665e09e53e8e9ccb1339224dabb504539a50d59` | `7d13451cd91ae4261d03d3db33cf5cac2c4532b43f3fa6b32937532f35023e62` | session-manager mock and internal-session registry |
| `mockstore.go` | 454 | `1ad243d1daffaef7f79fab5c646640661c1df090` | `392ae83acf48644fe3554a2a15c9ab9e9a9b813e8e0a66e60a11b361f71e20ce` | mock-store/domain bootstrap and options |
| `result.go` | 216 | `7ec727f1e9ba94656086ea7645207de93120ea9c` | `5072bf58acb6e74adb6d27b59433832c0db585d86a7a584aed519a30fea2d47e` | SQL result formatting and assertions |
| `stepped.go` | 256 | `8103f6ef5cf662f23bc12257cb2951ad39e49968` | `6ca9a6366fbe2f74fd58095db3e3b85915efe21772057ce8fc6a2bbe36b02f5c` | breakpoint-controlled test runner |
| `testdata/BUILD.bazel` | 12 | `c2e480ca6ba2ca0fda6767e9e2d7ecb1c3780eb4` | `ae838a8a1d30d38a02f84730d4ae136e0b1b5a2a57af8516697f61362640f009` | testdata support target |
| `testdata/testdata.go` | 322 | `9922b4d4b7da9ebcf604e30206e599d0156fed37` | `6786313b935d633626c489828b961a39a244a87bd0886e4e67240498edf4dcf8` | suite loading, recording, and result generation |
| `testenv/BUILD.bazel` | 13 | `1fbb9d91e91a581c4110958adaf9210ede8b9e33` | `542a9b21ce3a717c525fada485762de72fd6eda03079f7979aeca569690d842e` | test-environment target |
| `testenv/testenv.go` | 45 | `794c54d190161d053852cb1d6a8d94804c8e2063` | `093480f03baf9992d1982b2479f2a76903863b4e05c87173efd281e6d7e8e78c` | process/config test helpers |
| `testfailpoint/BUILD.bazel` | 12 | `16c1fd1d485bc974b689335328c83edaf865014b` | `c26541ea559b52574c3aa8900caeb460ffb854ff38712531cf84244d5d5b2693` | failpoint helper target |
| `testfailpoint/failpoint.go` | 43 | `381b53c129c23e6e19ee87bbd60a93bcb32c10fa` | `be9ccfdee6b4dfa6c182cf37969c15b4466d2002073109c57bfb1f69bc1459e6` | failpoint enable/disable wrappers |
| `testflag/BUILD.bazel` | 8 | `a93e7f0a96c8a7368c6c03b93af6138ee3a57661` | `f5bbfd6b78374c600e343031ae3a58a1c31aadef014f5233c9966c9e2547d01e` | test flag target |
| `testflag/flag.go` | 24 | `176eb24c412fbfb561f3ed3ab984c81adccb2cd7` | `67ca85996c22d97b14a94c613141ab758d70bf6ef40c40c29f787e6513d11b39` | long-test flag |
| `testfork/BUILD.bazel` | 21 | `904edd3f64bc584be02616749d609795a2ba6fc4` | `0115586a4901f9effa7c8ab8c92f2c71095e0b7c8d2bc9e25e0d8bcb5678efc9` | fork helper library/test target |
| `testfork/fork.go` | 138 | `a7873bcbd76e705e815d2d0d6fda78215086418b` | `994516f91638b80b2a79c01bfec63de90bdbcf419088c82652fd80409d957b0c` | subtest selection and stack helpers |
| `testfork/fork_test.go` | 50 | `64ed11b7a82e56dc6c699785fd02ec9ef3f5c178` | `255ecbe363e6da85590139a364dac61541ecc8d099343d7e31d40c2ce6369b2f` | test-fork source test |
| `testkit.go` | 847 | `07567a6e78233b18ef1936de788de120340df252` | `cbd84f6188a2eed7f9ecf88042f3f76ad0913fb1fc4c351ed6464d127e765a45` | TestKit session, SQL, plan, and stats helpers |
| `testkit_test.go` | 34 | `097582d61b2c00866022e0c2c6ec6291ebf52dc7` | `2c5eceb7bc65e9613f435f35be1e48ec207e5ee2b7643205e45a2d01dd8c4684` | multi-statement TestKit test |
| `testmain/BUILD.bazel` | 12 | `5667a25a44d0b4f76d609db527d75704b91800e4` | `228d68b453afb27345c9c7ddf83dd1b5a411e532ad2636273fbe25338c28465d` | test-main target |
| `testmain/bench.go` | 35 | `0a245d67e11b7c2d290d2d49350bfef8f86c54ce` | `62b5916c58b9a92eef9a23d4ef90483b9a3e180d38345a018da474077462cc84` | benchmark short-circuit helper |
| `testmain/wrapper.go` | 42 | `2c2eaa58cc1bad2907e08e7999ca83e95ca05b8f` | `bd238d0a591553732694f3d03a5c9b4b63c0f90bd31f576e5a8b7b71f31bd914` | goleak testing wrapper |
| `testsetup/BUILD.bazel` | 13 | `0db510570d0e8457188a8811946c6a921d8a093b` | `b6bba2f0aceadad91a60c680619e67f0ce3280f6d53dc7d8b4840b3c2d273461` | test setup target |
| `testsetup/bridge.go` | 48 | `f2096fa8c947e07339aad1d1c139c263f2823e02` | `f39ea11a53440ed4a67237d1ad95dc9ec1896600f45e08df7ebe826368ca72c3` | logging setup bridge |
| `testutil/BUILD.bazel` | 34 | `95bbde2957f491953f220e8397868729daefa57b` | `e2e84d4b1ec3d315dc890ee687ddee56dec6d5979a7caabd526ea98c98132da1` | utility library/test target |
| `testutil/handle.go` | 51 | `7d6532a107b2685a79db2ef43b8326a8842d8625` | `1dcc1e5de2f15a957c86a2d4f2cfdbeb6028dd494f47702dbd5977544d79de7a` | handle construction/masking |
| `testutil/loghook.go` | 116 | `8ed5dcf8a5298d3a6ad43a5467255840b36ecf58` | `1e7be9d3cbe892bcda00f074e23e558419727d241fce86ded81f01d83674eb38` | structured log capture/assertion |
| `testutil/require.go` | 109 | `9454689fdf251d4c395f8217d55bd104c7b06277` | `3c1a04055e04811e8b6b53eec0f43b05d411668d5acec6dd0bdac467b8102224` | datum/handle comparison helpers |
| `testutil/require_test.go` | 31 | `1e787c37ee4648c0e7e6e0a6c9f052b92ac85b1c` | `f908664f9d623d6cb6d47bb4fc556a1510c0994e995f782e7fde26064b1238bb` | unordered-string comparison test |

The production/support surface covers mock-store and domain bootstrap,
session-manager state, SQL-driver and prepared-statement adapters, result
assertion formatting, asynchronous and stepped execution, testdata recording,
failpoint and environment switches, fork selection, logging hooks, and DDL,
ANALYZE, table, and index helpers. The test files cover the driver,
multi-statement, fork, and unordered-result contracts. The nested Bazel files
enumerate the same support libraries and test targets; no hidden generated or
platform source is omitted.

The line-by-line audit covers 229 function/method declarations across the 26
Go files. The four top-level source tests are `TestMockDB`,
`TestMultiStatementInTk`, `TestForkSubTest`, and
`TestCompareUnorderedString`; all were read and run. Per-file declaration
counts are: analyze helper 1, async testkit 12, DB driver 23 plus 1 test,
DBTestKit 8, DDL helper 1, external helpers 3, mock session manager 16,
mock-store helpers 19, result helpers 14, stepped runner 19, testdata 12,
testenv 2, testfailpoint 3, testflag 1, testfork 9 plus 1 test, root TestKit 60
plus 1 test, testmain 3, testsetup 2, testutil 17 plus 1 test. Those counts,
the artifact hashes above, and the passing package command bind the audit to
the exact source revision rather than a sampled API subset.

## Rust ownership and explicit boundary

Rust has no dependency-closed equivalent of Go `pkg/testkit`. Existing Rust
tests use crate-local fixtures, direct in-memory stores, SQL protocol harnesses,
or captured source tests; those are consumers of testkit semantics rather than
an owner of its public helper API. No crate owns the Go mock-store/domain
bootstrap, `database/sql` driver, `TestKit` session/query API, result matcher,
async/stepped runner, testdata recorder, failpoint bridge, or log-hook utility.

No Rust-only behavior was found to remove, and adding a compatibility testkit
crate would invent a second test harness without a dependency-closed owner or
callers. The complete Go package is therefore recorded as an explicit boundary;
future Rust session/integration work may consume this inventory only after a
real owner and lifecycle are established.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, Rust compilation gates, and the Ready lint gate are not
required for this batch.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test -tags=intest ./pkg/testkit/... -count=1
# passed: root and all nested testkit packages
```

The root package also compiled with `go test ./pkg/testkit -run '^$' -count=1`.
Running the two root tests without `-tags=intest` intentionally fails at the
existing `TestKit` guard that requires the test tag; the same tests pass under
the command above. No Rust code changed, so Rust owner checks and `make lint`
were not applicable. Not verified here: Bazel execution, full Go repository
tests, or a future Rust testkit/session integration owner.

This receipt certifies the bounded `pkg/testkit` inventory and ownership
decision; it is not a repository-wide transcreation claim.
