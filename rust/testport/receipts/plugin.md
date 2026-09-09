# `pkg/plugin` — Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The top-level plugin package and its `conn_ip_example` fixture/example contain
18 tracked artifacts and 2,734 lines. Every production file, source test,
integration test, example fixture, manifest, README, and Bazel target was read
line by line before comparing Rust owners. There is no generated source,
benchmark, fuzz target, or platform-specific Go variant. The example is a
real build/test fixture: its `manifest.toml`, private library target, and
goleak-enabled test target are included below.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 56 | `8dd7c9ca65a282171ac31f0e533aafbbc5fc340f` | `dd6bf9263ac978059d373857420932d5a5e01ec6375fba4e089fdea0ee067426` | framework library and six-shard tests |
| `README.md` | 3 | `7735c7cd15efa3179dc12729aba8635ecb8c11ac` | `fd87be4c5ca19f0689bb14de2a55b314125f0d4c48eac993f75befc68e0c96b3` | framework design link |
| `audit.go` | 138 | `404b303c6a4dd888ec4725fbae174fab7e40c1a7` | `17717674430ac513426bedf0e32650f2ee25066044ea6b1a114f70d9dc8cdf20` | audit/general/connection/parse event SPI |
| `const.go` | 71 | `ca285e4436492c93a85ab49035568976b7937f50` | `f733d61122325419af3168b32ad31e8d90dede5f9401cb0a643ea0e6a6e60363` | plugin kind/state enums and strings |
| `errors.go` | 29 | `ea1380576519a9cd5472f46eb09e9ef2915d7d71` | `16983e9c6fb1f58cda32babe6a5e4199f06616d0cf14d192364447019f86dbe7` | plugin error identities |
| `helper.go` | 115 | `7262cb9940a33d62293c832e3aec23e6e2962dc2` | `ae23fc80cbfe3d6b95b75239ae95565efc319fd0c906befc4d1c10d75eb031e` | manifest casts, ID decode, test loader |
| `plugin.go` | 568 | `10703f87d557a95a9fca5d430028e74b52a69bd5` | `7744d02b063c70676385f2c8c24f1d8c3befb3ebd1b7b50567266b671146b606` | COW registry, dynamic/static load, watcher, lifecycle |
| `spi.go` | 82 | `7267b314fa293ce9d1792d3d8a10a0ab8f170634` | `963bc892edd869f6057f835ac95f88972d673e031f679f527cc2cc9868d9c902` | manifest and sub-manifest shapes |
| `const_test.go` | 83 | `20961cd5f338fe5010057c2e5d43687bf080bfc1` | `eb5343dc4113f656384a8e5bedf18e365418989498f012985e5cfec464e6a550` | enum/string tests |
| `helper_test.go` | 49 | `99a310fa69b2f7069172339f83ac5efe8eee3f72` | `7a1ad361931c7eab3ef356e18a654a369acdb2dacf219c4aff68447132945470` | manifest cast and ID tests |
| `integration_test.go` | 752 | `b522931f9afe6a3b94b34f73baf6a187bc17c965` | `5c62e767ab60462dec60e309128335a70982f46be467abae20abe04e3adb9ff9` | audit event integration matrix |
| `main_test.go` | 37 | `80379b90d8e0f0a0b0122cd1f55a2b32af9a4e98` | `c82a33b7ffaff8a4f17ef4c26e6810a7743bce6adc0214cfd354464d3bd89f9e` | common setup and goleak harness |
| `plugin_test.go` | 361 | `daa1965b7758f89705324adcc0bad3e40aecb265` | `2990d807be2889f2e51cf0eb4330ceb412b929cc5236549f2a0e570a185ffa63` | load/validate/init/clone/watcher tests |
| `spi_test.go` | 52 | `7cfb0973fe0283cb525bfd8139a0b95bf3580b17` | `1a61cea43bd0ffbccc98413cd76627e72fd868a631694e42e7e5910af9002e33` | exported-manifest callback test |
| `conn_ip_example/BUILD.bazel` | 31 | `3efb4b83bb1d60b89445ca761c6acbe9c993552e` | `994e53edad8fe6d70020513014063317fcf274ffae04b57441b51734b4543850` | example library and test target |
| `conn_ip_example/conn_ip_example.go` | 132 | `7fa03e598a4f6ceac4d58cb99492b3e26261eaf9` | `1fb00e16f83741dee5d3b84efec787ca57e26f67c0c877c8031992a5ec21122e` | example audit plugin implementation |
| `conn_ip_example/conn_ip_example_test.go` | 115 | `fd0a57c5ef7372974c21cc8222765fbe843ed7e4` | `02c5af277fd15a373c304da10eb281f1194456e95d1346a4da3d0b998a0df0d9` | example load/event/connection test |
| `conn_ip_example/main_test.go` | 34 | `40f20a36867ac6d27616ab84d96e7e18ade4088c` | `fc38f6dd68e07459365ae1d2ee77fbd43d3b5b405ee4cc6ddda7213aef927291` | example setup and goleak harness |
| `conn_ip_example/manifest.toml` | 26 | `ad82c97dbd64a30e4123755222dc0a22891de2fd` | `a720e85afd25941c7254d1cdfdeffa9573a56da4bb10c9109201b3960738827b` | example manifest metadata and exports |

### Production symbols

`const.go` defines `Kind` (`Audit`, `Authentication`, `Schema`, `Daemon`),
`State` (`Uninitialized`, `Ready`, `Dying`, `Disable`), and both `String`
methods. `errors.go` defines the six plugin dbterror identities.

`spi.go` defines `Manifest`, `AuthenticationManifest`, `SchemaManifest`,
`DaemonManifest`, `ExportManifest`, and the unsafe ABI cast. `audit.go` defines
the general/connection/parse event enums and strings,
`GeneralEventFromString`, `AuditManifest`, and the context-key values used by
the connection, execution, prepared-statement, and retry callbacks.

`helper.go` implements the four `Declare*Manifest` casts, `ID.Decode`, and
`LoadPluginForTest`. `plugin.go` implements the COW registry and all lifecycle
functions: `plugins.clone`, `plugins.add`, `copyOnWriteContext.plugins`,
`Plugin.StateValue`, `Plugin.DisableFlag`, `Plugin.validate`, `Load`, `Init`,
`flushWatcher.refreshPluginState`, `flushWatcher.watchLoop`,
`flushWatcher.watchLoopWithChan`, `flushWatcher.getPluginDisabledFlag`,
`staticPlugins.Add/Get/Clear`, `loadOne`, `SetTestHook`,
`loadManifestByGoPlugin`, `Shutdown`, `Get`, `ForeachPlugin`, `IsEnable`,
`GetAll`, `Plugin.supportsFlush`, `NotifyFlush`,
`ChangeDisableFlagAndFlush`, and `getByName`. This covers dynamic `.so`
loading, static registration, version/duplicate validation, skip-on-failure
state transitions, domain-init callbacks, etcd flush watches/re-watches,
shutdown cleanup, and enable/flush propagation.

The example fixture implements `Validate`, `OnInit`, `OnShutdown`,
`OnGeneralEvent`, and `OnConnectionEvent`, including registration of a test
system variable and connection-count reset/increment behavior.

### Tests, test by test

Top-level source tests are `TestConstToString`, `TestGeneralEventString`,
`TestPluginDeclare`, `TestDecode`, `TestLoadStaticRegisteredPlugin`,
`TestLoadPluginSuccess`, `TestLoadPluginSkipError`, `TestLoadFail`,
`TestPluginsClone`, `TestPluginWatcherLoop`, `TestExportManifest`, and the
large `TestAuditLogNormal` statement matrix (DDL, DML, transaction, SHOW,
prepared, EXPLAIN, FLUSH, and SELECT commands; event, statement type,
affected rows, retry, database, and table assertions). `TestMain` in the
package and example configures common setup and goleak exclusions.

The fixture test `conn_ip_example.TestLoadPlugin` verifies test-hook loading,
manifest validation/init, audit event delivery, five connection callbacks,
connection accumulation, shutdown reset, and the documented output transcript.

## Rust ownership and decision

Rust has only narrow metadata/configuration fragments: `tidb-config` models
plugin directory/load and audit-log settings, while `tidb-server` has an
authentication-plugin registry for the separate `pkg/extension` auth callback
surface. No Rust crate provides Go's unsafe manifest ABI, dynamic Go plugin
loading, static plugin registry, COW state, etcd flush watcher, audit event
dispatch, schema/daemon plugin lifecycle, or the example fixture contract.
Those fragments are not a dependency-closed owner, and combining them into a
Rust plugin framework would introduce uncalled Rust-only behavior.

This package is recorded as an explicit boundary with no speculative source
change and no new regression test. A future owner must port the framework,
event dispatch, dynamic/static loading, flush/shutdown lifecycle, and example
fixture together; auth metadata alone does not constitute parity.

## Validation and risk

Profile: **WIP** for this docs-only audit; the rolling repository loop remains
in progress. No Go or Bazel source changed, so `make bazel_prepare` is not
required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/plugin/... -count=1
# passed
```

- Correctness: no plugin loading, callback, audit, or flush behavior changed;
  Go remains authoritative.
- Compatibility: a future Rust owner must preserve manifest ABI/layout,
  version and duplicate rules, skip-on-failure states, event ordering,
  context keys, etcd path/value encoding, and shutdown callback semantics.
- Performance: unchanged.
- Not verified locally: real `.so` loading on deployment hosts, live etcd
  flush propagation, plugin ABI compatibility across Go toolchains, Bazel
  analysis, and workspace-wide Ready validation.
