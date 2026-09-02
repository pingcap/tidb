# Configuration parity receipt

Date: 2026-09-02

## Scope and inventory

The complete `pkg/config` tree was read before editing: 28 artifacts and 6,811
lines, including 15 root-package files (6,092 lines), the configtypes,
deploymode, and kerneltype subpackages, TOML examples, tests, OWNERS, and all
Bazel inputs. No generated, platform-specific, fixture, fuzz, or benchmark
artifact was omitted.

Go master behavior restored in this batch:

- Starter deployments default `starter-params.max-import-data-size` to 25 GiB,
  while an explicit `0B` remains unlimited.
- Added versioned Starter bootstrap manifest configuration and deploy-mode
  validation.
- Added hosted-embedding configuration and Starter-only validation.
- Added the next-gen foreign-key shared-lock experimental gate and synchronized
  the TOML example, BUILD dependencies, and OWNERS routing.

These options remain Go-owned configuration contracts; no dependency-closed
Rust configuration owner exists, so this receipt does not claim a Rust
transcreated package.

## Validation

The focused failpoint-safe Ready checks passed:

```text
tools/check/failpoint-go-test.sh ./pkg/config -run '^(TestConfig|TestDeployModeConfig|TestKeyspaceActivateModeConfig|TestExternalWorkloadValid|TestGetGlobalKeyspaceName|TestGetGlobalTiKVWorkerURL|TestGetTiKVConfigKeepsZeroRUV2RUScale)$' -count=1
git diff --check
```

`make lint` is rerun for the batch before commit. `make bazel_prepare` is
required because BUILD and Go files changed, but the local environment has no
`bazel` executable; no generated BUILD output was invented.
