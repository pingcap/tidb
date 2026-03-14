# TiDB Testing Flow for Agents

This document provides command playbooks for test execution.
Root `AGENTS.md` is the source of truth for policy-level requirements; this file is operational guidance.
Use `AGENTS.md` -> `Task -> Validation Matrix` first, run the smallest valid command set, and report exact commands.
Detailed command snippets in this file are the canonical operational reference; skills under `.agents/skills/` should point here rather than duplicating long command blocks.

## Unit tests (`/pkg/...`)

```bash
pushd pkg/<package_name>
go test -run <TestName> -tags=intest,deadlock
popd
```

- If execution succeeds, review changed result/testdata files.
- Prefer targeted runs (`-run <TestName>`); use package-wide runs only when needed.
- Use `-record` only for test suites that explicitly support it.

## Failpoint decision for unit tests

- Policy reference: `AGENTS.md` -> `Quick Decision Matrix` (`Unit tests in a package that uses failpoints`).

```bash
rg -n --fixed-strings -- "failpoint." pkg/<package_name>
rg -n --fixed-strings -- "testfailpoint." pkg/<package_name>
# If BUILD.bazel exists, also check failpoint dependency.
test -f pkg/<package_name>/BUILD.bazel && rg -n --fixed-strings -- "@com_github_pingcap_failpoint//:failpoint" pkg/<package_name>/BUILD.bazel
```

- Use the checks above as the default decision basis.
- If `rg` finds matches, run with failpoints enabled.
- If `rg` finds no matches, run without failpoint enable/disable and state the check evidence in the final report.
- `-tags=intest,deadlock` does not enable failpoints.

### Failpoint-enabled run

```bash
(
  enabled=0
  cleanup() { [ "${enabled}" -eq 1 ] && make failpoint-disable; }
  trap cleanup EXIT INT TERM

  make failpoint-enable
  enabled=1

  pushd pkg/<package_name>
  go test -run <TestName> -tags=intest,deadlock
  rc=$?
  popd
  exit $rc
)
```

- If running Bazel directly (for example `bazel test`), run `make bazel-failpoint-enable` first, then `make bazel-failpoint-disable` after tests.
- If using `make bazel_test`, do not run `make bazel-failpoint-enable` separately because `bazel_test` already depends on it; still run `make bazel-failpoint-disable` after tests.

## Related guidance

- Bug-fix regression policy lives in `AGENTS.md` -> `Quick Decision Matrix` (`Bug fix`).
- Test placement, naming, `shard_count`, and package-specific testdata rules live in `.agents/skills/tidb-test-guidelines`.
- Include exact test commands in PR description under `Tests` when preparing a PR update.

## Integration tests (`/tests/integrationtest`)

- Test inputs are in `tests/integrationtest/t`.
- Expected results are in `tests/integrationtest/r`.

```bash
pushd tests/integrationtest
./run-tests.sh -r <TestName>
popd
```

- Review changed files in `tests/integrationtest/r` and confirm each diff matches expected behavior.
- Result files usually do not need manual edits; if edits are necessary, keep them minimal and verify correctness before reporting.
- Mapping example: if you modify `t/planner/core/binary_plan.test`, then `TestName` is `planner/core/binary_plan`.

## RealTiKV tests (`/tests/realtikvtest`)

- Use for cases requiring real TiKV/TiUP Playground behavior and tests under `tests/realtikvtest/`.
- Policy reference: `AGENTS.md` -> `Quick Decision Matrix` (`RealTiKV tests`).

Start playground in background:

```bash
tiup playground --mode tikv-slim --tag realtikvtest &
PLAYGROUND_PID=$!
```

Default PD is `127.0.0.1:2379`; if it is unavailable, use a non-default port or port offset.
Using `--tag realtikvtest` keeps data under `${HOME}/.tiup/data/realtikvtest` after exit; remove it during cleanup.
```bash
tiup playground --mode tikv-slim --tag realtikvtest --pd.port 12379 &
PLAYGROUND_PID=$!
# or
tiup playground --mode tikv-slim --tag realtikvtest --port-offset 10000 &
PLAYGROUND_PID=$!
```

```bash
PD_ADDR=127.0.0.1:2379
# If started with `--pd.port 12379` or `--port-offset 10000`, use:
# PD_ADDR=127.0.0.1:12379
curl -f "http://${PD_ADDR}/pd/api/v1/version"
until curl -sf "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; do sleep 1; done
```

```bash
go test -run <TestName> -tags=intest,deadlock ./tests/realtikvtest/<dir>/...
# non-default PD example
go test -run <TestName> -tags=intest,deadlock ./tests/realtikvtest/<dir>/... -args \
  -tikv-path "tikv://127.0.0.1:12379?disableGC=true"
```

- If failpoints are used, enable before running and disable afterward.
- Do not add `-v` by default; add it only for debugging.

```bash
[ -n "${PLAYGROUND_PID:-}" ] && kill "${PLAYGROUND_PID}" 2>/dev/null || true
[ -n "${PLAYGROUND_PID:-}" ] && wait "${PLAYGROUND_PID}" 2>/dev/null || true
rm -rf "${HOME}/.tiup/data/realtikvtest"
```

```bash
# Cleanup check: PD endpoint should be unreachable after teardown.
! curl -sf "http://${PD_ADDR}/pd/api/v1/version"
```

Cleanup-safe template (recommended for long local debug runs):

```bash
PD_ADDR=127.0.0.1:2379
(
  cleanup() {
    [ -n "${PLAYGROUND_PID:-}" ] && kill "${PLAYGROUND_PID}" 2>/dev/null || true
    [ -n "${PLAYGROUND_PID:-}" ] && wait "${PLAYGROUND_PID}" 2>/dev/null || true
    rm -rf "${HOME}/.tiup/data/realtikvtest"
  }
  trap cleanup EXIT INT TERM

  tiup playground --mode tikv-slim --tag realtikvtest &
  PLAYGROUND_PID=$!

  until curl -sf "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; do sleep 1; done
  go test -run <TestName> -tags=intest,deadlock ./tests/realtikvtest/<dir>/...
)
! curl -sf "http://${PD_ADDR}/pd/api/v1/version"
```

- For fmt-only PRs, follow `Quick Decision Matrix` and skip costly `realtikvtest`.
- Alternative RealTiKV workflows are available in `tests/realtikvtest/scripts/classic/` and `tests/realtikvtest/scripts/next-gen/`.
