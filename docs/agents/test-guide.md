# TiDB Test Guide

This document is the testing guide linked from root `AGENTS.md`.

## Unit Tests

```
make failpoint-enable && pushd pkg/<package_name>
go test -run <test_name> -tags=intest,deadlock
popd && make failpoint-disable
```

## Integration Tests

```bash
pushd tests/integrationtest
./run-tests.sh -t <test_name>
popd
```

- Test inputs are in `tests/integrationtest/t`.
- Expected results are in `tests/integrationtest/r`.
- Test name: for patterns like `t/a/b.test`, the test name is `a/b`.

To update integration test results, use `./run-tests.sh -r <test_name>`. Do not use `-record` here.

## Real TiKV Tests (run only if necessary)

```
tiup playground --mode tikv-slim &
PID=$!
make failpoint-enable
go test -run <test_name> -tags=intest,deadlock ./tests/realtikvtest/<dir>/... -args \
  -tikv-path "tikv://127.0.0.1:2379?disableGC=true"
make failpoint-disable
kill $PID
```

Alternative Real TiKV workflows are available in `tests/realtikvtest/scripts/classic/`.
