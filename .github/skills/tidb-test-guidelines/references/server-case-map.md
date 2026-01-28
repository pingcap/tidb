# TiDB server Test Case Map (pkg/server)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/server

### Tests
- `pkg/server/conn_stmt_params_test.go` - server: Tests parse exec args.
- `pkg/server/conn_stmt_test.go` - server: Tests cursor exists flag.
- `pkg/server/conn_test.go` - server: Tests issue33699.
- `pkg/server/driver_tidb_test.go` - server: Tests convert column info.
- `pkg/server/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/server/mock_conn_test.go` - server: Tests mock conn.
- `pkg/server/server_test.go` - server: Tests issue46197.
- `pkg/server/stat_test.go` - server: Tests uptime.
- `pkg/server/tidb_library_test.go` - server: Tests memory leak.
- `pkg/server/tidb_test.go` - server: Tests RC read check TSO conflict.
- `pkg/server/user_connections_test.go` - server: Tests user connection count.

## pkg/server/handler/extractorhandler

### Tests
- `pkg/server/handler/extractorhandler/extract_test.go` - server/handler: Tests extract handler.
- `pkg/server/handler/extractorhandler/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/server/handler/optimizor

### Tests
- `pkg/server/handler/optimizor/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/server/handler/optimizor/plan_replayer_test.go` - server/handler: Tests dump plan replayer API.
- `pkg/server/handler/optimizor/statistics_handler_test.go` - server/handler: Tests dump stats API.

## pkg/server/handler/tests

### Tests
- `pkg/server/handler/tests/dxf_test.go` - server/handler: Tests DXF API.
- `pkg/server/handler/tests/http_handler_serial_test.go` - server/handler: Tests post settings.
- `pkg/server/handler/tests/http_handler_test.go` - server/handler: Tests region index range.
- `pkg/server/handler/tests/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/server/handler/tikvhandler

### Tests
- `pkg/server/handler/tikvhandler/dxf_test.go` - server/handler: Tests parse pause scale-in flag.

## pkg/server/internal

### Tests
- `pkg/server/internal/packetio_test.go` - server/internal: Tests packet IO write.

## pkg/server/internal/column

### Tests
- `pkg/server/internal/column/column_test.go` - server/internal: Tests dump column.
- `pkg/server/internal/column/result_encoder_test.go` - server/internal: Tests result encoder.

## pkg/server/internal/dump

### Tests
- `pkg/server/internal/dump/dump_test.go` - server/internal: Tests dump binary time.

## pkg/server/internal/parse

### Tests
- `pkg/server/internal/parse/handshake_test.go` - server/internal: Tests auth switch request.
- `pkg/server/internal/parse/parse_test.go` - server/internal: Tests parse stmt fetch cmd.

## pkg/server/internal/util

### Tests
- `pkg/server/internal/util/util_test.go` - server/internal: Tests append format float.

## pkg/server/tests

### Tests
- `pkg/server/tests/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/server/tests/tidb_serial_test.go` - server/tests: Tests load data.

## pkg/server/tests/commontest

### Tests
- `pkg/server/tests/commontest/cursor_test.go` - server/tests: Tests lazy row iterator.
- `pkg/server/tests/commontest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/server/tests/commontest/tidb_test.go` - server/tests: Tests vector type text protocol.

## pkg/server/tests/cursor

### Tests
- `pkg/server/tests/cursor/cursor_test.go` - server/tests: Tests cursor fetch error in fetch.
- `pkg/server/tests/cursor/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/server/tests/standby

### Tests
- `pkg/server/tests/standby/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/server/tests/standby/standby_test.go` - server/tests: Tests standby.

## pkg/server/tests/tls

### Tests
- `pkg/server/tests/tls/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/server/tests/tls/tls_test.go` - server/tests: Tests TLS verify.
