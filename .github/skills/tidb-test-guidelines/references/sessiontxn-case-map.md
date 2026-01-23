# TiDB sessiontxn Test Case Map (pkg/sessiontxn)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/sessiontxn

### Tests
- `pkg/sessiontxn/txn_context_test.go` - Tests txn context.
- `pkg/sessiontxn/txn_manager_test.go` - Tests enter new txn.
- `pkg/sessiontxn/txn_rc_tso_optimize_test.go` - Tests RC TSO cmd count for prepare execute normal.

## pkg/sessiontxn/isolation

### Tests
- `pkg/sessiontxn/isolation/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/sessiontxn/isolation/optimistic_test.go` - Tests optimistic txn context provider TS.
- `pkg/sessiontxn/isolation/readcommitted_test.go` - Tests pessimistic RC txn context provider RC check.
- `pkg/sessiontxn/isolation/repeatable_read_test.go` - Tests pessimistic RR error handle.
- `pkg/sessiontxn/isolation/serializable_test.go` - Tests pessimistic serializable txn provider TS.

## pkg/sessiontxn/staleread

### Tests
- `pkg/sessiontxn/staleread/externalts_test.go` - Tests external timestamp read-only.
- `pkg/sessiontxn/staleread/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/sessiontxn/staleread/processor_test.go` - Tests stale read processor with select table.
- `pkg/sessiontxn/staleread/provider_test.go` - Tests stale read txn scope.
