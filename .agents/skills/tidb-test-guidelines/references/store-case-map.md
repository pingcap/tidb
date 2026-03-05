# TiDB store Test Case Map (pkg/store)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/store

### Tests
- `pkg/store/batch_coprocessor_test.go` - store: Tests store err.
- `pkg/store/etcd_test.go` - store: Tests new etcd client get etcd addresses.
- `pkg/store/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/store/store_test.go` - store: Tests new store.

## pkg/store/copr

### Tests
- `pkg/store/copr/batch_coprocessor_test.go` - store/copr: Tests balance batch cop task with continuity.
- `pkg/store/copr/coprocessor_cache_test.go` - store/copr: Tests build cache key.
- `pkg/store/copr/coprocessor_test.go` - store/copr: Tests ensure monotonic key ranges.
- `pkg/store/copr/key_ranges_test.go` - store/copr: Tests cop ranges.
- `pkg/store/copr/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/store/copr/mpp_probe_test.go` - store/copr: Tests MPP failed store probe.
- `pkg/store/copr/region_cache_test.go` - store/copr: Tests validate location coverage.

## pkg/store/copr/copr_test

### Tests
- `pkg/store/copr/copr_test/coprocessor_test.go` - store/copr: Tests build cop iterator with row count hint.
- `pkg/store/copr/copr_test/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/store/driver

### Tests
- `pkg/store/driver/client_test.go` - store/driver: Tests inject tracing client.
- `pkg/store/driver/config_test.go` - store/driver: Tests set default and options.
- `pkg/store/driver/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/store/driver/snap_interceptor_test.go` - store/driver: Tests snapshot without interceptor.
- `pkg/store/driver/sql_fail_test.go` - store/driver: Tests fail busy server cop.
- `pkg/store/driver/txn_test.go` - store/driver: Tests txn get.

## pkg/store/driver/error

### Tests
- `pkg/store/driver/error/error_test.go` - store/driver: Tests error wrapping.

## pkg/store/driver/txn

### Tests
- `pkg/store/driver/txn/batch_getter_test.go` - store/driver: Tests buffer batch getter.
- `pkg/store/driver/txn/driver_test.go` - store/driver: Tests lock not found print.
- `pkg/store/driver/txn/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/store/driver/txn/union_iter_test.go` - store/driver: Tests union iter.

## pkg/store/gcworker

### Tests
- `pkg/store/gcworker/gc_worker_test.go` - store/gcworker: Tests get oracle time.
- `pkg/store/gcworker/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/store/helper

### Tests
- `pkg/store/helper/helper_test.go` - store/helper: Tests hot region.
- `pkg/store/helper/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/store/mockstore

### Tests
- `pkg/store/mockstore/cluster_test.go` - store/mockstore: Tests cluster split.
- `pkg/store/mockstore/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/store/mockstore/tikv_test.go` - store/mockstore: Tests config.

## pkg/store/mockstore/mockcopr

### Tests
- `pkg/store/mockstore/mockcopr/executor_test.go` - store/mockstore: Tests resolved large txn locks.
- `pkg/store/mockstore/mockcopr/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/store/mockstore/unistore

### Tests
- `pkg/store/mockstore/unistore/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/store/mockstore/unistore/pd_test.go` - store/mockstore: Tests load.
- `pkg/store/mockstore/unistore/raw_handler_test.go` - store/mockstore: Tests raw handler.

## pkg/store/mockstore/unistore/cophandler

### Tests
- `pkg/store/mockstore/unistore/cophandler/cop_handler_test.go` - store/mockstore: Tests is prefix next.
- `pkg/store/mockstore/unistore/cophandler/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/store/mockstore/unistore/lockstore

### Tests
- `pkg/store/mockstore/unistore/lockstore/lockstore_test.go` - store/mockstore: Tests mem store.
- `pkg/store/mockstore/unistore/lockstore/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/store/mockstore/unistore/tikv

### Tests
- `pkg/store/mockstore/unistore/tikv/detector_test.go` - store/mockstore: Tests deadlock.
- `pkg/store/mockstore/unistore/tikv/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/store/mockstore/unistore/tikv/mock_pd_test.go` - store/mockstore: Tests mock GC states manager.
- `pkg/store/mockstore/unistore/tikv/mvcc_test.go` - store/mockstore: Tests basic optimistic.
- `pkg/store/mockstore/unistore/tikv/util_test.go` - store/mockstore: Tests exceed end key.

## pkg/store/mockstore/unistore/util/lockwaiter

### Tests
- `pkg/store/mockstore/unistore/util/lockwaiter/lockwaiter_test.go` - store/mockstore: Tests lockwaiter basic.
- `pkg/store/mockstore/unistore/util/lockwaiter/main_test.go` - Configures default goleak settings and registers testdata.
