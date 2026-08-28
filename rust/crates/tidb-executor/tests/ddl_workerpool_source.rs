// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Port ledger for `pkg/ddl/ddl_workerpool_test.go` (`pkg/ddl.part6` batch
//! b105, item 305 of the pkg/ddl enumeration).
//!
//! Go's DDL worker pool wraps an `ngaut/pools.ResourcePool`: workers are
//! `pools.Resource`s checked out per reorg job, and the pool reports its
//! free-resource count. Nothing in this workspace transcreates that pool or
//! the `newWorker` resource it hands out.

/// GO PORT of `pkg/ddl/ddl_workerpool_test.go:25 TestDDLWorkerPool`.
///
/// Re-derived contract (pkg/ddl/ddl_workerpool.go:31 `newDDLWorkerPool` over
/// a `pools.NewResourcePool(factory, 1, 2, 0)`): a fresh pool over capacity
/// 1 reports `available() == 1`; after `pool.close()` it reports 0; and
/// `pool.put(nil)` -- putting a NIL resource back, which Go's
/// ResourcePool close/put path tolerates -- leaves it at 0. The factory
/// builds `newWorker(ctx, addIdxWorker, ...)` resources, i.e. the pool is
/// the reorg-worker allocator keyed by `jobTypeReorg`.
#[test]
#[ignore = "go-parity-gap: DDLWorkerPool/newDDLWorkerPool (pkg/ddl/ddl_workerpool.go) over ngaut/pools ResourcePool and the newWorker resource are not transcreated"]
fn ddl_worker_pool_counts_available_resources_through_close_and_nil_put() {}
