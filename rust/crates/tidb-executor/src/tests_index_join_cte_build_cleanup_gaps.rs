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

//! Gap tests for Go `pkg/executor/builder_index_join_cleanup_test.go`: the
//! two build-time cleanup contracts. Go drives them by feeding a plan node
//! the builder cannot build (`mockPhysicalIndexReader` ->
//! "Unknown Plan *executor.mockPhysicalIndexReader") and asserting the
//! partial state is rolled back through `defer Close` /
//! map removal. This tier's builder is a typed AST walk with no
//! unknown-plan failure arm and no close-on-error teardown, so both
//! contracts stay documented gaps.

/// Go
/// `pkg/executor/builder_index_join_cleanup_test.go:45::TestBuildExecutorForIndexJoinHashJoinErrorCleansChildren`:
/// `dataReaderBuilder.BuildExecutorForIndexJoin`
/// (`pkg/executor/builder.go:4919`) building a PhysicalHashJoin whose
/// lookup-side child fails to build must Close the already-built lookup
/// executor exactly once and leave the not-yet-built other side un-Closed.
#[test]
#[ignore = "go-parity-gap: no unknown-plan build failure or close-on-error teardown; the typed builder cannot produce Go's partial-build state"]
fn index_join_hash_join_build_error_cleans_the_built_lookup_child() {}

/// Go
/// `pkg/executor/builder_index_join_cleanup_test.go:106::TestBuildCTEStorageProducerCleansStoragesOnRecursiveBuildError`:
/// `buildCTEStorageProducer` (`pkg/executor/builder.go:6059`) on a recursive
/// CTE whose producer build fails must leave the `CTEStorages` entry's
/// ResTbl/IterInTbl/Producer fields nil, and `resetCTEStorageMap`
/// (`pkg/executor/adapter.go:1908`) must clear the session's CTEStorageMap.
#[test]
#[ignore = "go-parity-gap: no CTEStorages build-failure teardown or resetCTEStorageMap seam; the tier's CTE storage is created lazily at run time"]
fn cte_storage_producer_build_error_cleans_storages() {}
