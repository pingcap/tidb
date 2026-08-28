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

//! Gap test for Go `pkg/executor/checksum_test.go::TestChecksum` (:23): the
//! `ADMIN CHECKSUM TABLE` executor over a partitioned table. The statement
//! parses on this tier (`AdminChecksumStmt` in `tidb-ast` mirrors Go's
//! `ast.AdminChecksumStmt`) but nothing dispatches it, and the expected
//! numbers come from unistore's mock coprocessor
//! (`pkg/store/mockstore/unistore/cophandler/cop_handler.go:758
//! handleCopChecksumRequest` answers Checksum=1, TotalKvs=1, TotalBytes=1
//! per request).

/// Go `pkg/executor/checksum_test.go:23::TestChecksum`: over a RANGE
/// partitioned table with a secondary index on 2 partitions, `ADMIN
/// CHECKSUM TABLE t` sends (indices+1)*(partitions+1) = 6 checksum requests
/// (`pkg/executor/checksum.go:223 buildTasks`), and unistore's mock answers
/// Checksum=1/TotalKvs=1/TotalBytes=1 per request
/// (`pkg/store/mockstore/unistore/cophandler/cop_handler.go:758`). The
/// executor folds them with XOR for the checksum and addition for the
/// counts (`pkg/executor/checksum.go:332 updateChecksumResponse`), so the
/// row is `test t 0 6 6` (six 1s XOR to 0) under the
/// Db_name/Table_name/Checksum_crc64_xor/Total_kvs/Total_bytes schema
/// (`pkg/planner/core/planbuilder.go:6196`).
#[test]
#[ignore = "go-parity-gap: ADMIN CHECKSUM TABLE has no executor on this tier and the expected counts encode unistore's mock coprocessor responses (cophandler cop_handler.go:758)"]
fn admin_checksum_table_sums_per_partition_mock_checksums() {}
