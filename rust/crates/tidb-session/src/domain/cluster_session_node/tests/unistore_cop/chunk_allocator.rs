// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use crate::domain::resultset_source::ResultSetSource;
use tidb_chunk::alloc::{Allocator, ChunkAllocator};
use tidb_datatype::{FieldType, FieldTypeCode};

/// Go `TraceExec.Next` / `executeChild`, pinned by zzprobe/c02.TestC02TraceWriter:
/// the writer owns the three warmed columns before the child's HashAgg opens.
#[test]
fn a_trace_takes_its_writer_chunk_before_opening_its_child_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(154)).unwrap();
    for sql in [
        "CREATE TABLE test.tr(a INT,b VARCHAR(20))",
        "INSERT INTO test.tr VALUES(1,'x'),(2,'yy')",
        "SET tidb_executor_concurrency=3",
    ] {
        rows(&mut session, sql);
    }
    session.attach_client_conn_writer();
    for sql in [
        "TRACE SELECT /*+ hash_agg() */ b,count(a) FROM test.tr GROUP BY b",
        "TRACE TRACE SELECT /*+ hash_agg() */ b,count(a) FROM test.tr GROUP BY b",
        "TRACE EXPLAIN ANALYZE SELECT /*+ hash_agg() */ b,count(a) FROM test.tr GROUP BY b",
        "TRACE SELECT missing FROM test.tr",
    ] {
        session.chunk_alloc = ChunkAllocator::new();
        let fields = vec![FieldType::new(FieldTypeCode::VarString); 3];
        let mut seed = session
            .chunk_alloc
            .alloc(&fields, 32, 1024)
            .into_pooled_chunk();
        for (column, size) in [1024, 2048, 4096].into_iter().enumerate() {
            seed.append_string(column, &"x".repeat(size));
        }
        drop(seed);
        let mut result = session.node_conn().execute(sql).unwrap();
        let source = result.source();
        let mut chunk = source.new_write_chunk().expect("connection writer chunk");
        assert_eq!(
            chunk.memory_usage(),
            8308,
            "Go takes the warmed columns before the child"
        );
        loop {
            source.next_chunk(&mut chunk).unwrap();
            if chunk.num_rows() == 0 {
                break;
            }
        }
        source.finish().unwrap();
        source.close().unwrap();
    }
}
