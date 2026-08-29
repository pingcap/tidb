// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Source-backed port of `pkg/executor/join/joiner_test.go:93::TestRequiredRows`.
//!
//! Go constructs one outer row and a full inner chunk for each row-producing
//! join type and asks the result chunk for a random positive number of rows.
//! `TryToMatchInners` must stop at that required-row budget even when the inner
//! chunk contains `MaxChunkSize` matching rows. The source uses zero-valued
//! LONG/FLOAT cells (`joiner_test.go:144::genTestChunk`), so every pair matches.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::iterator::LendingIterator;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::Columns;
use tidb_vardef::defaults::DEF_MAX_CHUNK_SIZE;

use crate::joiner::{JoinType, JoinerChunkSizes, NAAJType, new_joiner};

#[derive(Clone)]
struct TestCtx;

impl Columns for TestCtx {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
}

const LONG: FieldTypeCode = FieldTypeCode::Long;
const FLOAT: FieldTypeCode = FieldTypeCode::Float;

fn max_chunk_size() -> usize {
    DEF_MAX_CHUNK_SIZE as usize
}

fn test_chunk(num_rows: usize, codes: &[FieldTypeCode]) -> Chunk {
    let fields: Vec<FieldType> = codes.iter().map(|&code| FieldType::new(code)).collect();
    let mut chunk = Chunk::new(&fields, max_chunk_size(), max_chunk_size());
    for _ in 0..num_rows {
        for (column, &code) in codes.iter().enumerate() {
            match code {
                FieldTypeCode::Long => chunk.append_int64(column, 0),
                FieldTypeCode::Float => chunk.append_float32(column, 0.0),
                _ => panic!("unsupported test field type"),
            }
        }
    }
    chunk
}

struct RequiredDraws(u64);

impl RequiredDraws {
    fn next(&mut self) -> usize {
        // The Go test uses rand.Int()%MaxChunkSize + 1. A deterministic draw
        // keeps the same range without making this test depend on Go's global
        // random source state.
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        ((self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 33) as usize) % max_chunk_size() + 1
    }
}

/// Go `pkg/executor/join/joiner_test.go:93::TestRequiredRows`.
#[test]
fn required_rows_truncate_joiner_output_to_the_requested_count() {
    let join_types = [JoinType::Inner, JoinType::LeftOuter, JoinType::RightOuter];
    let type_shapes: &[&[FieldTypeCode]] = &[&[LONG], &[FLOAT], &[LONG, FLOAT]];
    let mut draws = RequiredDraws(0x9E37_79B9_7F4A_7C15);

    for &join_type in &join_types {
        for left_shape in type_shapes {
            for right_shape in type_shapes {
                let left_fields: Vec<FieldType> = left_shape
                    .iter()
                    .map(|&code| FieldType::new(code))
                    .collect();
                let right_fields: Vec<FieldType> = right_shape
                    .iter()
                    .map(|&code| FieldType::new(code))
                    .collect();
                let outer = test_chunk(1, left_shape);
                let inner = test_chunk(max_chunk_size(), right_shape);
                let default_inner: Vec<Datum> = (0..right_fields.len())
                    .map(|column| inner.get_row(0).get_datum(column, &right_fields[column]))
                    .collect();
                let mut joiner = new_joiner(
                    TestCtx,
                    join_type,
                    false,
                    &default_inner,
                    Vec::new(),
                    &left_fields,
                    &right_fields,
                    None,
                    false,
                    JoinerChunkSizes {
                        init_chunk_size: 32,
                        max_chunk_size: max_chunk_size(),
                    },
                );

                // The Go test declares the result fields as right then left
                // (`joiner_test.go:131-135`) and asserts only its row count.
                let mut result_fields = right_fields.clone();
                result_fields.extend_from_slice(&left_fields);
                let mut result = Chunk::new(&result_fields, max_chunk_size(), max_chunk_size());

                for _ in 0..10 {
                    let required = draws.next();
                    result.set_required_rows(required as isize, max_chunk_size());
                    result.reset();
                    let mut iterator = LendingIterator::chunk(&inner);
                    iterator.begin();
                    let (matched, _) = joiner
                        .try_to_match_inners(
                            outer.get_row(0),
                            &mut iterator,
                            &mut result,
                            NAAJType::Unknown,
                        )
                        .expect("joiner should evaluate zero-valued rows");
                    assert!(matched);
                    assert_eq!(result.num_rows(), required);
                }
            }
        }
    }
}
