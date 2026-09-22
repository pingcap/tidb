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

use super::*;
use tidb_ast::CiString;
use tidb_datatype::{Decimal, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::scalar_function::ScalarFunction;
pub(super) use tidb_expr::NoColumns;

const CHUNK: usize = 1024;

fn long() -> FieldType {
    FieldType::new(FieldTypeCode::Long)
}

/// Go Selection evaluates NULL predicates over chunk rows before the index
/// join retains its inner chunks. Existing selections are logical row order.
#[test]
fn index_inner_null_selection_preserves_chunk_rows() {
    let types = [long(), FieldType::new(FieldTypeCode::VarString)];
    let mut chunk = Chunk::new_with_capacity(&types, 4);
    for key in [Some(1), None, Some(3), None] {
        match key {
            Some(key) => chunk.append_int64(0, key),
            None => chunk.append_null(0),
        }
        chunk.append_bytes(1, b"payload kept in the original column");
    }
    let payload = chunk.column(1).get_raw(0).as_ptr();
    // The existing selection reorders and duplicates rows.
    chunk.set_sel(Some(vec![2, 1, 0, 2, 3]));
    filter_index_inner_chunk(&mut chunk, &[0]).unwrap();
    assert_eq!(chunk.sel(), Some([2, 0, 2].as_slice()));
    assert_eq!(chunk.column(1).get_raw(0).as_ptr(), payload);
    assert_eq!(
        (0..chunk.num_rows())
            .map(|row| chunk.get_row(row).get_int64(0))
            .collect::<Vec<_>>(),
        [3, 1, 3]
    );
    let selection = chunk.sel().unwrap().as_ptr();
    filter_index_inner_chunk(&mut chunk, &[0]).unwrap();
    assert_eq!(chunk.sel().unwrap().as_ptr(), selection);

    chunk.set_sel(Some(vec![1, 3]));
    filter_index_inner_chunk(&mut chunk, &[0]).unwrap();
    assert_eq!(chunk.num_rows(), 0);
    chunk.set_sel(None);
    filter_index_inner_chunk(&mut chunk, &[1]).unwrap();
    assert!(chunk.sel().is_none(), "all-pass batches need no selection");
    filter_index_inner_chunk(&mut chunk, &[0, 1]).unwrap();
    assert_eq!(chunk.sel(), Some([0, 2].as_slice()));
    assert!(filter_index_inner_chunk(&mut chunk, &[2]).is_err());
    assert_eq!(chunk.sel(), Some([0, 2].as_slice()));
    assert_eq!(chunk.column(1).get_raw(0).as_ptr(), payload);
}

fn decimal(text: &str) -> Decimal {
    let (value, error) = Decimal::parse_mysql(text);
    assert_eq!(error, None, "invalid decimal fixture {text}");
    value
}

/// Go IndexLookUpJoin.Next resumes innerIter across RequiredRows boundaries.
#[test]
fn index_join_resumes_one_outer_rows_matches_across_requested_chunks() {
    for ((worker_prepared, kind, residual_limit), hash_order) in [false, true]
        .into_iter()
        .flat_map(|worker_prepared| {
            [
                JoinKind::Inner,
                JoinKind::Left,
                JoinKind::Right,
                JoinKind::Semi,
                JoinKind::AntiSemi,
            ]
            .into_iter()
            .flat_map(move |kind| {
                [None, Some(2), Some(0)]
                    .into_iter()
                    .map(move |limit| (worker_prepared, kind, limit))
            })
        })
        .flat_map(|case| {
            [None, Some(false), Some(true)]
                .into_iter()
                .map(move |order| (case, order))
        })
    {
        let types = [long(), long()];
        let mut conditions = vec![eq_on(0, 0, 2)];
        if let Some(limit) = residual_limit {
            let offset = if kind == JoinKind::Right { 1 } else { 3 };
            let mut column = Column::new(offset + 1, long());
            column.index = offset;
            conditions.push(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("lt"),
                long(),
                vec![
                    Expression::Column(column),
                    Expression::Constant(Constant::new(Datum::Int(limit), long())),
                ],
            )));
        }
        let mut join = join_of(kind, conditions, vec![], vec![], 2);
        join.index_hash = hash_order;
        let mut outer_chunk = Chunk::new_with_capacity(&types, 2);
        for (key, value) in [(1, 77), (2, 88)] {
            outer_chunk.append_int64(0, key);
            outer_chunk.append_int64(1, value);
        }
        let mut outer = OuterBatch::new(&types, 2, CHUNK);
        for index in 0..outer_chunk.num_rows() {
            outer.push(outer_chunk.get_row(index));
        }
        let mut inner = List::new(&types, 2, CHUNK);
        let mut input = Chunk::new_with_capacity(&types, 9);
        for value in 0..9 {
            input.append_int64(0, 1);
            input.append_int64(1, value);
        }
        let prepared = worker_prepared.then(|| {
            prepare_index_inner(&index_task_shared_for_test(&join), vec![input.clone()]).unwrap()
        });
        inner.add(input);
        let mut matched: FastBytesMap<Vec<RowPtr>> = FastBytesMap::default();
        for index in 0..inner.len() {
            let ptr = RowPtr::new(0, index as u32);
            let row = inner.get_row(ptr);
            let key = row_key_by(&join.keys, |key| {
                row.get_datum(key.right, &types[key.right])
            })
            .unwrap()
            .unwrap();
            matched.entry(key).or_default().push(ptr);
        }
        let matched_count = residual_limit.unwrap_or(9);
        let mut expected = Vec::new();
        for (key, payload) in [(1, 77), (2, 88)] {
            let count = if key == 1 { matched_count } else { 0 };
            match kind {
                JoinKind::Semi if count > 0 => {
                    expected.push(vec![Datum::Int(key), Datum::Int(payload)])
                }
                JoinKind::AntiSemi if count == 0 => {
                    expected.push(vec![Datum::Int(key), Datum::Int(payload)])
                }
                JoinKind::Left | JoinKind::Right if count == 0 => {
                    expected.push(if kind == JoinKind::Left {
                        vec![
                            Datum::Int(key),
                            Datum::Int(payload),
                            Datum::Null,
                            Datum::Null,
                        ]
                    } else {
                        vec![
                            Datum::Null,
                            Datum::Null,
                            Datum::Int(key),
                            Datum::Int(payload),
                        ]
                    });
                }
                JoinKind::Inner | JoinKind::Left | JoinKind::Right => {
                    for value in 0..count {
                        expected.push(if kind == JoinKind::Right {
                            vec![
                                Datum::Int(1),
                                Datum::Int(value),
                                Datum::Int(key),
                                Datum::Int(payload),
                            ]
                        } else {
                            vec![
                                Datum::Int(key),
                                Datum::Int(payload),
                                Datum::Int(1),
                                Datum::Int(value),
                            ]
                        });
                    }
                }
                _ => {}
            }
        }
        join.index_state = Some(IndexLookupState {
            unordered: None,
            hash: None,
            hash_results: None,
            hash_chunk: None,
            hash_task_bytes: 0,
            outer,
            cursor: 0,
            probe: None,
            condition_scratch: residual_limit
                .map(|_| Chunk::new_with_capacity(&join.condition_types, 1)),
            inner,
            inner_bytes: 0,
            matched,
            outer_chunk,
            outer_row: 2,
            outer_done: true,
            batch_size: CHUNK,
            pending: VecDeque::new(),
            prefetch_disabled: true,
            max_batch_size: INDEX_JOIN_BATCH_SIZE,
        });
        if let Some(prepared) = prepared {
            prepared.install(join.index_state.as_mut().unwrap());
        }
        let mut actual = Vec::new();
        let mut output = join.new_chunk();
        for required in [1, 3, 2, 1, 3, 2] {
            output.reset();
            output.set_required_rows(required, CHUNK);
            join.drain_index_batch(&mut output).unwrap();
            assert!(
                output.num_rows() <= required as usize,
                "worker_prepared={worker_prepared}: {} > {required}",
                output.num_rows()
            );
            actual.extend(
                (0..output.num_rows())
                    .map(|index| output.get_row(index).get_datum_row(join.ret_field_types())),
            );
        }
        assert_eq!(
            actual, expected,
            "worker_prepared={worker_prepared} hash_order={hash_order:?} kind={kind:?} residual={residual_limit:?}"
        );
        join.close().unwrap();
        assert_eq!(join.tracker.bytes_consumed(), 0);
    }
}

fn schema_of(width: usize) -> Schema {
    schema_with_types(&vec![long(); width])
}

/// Go indexHashJoinInnerWorker.doJoinUnordered builds outer rows and probes
/// inner rows; doJoinInOrder retains match pointers in outer-row order.
#[test]
fn index_hash_join_builds_outer_and_probes_inner_rows() {
    for ordered in [false, true] {
        let mut join = join_of(JoinKind::Inner, vec![eq_on(0, 0, 1)], vec![], vec![], 1);
        join.index_hash = Some(ordered);
        let types = [long()];
        let mut outer = OuterBatch::new(&types, 2, CHUNK);
        let mut outer_chunk = Chunk::new_with_capacity(&types, 2);
        for key in [1, 2] {
            outer_chunk.append_int64(0, key);
        }
        for index in 0..2 {
            outer.push(outer_chunk.get_row(index));
        }
        let mut inner = List::new(&types, 2, CHUNK);
        let mut chunk = Chunk::new_with_capacity(&types, 2);
        for key in [2, 1] {
            chunk.append_int64(0, key);
        }
        inner.add(chunk);
        let matched = build_index_lookup_map(&inner, &join.keys, &types, true).unwrap();
        join.index_state = Some(IndexLookupState {
            unordered: None,
            hash: None,
            hash_results: None,
            hash_chunk: None,
            hash_task_bytes: 0,
            outer,
            cursor: 0,
            probe: None,
            condition_scratch: None,
            inner,
            inner_bytes: 0,
            matched,
            outer_chunk,
            outer_row: 2,
            outer_done: true,
            batch_size: CHUNK,
            pending: VecDeque::new(),
            prefetch_disabled: true,
            max_batch_size: INDEX_JOIN_BATCH_SIZE,
        });
        let mut output = join.new_chunk();
        join.drain_index_batch(&mut output).unwrap();
        let keys = (0..output.num_rows())
            .map(|i| output.get_row(i).get_int64(0))
            .collect::<Vec<_>>();
        assert_eq!(keys, if ordered { vec![1, 2] } else { vec![2, 1] });
        join.close().unwrap();
    }
}

/// Go `constructLookupContent` sorts one batch and removes duplicate lookup
/// keys; it does not allocate one ordered-map node per outer row.
#[test]
fn index_lookup_probe_collection_batches_sort_and_dedup() {
    let types = [long()];
    let mut source = Chunk::new_with_capacity(&types, 3);
    for key in [2, 1, 2] {
        source.append_int64(0, key);
    }
    let mut outer = OuterBatch::new(&types, 3, CHUNK);
    for row in 0..source.num_rows() {
        outer.push(source.get_row(row));
    }
    let keys = [EquiKey {
        left: 0,
        right: 0,
        class: KeyClass::Int,
        null_safe: false,
    }];
    let plan = IndexProbePlan::new(&keys, vec![0], Vec::new(), Vec::new()).unwrap();
    let probes = index_task_probes(&NoColumns, &keys, &plan, &outer, &types, true).unwrap();
    assert_eq!(
        probes
            .iter()
            .map(|probe| probe.key[0].clone())
            .collect::<Vec<_>>(),
        vec![Datum::Int(1), Datum::Int(2)]
    );
}

/// Go fetchInnerResults retains the reader and outer match status across windows.
#[test]
fn index_hash_fetches_inner_windows_before_final_unmatched_rows() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Go supportIncrementalLookUp/fetchInnerResults: preserve match status
    // across fetch windows; a match in the last window is not an early miss.
    for kind in [
        JoinKind::Left,
        JoinKind::Inner,
        JoinKind::Right,
        JoinKind::AntiSemi,
        JoinKind::Semi,
    ] {
        for (ordered, fail) in [false, true]
            .into_iter()
            .flat_map(|ordered| [false, true].map(|fail| (ordered, fail)))
        {
            let outer = vec![
                vec![Datum::Int(1)],
                vec![Datum::Int(2)],
                vec![Datum::Int(3)],
            ];
            let (left, right) = if kind == JoinKind::Right {
                (vec![], outer)
            } else {
                (outer, vec![])
            };
            let mut join = join_of(kind, vec![eq_on(0, 0, 1)], left, right, 1);
            join.index_hash = Some(ordered);
            let read_rows = Arc::new(AtomicUsize::new(0));
            let mut rows = vec![vec![Datum::Int(1)]; 8192];
            rows.push(vec![Datum::Int(2)]);
            let mut source = RowSource::new(rows, 1);
            source.read_rows = Some(Arc::clone(&read_rows));
            source.fail_after = fail.then_some(8192);
            join.index_lookup = Some(IndexLookupPlan {
                lookup_is_left: kind == JoinKind::Right,
                probe_keys: vec![0],
                probe_key_domains: vec![IndexProbeKeyDomain {
                    field_type: long(),
                    prefix_length: -1,
                }],
                source: IndexLookupSource::Composite {
                    exec: Box::new(source),
                    probes: Arc::new(std::sync::Mutex::new(Default::default())),
                },
                outer_not_null: vec![],
                inner_not_null: vec![],
                probe_bounds: vec![],
            });
            join.open().unwrap();
            let incremental = !ordered && kind != JoinKind::Semi;
            let loaded = join.fill_index_batch(3);
            if fail && !incremental {
                assert!(loaded.is_err());
                join.close().unwrap();
                assert_eq!(join.tracker.bytes_consumed(), 0);
                continue;
            }
            assert!(loaded.unwrap());
            assert_eq!(
                read_rows.load(Ordering::SeqCst),
                if incremental { 4096 } else { 8193 }
            );
            let mut result = Vec::new();
            loop {
                let mut chunk = join.new_chunk();
                chunk.set_required_rows(17, CHUNK);
                if let Err(error) = join.next(&mut chunk) {
                    assert!(fail && format!("{error:?}").contains("inner reader failed"));
                    // Go TestIndexLookupJoinHang calls Next repeatedly after
                    // an error: the failed task cannot strand the consumer.
                    for _ in 0..3 {
                        join.next(&mut chunk).unwrap();
                        assert_eq!(chunk.num_rows(), 0);
                    }
                    break;
                }
                if chunk.num_rows() == 0 {
                    break;
                }
                assert!(chunk.num_rows() <= 17);
                result.extend(
                    (0..chunk.num_rows())
                        .map(|i| chunk.get_row(i).get_datum_row(join.ret_field_types())),
                );
            }
            if fail {
                assert_eq!(read_rows.load(Ordering::SeqCst), 8192);
                assert!(result.iter().all(|row| !row.contains(&Datum::Null)));
                join.close().unwrap();
                assert_eq!(join.tracker.bytes_consumed(), 0);
                continue;
            }
            assert_eq!(read_rows.load(Ordering::SeqCst), 8193);
            match kind {
                JoinKind::AntiSemi => assert_eq!(result, vec![vec![Datum::Int(3)]]),
                JoinKind::Semi => {
                    assert_eq!(result, vec![vec![Datum::Int(1)], vec![Datum::Int(2)]])
                }
                _ => {
                    assert_eq!(
                        result.len(),
                        if kind == JoinKind::Inner { 8193 } else { 8194 }
                    );
                    assert_eq!(
                        result
                            .iter()
                            .filter(|row| **row == vec![Datum::Int(2), Datum::Int(2)])
                            .count(),
                        1
                    );
                    assert_eq!(
                        result
                            .iter()
                            .filter(|row| row.contains(&Datum::Null))
                            .count(),
                        usize::from(kind != JoinKind::Inner)
                    );
                }
            }
            join.close().unwrap();
            assert_eq!(join.tracker.bytes_consumed(), 0);
        }
    }
}

#[test]
fn index_hash_worker_streams_bounded_output_and_releases_on_close() {
    for ordered in [false, true] {
        for early_close in [false, true] {
            let mut join = join_of(JoinKind::Left, vec![eq_on(0, 0, 1)], vec![], vec![], 1);
            join.index_hash = Some(ordered);
            let mut shared = index_task_shared_for_test(&join);
            shared.max_chunk_size = 2;
            let types = [long()];
            shared.inner_types = types.to_vec();
            let mut outer = OuterBatch::new(&types, 2, CHUNK);
            let mut outer_chunk = Chunk::new_with_capacity(&types, 2);
            for key in [1, 1, 2] {
                outer_chunk.append_int64(0, key);
            }
            for index in 0..3 {
                outer.push(outer_chunk.get_row(index));
            }
            let bytes = outer.settle_bytes();
            if ordered {
                join.tracker.consume(bytes);
            }
            let read_rows = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut input = vec![vec![Datum::Int(1)]; 8192];
            input.push(vec![Datum::Int(2)]);
            let mut source = RowSource::new(input, 1);
            source.read_rows = Some(Arc::clone(&read_rows));
            let (inner, inner_rows, done) = fetch_index_inner(
                &shared,
                &mut source,
                shared.hash_output.as_ref().unwrap().max_fetch_rows(),
                None,
            )
            .unwrap();
            assert!(
                inner.matched.is_empty(),
                "hash variant must not build the inner map"
            );
            let outcome = IndexTaskOutcome::Prepared {
                outer,
                inner,
                inner_rows,
            };
            let (sender, receiver, mut unordered) = if ordered {
                let (sender, receiver) = std::sync::mpsc::sync_channel(1);
                (sender.into(), Some(receiver), None)
            } else {
                let mut tasks = IndexUnordered::new(2, &join.tracker);
                (tasks.admit(bytes), None, Some(tasks))
            };
            let worker = std::thread::spawn(move || {
                index_hash::send_prepared_with_hash(
                    &shared,
                    outcome,
                    sender,
                    None,
                    (!done).then_some(&mut source as &mut dyn Executor),
                );
                source.close().unwrap();
            });
            let first = if let Some(tasks) = unordered.as_mut() {
                tasks.recv().unwrap()
            } else {
                receiver.as_ref().unwrap().recv().unwrap()
            };
            let IndexTaskOutcome::HashChunk(chunk) = first else {
                panic!("worker must return chunk output");
            };
            assert_eq!(chunk.chunk.num_rows(), 2);
            assert_eq!(
                read_rows.load(std::sync::atomic::Ordering::SeqCst),
                if ordered { 8193 } else { 4096 }
            );
            let first_buffer = chunk.chunk.column(0).get_raw(0).as_ptr();
            join.index_state = Some(IndexLookupState {
                unordered,
                hash: None,
                hash_results: receiver,
                hash_chunk: Some(chunk),
                hash_task_bytes: if ordered { bytes } else { 0 },
                outer: OuterBatch::new(&types, 1, CHUNK),
                cursor: 0,
                probe: None,
                condition_scratch: None,
                inner: List::new(&types, 1, CHUNK),
                inner_bytes: 0,
                matched: FastBytesMap::default(),
                outer_chunk,
                outer_row: 3,
                outer_done: true,
                batch_size: CHUNK,
                pending: VecDeque::new(),
                prefetch_disabled: false,
                max_batch_size: INDEX_JOIN_BATCH_SIZE,
            });
            let mut rows = Vec::new();
            loop {
                let state = join.index_state.as_mut().unwrap();
                if state.hash_chunk.is_none() {
                    if let Some(tasks) = state.unordered.as_mut() {
                        if tasks.active == 0 {
                            break;
                        }
                        match tasks.recv().unwrap() {
                            IndexTaskOutcome::HashChunk(chunk) => state.hash_chunk = Some(chunk),
                            IndexTaskOutcome::HashFinished { .. } => continue,
                            _ => panic!("unexpected worker outcome"),
                        }
                    } else if state.hash_results.is_none() {
                        break;
                    }
                }
                let mut output = join.new_chunk();
                let requested = if early_close { 2 } else { 1 };
                output.set_required_rows(requested, CHUNK);
                join.drain_index_batch(&mut output).unwrap();
                assert!(output.num_rows() <= requested as usize);
                if early_close {
                    assert_eq!(
                        output.column(0).get_raw(0).as_ptr(),
                        first_buffer,
                        "complete output chunks transfer their column buffers"
                    );
                }
                rows.extend(
                    (0..output.num_rows())
                        .map(|i| output.get_row(i).get_datum_row(join.ret_field_types())),
                );
                if early_close {
                    break;
                }
            }
            if !early_close {
                assert_eq!(rows.len(), 16385);
                assert_eq!(rows.last().unwrap(), &vec![Datum::Int(2), Datum::Int(2)]);
            }
            join.close().unwrap();
            worker.join().unwrap();
            assert_eq!(
                read_rows.load(std::sync::atomic::Ordering::SeqCst),
                if early_close && !ordered { 4096 } else { 8193 }
            );
            assert_eq!(join.tracker.bytes_consumed(), 0);
        }
    }
}

/// Go innerWorker.handleTask prepares the inner lookup; ON evaluation belongs
/// to IndexLookUpJoin.Next. A later output error must not run in preparation.
#[test]
fn index_inner_windows_reuse_buffers_and_close_on_fetch_error() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let mut join = join_of(JoinKind::Left, vec![eq_on(0, 0, 1)], vec![], vec![], 1);
    join.index_hash = Some(false);
    let mut shared = index_task_shared_for_test(&join);
    shared.inner_types = vec![long()];
    let closes = Arc::new(AtomicUsize::new(0));
    let mut source = RowSource::new(vec![vec![Datum::Int(1)]; 12288], 1);
    source.fail_after = Some(8192);
    source.closes = Some(Arc::clone(&closes));
    let (first, count, done) = fetch_index_inner(&shared, &mut source, Some(4096), None).unwrap();
    assert_eq!((count, done), (4096, false));
    let pointers = |inner: &PreparedIndexInner| {
        let mut pointers = (0..inner.rows.num_chunks())
            .map(|i| inner.rows.get_chunk(i).column(0).get_raw(0).as_ptr() as usize)
            .collect::<Vec<_>>();
        pointers.sort_unstable();
        pointers
    };
    let first_pointers = pointers(&first);
    let bytes = join.tracker.bytes_consumed();
    let (second, count, done) =
        fetch_index_inner(&shared, &mut source, Some(4096), Some(first)).unwrap();
    assert_eq!((count, done), (4096, false));
    assert_eq!(pointers(&second), first_pointers);
    assert_eq!(join.tracker.bytes_consumed(), bytes);
    assert_eq!(closes.load(Ordering::SeqCst), 0);
    assert!(fetch_index_inner(&shared, &mut source, Some(4096), Some(second)).is_err());
    assert_eq!(closes.load(Ordering::SeqCst), 1);
    assert_eq!(join.tracker.bytes_consumed(), 0);
}

#[test]
fn index_hash_worker_fetch_error_does_not_emit_unmatched_rows() {
    let mut join = join_of(JoinKind::Left, vec![eq_on(0, 0, 1)], vec![], vec![], 1);
    join.index_hash = Some(false);
    let mut shared = index_task_shared_for_test(&join);
    shared.inner_types = vec![long()];
    let mut source = RowSource::new(vec![vec![Datum::Int(1)]; 8192], 1);
    source.fail_after = Some(4096);
    let (inner, inner_rows, done) =
        fetch_index_inner(&shared, &mut source, Some(4096), None).unwrap();
    assert!(!done);
    let mut outer = OuterBatch::new(&[long()], 2, CHUNK);
    let mut chunk = Chunk::new_with_capacity(&[long()], 2);
    chunk.append_int64(0, 1);
    chunk.append_int64(0, 2);
    for i in 0..2 {
        outer.push(chunk.get_row(i));
    }
    let mut tasks = IndexUnordered::new(2, &join.tracker);
    let sender = tasks.admit(outer.settle_bytes());
    let worker = std::thread::spawn(move || {
        index_hash::send_prepared_with_hash(
            &shared,
            IndexTaskOutcome::Prepared {
                outer,
                inner,
                inner_rows,
            },
            sender,
            None,
            Some(&mut source),
        );
    });
    let mut count = 0;
    loop {
        match tasks.recv() {
            Ok(IndexTaskOutcome::HashChunk(chunk)) => {
                for i in 0..chunk.chunk.num_rows() {
                    assert!(!chunk.chunk.get_row(i).is_null(1));
                }
                count += chunk.chunk.num_rows();
            }
            Err(error) => {
                assert!(format!("{error:?}").contains("inner reader failed"));
                break;
            }
            _ => panic!("failed fetch must not finish the task"),
        }
    }
    assert_eq!(count, 4096);
    assert_eq!(tasks.active, 0);
    worker.join().unwrap();
    drop(tasks);
    assert_eq!(join.tracker.bytes_consumed(), 0);
}

#[test]
fn index_worker_preparation_does_not_evaluate_join_residuals() {
    let mut column = Column::new(4, long());
    column.index = 3;
    let residual = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("gt"),
        long(),
        vec![
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("abs"),
                long(),
                vec![Expression::Column(column)],
            )),
            Expression::Constant(Constant::new(Datum::Int(0), long())),
        ],
    ));
    let mut join = join_of(
        JoinKind::Left,
        vec![eq_on(0, 0, 2), residual],
        vec![],
        vec![],
        2,
    );
    let types = [long(), long()];
    let mut outer = OuterBatch::new(&types, 1, CHUNK);
    let mut input = Chunk::new_with_capacity(&types, 1);
    input.append_int64(0, 1);
    input.append_int64(1, 1);
    outer.push(input.get_row(0));
    let shared = index_task_shared_for_test(&join);
    let mut input = Chunk::new_with_capacity(&types, 2);
    for value in [1, i64::MIN] {
        input.append_int64(0, 1);
        input.append_int64(1, value);
    }
    let prepared = prepare_index_inner(&shared, vec![input.clone()]).unwrap_or_else(|error| {
        panic!("preparing inner rows evaluated the ON expression: {error:?}")
    });
    assert_eq!(prepared.rows.len(), 2);
    assert!(join.tracker.bytes_consumed() > 0);
    drop(prepared);
    assert_eq!(join.tracker.bytes_consumed(), 0);
    let (sender, receiver) = std::sync::mpsc::channel();
    drop(receiver);
    drop(sender.send(prepare_index_inner(&shared, vec![input.clone()]).unwrap()));
    assert_eq!(
        join.tracker.bytes_consumed(),
        0,
        "failed delivery releases the prepared batch"
    );

    let prepared = prepare_index_inner(&shared, vec![input]).unwrap();
    join.index_state = Some(IndexLookupState {
        unordered: None,
        hash: None,
        hash_results: None,
        hash_chunk: None,
        hash_task_bytes: 0,
        outer,
        cursor: 0,
        probe: None,
        condition_scratch: Some(Chunk::new_with_capacity(&join.condition_types, 1)),
        inner: List::new(&types, 1, CHUNK),
        inner_bytes: 0,
        matched: FastBytesMap::default(),
        outer_chunk: Chunk::new_with_capacity(&types, 1),
        outer_row: 0,
        outer_done: true,
        batch_size: CHUNK,
        pending: VecDeque::new(),
        prefetch_disabled: false,
        max_batch_size: INDEX_JOIN_BATCH_SIZE,
    });
    prepared.install(join.index_state.as_mut().unwrap());
    let mut output = join.new_chunk();
    output.set_required_rows(1, CHUNK);
    join.drain_index_batch(&mut output).unwrap();
    assert_eq!(
        output.num_rows(),
        1,
        "LIMIT can stop before the overflowing candidate"
    );
    assert_eq!(join.condition_evals(), 1);
    output.reset();
    assert!(join.drain_index_batch(&mut output).is_err());
    assert_eq!(join.condition_evals(), 2);
    // Failed workers must return the outer batch that owns the pending
    // task's charge, so the consumer's Close path can release it.
    let mut outer = OuterBatch::new(&types, 1, CHUNK);
    let mut input = Chunk::new_with_capacity(&types, 1);
    input.append_int64(0, 1);
    input.append_int64(1, 1);
    outer.push(input.get_row(0));
    let bytes = outer.settle_bytes();
    join.tracker.consume(bytes);
    let mut invalid = index_task_shared_for_test(&join);
    invalid.probe_plan = IndexProbePlan::new(
        &invalid.keys,
        vec![0],
        vec![
            IndexProbeKeyDomain {
                field_type: long(),
                prefix_length: -1,
            };
            2
        ],
        Vec::new(),
    )
    .unwrap();
    match run_index_task(&invalid, outer) {
        IndexTaskOutcome::Failed { outer, .. } => {
            assert_eq!(outer.bytes, bytes);
            join.index_state.as_mut().unwrap().outer = outer;
        }
        _ => panic!("invalid lookup domains must return the failed task"),
    }
    join.close().unwrap();
    assert_eq!(join.tracker.bytes_consumed(), 0);
}

fn index_task_shared_for_test(join: &JoinExec<NoColumns>) -> IndexTaskShared<NoColumns> {
    IndexTaskShared {
        hash_output: join.index_hash_output(),
        template: None,
        probe_plan: IndexProbePlan::new(&join.keys, vec![], vec![], vec![]).unwrap(),
        outer_is_left: join.outer_is_left(),
        keys: join.keys.clone(),
        ctx: NoColumns,
        outer_types: vec![long(), long()],
        inner_types: vec![long(), long()],
        inner_not_null: vec![],
        init_cap: 1,
        max_chunk_size: CHUNK,
        tracker: Arc::clone(&join.tracker),
        memory: join.memory.clone(),
    }
}

fn schema_with_types(types: &[FieldType]) -> Schema {
    Schema::new(
        types
            .iter()
            .enumerate()
            .map(|(i, field_type)| {
                let mut column = Column::new(i as i64 + 1, field_type.clone());
                column.index = i as i64;
                column
            })
            .collect(),
    )
}

/// A source that hands out prebuilt rows in `max_chunk_size` batches, so
/// the probe side really is pulled incrementally rather than in one go.
struct RowSource {
    meta: ExecutorMeta,
    rows: Vec<Vec<Datum>>,
    cursor: usize,
    read_rows: Option<Arc<std::sync::atomic::AtomicUsize>>,
    fail_after: Option<usize>,
    closes: Option<Arc<std::sync::atomic::AtomicUsize>>,
}

impl RowSource {
    fn new(rows: Vec<Vec<Datum>>, width: usize) -> Self {
        Self::with_types(rows, &vec![long(); width])
    }

    fn with_types(rows: Vec<Vec<Datum>>, types: &[FieldType]) -> Self {
        RowSource {
            meta: ExecutorMeta::new(schema_with_types(types), 0, CHUNK, CHUNK),
            rows,
            cursor: 0,
            read_rows: None,
            fail_after: None,
            closes: None,
        }
    }
}

impl Executor for RowSource {
    fn open(&mut self) -> Result<(), ExecError> {
        self.cursor = 0;
        Ok(())
    }
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.fail_after.is_some_and(|limit| self.cursor >= limit) {
            return Err(ExecError::internal(
                "inner reader failed after its first window",
            ));
        }
        let end = (self.cursor + CHUNK).min(self.rows.len());
        for row in &self.rows[self.cursor..end] {
            for (c, value) in row.iter().enumerate() {
                req.append_datum(c, value);
            }
        }
        self.cursor = end;
        if let Some(read_rows) = &self.read_rows {
            read_rows.store(end, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }
    fn close(&mut self) -> Result<(), ExecError> {
        if let Some(closes) = &self.closes {
            closes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }
    fn schema(&self) -> &Schema {
        self.meta.schema()
    }
    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }
    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }
    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }
    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

/// `left.<lhs> = right.<rhs>`, addressed against the joined schema.
pub(super) fn eq_on(lhs: usize, rhs: usize, left_width: usize) -> Expression {
    let column = |index: usize| {
        let mut column = Column::new(index as i64 + 1, long());
        column.index = index as i64;
        Expression::Column(column)
    };
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("eq"),
        long(),
        vec![column(lhs), column(left_width + rhs)],
    ))
}

pub(super) fn join_of(
    kind: JoinKind,
    conditions: Vec<Expression>,
    left: Vec<Vec<Datum>>,
    right: Vec<Vec<Datum>>,
    width: usize,
) -> JoinExec<NoColumns> {
    join_with_memory(
        kind,
        conditions,
        left,
        right,
        width,
        StatementMemory::default(),
    )
}

pub(super) fn join_with_memory(
    kind: JoinKind,
    conditions: Vec<Expression>,
    left: Vec<Vec<Datum>>,
    right: Vec<Vec<Datum>>,
    width: usize,
    memory: StatementMemory,
) -> JoinExec<NoColumns> {
    let output_width = if matches!(kind, JoinKind::Semi | JoinKind::AntiSemi) {
        width
    } else {
        2 * width
    };
    let mut executor = JoinExec::new(
        ExecutorMeta::new(schema_of(output_width), 1, CHUNK, CHUNK),
        kind,
        conditions,
        Box::new(RowSource::new(left, width)),
        Box::new(RowSource::new(right, width)),
        NoColumns,
        memory,
    );
    // Go's `HashJoinConcurrency()` falls back to `tidb_executor_concurrency`
    // (5) when `tidb_hash_join_concurrency` is unset; the production builder
    // sets it from the plan. The test harness must too, or the bounded
    // parallel probe path never runs.
    executor.set_parallelism(5);
    executor
}

fn join_with_types(
    conditions: Vec<Expression>,
    left: Vec<Vec<Datum>>,
    left_types: &[FieldType],
    right: Vec<Vec<Datum>>,
    right_types: &[FieldType],
) -> JoinExec<NoColumns> {
    let output_types = left_types
        .iter()
        .chain(right_types)
        .cloned()
        .collect::<Vec<_>>();
    let mut executor = JoinExec::new(
        ExecutorMeta::new(schema_with_types(&output_types), 1, CHUNK, CHUNK),
        JoinKind::Inner,
        conditions,
        Box::new(RowSource::with_types(left, left_types)),
        Box::new(RowSource::with_types(right, right_types)),
        NoColumns,
        StatementMemory::default(),
    );
    executor.set_parallelism(5);
    executor
}

fn run_datums(join: &mut JoinExec<NoColumns>) -> Vec<Vec<Datum>> {
    join.open().unwrap();
    let types = join.ret_field_types().to_vec();
    let mut out = Vec::new();
    let mut req = join.new_chunk();
    loop {
        join.next(&mut req).unwrap();
        if req.num_rows() == 0 {
            break;
        }
        for row in 0..req.num_rows() {
            out.push(req.get_row(row).get_datum_row(&types));
        }
    }
    join.close().unwrap();
    out
}

#[test]
fn decimal_residual_uses_the_general_join_evaluator() {
    let decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
    let column = |index: usize, field_type: FieldType| {
        let mut column = Column::new(index as i64 + 1, field_type);
        column.index = index as i64;
        Expression::Column(column)
    };
    let residual = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("lt"),
        long(),
        vec![
            column(1, decimal_type.clone()),
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("mul"),
                decimal_type.clone(),
                vec![
                    Expression::Constant(Constant::new(
                        Datum::Decimal(decimal("0.2")),
                        decimal_type.clone(),
                    )),
                    column(3, decimal_type.clone()),
                ],
            )),
        ],
    ));
    let conditions = vec![eq_on(0, 0, 2), residual];
    let left = vec![
        vec![Datum::Int(1), Datum::Decimal(decimal("3.00"))],
        vec![Datum::Int(1), Datum::Decimal(decimal("12.00"))],
    ];
    let right = vec![vec![Datum::Int(1), Datum::Decimal(decimal("25.00"))]];
    let types = [long(), decimal_type];

    let mut join = join_with_types(conditions, left, &types, right, &types);
    let rows = run_datums(&mut join);

    assert_eq!(
        rows,
        vec![vec![
            Datum::Int(1),
            Datum::Decimal(decimal("3.00")),
            Datum::Int(1),
            Datum::Decimal(decimal("25.00")),
        ]]
    );
}

#[test]
fn decimal_residual_unique_integer_join_uses_parallel_probe_window() {
    let decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
    let column = |index: usize, field_type: FieldType| {
        let mut column = Column::new(index as i64 + 1, field_type);
        column.index = index as i64;
        Expression::Column(column)
    };
    let residual = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("lt"),
        long(),
        vec![
            column(1, decimal_type.clone()),
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("mul"),
                decimal_type.clone(),
                vec![
                    Expression::Constant(Constant::new(
                        Datum::Decimal(decimal("0.2")),
                        decimal_type.clone(),
                    )),
                    column(3, decimal_type.clone()),
                ],
            )),
        ],
    ));
    let left = (0..10_000)
        .map(|row| {
            vec![
                Datum::Int(1),
                Datum::Decimal(decimal(if row % 2 == 0 { "3.00" } else { "12.00" })),
            ]
        })
        .collect();
    let right = vec![vec![Datum::Int(1), Datum::Decimal(decimal("25.00"))]];
    let types = [long(), decimal_type];
    let mut join = join_with_types(vec![eq_on(0, 0, 2), residual], left, &types, right, &types);

    assert_eq!(run_datums(&mut join).len(), 5_000);
    assert!(
        join.parallel_probe_windows() > 0,
        "Go evaluates decimal hash-join residuals on its probe workers"
    );
}

#[test]
fn general_residual_unique_integer_join_uses_parallel_probe_window() {
    let column = |index: usize| {
        let mut column = Column::new(index as i64 + 1, long());
        column.index = index as i64;
        Expression::Column(column)
    };
    let residual = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("lt"),
        long(),
        vec![column(1), column(3)],
    ));
    let left = (0..10_000)
        .map(|row| vec![Datum::Int(1), Datum::Int(if row % 2 == 0 { 3 } else { 12 })])
        .collect();
    let right = vec![vec![Datum::Int(1), Datum::Int(5)]];
    let types = [long(), long()];
    let mut join = join_with_types(vec![eq_on(0, 0, 2), residual], left, &types, right, &types);

    assert_eq!(run_datums(&mut join).len(), 5_000);
    assert!(
        join.parallel_probe_windows() > 0,
        "Go evaluates arbitrary hash-join residuals on its probe workers"
    );
}

/// Drains a join to completion, exactly as a caller does: repeated
/// `next()` until an empty chunk.
pub(super) fn run(join: &mut JoinExec<NoColumns>) -> Vec<Vec<i64>> {
    join.open().unwrap();
    let types = join.ret_field_types().to_vec();
    let mut out = Vec::new();
    let mut req = join.new_chunk();
    loop {
        join.next(&mut req).unwrap();
        if req.num_rows() == 0 {
            break;
        }
        for r in 0..req.num_rows() {
            let row = req.get_row(r);
            out.push(
                (0..types.len())
                    .map(|c| match row.get_datum(c, &types[c]) {
                        Datum::Int(value) => value,
                        // NULL padding, distinguishable from any test
                        // value because every fixture value is >= 0.
                        Datum::Null => -1,
                        other => panic!("unexpected datum {other:?}"),
                    })
                    .collect(),
            );
        }
    }
    join.close().unwrap();
    out
}

/// Left rows: key `i % 7` (so keys repeat and both sides fan out), value
/// `i`, with every 11th key NULL. Right rows: key `i % 5`, so some keys
/// match nothing on either side.
fn fixture(n: i64, modulus: i64) -> Vec<Vec<Datum>> {
    (0..n)
        .map(|i| {
            let key = if i % 11 == 10 {
                Datum::Null
            } else {
                Datum::Int(i % modulus)
            };
            vec![key, Datum::Int(i)]
        })
        .collect()
}

#[test]
fn exact_integer_build_table_detects_duplicate_buckets() {
    let types = vec![long(), long()];
    let rows = fixture(200, 5);
    let mut chunk = Chunk::new_with_capacity(&types, rows.len());
    for row in &rows {
        for (column, datum) in row.iter().enumerate() {
            chunk.append_datum(column, datum);
        }
    }
    let key = EquiKey {
        left: 0,
        right: 0,
        class: KeyClass::Int,
        null_safe: false,
    };
    let mut table = BuildTable::new(
        &types,
        CHUNK,
        StatementMemory::default().spill_storage(),
        false,
        true,
    );
    table
        .index_chunk(chunk, &[key], &types, false)
        .expect("build table");

    assert_eq!(table.probe_exact_int(0).len(), 36);
    assert!(!table.exact_int_is_unique());
}

/// The hash path must reproduce the nested loop ROW FOR ROW -- same
/// rows, same order -- for every join kind, over data with duplicate
/// keys, unmatched keys on both sides, and NULL keys on both sides.
///
/// The NULL rows are the point of the fixture: a NULL key matches
/// nothing (not even another NULL), so an inner join must drop those
/// rows and an outer join must still emit them NULL-padded. Getting that
/// wrong is exactly the failure a bucket-based key can introduce.
#[test]
fn hash_path_matches_the_nested_loop_row_for_row() {
    for kind in [
        JoinKind::Inner,
        JoinKind::Left,
        JoinKind::Right,
        JoinKind::Semi,
        JoinKind::AntiSemi,
    ] {
        let left = fixture(200, 7);
        let right = fixture(200, 5);
        let mut hashed = join_of(kind, vec![eq_on(0, 0, 2)], left.clone(), right.clone(), 2);
        assert!(hashed.is_hash_join());
        let mut looped = join_of(kind, vec![eq_on(0, 0, 2)], left, right, 2);
        looped.force_nested_loop();
        let hashed_rows = run(&mut hashed);
        assert!(
            !hashed.parallel_exact_int_enabled(),
            "duplicate build keys must not be classified as unique for {kind:?}"
        );
        assert!(
            hashed.parallel_probe_windows() > 0,
            "duplicate build keys probe on workers too, as Go's hash_join_v2 does, for {kind:?}"
        );
        assert_eq!(hashed_rows, run(&mut looped), "{kind:?}");
    }
}

#[test]
fn duplicate_integer_left_join_matches_loop_in_isolation() {
    let left = fixture(200, 7);
    let right = fixture(200, 5);
    let mut hashed = join_of(
        JoinKind::Left,
        vec![eq_on(0, 0, 2)],
        left.clone(),
        right.clone(),
        2,
    );
    let mut looped = join_of(JoinKind::Left, vec![eq_on(0, 0, 2)], left, right, 2);
    looped.force_nested_loop();
    assert_eq!(run(&mut hashed), run(&mut looped));
}

/// Go hash join v2 may build the preserved side of an outer join. Matches are
/// emitted while the non-preserved side probes, then unmatched build rows are
/// emitted by scanning the row table. Both orientations must still produce
/// the same SQL result as the nested-loop reference.
#[test]
fn outer_hash_join_can_build_the_preserved_side() {
    let cases = [
        (
            JoinKind::Left,
            true,
            vec![
                vec![1, 10, 1, 100],
                vec![1, 11, 1, 100],
                vec![1, 10, 1, 101],
                vec![1, 11, 1, 101],
                vec![2, 20, -1, -1],
                vec![-1, 30, -1, -1],
            ],
        ),
        (
            JoinKind::Right,
            false,
            vec![
                vec![1, 10, 1, 100],
                vec![1, 10, 1, 101],
                vec![1, 11, 1, 100],
                vec![1, 11, 1, 101],
                vec![-1, -1, 3, 300],
                vec![-1, -1, -1, 400],
            ],
        ),
    ];

    for (kind, build_is_left, expected) in cases {
        let left = vec![
            vec![Datum::Int(1), Datum::Int(10)],
            vec![Datum::Int(2), Datum::Int(20)],
            vec![Datum::Null, Datum::Int(30)],
            vec![Datum::Int(1), Datum::Int(11)],
        ];
        let right = vec![
            vec![Datum::Int(1), Datum::Int(100)],
            vec![Datum::Int(3), Datum::Int(300)],
            vec![Datum::Int(1), Datum::Int(101)],
            vec![Datum::Null, Datum::Int(400)],
        ];
        let conditions = vec![eq_on(0, 0, 2)];
        let mut hashed = join_of(kind, conditions.clone(), left.clone(), right.clone(), 2);
        hashed.set_hash_build_is_left(build_is_left);
        let actual = run(&mut hashed);
        assert_eq!(actual, expected, "{kind:?} build_is_left={build_is_left}");

        let mut looped = join_of(kind, conditions, left, right, 2);
        looped.force_nested_loop();
        let mut actual_set = actual;
        let mut reference_set = run(&mut looped);
        actual_set.sort();
        reference_set.sort();
        assert_eq!(
            actual_set, reference_set,
            "{kind:?} build_is_left={build_is_left}"
        );
    }
}

/// Go hash join v2 may also build the preserved left side of a semi or
/// anti-semi join. The build rows are emitted only after the right probe has
/// marked them, once for semi and only when unmarked for anti-semi.
#[test]
fn semi_hash_join_can_build_the_preserved_left_side() {
    for kind in [JoinKind::Semi, JoinKind::AntiSemi] {
        let left = fixture(200, 7);
        let right = fixture(200, 5);
        let conditions = vec![eq_on(0, 0, 2)];
        let mut hashed = join_of(kind, conditions.clone(), left.clone(), right.clone(), 2);
        hashed.set_hash_build_is_left(true);
        assert!(hashed.hash_build_is_left());
        let mut actual = run(&mut hashed);

        let mut looped = join_of(kind, conditions, left, right, 2);
        looped.force_nested_loop();
        let mut expected = run(&mut looped);
        actual.sort();
        expected.sort();
        assert_eq!(
            actual, expected,
            "{kind:?} with the preserved left side built"
        );
    }
}

#[test]
fn preserved_build_row_is_matched_only_after_every_on_condition_passes() {
    for (kind, build_is_left) in [(JoinKind::Left, true), (JoinKind::Right, false)] {
        let left = vec![vec![Datum::Int(1), Datum::Int(10)]];
        let right = vec![vec![Datum::Int(1), Datum::Int(20)]];
        let conditions = vec![eq_on(0, 0, 2), eq_on(1, 1, 2)];

        let mut hashed = join_of(kind, conditions.clone(), left.clone(), right.clone(), 2);
        hashed.set_hash_build_is_left(build_is_left);
        let actual = run(&mut hashed);

        let mut looped = join_of(kind, conditions, left, right, 2);
        looped.force_nested_loop();
        assert_eq!(actual, run(&mut looped), "{kind:?}");
        assert_eq!(
            actual,
            if kind == JoinKind::Left {
                vec![vec![1, 10, -1, -1]]
            } else {
                vec![vec![-1, -1, 1, 20]]
            },
            "the equal hash key alone must not mark a preserved row"
        );
    }
}

/// The same, with a non-equi conjunct riding along: the hash table
/// selects candidates on the equal condition, and the residue still has
/// to reject the pairs it rejects.
#[test]
fn residual_conditions_still_filter_hashed_candidates() {
    let left = fixture(150, 7);
    let right = fixture(150, 5);
    // `l.key = r.key AND l.value = r.value` -- the second conjunct is
    // also an equal condition, so both become keys; the composite key is
    // what must not let one column borrow the other's bytes.
    let conditions = vec![eq_on(0, 0, 2), eq_on(1, 1, 2)];
    let mut hashed = join_of(
        JoinKind::Left,
        conditions.clone(),
        left.clone(),
        right.clone(),
        2,
    );
    let mut looped = join_of(JoinKind::Left, conditions, left, right, 2);
    looped.force_nested_loop();
    assert_eq!(run(&mut hashed), run(&mut looped));
}

#[test]
fn residual_hash_join_resumes_candidates_after_a_full_output_chunk() {
    let column = |index: usize| {
        let mut column = Column::new(index as i64 + 1, long());
        column.index = index as i64;
        Expression::Column(column)
    };
    let residual = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("lt"),
        long(),
        vec![column(3), column(1)],
    ));
    let left = vec![vec![Datum::Int(1), Datum::Int(2)]];
    let right = vec![vec![Datum::Int(1), Datum::Int(1)]; CHUNK + 1];
    let mut join = join_of(
        JoinKind::Inner,
        vec![eq_on(0, 0, 2), residual],
        left,
        right,
        2,
    );

    assert_eq!(run(&mut join).len(), CHUNK + 1);
}

/// Go's `nullAwareAntiSemiJoiner.TryToMatchInners` calls the SAME
/// `EvalBool` the ordinary `antiSemiJoiner` does, but keeps only its first
/// return value (`valid, _, err := ...`) -- and `EvalBool` only ever
/// returns `hasNull=true` alongside `valid=false`, so Go's own NAAJ joiner
/// never observes the forgiveness the ordinary joiner's `matched ||
/// hasNull` applies. This is only OBSERVABLE when a residual conjunct
/// itself carries the `IsEQCondFromIn` marker (`EvalBool` forgives a NULL
/// into `hasNull` only then) -- the rare leftover the planner's
/// `adjustKeyForm` could not promote into `na_keys` (a mutable-effects
/// expression, per its own `keepAsOtherCond` escape hatch) -- so this test
/// marks the residual conjunct itself, not just the promoted NA key.
#[test]
fn naaj_residual_condition_gets_no_has_null_forgiveness() {
    let mut na_key_left = Column::new(101, long());
    na_key_left.index = 0;
    na_key_left.in_operand = true;
    let mut na_key_right = Column::new(102, long());
    na_key_right.index = 2;
    let na_key = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("eq"),
        long(),
        vec![
            Expression::Column(na_key_left),
            Expression::Column(na_key_right),
        ],
    ));
    let mut leftover_left = Column::new(103, long());
    leftover_left.index = 1;
    leftover_left.in_operand = true;
    let mut leftover_right = Column::new(104, long());
    leftover_right.index = 3;
    let leftover_residual = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("eq"),
        long(),
        vec![
            Expression::Column(leftover_left),
            Expression::Column(leftover_right),
        ],
    ));
    // Same-key bucket match on column 0 (1 = 1); the leftover residual then
    // compares the probe's NULL value column against the build row's,
    // which Go's own NAAJ joiner treats as a plain non-match, never a
    // forgiven one.
    let left = vec![vec![Datum::Int(1), Datum::Null]];
    let right = vec![vec![Datum::Int(1), Datum::Int(5)]];
    let mut join = join_of(JoinKind::AntiSemi, vec![leftover_residual], left, right, 2);
    join.set_na_conditions(vec![na_key]);
    assert_eq!(
        run(&mut join),
        vec![vec![1, -1]],
        "the leftover residual's NULL result must not be forgiven into a match: the probe row survives"
    );
}

/// A join with no equal condition keeps the nested loop, as documented.
#[test]
fn cross_join_falls_back_to_the_nested_loop() {
    let mut join = join_of(JoinKind::Inner, Vec::new(), fixture(4, 7), fixture(4, 5), 2);
    assert!(!join.is_hash_join());
    assert_eq!(run(&mut join).len(), 16);
}

/// Go executes a TRUE cross join through hash join v1 with an empty key:
/// one chain, head-inserted (`hash_table_v1.go:634`), so each probe row
/// sees the build rows NEWEST-FIRST. A join that only FALLS BACK here
/// because its equality is not a bare `col = col` (`join_key_type_cast`'s
/// cast keys) is keyed in Go and reads forward. Both directions are pinned
/// by recordings; see `BuildTable`'s doc for the 15 -> 21 measurement that
/// killed the uniform-reverse theory.
#[test]
fn nested_loop_emission_order_matches_gos_chain_direction() {
    // Bare cross join: inner rows arrive in reverse input order.
    let left = vec![vec![Datum::Int(1), Datum::Int(0)]];
    let right = vec![
        vec![Datum::Int(7), Datum::Int(70)],
        vec![Datum::Int(8), Datum::Int(80)],
    ];
    let mut join = join_of(JoinKind::Inner, Vec::new(), left.clone(), right.clone(), 2);
    assert!(!join.is_hash_join());
    assert_eq!(
        run(&mut join),
        vec![vec![1, 0, 8, 80], vec![1, 0, 7, 70]],
        "a keyless cross join walks Go's single chain newest-first"
    );

    // A cross-side equality that key extraction cannot take (one side is
    // an expression, Go's `updateEQCond` injected-projection case) still
    // marks the join KEYED, and keyed order reads forward.
    let uneven_eq = {
        let column = |index: i64| {
            let mut column = Column::new(index + 1, long());
            column.index = index;
            Expression::Column(column)
        };
        let plus = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            long(),
            vec![
                column(0),
                Expression::Constant(Constant::new(Datum::Int(0), long())),
            ],
        ));
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("eq"),
            long(),
            vec![plus, column(2)],
        ))
    };
    let right = vec![
        vec![Datum::Int(1), Datum::Int(70)],
        vec![Datum::Int(1), Datum::Int(80)],
    ];
    let mut join = join_of(JoinKind::Inner, vec![uneven_eq], left, right, 2);
    assert!(!join.is_hash_join());
    assert_eq!(
        run(&mut join),
        vec![vec![1, 0, 1, 70], vec![1, 0, 1, 80]],
        "a cast-shaped equality keeps Go's keyed forward order"
    );
}

/// The scaling claim, asserted on the cost the hash table exists to
/// remove rather than on the wall clock.
///
/// 10k x 10k over 10k distinct keys: the nested loop would evaluate the
/// `ON` clause 100_000_000 times. The hash join evaluates it once per
/// candidate pair a bucket produces -- here exactly once per matching
/// row, because the keys are distinct.
#[test]
fn ten_thousand_by_ten_thousand_is_linear_not_quadratic() {
    let rows = 10_000i64;
    let side: Vec<Vec<Datum>> = (0..rows)
        .map(|i| vec![Datum::Int(i), Datum::Int(i * 2)])
        .collect();
    let mut join = join_of(JoinKind::Inner, vec![eq_on(0, 0, 2)], side.clone(), side, 2);
    assert!(join.is_hash_join());
    let out = run(&mut join);
    assert_eq!(out.len(), rows as usize);
    // Every output row is the key joined to itself.
    assert_eq!(out[0], vec![0, 0, 0, 0]);
    assert_eq!(out[9_999], vec![9_999, 19_998, 9_999, 19_998]);

    let evals = join.condition_evals();
    let nested_loop_evals = (rows * rows) as u64;
    assert_eq!(
        evals, 0,
        "pure equal conditions are enforced by the hash key"
    );
    // Stated as a ratio so the assertion says what it means: at least
    // four orders of magnitude fewer, not a tuned constant.
    assert!(
        evals * 10_000 <= nested_loop_evals,
        "{evals} evaluations vs the nested loop's {nested_loop_evals}"
    );
}

/// Go initializes its probe-worker pipeline even when the configured
/// concurrency is one. A large pure integer equality join must take the same
/// bounded path instead of silently falling back to the session-thread loop.
#[test]
fn exact_integer_hash_join_uses_parallel_probe_window() {
    let rows = 10_000i64;
    let side: Vec<Vec<Datum>> = (0..rows)
        .map(|i| vec![Datum::Int(i), Datum::Int(i * 2)])
        .collect();
    let mut join = join_of(JoinKind::Inner, vec![eq_on(0, 0, 2)], side.clone(), side, 2);
    join.set_parallelism(1);

    assert_eq!(run(&mut join).len(), rows as usize);
    assert!(
        join.parallel_probe_windows() > 0,
        "large exact-integer probes must use the parallel worker path"
    );
}

/// Go's probe worker fills one result chunk across the probe chunks it
/// processes and sends it on only when full, so a selective probe (few
/// matches per probe chunk) hands the parent full chunks rather than one
/// small chunk per probe chunk. The parallel path here must do the same from
/// the session thread.
#[test]
fn parallel_exact_integer_probe_fills_the_caller_chunk_across_probe_chunks() {
    let build = (0..10i64)
        .map(|key| vec![Datum::Int(key), Datum::Int(key * 10)])
        .collect::<Vec<_>>();
    let probe = (0..20_000i64)
        .map(|value| vec![Datum::Int(value % 1_000), Datum::Int(value)])
        .collect::<Vec<_>>();
    let mut join = join_of(JoinKind::Inner, vec![eq_on(0, 0, 2)], probe, build, 2);
    join.set_hash_build_is_left(false);

    join.open().unwrap();
    let mut req = join.new_chunk();
    let mut rows = 0;
    let mut chunks = Vec::new();
    loop {
        join.next(&mut req).unwrap();
        if req.num_rows() == 0 {
            break;
        }
        rows += req.num_rows();
        chunks.push(req.num_rows());
    }
    join.close().unwrap();
    assert!(
        join.parallel_probe_windows() > 1,
        "the probe must span several worker chunks"
    );
    // 20 probe rows match each of the 10 build keys.
    assert_eq!(rows, 200);
    // Every chunk but the last is full.
    assert!(
        chunks[..chunks.len() - 1].iter().all(|&rows| rows == CHUNK),
        "chunks: {chunks:?}"
    );
}

/// TPC-H q13 builds the preserved customer side and probes orders. Parallel
/// workers must report matches back to the session thread so the post-probe
/// scan emits only truly unmatched build rows.
#[test]
fn parallel_exact_integer_probe_marks_preserved_build_rows() {
    let left = (0..6_000i64)
        .map(|key| vec![Datum::Int(key), Datum::Int(key * 10)])
        .collect::<Vec<_>>();
    let right = (0..7_000i64)
        .map(|value| vec![Datum::Int(value % 5_000), Datum::Int(value)])
        .collect::<Vec<_>>();
    let mut join = join_of(JoinKind::Left, vec![eq_on(0, 0, 2)], left, right, 2);
    join.set_hash_build_is_left(true);

    let mut actual = run(&mut join);
    assert!(join.parallel_probe_windows() > 0);
    let mut expected = (0..7_000i64)
        .map(|value| {
            let key = value % 5_000;
            vec![key, key * 10, key, value]
        })
        .chain((5_000..6_000i64).map(|key| vec![key, key * 10, -1, -1]))
        .collect::<Vec<_>>();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}

/// A join whose output is the left row plus one marker column, the shape
/// of `LeftOuterSemi`/`AntiLeftOuterSemi` (`JoinOutput::all` keeps only the
/// left columns for them and the executor appends the marker at
/// `output.width()`).
pub(super) fn join_of_marker(
    kind: JoinKind,
    conditions: Vec<Expression>,
    left: Vec<Vec<Datum>>,
    right: Vec<Vec<Datum>>,
    width: usize,
) -> JoinExec<NoColumns> {
    let mut executor = JoinExec::new(
        ExecutorMeta::new(schema_of(width + 1), 1, CHUNK, CHUNK),
        kind,
        conditions,
        Box::new(RowSource::new(left, width)),
        Box::new(RowSource::new(right, width)),
        NoColumns,
        StatementMemory::default(),
    );
    executor.set_parallelism(5);
    executor
}

/// The `IN`-clause's own equality kept as a residual condition -- Go
/// `IsEQCondFromIn`, the one condition whose NULL `expression.EvalBool`
/// reports as `hasNull` instead of a definite non-match. `updateEQCond` never
/// promotes it to a join key, so it survives as a residual whenever ANOTHER
/// equality became the key (`select a in (select a from s where t.b = s.b)
/// from t`, the example on `EvalBool` itself).
pub(super) fn in_eq_residual(lhs: usize, rhs: usize, left_width: usize) -> Expression {
    let mut left = Column::new(lhs as i64 + 1, long());
    left.index = lhs as i64;
    left.in_operand = true;
    let mut right = Column::new((left_width + rhs) as i64 + 1, long());
    right.index = (left_width + rhs) as i64;
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("eq"),
        long(),
        vec![Expression::Column(left), Expression::Column(right)],
    ))
}

/// An ordinary predicate on the same columns, for the control cases.
fn gt_residual(lhs: usize, rhs: usize, left_width: usize) -> Expression {
    let mut left = Column::new(lhs as i64 + 1, long());
    left.index = lhs as i64;
    let mut right = Column::new((left_width + rhs) as i64 + 1, long());
    right.index = (left_width + rhs) as i64;
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("gt"),
        long(),
        vec![Expression::Column(right), Expression::Column(left)],
    ))
}

/// Go `leftOuterSemiJoiner.TryToMatchInners` accumulates `hasNull` across
/// the candidate group and `OnMissMatch(hasNull, ...)` marks NULL when no
/// candidate was accepted (`joiner.go:596-609,652-659`); the anti variant
/// mirrors it. The key matches; the `IN` equality itself is UNKNOWN.
#[test]
fn marker_kinds_mark_null_when_the_in_equality_is_unknown_for_every_candidate() {
    for (kind, expected) in [
        (JoinKind::LeftOuterSemi, vec![vec![1, -1, -1]]),
        (JoinKind::AntiLeftOuterSemi, vec![vec![1, -1, -1]]),
    ] {
        let left = vec![vec![Datum::Int(1), Datum::Null]];
        let right = vec![vec![Datum::Int(1), Datum::Int(5)]];
        let mut join = join_of_marker(
            kind,
            vec![eq_on(0, 0, 2), in_eq_residual(1, 1, 2)],
            left,
            right,
            2,
        );
        assert!(join.is_hash_join());
        assert_eq!(
            run(&mut join),
            expected,
            "{kind:?}: NULL = 5 is UNKNOWN, so the marker is NULL"
        );
    }
}

/// `EvalBool` only reports `hasNull` for the `IsEQCondFromIn` condition;
/// an ORDINARY predicate's NULL is a definite non-match
/// (`expression.go:356-366`), so the same data through `t2.b > t1.b`
/// answers a hard 0 / 1, never NULL.
#[test]
fn marker_kinds_treat_an_ordinary_predicates_null_as_a_definite_miss() {
    for (kind, expected) in [
        (JoinKind::LeftOuterSemi, vec![vec![1, -1, 0]]),
        (JoinKind::AntiLeftOuterSemi, vec![vec![1, -1, 1]]),
    ] {
        let left = vec![vec![Datum::Int(1), Datum::Null]];
        let right = vec![vec![Datum::Int(1), Datum::Int(5)]];
        let mut join = join_of_marker(
            kind,
            vec![eq_on(0, 0, 2), gt_residual(1, 1, 2)],
            left,
            right,
            2,
        );
        assert_eq!(
            run(&mut join),
            expected,
            "{kind:?}: an ordinary NULL predicate is FALSE"
        );
    }
}

/// A definite match anywhere in the group wins over an UNKNOWN candidate
/// seen before it (`TryToMatchInners` returns `matched=true, hasNull=false`
/// the moment a pair is accepted), and a group with no NULL at all is a
/// hard miss.
#[test]
fn marker_kinds_prefer_a_definite_match_and_keep_a_clean_miss_definite() {
    let left = vec![
        vec![Datum::Int(1), Datum::Int(7)],
        vec![Datum::Int(2), Datum::Int(7)],
    ];
    let right = vec![
        vec![Datum::Int(1), Datum::Null],
        vec![Datum::Int(1), Datum::Int(7)],
        vec![Datum::Int(2), Datum::Int(8)],
    ];
    let mut join = join_of_marker(
        JoinKind::LeftOuterSemi,
        vec![eq_on(0, 0, 2), in_eq_residual(1, 1, 2)],
        left,
        right,
        2,
    );
    assert_eq!(run(&mut join), vec![vec![1, 7, 1], vec![2, 7, 0]]);
}

/// An `IN` equality is never a hash key (`equi_key` refuses
/// `IsEQCondFromIn`, as Go's `updateEQCond` never promotes one), so with
/// nothing else to key on this is Go's cartesian hash join with the `IN`
/// equality as the whole residual -- `select t1.b in (select t2.b from t2)`.
#[test]
fn cartesian_marker_kinds_mark_null_for_an_unknown_in_equality() {
    for (kind, expected) in [
        (JoinKind::LeftOuterSemi, vec![vec![-1, -1]]),
        (JoinKind::AntiLeftOuterSemi, vec![vec![-1, -1]]),
    ] {
        let mut join = join_of_marker(
            kind,
            vec![in_eq_residual(0, 0, 1)],
            vec![vec![Datum::Null]],
            vec![vec![Datum::Int(5)]],
            1,
        );
        assert!(join.keys.is_empty(), "the IN equality must stay a residual");
        assert_eq!(run(&mut join), expected, "{kind:?}");
    }
}

/// The index-lookup strategies: the row-at-a-time probe (`IndexRowProbe`)
/// and both index-hash variants keep `hasNull` per outer row across the
/// inner rows a batch fetched.
#[test]
fn index_join_marker_kinds_mark_null_for_an_unknown_in_equality() {
    for hash_order in [None, Some(false), Some(true)] {
        for (kind, expected) in [
            (
                JoinKind::LeftOuterSemi,
                vec![vec![1, -1, -1], vec![2, 7, 1], vec![3, 9, 0]],
            ),
            (
                JoinKind::AntiLeftOuterSemi,
                vec![vec![1, -1, -1], vec![2, 7, 0], vec![3, 9, 1]],
            ),
        ] {
            let outer = vec![
                vec![Datum::Int(1), Datum::Null],
                vec![Datum::Int(2), Datum::Int(7)],
                vec![Datum::Int(3), Datum::Int(9)],
            ];
            let mut join = join_of_marker(
                kind,
                vec![eq_on(0, 0, 2), in_eq_residual(1, 1, 2)],
                outer,
                vec![],
                2,
            );
            join.index_hash = hash_order;
            let inner = RowSource::new(
                vec![
                    vec![Datum::Int(1), Datum::Int(5)],
                    vec![Datum::Int(2), Datum::Int(7)],
                ],
                2,
            );
            join.index_lookup = Some(IndexLookupPlan {
                lookup_is_left: false,
                probe_keys: vec![0],
                probe_key_domains: vec![IndexProbeKeyDomain {
                    field_type: long(),
                    prefix_length: -1,
                }],
                source: IndexLookupSource::Composite {
                    exec: Box::new(inner),
                    probes: Arc::new(std::sync::Mutex::new(Default::default())),
                },
                outer_not_null: vec![],
                inner_not_null: vec![],
                probe_bounds: vec![],
            });
            let mut rows = run(&mut join);
            rows.sort_unstable();
            assert_eq!(rows, expected, "{kind:?} index_hash={hash_order:?}");
        }
    }
}

/// Go `wait4BuildSide` (`hash_join_base.go:119-121`) with the v2 rule
/// `canSkipProbeIfHashTableIsEmpty` (`hash_join_v2.go:763-775`): once the
/// build side finishes empty, an inner join and a semi join probing the
/// build never read the probe child; the kinds that still emit probe rows
/// read it as before.
#[test]
fn empty_build_side_skips_reading_the_probe_child_where_go_does() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for (kind, skips, rows_out) in [
        (JoinKind::Inner, true, 0),
        (JoinKind::Semi, true, 0),
        (JoinKind::AntiSemi, false, 3),
        (JoinKind::Left, false, 3),
    ] {
        let read = Arc::new(AtomicUsize::new(0));
        let mut probe = RowSource::new(vec![vec![Datum::Int(1), Datum::Int(1)]; 3], 2);
        probe.read_rows = Some(Arc::clone(&read));
        let width = if matches!(kind, JoinKind::Semi | JoinKind::AntiSemi) {
            2
        } else {
            4
        };
        let mut join = JoinExec::new(
            ExecutorMeta::new(schema_of(width), 1, CHUNK, CHUNK),
            kind,
            vec![eq_on(0, 0, 2)],
            Box::new(probe),
            Box::new(RowSource::new(vec![], 2)),
            NoColumns,
            StatementMemory::default(),
        );
        join.set_parallelism(5);
        assert_eq!(run(&mut join).len(), rows_out, "{kind:?}");
        assert_eq!(
            read.load(Ordering::SeqCst) == 0,
            skips,
            "{kind:?}: probe rows read = {}",
            read.load(Ordering::SeqCst)
        );
    }
}
