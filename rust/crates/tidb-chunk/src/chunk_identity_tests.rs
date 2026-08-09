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

use std::collections::HashMap;
use std::sync::Arc;

use tidb_datatype::{FieldType, FieldTypeCode};

use crate::chunk::Chunk;
use crate::chunk_in_disk::{deserialize_data_to_chunk, serialize_data_to_buf};
use crate::chunk_util::{ColumnSwapHelper, MSG_ERR_SEL_NOT_NIL};
use crate::codec::Codec;

fn int_fields(width: usize) -> Vec<FieldType> {
    vec![FieldType::new(FieldTypeCode::LongLong); width]
}

fn chunk_with_values(values: &[i64]) -> Chunk {
    let mut chunk = Chunk::new_with_capacity(&int_fields(values.len()), 4);
    for (index, &value) in values.iter().enumerate() {
        chunk.append_int64(index, value);
    }
    chunk
}

#[test]
fn make_ref_shares_mutations_and_deep_copies_break_identity() {
    let mut chunk = chunk_with_values(&[7, 9]);
    chunk.make_ref(0, 1);
    assert!(chunk.columns_share_identity(0, &chunk, 1));

    chunk.column_mut(1).with_int64s_mut(|values| values[0] = 42);
    assert_eq!(chunk.column(0).get_int64(0), 42);

    let copied = chunk.copy_construct();
    assert!(!copied.columns_share_identity(0, &copied, 1));
    assert!(!chunk.columns_share_identity(0, &copied, 0));
    let cloned = chunk.clone();
    assert!(!cloned.columns_share_identity(0, &cloned, 1));

    chunk.column_mut(0).with_int64s_mut(|values| values[0] = 99);
    assert_eq!(copied.column(0).get_int64(0), 42);
    assert_eq!(cloned.column(0).get_int64(0), 42);
}

#[test]
fn make_ref_to_survives_source_drop_and_selection_errors_are_atomic() {
    let mut destination = chunk_with_values(&[-1, -2]);
    {
        let mut source = chunk_with_values(&[11, 22]);
        destination
            .make_ref_to(0, &mut source, 1)
            .expect("neither chunk has a selection");
        assert!(destination.columns_share_identity(0, &source, 1));
    }
    destination
        .column_mut(0)
        .with_int64s_mut(|values| values[0] = 33);
    assert_eq!(destination.column(0).get_int64(0), 33);

    let mut source = chunk_with_values(&[44]);
    let mut selected_destination = chunk_with_values(&[55]);
    selected_destination.set_sel(Some(vec![0]));
    let error = selected_destination
        .make_ref_to(0, &mut source, 0)
        .expect_err("a selection must be rejected");
    assert_eq!(error, MSG_ERR_SEL_NOT_NIL);
    assert_eq!(selected_destination.column(0).get_int64(0), 55);

    selected_destination.set_sel(None);
    source.set_sel(Some(vec![0]));
    let error = selected_destination
        .make_ref_to(0, &mut source, 0)
        .expect_err("a source selection must be rejected");
    assert_eq!(error, MSG_ERR_SEL_NOT_NIL);
    assert_eq!(selected_destination.column(0).get_int64(0), 55);
}

#[test]
fn appending_from_the_same_shared_owner_snapshots_before_mutation() {
    let mut source = chunk_with_values(&[71]);
    let mut destination = chunk_with_values(&[-1]);
    destination
        .make_ref_to(0, &mut source, 0)
        .expect("neither chunk has a selection");
    destination.append_row(source.get_row(0));
    assert_eq!(source.num_rows(), 2);
    assert_eq!(source.column(0).get_int64(0), 71);
    assert_eq!(source.column(0).get_int64(1), 71);
    assert_eq!(destination.column(0).get_int64(1), 71);
}

#[test]
fn prune_preserves_metadata_order_duplicates_and_live_owners() {
    let mut source = chunk_with_values(&[10, 20]);
    source.set_sel(Some(vec![0]));
    source.set_num_virtual_rows(7);
    source.set_required_rows(3, 8);
    source.set_incomplete_chunk(true);

    let mut pruned = source.prune(&[1, 0, 1]);
    assert_eq!(pruned.sel(), Some(&[0][..]));
    assert_eq!(pruned.num_virtual_rows(), 7);
    assert_eq!(pruned.capacity(), source.capacity());
    assert_eq!(pruned.required_rows(), 3);
    assert!(pruned.is_incomplete_chunk());
    assert_eq!(pruned.column(0).get_int64(0), 20);
    assert_eq!(pruned.column(1).get_int64(0), 10);
    assert!(pruned.columns_share_identity(0, &pruned, 2));
    assert!(pruned.columns_share_identity(0, &source, 1));

    source
        .column_mut(1)
        .with_int64s_mut(|values| values[0] = 88);
    assert_eq!(pruned.column(0).get_int64(0), 88);
    assert_eq!(pruned.column(2).get_int64(0), 88);

    pruned.reset();
    assert_eq!(pruned.num_virtual_rows(), 0);
}

#[test]
fn set_col_returns_only_a_different_displaced_owner() {
    let mut chunk = chunk_with_values(&[1, 2]);
    let replacement = chunk.column_handle(0);
    let mut displaced = chunk
        .set_col(1, replacement)
        .expect("different owner is returned");
    assert_eq!(displaced.read().get_int64(0), 2);
    assert!(chunk.columns_share_identity(0, &chunk, 1));

    displaced.write().with_int64s_mut(|values| values[0] = 9);
    assert_eq!(displaced.read().get_int64(0), 9);
    assert_eq!(chunk.column(0).get_int64(0), 1);

    let same = chunk.column_handle(0);
    assert!(chunk.set_col(1, same).is_none());
    assert!(chunk.columns_share_identity(0, &chunk, 1));
}

#[test]
fn memory_usage_is_charged_per_alias_slot() {
    let mut chunk = chunk_with_values(&[1, 2]);
    let one_slot = chunk.column(0).memory_usage();
    chunk.make_ref(0, 1);
    assert_eq!(chunk.memory_usage(), one_slot * 2);
}

fn assert_two_alias_groups(first: &Chunk, second: &Chunk) {
    assert!(first.columns_share_identity(0, first, 1));
    assert!(second.columns_share_identity(0, second, 1));
    assert!(!first.columns_share_identity(0, second, 0));
}

#[test]
fn single_column_swaps_preserve_complete_alias_groups() {
    let mut first = chunk_with_values(&[1, 2, 3]);
    first.make_ref(0, 1);
    let mut second = chunk_with_values(&[4, 5, 6]);
    second.make_ref(0, 1);
    assert_two_alias_groups(&first, &second);

    first
        .swap_column_with(0, &mut second, 0)
        .expect("no selections");
    assert_two_alias_groups(&first, &second);
    assert_eq!(first.column(0).get_int64(0), 4);
    assert_eq!(second.column(0).get_int64(0), 1);

    first
        .swap_column_with(1, &mut second, 0)
        .expect("references resolve to their owner");
    assert_two_alias_groups(&first, &second);

    second.swap_column(1, 0).expect("reference/owner self swap");
    assert_two_alias_groups(&first, &second);
    second.swap_column(1, 1).expect("same slot self swap");
    assert_two_alias_groups(&first, &second);

    second.swap_column(1, 2).expect("reference/other swap");
    assert!(second.columns_share_identity(0, &second, 1));
    assert_eq!(second.column(0).get_int64(0), 6);
    assert_eq!(second.column(2).get_int64(0), 4);
    second
        .swap_column(2, 0)
        .expect("other/owner swap restores groups");
    assert_two_alias_groups(&first, &second);
}

#[test]
fn swap_selection_error_does_not_move_any_owner() {
    let mut first = chunk_with_values(&[1]);
    let mut second = chunk_with_values(&[2]);
    first.set_sel(Some(vec![0]));
    assert_eq!(
        first.swap_column_with(0, &mut second, 0),
        Err(MSG_ERR_SEL_NOT_NIL)
    );
    assert_eq!(first.column(0).get_int64(0), 1);
    assert_eq!(second.column(0).get_int64(0), 2);
}

#[test]
fn column_swap_helper_merges_runtime_aliases_and_caches_mapping() {
    let mut mapping = HashMap::new();
    mapping.insert(0, vec![0, 1]);
    mapping.insert(1, vec![2, 3]);
    let helper = ColumnSwapHelper::from_mapping(mapping);

    let mut input = chunk_with_values(&[99, 100]);
    input.make_ref(0, 1);
    let mut output = chunk_with_values(&[-1, -2, -3, -4]);
    helper
        .swap_columns(&mut input, &mut output)
        .expect("no selections");

    for index in 1..4 {
        assert!(output.columns_share_identity(0, &output, index));
    }
    assert_eq!(output.column(0).get_int64(0), 99);
    let merged = helper.merged_mapping().expect("mapping cached");
    assert_eq!(merged.len(), 1);
    let mut output_indexes = merged.values().next().unwrap().clone();
    output_indexes.sort_unstable();
    assert_eq!(output_indexes, vec![0, 1, 2, 3]);

    // Projection reuses the helper after its input/output chunks rotate their
    // scratch owners. The cached topology remains valid and moves the next
    // batch without re-inspecting or invalidating either alias class.
    input.reset();
    output.reset();
    input.append_int64(0, 123);
    helper
        .swap_columns(&mut input, &mut output)
        .expect("cached topology remains valid");
    for index in 0..4 {
        assert_eq!(output.column(index).get_int64(0), 123);
        assert!(output.columns_share_identity(0, &output, index));
    }
}

#[test]
fn nonempty_column_swap_helper_rejects_selection_before_mutation() {
    let helper = ColumnSwapHelper::new(&[0, 0]);
    let mut input = chunk_with_values(&[5]);
    let mut output = chunk_with_values(&[6, 7]);
    input.set_sel(Some(vec![0]));
    assert_eq!(
        helper.swap_columns(&mut input, &mut output),
        Err(MSG_ERR_SEL_NOT_NIL)
    );
    assert_eq!(input.column(0).get_int64(0), 5);
    assert_eq!(output.column(0).get_int64(0), 6);
    assert_eq!(output.column(1).get_int64(0), 7);
    assert!(!output.columns_share_identity(0, &output, 1));
    assert!(helper.merged_mapping().is_none());
}

#[test]
fn column_swap_helper_initializes_its_runtime_cache_once_concurrently() {
    let helper = Arc::new(ColumnSwapHelper::new(&[0, 0, 1]));
    let workers: Vec<_> = (0..4)
        .map(|worker| {
            let helper = Arc::clone(&helper);
            std::thread::spawn(move || {
                let mut input = chunk_with_values(&[worker, worker + 100]);
                input.make_ref(0, 1);
                let mut output = chunk_with_values(&[-1, -2, -3]);
                helper
                    .swap_columns(&mut input, &mut output)
                    .expect("worker chunks have no selection");
                for index in 0..3 {
                    assert_eq!(output.column(index).get_int64(0), worker);
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().expect("column-swap worker");
    }
    assert_eq!(helper.merged_mapping().unwrap().len(), 1);
}

#[test]
fn empty_column_swap_helper_is_a_noop_even_with_selections() {
    let helper = ColumnSwapHelper::new(&[]);
    let mut input = chunk_with_values(&[1]);
    let mut output = chunk_with_values(&[2]);
    input.set_sel(Some(vec![0]));
    output.set_sel(Some(vec![0]));
    helper
        .swap_columns(&mut input, &mut output)
        .expect("empty mapping does not inspect selections");
    assert_eq!(input.column(0).get_int64(0), 1);
    assert_eq!(output.column(0).get_int64(0), 2);
}

#[test]
fn codec_and_spill_images_repeat_duplicate_alias_slots() {
    let fields = int_fields(2);
    let mut source = chunk_with_values(&[7, 8]);
    source.make_ref(0, 1);

    let codec = Codec::new(fields.clone());
    let encoded = codec.encode(&source);
    let (decoded, tail) = codec.decode(&encoded);
    assert!(tail.is_empty());
    assert_eq!(decoded.num_cols(), 2);
    assert_eq!(decoded.column(0).get_int64(0), 7);
    assert_eq!(decoded.column(1).get_int64(0), 7);

    let mut image = Vec::new();
    serialize_data_to_buf(&source, &mut image);
    let mut restored = Chunk::new_empty(&fields);
    deserialize_data_to_chunk(&mut restored, &image);
    assert_eq!(restored.column(0).get_int64(0), 7);
    assert_eq!(restored.column(1).get_int64(0), 7);

    let mut aliased_destination = Chunk::new_empty(&fields);
    aliased_destination.make_ref(0, 1);
    deserialize_data_to_chunk(&mut aliased_destination, &image);
    assert!(aliased_destination.columns_share_identity(0, &aliased_destination, 1));
    assert_eq!(aliased_destination.column(0).get_int64(0), 7);
}
