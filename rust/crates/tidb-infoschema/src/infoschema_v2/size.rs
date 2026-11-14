// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `internal.Sizeof`: native retained metadata sizes, with pointer deduplication.
//! Storage, clients and allocators are services, not cache-owned allocations.
use std::{
    collections::{BTreeMap, HashSet},
    mem::size_of,
};
use tidb_ast::CiString;
use tidb_goruntime::go_runtime::GoSharedSlice;
use tidb_model::{table::*, *};

#[derive(Default)]
struct Sizer {
    pointers: HashSet<usize>,
}
trait Heap {
    fn heap(&self, s: &mut Sizer) -> usize;
}
macro_rules! fields {
    ($ty:ty; $($field:ident),* $(,)?) => {
        impl Heap for $ty { fn heap(&self, s: &mut Sizer) -> usize { 0 $(+ self.$field.heap(s))* } }
    };
}
macro_rules! scalar { ($($ty:ty),*) => { $(impl Heap for $ty { fn heap(&self, _: &mut Sizer) -> usize { 0 } })* }; }
scalar!(
    u8,
    i64,
    bool,
    PartitionState,
    ExchangePartitionInfo,
    table::StatsWindowSettings,
    index::InvertedIndexInfo,
    column::ChangeStateInfo
);
impl Heap for String {
    fn heap(&self, _: &mut Sizer) -> usize {
        self.capacity()
    }
}
impl Heap for CiString {
    fn heap(&self, _: &mut Sizer) -> usize {
        self.original().len() + self.lowercase().len()
    }
}
impl<T: Heap> Heap for Option<T> {
    fn heap(&self, s: &mut Sizer) -> usize {
        self.as_ref().map_or(0, |v| v.heap(s))
    }
}
impl<T: Heap> Heap for Box<T> {
    fn heap(&self, s: &mut Sizer) -> usize {
        size_of::<T>() + (**self).heap(s)
    }
}
impl<T: Heap> Heap for Vec<T> {
    fn heap(&self, s: &mut Sizer) -> usize {
        self.capacity() * size_of::<T>() + self.iter().map(|v| v.heap(s)).sum::<usize>()
    }
}
impl<T: Heap> Heap for GoShared<T> {
    fn heap(&self, s: &mut Sizer) -> usize {
        let value = self.read();
        if !s.pointers.insert((&*value as *const T) as usize) {
            return 0;
        }
        size_of::<std::sync::RwLock<T>>() + 2 * size_of::<usize>() + value.heap(s)
    }
}
impl<T: Heap> Heap for GoSharedSlice<T> {
    fn heap(&self, s: &mut Sizer) -> usize {
        if !self.is_allocated() {
            return 0;
        }
        self.with_visible(|values| {
            if !values.is_empty() && !s.pointers.insert(values.as_ptr() as usize) {
                return 0;
            }
            size_of::<std::sync::RwLock<Vec<T>>>()
                + 2 * size_of::<usize>()
                + self.capacity() * size_of::<T>()
                + values.iter().map(|v| v.heap(s)).sum::<usize>()
        })
    }
}
impl<T: Heap + Clone> Heap for GoSharedPointerSlice<T> {
    fn heap(&self, s: &mut Sizer) -> usize {
        // The pointer slots own shared pointees; duplicate pointers are charged once.
        self.capacity() * size_of::<Option<GoShared<T>>>()
            + self.iter_handles().map(|v| v.heap(s)).sum::<usize>()
    }
}
impl<T: Heap> Heap for tidb_model::serde_helpers::GoValueSlice<T> {
    fn heap(&self, s: &mut Sizer) -> usize {
        self.capacity() * size_of::<T>() + self.iter().map(|v| v.heap(s)).sum::<usize>()
    }
}
impl<K: Heap, V: Heap> Heap for BTreeMap<K, V> {
    fn heap(&self, s: &mut Sizer) -> usize {
        // Go's sizer uses 10.79 bytes of map bucket overhead per pair.
        (self.len() as f64 * 10.79) as usize
            + self
                .iter()
                .map(|(k, v)| size_of::<(K, V)>() + k.heap(s) + v.heap(s))
                .sum::<usize>()
    }
}
impl Heap for tidb_datatype::FieldType {
    fn heap(&self, _: &mut Sizer) -> usize {
        self.memory_usage().saturating_sub(120)
    }
}
impl Heap for column::GoStringSet {
    fn heap(&self, _: &mut Sizer) -> usize {
        self.snapshot()
            .iter()
            .map(|s| size_of::<tidb_goruntime::GoString>() + s.as_bytes().len() + 11)
            .sum()
    }
}
impl Heap for GoAny {
    fn heap(&self, s: &mut Sizer) -> usize {
        self.retained_memory_usage(&mut s.pointers)
    }
}
fields!(TableInfo; name, charset, collate, columns, indices, constraints, foreign_keys, comment,
    partition, compression, view, sequence, lock, tiflash_replica, placement_policy_ref, stats_options,
    exchange_partition_info, ttl_info, softdelete_info, affinity, table_split_policy, engine_attribute,
    storage_class_tier, storage_class_transitions);
fields!(ColumnInfo; name, origin_default_value, origin_default_value_bit, default_value, default_value_bit,
    generated_expr_string, dependences, field_type, changing_field_type, comment, change_state_info);
fields!(IndexInfo; name, table, columns, comment, vector_info, inverted_info, full_text_info, condition_expr_string, affect_column, region_split_policy);
fields!(IndexColumn; name);
fields!(VectorIndexInfo; distance_metric);
fields!(FullTextIndexInfo; parser_type);
fields!(RegionSplitPolicy; lower, upper);
fields!(ConstraintInfo; name, table, constraint_cols, expr_string);
fields!(FKInfo; name, ref_schema, ref_table, ref_cols, cols);
fields!(PartitionInfo; expr, columns, definitions, adding_definitions, dropping_definitions, new_partition_ids,
    original_partition_ids_order, states, ddl_expr, ddl_columns, ddl_update_indexes, ddl_changed_index);
fields!(PartitionDefinition; name, less_than, in_values, placement_policy_ref, comment, storage_class_tier, storage_class_transitions);
fields!(UpdateIndexInfo; index_name);
fields!(ViewInfo; definer, select_stmt, cols);
fields!(tidb_parser::auth::UserIdentity; username, hostname, auth_username, auth_hostname, auth_plugin);
fields!(SequenceInfo; comment);
fields!(TableLockInfo; sessions);
fields!(SessionInfo; server_id);
fields!(TiFlashReplicaInfo; location_labels, available_partition_ids);
fields!(PolicyRefInfo; name);
fields!(StatsOptions; stats_window_settings, column_list);
fields!(TTLInfo; column_name, interval_expr_str, job_interval);
fields!(SoftdeleteInfo; retention, job_interval);
fields!(TableAffinityInfo; level);
fields!(engine_attribute::StorageClassTransitRule; tier);

/// Go `entry.Size`, using native Rust headers instead of Go ABI sizes.
pub(super) fn table_size(info: &TableInfo, table: &crate::TableEntry) -> u64 {
    let mut s = Sizer::default();
    s.pointers.insert(info as *const TableInfo as usize);
    let mut bytes = size_of::<super::CachedTable>()
        + size_of::<super::TableCacheKey>()
        + size_of::<std::sync::RwLock<TableInfo>>()
        + 2 * size_of::<usize>()
        + info.heap(&mut s);
    bytes += size_of::<crate::TableEntry>();
    if let crate::TableEntry::Kv(table) = table {
        bytes += table.cache_runtime_memory_usage();
        for col in table.columns() {
            bytes += col.info.heap(&mut s);
        }
        for index in table.indexes_arc().iter() {
            bytes += index.info.heap(&mut s);
        }
    }
    if let crate::TableEntry::View(view) = table {
        bytes += view.heap(&mut s);
    }
    if let crate::TableEntry::Sequence(sequence) = table {
        bytes += sequence.name.capacity() + sequence.comment.capacity();
    }
    bytes as u64
}
impl<A: Heap, B: Heap> Heap for (A, B) {
    fn heap(&self, s: &mut Sizer) -> usize {
        self.0.heap(s) + self.1.heap(s)
    }
}
fields!(crate::ViewDef; name, columns, select_sql, definer_user, definer_host, character_set_client,
    collation_connection, algorithm, security, check_option);
