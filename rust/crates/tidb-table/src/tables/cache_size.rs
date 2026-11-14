// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `infoschema/internal.Sizeof`'s table-owned runtime data.
use super::*;
use std::mem::size_of;

fn vec_bytes<T>(v: &Vec<T>) -> usize {
    v.capacity() * size_of::<T>()
}
fn strings(v: &Vec<String>) -> usize {
    vec_bytes(v) + v.iter().map(String::capacity).sum::<usize>()
}
fn datum(v: &Datum) -> usize {
    v.estimated_mem_usage()
        .saturating_sub(tidb_datatype::EMPTY_DATUM_SIZE) as usize
}
fn column(v: &crate::column::Column) -> usize {
    let mut bytes = v.origin_default.as_ref().map_or(0, datum);
    if let Some(generated) = &v.generated {
        bytes += generated.expr_text.capacity()
            + strings(&generated.dependencies)
            + generated.expr.memory_usage() as usize;
    }
    if let Some(default) = &v.default_value {
        bytes += match default {
            crate::column_default::ColumnDefault::Value(v) => datum(v),
            crate::column_default::ColumnDefault::Computed(v) => {
                size_of::<crate::column_default::ComputedDefault>()
                    + v.text.capacity()
                    + v.expr.as_ref().map_or(0, |e| e.memory_usage() as usize)
            }
        };
    }
    bytes
}
fn constraint(v: &KvCheckConstraint) -> usize {
    v.name.capacity() + strings(&v.dependencies) + v.expr.memory_usage() as usize
}
fn partition(v: &crate::partition_routing::PartitionExpr) -> usize {
    use crate::partition_routing::PartitionKind;
    let mut bytes = strings(&v.dependencies) + v.expr.memory_usage() as usize;
    bytes += match &v.kind {
        PartitionKind::Hash | PartitionKind::Key | PartitionKind::None => 0,
        PartitionKind::Range { less_than, .. } => vec_bytes(less_than),
        PartitionKind::RangeColumns {
            less_than,
            field_types,
        } => {
            vec_bytes(less_than)
                + less_than
                    .iter()
                    .map(|row| {
                        vec_bytes(row)
                            + row
                                .iter()
                                .map(|b| match b {
                                    crate::partition_routing::RangeColumnBound::Value(v) => {
                                        datum(v)
                                    }
                                    _ => 0,
                                })
                                .sum::<usize>()
                    })
                    .sum::<usize>()
                + vec_bytes(field_types)
                + field_types
                    .iter()
                    .map(|t| t.memory_usage().saturating_sub(120))
                    .sum::<usize>()
        }
        PartitionKind::List { values, .. } => vec_bytes(values),
        PartitionKind::ListColumns {
            values,
            keys,
            field_types,
            ..
        } => {
            vec_bytes(values)
                + values
                    .iter()
                    .map(|(row, _)| vec_bytes(row) + row.iter().map(datum).sum::<usize>())
                    .sum::<usize>()
                + keys.capacity() * size_of::<(Vec<u8>, usize)>()
                + keys.keys().map(Vec::capacity).sum::<usize>()
                + vec_bytes(field_types)
                + field_types
                    .iter()
                    .map(|t| t.memory_usage().saturating_sub(120))
                    .sum::<usize>()
        }
    };
    bytes
}
impl TableCommon {
    /// Go `internal.Sizeof(tableCommon)`: runtime allocations beyond shared
    /// model metadata, which the caller charges with pointer deduplication.
    /// Uses the expression package's own source-shaped memory accounting.
    pub fn cache_runtime_memory_usage(&self) -> usize {
        let mut bytes = size_of::<Self>() + 4 * (size_of::<Vec<usize>>() + 2 * size_of::<usize>());
        bytes += vec_bytes(&self.columns) + self.columns.iter().map(column).sum::<usize>();
        bytes += vec_bytes(&self.indexes)
            + self
                .indexes
                .iter()
                .map(|v| vec_bytes(&v.column_offsets))
                .sum::<usize>();
        bytes += vec_bytes(&self.nonpublic_columns)
            + self
                .nonpublic_columns
                .iter()
                .map(|v| column(&v.column))
                .sum::<usize>();
        bytes += vec_bytes(&self.nonpublic_indexes)
            + self
                .nonpublic_indexes
                .iter()
                .map(|v| vec_bytes(&v.index.column_offsets))
                .sum::<usize>();
        bytes += vec_bytes(&self.hidden_offsets)
            + vec_bytes(&self.common_handle_offsets)
            + vec_bytes(&self.common_handle_prefix_lengths);
        bytes += self.read_partitions.as_ref().map_or(0, vec_bytes);
        bytes += self
            .new_table_global_indexes
            .as_ref()
            .map_or(0, |(_, ids)| vec_bytes(ids));
        bytes += self.partition_storage_statistics.len() * size_of::<(i64, (u64, u64, u64, u64))>();
        bytes += self
            .mv_key_part_sources
            .iter()
            .chain(self.partial_index_condition_texts.iter())
            .map(|(_, s)| size_of::<(i64, String)>() + s.capacity())
            .sum::<usize>();
        bytes += self
            .changing_field_types
            .values()
            .map(|t| size_of::<(usize, FieldType)>() + t.memory_usage().saturating_sub(120))
            .sum::<usize>();
        bytes += self
            .changing_index_parts
            .values()
            .map(|v| size_of::<(i64, Vec<usize>)>() + vec_bytes(v))
            .sum::<usize>();
        bytes += vec_bytes(&self.check_constraints)
            + self.check_constraints.iter().map(constraint).sum::<usize>();
        bytes += self
            .partial_index_conditions
            .values()
            .map(|v| size_of::<(i64, KvCheckConstraint)>() + constraint(v))
            .sum::<usize>();
        bytes += vec_bytes(&self.check_constraint_infos)
            + self
                .check_constraint_infos
                .iter()
                .map(|v| {
                    v.name.memory_usage()
                        + v.table.memory_usage()
                        + v.expr_string.capacity()
                        + v.constraint_cols.capacity() * size_of::<tidb_ast::CiString>()
                        + v.constraint_cols
                            .iter()
                            .map(|c| c.original().len() + c.lowercase().len())
                            .sum::<usize>()
                })
                .sum::<usize>();
        bytes += self.partition.as_ref().map_or(0, |v| {
            size_of::<crate::partition_routing::PartitionExpr>() + partition(v)
        });
        if let Some(v) = &self.partition_reorg {
            bytes += size_of::<PartitionDoubleWrite>()
                + partition(&v.spec)
                + (v.reorganize.len() + v.double_write.len()) * size_of::<i64>()
                + v.index_modes
                    .values()
                    .map(|mode| {
                        size_of::<(i64, PartitionIndexMode)>()
                            + (mode.excluded.len() + mode.changed.len()) * size_of::<i64>()
                    })
                    .sum::<usize>();
        }
        bytes
    }
}
