// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use super::{valid_to_analyze, NOT_PARTITIONED_TABLE, SCHEMA_NOT_EXIST, TABLE_NOT_EXIST};
use crate::auto_analyze_runtime::model::{
    JobIndicators, PartitionIndexMap, RuntimeResult, SqlStatement, SqlValue,
};
use crate::auto_analyze_runtime::ports::{InfoSchemaPort, SqlPort};

#[must_use]
pub fn get_partition_sql(prefix: &str, suffix: &str, partition_count: usize) -> String {
    let mut text = prefix.to_owned();
    for position in 0..partition_count {
        if position != 0 {
            text.push(',');
        }
        text.push_str(" %n");
    }
    text.push_str(suffix);
    text
}

#[must_use]
pub fn flatten_partition_names(groups: &[Vec<String>]) -> Vec<String> {
    groups.iter().flatten().cloned().collect()
}

#[derive(Clone, Debug, PartialEq)]
pub struct DynamicPartitionedJob {
    pub global_table_id: i64,
    pub partition_ids: BTreeSet<i64>,
    pub partition_index_ids: PartitionIndexMap,
    pub table_stats_version: i32,
    pub need_version_rewrite_warning: bool,
    pub indicators: JobIndicators,
    pub weight: f64,
    pub schema_name: String,
    pub table_name: String,
    pub partition_names: Vec<String>,
    pub partition_index_names: BTreeMap<String, Vec<String>>,
}

impl DynamicPartitionedJob {
    pub fn validate_and_prepare(
        &mut self,
        info: &impl InfoSchemaPort,
        sql: &mut impl SqlPort,
    ) -> RuntimeResult<(bool, String)> {
        let Some(table) = info.table_by_id(self.global_table_id) else {
            return Ok((false, TABLE_NOT_EXIST.to_owned()));
        };
        if table.schema_name.is_empty() {
            return Ok((false, SCHEMA_NOT_EXIST.to_owned()));
        }
        if table.partitions.is_empty() {
            return Ok((false, NOT_PARTITIONED_TABLE.to_owned()));
        }
        let names: BTreeMap<_, _> = table
            .partitions
            .iter()
            .map(|partition| (partition.id, partition.name.clone()))
            .collect();
        self.schema_name = table.schema_name;
        self.table_name = table.table_name;
        self.partition_names = self
            .partition_ids
            .iter()
            .filter_map(|id| names.get(id).cloned())
            .collect();
        self.partition_index_names.clear();
        for index in table.indexes {
            if let Some(ids) = self.partition_index_ids.get(&index.id) {
                let partitions: Vec<_> =
                    ids.iter().filter_map(|id| names.get(id).cloned()).collect();
                if !partitions.is_empty() {
                    self.partition_index_names.insert(index.name, partitions);
                }
            }
        }
        let mut all = self.partition_names.clone();
        all.extend(self.partition_index_names.values().flatten().cloned());
        if all.is_empty() {
            Ok((true, String::new()))
        } else {
            valid_to_analyze(sql, &self.schema_name, &self.table_name, &all)
        }
    }

    fn statement(&self, partitions: &[String], index: Option<&str>) -> SqlStatement {
        let mut text = get_partition_sql("analyze table %n.%n partition", "", partitions.len());
        let mut params = vec![
            SqlValue::Identifier(self.schema_name.clone()),
            SqlValue::Identifier(self.table_name.clone()),
        ];
        params.extend(partitions.iter().cloned().map(SqlValue::Identifier));
        if let Some(index) = index {
            text.push_str(" index %n");
            params.push(SqlValue::Identifier(index.to_owned()));
        }
        SqlStatement { sql: text, params }
    }

    pub fn analyze(&self, sql: &mut impl SqlPort, batch_size: usize) -> RuntimeResult<()> {
        let batch_size = batch_size.max(1);
        if !self.partition_index_ids.is_empty() {
            if let Some((index, partitions)) = self.partition_index_names.iter().next() {
                for batch in partitions.chunks(batch_size) {
                    sql.execute(&self.statement(batch, Some(index)))?;
                }
            } else {
                return Err(crate::auto_analyze_runtime::model::RuntimeError(
                    "no requested partition index remains in InfoSchema".to_owned(),
                ));
            }
        } else {
            for batch in self.partition_names.chunks(batch_size) {
                sql.execute(&self.statement(batch, None))?;
            }
        }
        Ok(())
    }
}
