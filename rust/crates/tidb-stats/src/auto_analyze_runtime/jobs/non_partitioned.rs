// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeSet;

use super::{valid_to_analyze, SCHEMA_NOT_EXIST, TABLE_NOT_EXIST};
use crate::auto_analyze_runtime::model::{JobIndicators, RuntimeResult, SqlStatement, SqlValue};
use crate::auto_analyze_runtime::ports::{InfoSchemaPort, SqlPort};

#[derive(Clone, Debug, PartialEq)]
pub struct NonPartitionedJob {
    pub table_id: i64,
    pub index_ids: BTreeSet<i64>,
    pub table_stats_version: i32,
    pub need_version_rewrite_warning: bool,
    pub indicators: JobIndicators,
    pub weight: f64,
    pub schema_name: String,
    pub table_name: String,
    pub index_names: Vec<String>,
}

impl NonPartitionedJob {
    pub fn validate_and_prepare(
        &mut self,
        info: &impl InfoSchemaPort,
        sql: &mut impl SqlPort,
    ) -> RuntimeResult<(bool, String)> {
        let Some(table) = info.table_by_id(self.table_id) else {
            return Ok((false, TABLE_NOT_EXIST.to_owned()));
        };
        if table.schema_name.is_empty() {
            return Ok((false, SCHEMA_NOT_EXIST.to_owned()));
        }
        self.schema_name = table.schema_name;
        self.table_name = table.table_name;
        self.index_names = table
            .indexes
            .into_iter()
            .filter(|index| self.index_ids.contains(&index.id))
            .map(|index| index.name)
            .collect();
        valid_to_analyze(sql, &self.schema_name, &self.table_name, &[])
    }

    #[must_use]
    pub fn table_statement(&self) -> SqlStatement {
        SqlStatement {
            sql: "analyze table %n.%n".to_owned(),
            params: vec![
                SqlValue::Identifier(self.schema_name.clone()),
                SqlValue::Identifier(self.table_name.clone()),
            ],
        }
    }

    #[must_use]
    pub fn index_statement(&self, index: &str) -> SqlStatement {
        SqlStatement {
            sql: "analyze table %n.%n index %n".to_owned(),
            params: vec![
                SqlValue::Identifier(self.schema_name.clone()),
                SqlValue::Identifier(self.table_name.clone()),
                SqlValue::Identifier(index.to_owned()),
            ],
        }
    }

    pub fn analyze(&self, sql: &mut impl SqlPort) -> RuntimeResult<()> {
        if self.index_ids.is_empty() {
            sql.execute(&self.table_statement())
        } else if let Some(first) = self.index_names.first() {
            sql.execute(&self.index_statement(first))
        } else {
            Ok(())
        }
    }
}
