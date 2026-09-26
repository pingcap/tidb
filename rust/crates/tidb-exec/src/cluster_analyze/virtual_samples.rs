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

//! Virtual values belong to the sample schema, but not to stored histograms.
use super::{AnalyzeError, AnalyzePlan};
use tidb_datatype::{Datum, FieldType};
use tidb_executor::generated_column::{self, GeneratedColumn, GeneratedColumnSlot};
use tidb_expr::Columns;
use tidb_model::TableInfo;

struct SampleColumn {
    name: String,
    field_type: FieldType,
    generated: Option<GeneratedColumn>,
}

impl GeneratedColumnSlot for SampleColumn {
    fn generation(&self) -> Option<&GeneratedColumn> {
        self.generated.as_ref()
    }
    fn column_type(&self) -> &FieldType {
        &self.field_type
    }
    fn column_name(&self) -> &str {
        &self.name
    }
}

pub(super) struct VirtualSamples {
    columns: Vec<SampleColumn>,
    has_virtual: bool,
}

impl VirtualSamples {
    pub fn new(
        table: &TableInfo,
        plan: &AnalyzePlan,
        context: &tidb_executor::StmtContext,
    ) -> Result<Self, AnalyzeError> {
        if !table
            .cols()
            .iter_deref()
            .any(|column| column.read().is_virtual_generated())
        {
            return Ok(Self {
                columns: Vec::new(),
                has_virtual: false,
            });
        }
        let names: Vec<_> = plan
            .columns()
            .iter()
            .map(|column| column.name.clone())
            .collect();
        let types: Vec<_> = plan
            .columns()
            .iter()
            .map(|column| column.field_type.clone())
            .collect();
        let mut columns = Vec::with_capacity(names.len());
        let mut has_virtual = false;
        for column in plan.columns() {
            let metadata = table
                .cols()
                .iter_deref()
                .find(|source| source.read().id == column.id)
                .ok_or_else(|| {
                    AnalyzeError::unsupported(format!("ANALYZE column {} is missing", column.id))
                })?;
            let metadata = metadata.read();
            let generated = if metadata.is_virtual_generated() {
                has_virtual = true;
                let source =
                    tidb_model::generated_expr::parse_expression(&metadata.generated_expr_string)
                        .map_err(|error| AnalyzeError::unsupported(error.message))?;
                Some(
                    generated_column::build_added_generated_column_with_like_default_escape(
                        &column.name,
                        &source,
                        false,
                        &names,
                        &types,
                        &context.time_zone(),
                        context.like_default_escape(),
                    )
                    .map_err(|error| {
                        AnalyzeError::unsupported(format!(
                            "ANALYZE virtual column {}: {error:?}",
                            column.name
                        ))
                    })?,
                )
            } else {
                None
            };
            columns.push(SampleColumn {
                name: column.name.clone(),
                field_type: column.field_type.clone(),
                generated,
            });
        }
        Ok(Self {
            columns,
            has_virtual,
        })
    }

    pub fn materialize(
        &self,
        row: &mut [Datum],
        context: &tidb_executor::StmtContext,
    ) -> Result<(), tidb_executor::analyze::AnalyzeError> {
        if !self.has_virtual {
            return Ok(());
        }
        // Extra handle slots follow the selected sample columns. Expressions
        // resolve against the physical sample schema, never the handle suffix.
        generated_column::materialize(&self.columns, &mut row[..self.columns.len()], true, context)
            .map_err(|error| {
                tidb_executor::analyze::AnalyzeError::Unsupported(format!(
                    "ANALYZE virtual column {}: {}",
                    error.column, error.detail
                ))
            })
    }
}
