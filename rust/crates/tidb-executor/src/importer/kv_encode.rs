//! Go `pkg/executor/importer/kv_encode.go`: the IMPORT INTO row encoder,
//! which maps a parsed row through the statement's column list, user
//! variables and SET assignments into the table's record before Lightning's
//! KV encoder turns it into KV pairs.

use tidb_datatype::{Datum, FieldTypeFlags};
use tidb_expr::expression::Expression;
use tidb_lightning_backend_encode::EncodingConfig;
use tidb_lightning_backend_kv::{AllocatorType, BaseKVEncoder, EncodeError, Pairs, PanickingAllocators};
use tidb_model::{ColumnInfo, TableInfo};

use super::{FieldMapping, LoadDataController};

/// Go `TableKVEncoder`.
pub struct TableKVEncoder {
    base: BaseKVEncoder,
    column_assignments: Vec<Expression>,
    field_mappings: Vec<FieldMapping>,
    /// The offsets of Go `insertColumns`.
    insert_columns: Vec<usize>,
}

/// Go `CreateColAssignSimpleExprs` over the encoder's expression context.
fn create_col_assign_simple_exprs(
    base: &BaseKVEncoder,
    assignments: &[tidb_ast::Assignment],
) -> Result<Vec<Expression>, EncodeError> {
    let ctx = base.session().context();
    let resolver = tidb_expr::column_resolver::ContextResolver(ctx);
    assignments
        .iter()
        .map(|a| {
            tidb_expr::simple_expr::build_simple_expr(&resolver, &a.value, &tidb_expr::simple_expr::BuildOptions::default())
                .map_err(|e| EncodeError::Other(e.to_string()))
        })
        .collect()
}

fn offset_of(info: &tidb_model::GoShared<ColumnInfo>) -> usize {
    usize::try_from(info.read().offset).unwrap_or(0)
}

impl TableKVEncoder {
    /// Go `NewTableKVEncoder`.
    pub fn new(
        config: &EncodingConfig,
        ctrl: &LoadDataController,
        allocators: PanickingAllocators,
    ) -> Result<Self, EncodeError> {
        let insert_columns = ctrl.insert_columns.iter().map(offset_of).collect();
        Self::new_inner(config, &ctrl.ast_args.column_assignments, ctrl.field_mappings.clone(), insert_columns, allocators)
    }

    /// Go `NewTableKVEncoderForDupResolve`: every visible column, in order,
    /// read from the row.
    pub fn new_for_dup_resolve(
        config: &EncodingConfig,
        ctrl: &LoadDataController,
        allocators: PanickingAllocators,
    ) -> Result<Self, EncodeError> {
        let (mappings, _) = super::table_vis_cols_2_field_mappings(&ctrl.table);
        let insert_columns = mappings.iter().filter_map(|m| m.column.as_ref().map(offset_of)).collect();
        Self::new_inner(config, &ctrl.ast_args.column_assignments, mappings, insert_columns, allocators)
    }

    fn new_inner(
        config: &EncodingConfig,
        assignments: &[tidb_ast::Assignment],
        field_mappings: Vec<FieldMapping>,
        insert_columns: Vec<usize>,
        allocators: PanickingAllocators,
    ) -> Result<Self, EncodeError> {
        let base = BaseKVEncoder::new(config, allocators)?;
        let column_assignments = create_col_assign_simple_exprs(&base, assignments)?;
        Ok(Self { base, column_assignments, field_mappings, insert_columns })
    }

    /// The embedded `BaseKVEncoder`.
    #[must_use]
    pub fn base(&self) -> &BaseKVEncoder {
        &self.base
    }

    /// Go `Encode`.
    pub fn encode(&mut self, row: &[Datum], row_id: i64) -> Result<Pairs, EncodeError> {
        let result = self.encode_inner(row, row_id);
        self.base.truncate_warns();
        result
    }

    fn encode_inner(&mut self, row: &[Datum], row_id: i64) -> Result<Pairs, EncodeError> {
        let (record, record_id) = self.parser_data_2_table_data(row, row_id)?;
        let pairs = self.base.record_to_kv(&record, record_id, row_id)?;
        Ok(Pairs::from_kv_pairs(pairs))
    }

    /// Go `parserData2TableData`.
    fn parser_data_2_table_data(&mut self, parser_data: &[Datum], row_id: i64) -> Result<(Vec<Datum>, Option<i64>), EncodeError> {
        let mut row = Vec::with_capacity(self.insert_columns.len());
        let session = self.base.session();
        let set_var = |name: &str, col: Option<&Datum>| match col {
            None | Some(Datum::Null) => session.unset_user_var(name),
            Some(value) => session.set_user_var_val(name, value.clone()),
        };
        let row_len = self.base.columns().len();
        let mut has_value = vec![false; row_len];
        for offset in &self.insert_columns {
            has_value[*offset] = true;
        }
        for (i, mapping) in self.field_mappings.iter().enumerate() {
            let col = mapping.column.as_ref();
            if i >= parser_data.len() {
                let Some(col) = col else {
                    set_var(mapping.user_var.as_deref().unwrap_or_default(), None);
                    continue;
                };
                let info = col.read();
                if is_type_time(&info.field_type) && info.field_type.flags() & FieldTypeFlags::NOT_NULL != 0 {
                    row.push(Datum::Time(tidb_datatype::Time::current(time_type_of(&info.field_type))));
                    continue;
                }
                row.push(Datum::Null);
                has_value[usize::try_from(info.offset).unwrap_or(0)] = false;
                continue;
            }
            if col.is_none() {
                set_var(mapping.user_var.as_deref().unwrap_or_default(), Some(&parser_data[i]));
                continue;
            }
            row.push(parser_data[i].clone());
        }
        let ctx = self.base.session().context();
        for expr in &self.column_assignments {
            let d = expr
                .eval(ctx, tidb_chunk::row::Row::empty())
                .map_err(|e| EncodeError::Other(tidb_expr::EvalErrorSql::to_mysql_error(&e).error_string()))?;
            row.push(d);
        }
        self.get_row(row, has_value, row_id)
    }

    /// Go `getRow`.
    fn get_row(&mut self, vals: Vec<Datum>, has_value: Vec<bool>, row_id: i64) -> Result<(Vec<Datum>, Option<i64>), EncodeError> {
        let mut row = vec![Datum::Null; self.base.columns().len()];
        for (i, offset) in self.insert_columns.iter().enumerate() {
            let info = self.base.columns()[*offset].read().clone();
            let casted = self
                .base
                .cast(vals[i].clone(), &info)
                .map_err(|e| self.base.log_kv_convert_failed(&vals, i as isize, &info, e))?;
            row[*offset] = casted;
        }
        self.fill_row(row, &has_value, row_id)
    }

    /// Go `fillRow`.
    fn fill_row(&mut self, row: Vec<Datum>, has_value: &[bool], row_id: i64) -> Result<(Vec<Datum>, Option<i64>), EncodeError> {
        let columns = self.base.columns().to_vec();
        let mut record = Vec::with_capacity(columns.len() + 1);
        for (i, col) in columns.iter().enumerate() {
            let (datum, do_cast) = if has_value[i] { (Some(&row[i]), false) } else { (None, true) };
            let value = self
                .base
                .process_col_datum(col, row_id, datum, do_cast)
                .map_err(|e| self.base.log_kv_convert_failed(&row, i as isize, &col.read(), e))?;
            record.push(value);
        }
        let mut record_id = None;
        let meta: TableInfo = self.base.table_meta();
        if tidb_lightning_common::table_has_auto_row_id(&meta) {
            let new_row_id = self.base.auto_id(row_id);
            record.push(Datum::Int(new_row_id));
            record_id = Some(new_row_id);
            self.base.table_allocators().get(AllocatorType::RowId).rebase(row_id);
        }
        if self.base.has_generated_columns() {
            self.base
                .eval_generated_columns(&mut record)
                .map_err(|(column, e)| self.base.log_eval_gen_expr_failed(&column, e))?;
        }
        Ok((record, record_id))
    }
}

/// Go `types.IsTypeTime`.
fn is_type_time(ft: &tidb_datatype::FieldType) -> bool {
    matches!(
        ft.code(),
        tidb_datatype::FieldTypeCode::Date | tidb_datatype::FieldTypeCode::Datetime | tidb_datatype::FieldTypeCode::Timestamp
    )
}

fn time_type_of(ft: &tidb_datatype::FieldType) -> tidb_datatype::TimeType {
    match ft.code() {
        tidb_datatype::FieldTypeCode::Date => tidb_datatype::TimeType::Date,
        tidb_datatype::FieldTypeCode::Timestamp => tidb_datatype::TimeType::Timestamp,
        _ => tidb_datatype::TimeType::DateTime,
    }
}

/// Go `GenKVIndex`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenKVIndex {
    /// Go `Name`.
    pub name: String,
    /// Go `Unique`.
    pub unique: bool,
}

/// Go `GetIndicesGenKV`: the indexes that produce index KVs (public, and
/// not a clustered primary key).
#[must_use]
pub fn get_indices_gen_kv(tbl_info: &TableInfo) -> std::collections::BTreeMap<i64, GenKVIndex> {
    let mut res = std::collections::BTreeMap::new();
    for idx in tbl_info.indices.iter_deref() {
        let idx = idx.read();
        if idx.state != tidb_model::SchemaState::PUBLIC {
            continue;
        }
        if idx.primary && tbl_info.has_clustered_index() {
            continue;
        }
        res.insert(idx.id, GenKVIndex { name: idx.name.lowercase().to_owned(), unique: idx.unique });
    }
    res
}

/// Go `GetNumOfIndexGenKV`.
#[must_use]
pub fn get_num_of_index_gen_kv(tbl_info: &TableInfo) -> usize {
    get_indices_gen_kv(tbl_info).len()
}
