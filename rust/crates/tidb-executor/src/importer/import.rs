//! Go `pkg/executor/importer/import.go`: import plans and source interpretation.

use crate::{import_into_options, DriverError};
use std::collections::BTreeSet;
use tidb_ast::{ImportIntoStmt, ImportSource};
use tidb_lightning_mydump::CsvConfig;

/// Go `config.PostOpLevel`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PostOpLevel {
    #[default]
    Off,
    Optional,
    Required,
}

/// Go `Plan`: data interpretation and import options. The metadata and session
/// snapshot portion of `NewImportPlan` is ported separately from these options.
#[derive(Clone, Debug, Default)]
pub struct Plan {
    pub path: String,
    pub format: String,
    pub data_source_type: String,
    pub charset: Option<String>,
    pub field_null_def: Vec<String>,
    pub fields_terminated_by: String,
    pub fields_enclosed_by: String,
    pub fields_escaped_by: String,
    pub lines_starting_by: String,
    pub lines_terminated_by: String,
    pub null_value_opt_enclosed: bool,
    pub ignore_lines: u64,
    pub disk_quota: i64,
    pub checksum: PostOpLevel,
    pub thread_cnt: usize,
    pub max_write_speed: i64,
    pub split_file: bool,
    pub max_recorded_errors: i64,
    pub on_dup_key: String,
    pub detached: bool,
    pub disable_tikv_import_mode: bool,
    pub max_engine_size: i64,
    pub cloud_storage_uri: String,
    pub disable_precheck: bool,
    pub group_key: String,
    pub in_import_into: bool,
    pub force_merge_step: bool,
    pub manual_recovery: bool,
    pub specified_option_names: BTreeSet<String>,
    pub use_new_collate: Option<bool>,
}

impl Plan {
    /// Go `Plan.initDefaultOptions`; `cloud_storage_uri` is the URI returned
    /// by DXF's `handle.GetCloudStorageURI`, including its task prefix.
    pub fn init_default_options(&mut self, target_node_cpu_cnt: usize, cloud_storage_uri: &str) {
        self.thread_cnt = if self.data_source_type == "query" {
            2
        } else {
            (target_node_cpu_cnt / 2).max(1)
        };
        self.checksum = PostOpLevel::Required;
        self.max_write_speed = 0;
        self.split_file = false;
        self.max_recorded_errors = 100;
        self.on_dup_key = "error".to_owned();
        self.detached = false;
        self.disable_tikv_import_mode = false;
        self.max_engine_size = if tidb_config_kerneltype::is_next_gen() {
            100 << 30
        } else {
            500 << 30
        };
        self.cloud_storage_uri = cloud_storage_uri.to_owned();
        self.charset = Some("utf8mb4".to_owned());
    }

    /// Go `newDefaultLineFieldsInfo` and `NewImportPlan`'s interpretation defaults.
    pub fn new_import_plan(
        stmt: &ImportIntoStmt,
        target_node_cpu_cnt: usize,
        cloud_storage_uri: &str,
    ) -> Result<Self, DriverError> {
        let (path, format, data_source_type) = match &stmt.source {
            ImportSource::File { path, format } => (
                path.clone(),
                format.as_deref().unwrap_or("auto").to_ascii_lowercase(),
                "file",
            ),
            ImportSource::Select { .. } => (String::new(), "auto".to_owned(), "query"),
        };
        let mut plan = Self {
            path,
            format,
            data_source_type: data_source_type.to_owned(),
            in_import_into: true,
            fields_terminated_by: ",".to_owned(),
            fields_enclosed_by: "\"".to_owned(),
            fields_escaped_by: "\\".to_owned(),
            field_null_def: vec!["\\N".to_owned()],
            ..Self::default()
        };
        plan.init_options(stmt, target_node_cpu_cnt, cloud_storage_uri)?;
        Ok(plan)
    }

    /// Go `Plan.initOptions`: retain each validated value for the controller.
    pub fn init_options(
        &mut self,
        stmt: &ImportIntoStmt,
        target_node_cpu_cnt: usize,
        cloud_storage_uri: &str,
    ) -> Result<(), DriverError> {
        self.init_default_options(target_node_cpu_cnt, cloud_storage_uri);
        import_into_options::validate_import_into_options_with_cloud_storage(
            stmt,
            cloud_storage_uri,
        )?;
        self.specified_option_names.clear();
        for opt in &stmt.options {
            self.specified_option_names.insert(opt.name.clone());
            let text = || import_into_options::as_string(opt);
            let integer = || import_into_options::as_int(opt);
            let bytes = || {
                text().map(|text| {
                    import_into_options::ram_in_bytes(&text).expect("validated byte size")
                })
            };
            match opt.name.as_str() {
                "character_set" => self.charset = Some(text()?),
                "fields_terminated_by" => self.fields_terminated_by = text()?,
                "fields_enclosed_by" => self.fields_enclosed_by = text()?,
                "fields_escaped_by" => self.fields_escaped_by = text()?,
                "fields_defined_null_by" => self.field_null_def = vec![text()?],
                "lines_terminated_by" => self.lines_terminated_by = text()?,
                "skip_rows" => self.ignore_lines = integer()? as u64,
                "split_file" => self.split_file = true,
                "disk_quota" => self.disk_quota = bytes()?,
                "thread" => self.thread_cnt = integer()? as usize,
                "max_write_speed" => self.max_write_speed = bytes()?,
                "checksum_table" => {
                    self.checksum = match text()?.to_ascii_lowercase().as_str() {
                        "off" | "false" => PostOpLevel::Off,
                        "optional" => PostOpLevel::Optional,
                        _ => PostOpLevel::Required,
                    }
                }
                "on_duplicate_key" => self.on_dup_key = text()?.to_ascii_lowercase(),
                "group_key" => self.group_key = text()?,
                "record_errors" => self.max_recorded_errors = integer()?,
                "detached" => self.detached = true,
                "disable_tikv_import_mode" => self.disable_tikv_import_mode = true,
                "cloud_storage_uri" => self.cloud_storage_uri = text()?,
                "__max_engine_size" => self.max_engine_size = bytes()?,
                "disable_precheck" => self.disable_precheck = true,
                "__force_merge_step" => self.force_merge_step = true,
                "__manual_recovery" => self.manual_recovery = true,
                _ => unreachable!("validated option"),
            }
        }
        self.adjust_options(target_node_cpu_cnt);
        Ok(())
    }

    /// Go `Plan.adjustOptions`.
    pub fn adjust_options(&mut self, target_node_cpu_cnt: usize) {
        let limit = if self.data_source_type == "query" {
            2 * target_node_cpu_cnt
        } else {
            target_node_cpu_cnt
        };
        self.thread_cnt = self.thread_cnt.min(limit);
        if self.is_global_sort() {
            self.disable_tikv_import_mode = true;
        }
    }

    /// Go `Plan.IsLocalSort`.
    pub fn is_local_sort(&self) -> bool {
        self.cloud_storage_uri.is_empty()
    }
    /// Go `Plan.IsGlobalSort`.
    pub fn is_global_sort(&self) -> bool {
        !self.is_local_sort()
    }
    /// Go `Plan.GetOnDupKeyMode`.
    pub fn get_on_dup_key_mode(&self) -> &str {
        if self.on_dup_key.is_empty() {
            "error"
        } else {
            &self.on_dup_key
        }
    }
    /// Go `Plan.GetUseNewCollateOrDefault`.
    pub fn get_use_new_collate_or_default(&self, default: bool) -> bool {
        self.use_new_collate.unwrap_or(default)
    }
    /// Go `Plan.setUseNewCollate`.
    pub fn set_use_new_collate(&mut self, value: bool) {
        self.use_new_collate = Some(value);
    }

    /// Go `generateCSVConfig`.
    pub fn generate_csv_config(&self) -> CsvConfig {
        CsvConfig {
            fields_terminated_by: self.fields_terminated_by.as_bytes().to_vec(),
            fields_enclosed_by: self.fields_enclosed_by.as_bytes().to_vec(),
            fields_escaped_by: self.fields_escaped_by.as_bytes().to_vec(),
            lines_terminated_by: self.lines_terminated_by.as_bytes().to_vec(),
            lines_starting_by: self.lines_starting_by.as_bytes().to_vec(),
            field_null_defined_by: self
                .field_null_def
                .iter()
                .map(|v| v.as_bytes().to_vec())
                .collect(),
            allow_empty_line: !self.in_import_into,
            quoted_null_is_text: !self.in_import_into && !self.null_value_opt_enclosed,
            unescaped_quote: !self.in_import_into,
            ..CsvConfig::default()
        }
    }
}

/// Go `parseFileType`: remove at most one compression extension.
pub fn parse_file_type(path: &str) -> &'static str {
    let lower = path.to_lowercase();
    let extension = |path: &str| {
        path.rsplit('/')
            .next()
            .unwrap_or(path)
            .rsplit_once('.')
            .map(|(_, ext)| ext.to_owned())
    };
    let ext = extension(&lower).unwrap_or_default();
    let path = if matches!(ext.as_str(), "gz" | "gzip" | "zstd" | "zst" | "snappy") {
        &lower[..lower.len() - ext.len() - 1]
    } else {
        &lower
    };
    match extension(path).as_deref() {
        Some("sql") => "sql",
        Some("parquet") => "parquet",
        _ => "csv",
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

/// Go `ASTArgs`: the column mapping syntax retained for the import workers.
#[derive(Clone, Debug, Default)]
pub struct ASTArgs {
    pub columns_and_user_vars: Vec<tidb_ast::ColumnOrUserVar>,
    pub column_assignments: Vec<tidb_ast::Assignment>,
}

impl ASTArgs {
    /// Go `ASTArgsFromImportPlan`.
    pub fn from_import_plan(plan: &tidb_planner::common_plans::ImportInto) -> Self {
        Self {
            columns_and_user_vars: plan.columns_and_user_vars.clone(),
            column_assignments: plan.column_assignments.clone(),
        }
    }
}

/// Go `FieldMapping`.
#[derive(Clone, Debug)]
pub struct FieldMapping {
    pub column: Option<tidb_model::GoShared<tidb_model::ColumnInfo>>,
    pub user_var: Option<String>,
}

/// Go `LoadDataController`'s table and input-column interpretation.
#[derive(Clone, Debug)]
pub struct LoadDataController {
    pub plan: Plan,
    pub ast_args: ASTArgs,
    pub table: tidb_table::tables::TableCommon,
    pub field_mappings: Vec<FieldMapping>,
    pub insert_columns: Vec<tidb_model::GoShared<tidb_model::ColumnInfo>>,
    pub execute_nodes_cnt: usize,
}

impl LoadDataController {
    /// Go `NewLoadDataController`.
    pub fn new(
        plan: Plan,
        table: tidb_table::tables::TableCommon,
        ast_args: ASTArgs,
    ) -> Result<Self, DriverError> {
        let mut controller = Self {
            plan,
            table,
            ast_args,
            field_mappings: Vec::new(),
            insert_columns: Vec::new(),
            execute_nodes_cnt: 1,
        };
        controller.check_field_params()?;
        let names = controller.init_field_mappings();
        controller.init_load_columns(names)?;
        Ok(controller)
    }

    /// Go `LoadDataController.checkFieldParams`.
    pub fn check_field_params(&self) -> Result<(), DriverError> {
        let p = &self.plan;
        if p.data_source_type == "file" && p.path.is_empty() {
            return Err(DriverError::from(
                tidb_exeerrors::ERR_LOAD_DATA_EMPTY_PATH.gen_with_stack_by_args(&[]),
            ));
        }
        if p.in_import_into {
            if !matches!(p.format.as_str(), "csv" | "parquet" | "sql" | "auto") {
                return Err(DriverError::from(
                    tidb_exeerrors::ERR_LOAD_DATA_UNSUPPORTED_FORMAT
                        .gen_with_stack_by_args(&[p.format.as_str().into()]),
                ));
            }
        } else {
            if p.null_value_opt_enclosed && p.fields_enclosed_by.is_empty() {
                return Err(wrong_format("must specify FIELDS [OPTIONALLY] ENCLOSED BY when use NULL DEFINED BY OPTIONALLY ENCLOSED"));
            }
            if p.lines_terminated_by.is_empty() {
                return Err(wrong_format("LINES TERMINATED BY is empty"));
            }
            if p.fields_terminated_by.is_empty() {
                return Err(wrong_format("load data with empty field terminator"));
            }
        }
        if !p.fields_enclosed_by.is_empty()
            && (p.fields_enclosed_by.starts_with(&p.fields_terminated_by)
                || p.fields_terminated_by.starts_with(&p.fields_enclosed_by))
        {
            return Err(wrong_format(
                "FIELDS ENCLOSED BY and TERMINATED BY must not be prefix of each other",
            ));
        }
        Ok(())
    }

    /// Go `LoadDataController.initFieldMappings` / `buildFieldMappings`.
    fn init_field_mappings(&mut self) -> Vec<String> {
        let visible = self.table.visible_columns();
        let mut names = Vec::new();
        if self.ast_args.columns_and_user_vars.is_empty() {
            for (_, column) in visible {
                names.push(column.read().name.original().to_owned());
                self.field_mappings.push(FieldMapping {
                    column: Some(column.info.clone()),
                    user_var: None,
                });
            }
        } else {
            for field in &self.ast_args.columns_and_user_vars {
                let mapping = match field {
                    tidb_ast::ColumnOrUserVar::Column(name) => {
                        names.push(name.clone());
                        FieldMapping {
                            column: visible
                                .iter()
                                .find(|(_, column)| {
                                    column.read().name.lowercase() == tidb_hack::go_to_lower(name)
                                })
                                .map(|(_, column)| column.info.clone()),
                            user_var: None,
                        }
                    }
                    tidb_ast::ColumnOrUserVar::UserVar(name) => FieldMapping {
                        column: None,
                        user_var: Some(name.clone()),
                    },
                };
                self.field_mappings.push(mapping);
            }
        }
        names
    }

    /// Go `LoadDataController.initLoadColumns` / `buildInsertColumns`.
    fn init_load_columns(&mut self, mut names: Vec<String>) -> Result<(), DriverError> {
        let visible = self.table.visible_columns();
        if names.len() != visible.len() {
            names.extend(
                self.ast_args
                    .column_assignments
                    .iter()
                    .map(|assignment| assignment.col.last().cloned().unwrap_or_default()),
            );
        }
        let cols = tidb_model::GoSharedPointerSlice::from_handles(
            visible
                .iter()
                .map(|(_, col)| Some(col.info.clone()))
                .collect(),
        );
        let name_refs: Vec<_> = names.iter().map(String::as_str).collect();
        let (found, missing) =
            tidb_model::column::find_cols(&cols, &name_refs, self.table.meta().read().pk_is_handle);
        if !missing.is_empty() {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_UNKNOWN_COLUMN
                    .gen_with_stack_by_args(&[missing.into(), "field list".into()]),
            ));
        }
        let found = found.expect("FindCols has no missing column");
        if found.len() != names.len() {
            return Err(DriverError::from(
                tidb_exeerrors::ERR_COLUMNS_NOT_MATCHED.gen_with_stack_by_args(&[]),
            ));
        }
        tidb_model::column::check_once(&found).map_err(|error| {
            DriverError::from(
                tidb_table::table_errors::ERR_DUPLICATE_COLUMN.gen_with_stack(error.to_string()),
            )
        })?;
        // FindCols preserves names' order; Go's reorderColumnsByNames keeps
        // that order once CheckOnce has established there are no duplicates.
        self.insert_columns = found.iter_deref().collect();
        Ok(())
    }

    /// Go `LoadDataController.GetFieldCount`.
    pub fn get_field_count(&self) -> usize {
        self.field_mappings.len()
    }
    /// Go `LoadDataController.GenerateCSVConfig`.
    pub fn generate_csv_config(&self) -> CsvConfig {
        self.plan.generate_csv_config()
    }
}

/// Go `ErrLoadDataWrongFormatConfig.GenWithStackByArgs` (empty format string).
fn wrong_format(detail: &str) -> DriverError {
    DriverError::from(
        tidb_exeerrors::ERR_LOAD_DATA_WRONG_FORMAT_CONFIG
            .gen_with_stack(format!("%!(EXTRA string={detail})")),
    )
}

/// Go `HandleSkipNRows`: CSV skips physical lines; other parsers skip rows.
pub fn handle_skip_n_rows(
    parser: &mut dyn tidb_lightning_mydump::Parser,
    ignore_lines: u64,
) -> Result<(), tidb_lightning_mydump::CsvError> {
    for _ in 0..ignore_lines {
        let result = if let Some(csv) = parser
            .as_any_mut()
            .downcast_mut::<tidb_lightning_mydump::CsvParser>()
        {
            csv.read_until_terminator()
        } else {
            parser.read_row()
        };
        match result {
            Ok(()) => {}
            Err(tidb_lightning_mydump::CsvError::Eof) => return Ok(()),
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
