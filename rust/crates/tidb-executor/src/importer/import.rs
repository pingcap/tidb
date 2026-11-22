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
    /// Go `QuerySource`: the captured query of an IMPORT FROM SELECT that
    /// runs on workers.
    pub query_source: Option<super::query_source::QuerySource>,
    /// Go `TableInfo`.
    pub table_info: Option<tidb_model::TableInfo>,
    /// Go `DesiredTableInfo`.
    pub desired_table_info: Option<tidb_model::TableInfo>,
    /// Go `LineFieldsInfo.FieldsOptEnclosed`.
    pub fields_opt_enclosed: bool,
    /// Go `Parameters` (not persisted with the plan).
    pub parameters: Option<super::job::ImportParameters>,
    /// Go `DBName`.
    pub db_name: String,
    /// Go `DBID`.
    pub db_id: i64,
    /// Go `SQLMode`.
    pub sql_mode: i64,
    /// Go `ImportantSysVars`.
    pub important_sys_vars: std::collections::BTreeMap<String, String>,
    /// Go `LocationID`.
    pub location_id: String,
    /// Go `DistSQLScanConcurrency`.
    pub distsql_scan_concurrency: usize,
    /// Go `User`.
    pub user: String,
    /// Go `Keyspace`.
    pub keyspace: String,
    /// Go `TotalFileSize`.
    pub total_file_size: i64,
    /// Go `IsRaftKV2`.
    pub is_raft_kv2: bool,
    /// Go `MaxNodeCnt`.
    pub max_node_cnt: usize,
    /// Go `Restrictive`.
    pub restrictive: bool,
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

impl std::fmt::Debug for LoadDataController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadDataController")
            .field("plan", &self.plan)
            .field("field_mappings", &self.field_mappings)
            .field("insert_columns", &self.insert_columns)
            .finish_non_exhaustive()
    }
}

/// Go `tableVisCols2FieldMappings`: every visible column, read in order.
#[must_use]
pub fn table_vis_cols_2_field_mappings(table: &tidb_table::tables::TableCommon) -> (Vec<FieldMapping>, Vec<String>) {
    let mut mappings = Vec::new();
    let mut names = Vec::new();
    for (_, column) in table.visible_columns() {
        names.push(column.read().name.original().to_owned());
        mappings.push(FieldMapping { column: Some(column.info.clone()), user_var: None });
    }
    (mappings, names)
}

/// Go `LoadDataController`'s table and input-column interpretation.
#[derive(Clone)]
pub struct LoadDataController {
    pub plan: Plan,
    pub ast_args: ASTArgs,
    pub table: tidb_table::tables::TableCommon,
    pub field_mappings: Vec<FieldMapping>,
    pub insert_columns: Vec<tidb_model::GoShared<tidb_model::ColumnInfo>>,
    pub execute_nodes_cnt: usize,
    /// Go `dataStore`.
    pub data_store: Option<std::sync::Arc<dyn tidb_objstore::storeapi::Storage>>,
    /// Go `dataFiles`.
    pub data_files: Vec<tidb_lightning_mydump::SourceFileMeta>,
    /// Go `TotalRealSize`.
    pub total_real_size: i64,
    /// Go `globalSortStore`.
    pub global_sort_store: Option<std::sync::Arc<dyn tidb_objstore::storeapi::Storage>>,
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
            data_store: None,
            data_files: Vec::new(),
            total_real_size: 0,
            global_sort_store: None,
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
            csv.read_until_terminator().map(|_| ())
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

/// Go `newLoadDataParser`: the format's parser over the chunk's file.
pub fn new_load_data_parser(
    ctx: &tidb_gocontext::Context,
    plan: &Plan,
    csv_config: &CsvConfig,
    data_store: &std::sync::Arc<dyn tidb_objstore::storeapi::Storage>,
    file_meta: &tidb_lightning_mydump::SourceFileMeta,
) -> Result<Box<dyn tidb_lightning_mydump::Parser + Send>, String> {
    let wrong_format = |e: String| {
        tidb_terror::MysqlError::from_terror(&tidb_exeerrors::ERR_LOAD_DATA_WRONG_FORMAT_CONFIG.gen_with_stack(e))
            .error_string()
    };
    let decompress = tidb_objstore::compressedio::DecompressConfig { zstd_decode_concurrency: 1 };
    let open = || tidb_lightning_mydump::open_reader(ctx, file_meta, data_store, decompress);
    match plan.format.as_str() {
        "delimited data" | "csv" => {
            let reader = open()?;
            let convertor = match &plan.charset {
                Some(charset) => Some(tidb_lightning_mydump::CharsetConvertor::new(charset, "\u{FFFD}")?),
                None => None,
            };
            tidb_lightning_mydump::CsvParser::new(
                csv_config,
                Box::new(tidb_lightning_mydump::ParserReader(reader)),
                tidb_lightning_mydump::READ_BLOCK_SIZE,
                false,
                convertor,
            )
            .map(|p| Box::new(p) as Box<dyn tidb_lightning_mydump::Parser + Send>)
            .map_err(|e| wrong_format(e.to_string()))
        }
        "sql" => {
            let reader = open()?;
            let no_backslash = tidb_mysql::SqlMode(plan.sql_mode).has_no_backslash_escapes_mode();
            Ok(Box::new(tidb_lightning_mydump::ChunkParser::from_reader(
                Box::new(tidb_lightning_mydump::ParserReader(reader)),
                no_backslash,
                tidb_lightning_mydump::READ_BLOCK_SIZE,
            )))
        }
        "parquet" => tidb_lightning_mydump::ParquetParser::new(
            ctx,
            data_store.clone(),
            &file_meta.path,
            file_meta.file_size,
            tidb_dumpformat_parquetfile::FileMeta { loc: None },
        )
        .map(|p| Box::new(p) as Box<dyn tidb_lightning_mydump::Parser + Send>)
        .map_err(wrong_format),
        other => Err(tidb_terror::MysqlError::from_terror(
            &tidb_exeerrors::ERR_LOAD_DATA_UNSUPPORTED_FORMAT.gen_with_stack_by_args(&[other.into()]),
        )
        .error_string()),
    }
}

/// Go `GetImportantSysVars`: each important variable's session value, or
/// its default when the session has none.
pub fn get_important_sys_vars(
    get: impl Fn(&str) -> Option<String>,
) -> std::collections::BTreeMap<String, String> {
    tidb_lightning_common::DEFAULT_IMPORTANT_VARIABLES
        .iter()
        .chain(tidb_lightning_common::DEFAULT_IMPORT_VARIABLES_TIDB)
        .map(|(name, default)| ((*name).to_owned(), get(name).unwrap_or_else(|| (*default).to_owned())))
        .collect()
}

/// What Go `NewImportPlanFromInput` reads from the user's session and store.
#[derive(Clone, Debug, Default)]
pub struct ImportSessionInfo {
    /// Go `SQLMode`.
    pub sql_mode: i64,
    /// Go `GetImportantSysVars(userSctx)`.
    pub important_sys_vars: std::collections::BTreeMap<String, String>,
    /// Go `timeutil.ZoneName(vars.Location())`.
    pub location_id: String,
    /// Go `DistSQLScanConcurrency()`.
    pub distsql_scan_concurrency: usize,
    /// Go `User.String()`.
    pub user: String,
    /// Go `GetStore().GetKeyspace()`.
    pub keyspace: String,
    /// Go `collate.NewCollationEnabled()`.
    pub new_collation: bool,
    /// Go `tidb_max_dist_task_nodes`, when the session has it.
    pub max_dist_task_nodes: Option<String>,
    /// Go `CalcMaxNodeCountByStoresNum`, for `tidb_max_dist_task_nodes = -1`.
    pub max_node_count_by_stores: usize,
}

/// Go `variable.TidbOptInt`.
fn tidb_opt_int(value: &str, default: i64) -> i64 {
    value.parse().unwrap_or(default)
}

/// The value text Go's `initParameters` keeps: `fmt.Sprintf("%v", value)`.
fn option_value_text(option: &tidb_ast::LoadDataOption) -> Option<String> {
    option.value.as_ref()?;
    // Validated options hold a string or an integer constant.
    let text = import_into_options::as_string(option)
        .or_else(|_| import_into_options::as_int(option).map(|v| v.to_string()))
        .unwrap_or_default();
    Some(if option.name == "cloud_storage_uri" { tidb_ast::redact_url(&text) } else { text })
}

impl Plan {
    /// Go `NewImportPlanFromInput` over the statement's validated options:
    /// the session snapshot, then `initOptions` and `initParameters`.
    #[allow(clippy::too_many_arguments)]
    pub fn new_import_plan_from_input(
        stmt: &ImportIntoStmt,
        db_name: &str,
        db_id: i64,
        table_info: tidb_model::TableInfo,
        session: &ImportSessionInfo,
        target_node_cpu_cnt: usize,
        cloud_storage_uri: &str,
    ) -> Result<Self, DriverError> {
        let (path, format, data_source_type) = match &stmt.source {
            ImportSource::File { path, format } => {
                (path.clone(), format.as_deref().unwrap_or("auto").to_ascii_lowercase(), "file")
            }
            ImportSource::Select { .. } => (String::new(), "auto".to_owned(), "query"),
        };
        let mut plan = Self {
            table_info: Some(table_info.clone()),
            desired_table_info: Some(table_info),
            db_name: db_name.to_owned(),
            db_id,
            path,
            format,
            restrictive: tidb_mysql::SqlMode(session.sql_mode).has_strict_mode(),
            field_null_def: vec!["\\N".to_owned()],
            fields_terminated_by: ",".to_owned(),
            fields_enclosed_by: "\"".to_owned(),
            fields_escaped_by: "\\".to_owned(),
            location_id: session.location_id.clone(),
            sql_mode: session.sql_mode,
            important_sys_vars: session.important_sys_vars.clone(),
            distsql_scan_concurrency: session.distsql_scan_concurrency,
            in_import_into: true,
            data_source_type: data_source_type.to_owned(),
            user: session.user.clone(),
            keyspace: session.keyspace.clone(),
            ..Self::default()
        };
        plan.set_use_new_collate(session.new_collation);
        plan.init_options(stmt, target_node_cpu_cnt, cloud_storage_uri)?;
        if !tidb_config_kerneltype::is_next_gen() {
            if let Some(value) = &session.max_dist_task_nodes {
                let n = tidb_opt_int(value, 0);
                plan.max_node_cnt = if n == -1 {
                    session.max_node_count_by_stores
                } else {
                    usize::try_from(n).unwrap_or(0)
                };
            }
        }
        plan.init_parameters(stmt);
        Ok(plan)
    }

    /// Go `Plan.initParameters`: the job's displayed parameters, with the
    /// file location and the cloud storage URI redacted.
    pub fn init_parameters(&mut self, stmt: &ImportIntoStmt) {
        let columns_and_vars = if stmt.columns_and_user_vars.is_empty() {
            String::new()
        } else {
            let items: Vec<String> = stmt
                .columns_and_user_vars
                .iter()
                .map(|item| match item {
                    tidb_ast::ColumnOrUserVar::Column(name) => tidb_lightning_common::escape_identifier(name),
                    tidb_ast::ColumnOrUserVar::UserVar(name) => {
                        format!("@{}", tidb_lightning_common::escape_identifier(name))
                    }
                })
                .collect();
            format!("({})", items.join(", "))
        };
        let set_clause = stmt
            .column_assignments
            .iter()
            .map(tidb_ast::Assignment::restore)
            .collect::<Vec<_>>()
            .join(", ");
        let options = stmt
            .options
            .iter()
            .map(|option| {
                let value = option_value_text(option).map_or(serde_json::Value::Null, serde_json::Value::String);
                (option.name.clone(), value)
            })
            .collect();
        self.parameters = Some(super::job::ImportParameters {
            columns_and_vars,
            set_clause,
            file_location: tidb_ast::redact_url(&self.path),
            format: self.format.clone(),
            options,
        });
    }

    /// Go `json.Marshal(plan)`: the plan as DXF task metadata persists it.
    /// `Parameters` and `User` are `json:"-"` in Go.
    #[must_use]
    pub fn to_go_json(&self) -> serde_json::Value {
        use serde_json::{json, Map, Value};
        let table = |t: &Option<tidb_model::TableInfo>| {
            t.as_ref().map_or(Value::Null, |t| serde_json::to_value(t).unwrap_or(Value::Null))
        };
        let mut m = Map::new();
        if let Some(q) = &self.query_source {
            m.insert("QuerySource".into(), serde_json::to_value(q).unwrap_or(Value::Null));
        }
        let fields = json!({
            "DBName": self.db_name, "DBID": self.db_id,
            "TableInfo": table(&self.table_info), "DesiredTableInfo": table(&self.desired_table_info),
            "Path": self.path, "Format": self.format, "Restrictive": self.restrictive,
            "LocationID": self.location_id, "SQLMode": self.sql_mode, "Charset": self.charset,
            "ImportantSysVars": self.important_sys_vars, "FieldNullDef": self.field_null_def,
            "NullValueOptEnclosed": self.null_value_opt_enclosed,
            "FieldsTerminatedBy": self.fields_terminated_by, "FieldsEnclosedBy": self.fields_enclosed_by,
            "FieldsEscapedBy": self.fields_escaped_by, "FieldsOptEnclosed": self.fields_opt_enclosed,
            "LinesStartingBy": self.lines_starting_by, "LinesTerminatedBy": self.lines_terminated_by,
            "IgnoreLines": self.ignore_lines, "DiskQuota": self.disk_quota,
            "Checksum": self.checksum.as_str(), "ThreadCnt": self.thread_cnt, "MaxNodeCnt": self.max_node_cnt,
            "MaxWriteSpeed": self.max_write_speed, "SplitFile": self.split_file,
            "MaxRecordedErrors": self.max_recorded_errors, "OnDupKey": self.on_dup_key,
            "Detached": self.detached, "DisableTiKVImportMode": self.disable_tikv_import_mode,
            "MaxEngineSize": self.max_engine_size, "CloudStorageURI": self.cloud_storage_uri,
            "DisablePrecheck": self.disable_precheck, "GroupKey": self.group_key,
            "DistSQLScanConcurrency": self.distsql_scan_concurrency, "InImportInto": self.in_import_into,
            "DataSourceType": self.data_source_type,
        });
        if let Value::Object(fields) = fields {
            m.extend(fields);
        }
        if !self.specified_option_names.is_empty() {
            let names: Map<String, Value> =
                self.specified_option_names.iter().map(|n| (n.clone(), json!({}))).collect();
            m.insert("SpecifiedOptionNames".into(), Value::Object(names));
        }
        for (k, v) in [
            ("IsRaftKV2", json!(self.is_raft_kv2)),
            ("TotalFileSize", json!(self.total_file_size)),
            ("ForceMergeStep", json!(self.force_merge_step)),
            ("ManualRecovery", json!(self.manual_recovery)),
            ("Keyspace", json!(self.keyspace)),
        ] {
            m.insert(k.into(), v);
        }
        if let Some(v) = self.use_new_collate {
            m.insert("use_new_collate".into(), json!(v));
        }
        Value::Object(m)
    }

    /// Go `json.Unmarshal` into a `Plan`: missing fields keep Go's zero values.
    pub fn from_go_json(v: &serde_json::Value) -> Result<Self, String> {
        use serde_json::Value;
        let s = |k: &str| v.get(k).and_then(Value::as_str).unwrap_or_default().to_owned();
        let i = |k: &str| v.get(k).and_then(Value::as_i64).unwrap_or_default();
        let u = |k: &str| v.get(k).and_then(Value::as_u64).unwrap_or_default();
        let b = |k: &str| v.get(k).and_then(Value::as_bool).unwrap_or_default();
        let table = |k: &str| -> Result<Option<tidb_model::TableInfo>, String> {
            match v.get(k) {
                None | Some(Value::Null) => Ok(None),
                Some(t) => tidb_meta::value::parse_table_info(t.to_string().as_bytes(), 0)
                    .map(Some)
                    .map_err(|e| e.to_string()),
            }
        };
        let checksum = match v.get("Checksum").and_then(Value::as_str) {
            None => PostOpLevel::Off,
            Some(text) => PostOpLevel::from_string_value(text)?,
        };
        Ok(Self {
            query_source: match v.get("QuerySource").filter(|q| !q.is_null()) {
                Some(q) => Some(serde_json::from_value(q.clone()).map_err(|e| e.to_string())?),
                None => None,
            },
            table_info: table("TableInfo")?,
            desired_table_info: table("DesiredTableInfo")?,
            db_name: s("DBName"),
            db_id: i("DBID"),
            path: s("Path"),
            format: s("Format"),
            restrictive: b("Restrictive"),
            location_id: s("LocationID"),
            sql_mode: i("SQLMode"),
            charset: v.get("Charset").and_then(Value::as_str).map(str::to_owned),
            important_sys_vars: v
                .get("ImportantSysVars")
                .and_then(Value::as_object)
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_owned())).collect())
                .unwrap_or_default(),
            field_null_def: v
                .get("FieldNullDef")
                .and_then(Value::as_array)
                .map(|a| a.iter().map(|x| x.as_str().unwrap_or_default().to_owned()).collect())
                .unwrap_or_default(),
            null_value_opt_enclosed: b("NullValueOptEnclosed"),
            fields_terminated_by: s("FieldsTerminatedBy"),
            fields_enclosed_by: s("FieldsEnclosedBy"),
            fields_escaped_by: s("FieldsEscapedBy"),
            fields_opt_enclosed: b("FieldsOptEnclosed"),
            lines_starting_by: s("LinesStartingBy"),
            lines_terminated_by: s("LinesTerminatedBy"),
            ignore_lines: u("IgnoreLines"),
            disk_quota: i("DiskQuota"),
            checksum,
            thread_cnt: usize::try_from(i("ThreadCnt")).unwrap_or(0),
            max_node_cnt: usize::try_from(i("MaxNodeCnt")).unwrap_or(0),
            max_write_speed: i("MaxWriteSpeed"),
            split_file: b("SplitFile"),
            max_recorded_errors: i("MaxRecordedErrors"),
            on_dup_key: s("OnDupKey"),
            detached: b("Detached"),
            disable_tikv_import_mode: b("DisableTiKVImportMode"),
            max_engine_size: i("MaxEngineSize"),
            cloud_storage_uri: s("CloudStorageURI"),
            disable_precheck: b("DisablePrecheck"),
            group_key: s("GroupKey"),
            distsql_scan_concurrency: usize::try_from(i("DistSQLScanConcurrency")).unwrap_or(0),
            in_import_into: b("InImportInto"),
            data_source_type: s("DataSourceType"),
            specified_option_names: v
                .get("SpecifiedOptionNames")
                .and_then(Value::as_object)
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default(),
            is_raft_kv2: b("IsRaftKV2"),
            total_file_size: i("TotalFileSize"),
            force_merge_step: b("ForceMergeStep"),
            manual_recovery: b("ManualRecovery"),
            keyspace: s("Keyspace"),
            use_new_collate: v.get("use_new_collate").and_then(Value::as_bool),
            ..Self::default()
        })
    }

    /// Go `Plan.CheckNonCSVFormatOptions`.
    pub fn check_non_csv_format_options(&self) -> Result<(), DriverError> {
        for name in CSV_ONLY_OPTIONS {
            if self.specified_option_names.contains(*name) {
                return Err(DriverError::from(
                    tidb_exeerrors::ERR_LOAD_DATA_UNSUPPORTED_OPTION
                        .fast_gen_by_args(&[(*name).into(), "non-CSV format".into()]),
                ));
            }
        }
        Ok(())
    }
}

/// Go `csvOnlyOptions`.
const CSV_ONLY_OPTIONS: &[&str] = &[
    "character_set",
    "fields_terminated_by",
    "fields_enclosed_by",
    "fields_escaped_by",
    "fields_defined_null_by",
    "lines_terminated_by",
    "skip_rows",
    "split_file",
];

impl PostOpLevel {
    /// Go `PostOpLevel.String`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Optional => "optional",
            Self::Required => "required",
        }
    }

    /// Go `PostOpLevel.FromStringValue`.
    pub fn from_string_value(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "off" | "false" => Ok(Self::Off),
            "required" | "true" => Ok(Self::Required),
            "optional" => Ok(Self::Optional),
            _ => Err(format!(
                "invalid op level '{s}', please choose valid option between ['off', 'optional', 'required']"
            )),
        }
    }
}

/// Go `isSupportedCloudStorageBackend`: S3, GCS or Azure Blob.
#[must_use]
pub fn is_supported_cloud_storage_backend(backend: &tidb_proto::backup::StorageBackend) -> bool {
    use tidb_proto::backup::storage_backend::Backend;
    matches!(backend.backend, Some(Backend::S3(_) | Backend::Gcs(_) | Backend::AzureBlobStorage(_)))
}

/// Go `importdef.ImportIntoDataSource`.
const IMPORT_INTO_DATA_SOURCE: &str = "data source";

fn invalid_uri(target: &str, reason: &str) -> DriverError {
    DriverError::from(
        tidb_exeerrors::ERR_LOAD_DATA_INVALID_URI.gen_with_stack_by_args(&[target.into(), reason.into()]),
    )
}

fn cant_read(reason: &str, hint: &str) -> DriverError {
    DriverError::from(tidb_exeerrors::ERR_LOAD_DATA_CANT_READ.gen_with_stack_by_args(&[reason.into(), hint.into()]))
}

/// A Go error without a code (`errors.New`, `errors.Trace`): 1105.
pub(crate) fn plain(message: impl Into<String>) -> DriverError {
    DriverError::from(tidb_terror::MysqlError::unknown(message.into()))
}

/// Go `supportedSuffixForServerDisk`.
const SUPPORTED_SUFFIX_FOR_SERVER_DISK: &[&str] =
    &[".csv", ".sql", ".parquet", ".gz", ".gzip", ".zstd", ".zst", ".snappy"];

/// Go `filepath.Ext`.
fn filepath_ext(path: &str) -> &str {
    let base_start = path.rfind('/').map_or(0, |i| i + 1);
    path[base_start..].rfind('.').map_or("", |i| &path[base_start + i..])
}

/// Go `filepath.Dir` and `filepath.Base` of a server-disk path.
fn split_dir_base(path: &str) -> (String, String) {
    let p = std::path::Path::new(path);
    let dir = tidb_objstore::gopath::dir(path);
    let base = p.file_name().map_or_else(|| ".".to_owned(), |b| b.to_string_lossy().into_owned());
    (dir, base)
}

/// Go `initExternalStore`.
fn init_external_store(
    ctx: &tidb_gocontext::Context,
    u: &mut tidb_go_url::URL,
    target: &str,
) -> Result<std::sync::Arc<dyn tidb_objstore::storeapi::Storage>, DriverError> {
    let backend = tidb_objstore::parse_backend_from_url(u, None).map_err(|e| invalid_uri(target, &e))?;
    tidb_objstore::new_with_default_opt(ctx, &backend).map_err(|e| {
        DriverError::from(
            tidb_exeerrors::ERR_LOAD_DATA_CANT_ACCESS.gen_with_stack_by_args(&[target.into(), e.to_string().into()]),
        )
    })
}

/// Go `GetSortStore`.
pub fn get_sort_store(
    ctx: &tidb_gocontext::Context,
    url: &str,
) -> Result<std::sync::Arc<dyn tidb_objstore::storeapi::Storage>, DriverError> {
    let mut u = tidb_objstore::parse_raw_url(url).map_err(|e| invalid_uri("cloud storage", &e))?;
    init_external_store(ctx, &mut u, "cloud storage")
}

/// Go `checkDataSourceGlob`.
fn check_data_source_glob(file_name_key: &str) -> Result<(), DriverError> {
    let escaped = tidb_stringutil::escape_glob_question_mark(file_name_key.as_bytes());
    super::file_match::filepath_match(escaped.as_bytes(), b"")
        .map(|_| ())
        .map_err(|e| invalid_uri(IMPORT_INTO_DATA_SOURCE, &format!("Glob pattern error: {e}")))
}

/// Go `estimateFormatSizeExpansionRatio`: Parquet's decoded size over its
/// file size, sampled from the first row group.
fn estimate_format_size_expansion_ratio(
    ctx: &tidb_gocontext::Context,
    file_path: &str,
    file_size: i64,
    tp: tidb_lightning_mydump::SourceType,
    store: &std::sync::Arc<dyn tidb_objstore::storeapi::Storage>,
) -> Result<f64, DriverError> {
    if tp != tidb_lightning_mydump::SourceType::Parquet {
        return Ok(1.0);
    }
    let (rows, row_size) = tidb_dumpformat_parquetfile::sample_statistics_from_parquet(ctx, file_path, store.clone())
        .map_err(plain)?;
    if row_size == 0.0 || rows == 0 {
        return Ok(2.0);
    }
    Ok(((row_size * rows as f64) / file_size as f64).max(1.0))
}

/// Go `maxSampledCompressedFiles`.
const MAX_SAMPLED_COMPRESSED_FILES: usize = 512;

/// Go `getHarmonicMean`.
fn get_harmonic_mean(rs: &[f64]) -> f64 {
    let positive: Vec<f64> = rs.iter().copied().filter(|r| *r > 0.0).collect();
    if positive.is_empty() {
        return 1.0;
    }
    positive.len() as f64 / positive.iter().map(|r| 1.0 / r).sum::<f64>()
}

/// Go `compressionEstimator`: once 512 files of one compression type are
/// sampled, their harmonic mean stands for the rest.
#[derive(Default)]
struct CompressionEstimator {
    records: std::sync::Mutex<std::collections::BTreeMap<tidb_lightning_mydump::Compression, Vec<f64>>>,
    ratio: std::sync::RwLock<std::collections::BTreeMap<tidb_lightning_mydump::Compression, f64>>,
}

impl CompressionEstimator {
    /// Go `estimate`.
    fn estimate(
        &self,
        ctx: &tidb_gocontext::Context,
        file_meta: &tidb_lightning_mydump::SourceFileMeta,
        store: &dyn tidb_objstore::storeapi::Storage,
    ) -> f64 {
        let tp = tidb_lightning_mydump::parse_compression_on_file_extension(&file_meta.path);
        if tp == tidb_lightning_mydump::Compression::None {
            return 1.0;
        }
        if let Some(v) = self.ratio.read().unwrap_or_else(std::sync::PoisonError::into_inner).get(&tp) {
            return *v;
        }
        let Ok(mut ratio) = tidb_lightning_mydump::sample_file_compress_ratio(ctx, file_meta, store) else {
            return 1.0;
        };
        let mut records = self.records.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut ratios = self.ratio.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        if ratios.contains_key(&tp) {
            return ratio;
        }
        let rs = records.entry(tp).or_default();
        if rs.len() < MAX_SAMPLED_COMPRESSED_FILES {
            rs.push(ratio);
        }
        if rs.len() >= MAX_SAMPLED_COMPRESSED_FILES {
            ratio = get_harmonic_mean(rs);
            ratios.insert(tp, ratio);
        }
        ratio
    }
}

type StoreArc = std::sync::Arc<dyn tidb_objstore::storeapi::Storage>;

impl LoadDataController {
    /// Go `InitDataStore`: the global sort store, then the data source's.
    pub fn init_data_store(&mut self, ctx: &tidb_gocontext::Context) -> Result<(), DriverError> {
        if self.plan.is_global_sort() {
            self.global_sort_store = Some(get_sort_store(ctx, &self.plan.cloud_storage_uri)?);
        }
        if self.plan.path.is_empty() {
            return Ok(());
        }
        let result = self
            .parse_data_source_path()
            .and_then(|(mut u, _)| init_external_store(ctx, &mut u, IMPORT_INTO_DATA_SOURCE));
        match result {
            Ok(store) => {
                self.data_store = Some(store);
                Ok(())
            }
            Err(e) => {
                if let Some(store) = self.global_sort_store.take() {
                    store.close();
                }
                Err(e)
            }
        }
    }

    /// Go `parseDataSourcePath`: the store's URL and the key of the file
    /// (or glob) in it.
    fn parse_data_source_path(&self) -> Result<(tidb_go_url::URL, String), DriverError> {
        let mut u = tidb_objstore::parse_raw_url(&self.plan.path)
            .map_err(|e| invalid_uri(IMPORT_INTO_DATA_SOURCE, &e))?;
        let file_name_key = if tidb_objstore::is_local(&u) {
            let (dir, base) = split_dir_base(&self.plan.path);
            u.path = dir.into_bytes();
            base
        } else {
            let key = String::from_utf8_lossy(&u.path).trim_matches('/').to_owned();
            u.path.clear();
            key
        };
        Ok((u, file_name_key))
    }

    /// Go `CheckDataSourceAccess`: one matched file can be opened.
    pub fn check_data_source_access(&self, ctx: &tidb_gocontext::Context) -> Result<(), DriverError> {
        let (mut u, file_name_key) = self.parse_data_source_path()?;
        check_data_source_glob(&file_name_key)?;
        let store = init_external_store(ctx, &mut u, IMPORT_INTO_DATA_SOURCE)?;
        let result = match file_name_key.find(['*', '[']) {
            None => match store.open(ctx, &file_name_key, None) {
                Ok(mut reader) => {
                    let _ = reader.close();
                    Ok(())
                }
                Err(e) => Err(cant_read(&e.to_string(), "Please check the file location is correct")),
            },
            Some(idx) => {
                let prefix = if tidb_objstore::is_local(&u) { String::new() } else { file_name_key[..idx].to_owned() };
                let opt = tidb_objstore::storeapi::WalkOption { obj_prefix: prefix, skip_sub_dir: true, ..Default::default() };
                let mut first: Option<String> = None;
                let walked = store.walk_dir(ctx, Some(&opt), &mut |path, _| {
                    if first.is_none() {
                        first = Some(path.to_owned());
                    }
                    Ok(())
                });
                walked
                    .and_then(|()| first.map_or(Ok(()), |p| store.open(ctx, &p, None).map(|mut r| { let _ = r.close(); })))
                    .map_err(|e| cant_read(&e.to_string(), "failed to access data source"))
            }
        };
        store.close();
        result
    }

    /// Go `InitDataFiles`: validates a server-disk path, opens the data
    /// store and lists the matched files with their estimated real sizes.
    pub fn init_data_files(&mut self, ctx: &tidb_gocontext::Context) -> Result<(), DriverError> {
        let (u, file_name_key) = self.parse_data_source_path()?;
        if tidb_objstore::is_local(&u) {
            if !self.plan.in_import_into {
                return Err(DriverError::from(
                    tidb_exeerrors::ERR_LOAD_DATA_FROM_SERVER_DISK.gen_with_stack_by_args(&[self.plan.path.as_str().into()]),
                ));
            }
            if !self.plan.path.starts_with('/') {
                return Err(invalid_uri(
                    IMPORT_INTO_DATA_SOURCE,
                    "file location should be absolute path when import from server disk",
                ));
            }
            let ext = filepath_ext(&self.plan.path).to_lowercase();
            if !SUPPORTED_SUFFIX_FOR_SERVER_DISK.contains(&ext.as_str()) {
                return Err(invalid_uri(
                    IMPORT_INTO_DATA_SOURCE,
                    "the file suffix is not supported when import from server disk",
                ));
            }
            let (dir, _) = split_dir_base(&self.plan.path);
            if let Err(e) = std::fs::metadata(&dir) {
                return Err(invalid_uri(IMPORT_INTO_DATA_SOURCE, &tidb_objstore::gopath::path_error("stat", &dir, &e).to_string()));
            }
        }
        check_data_source_glob(&file_name_key)?;
        self.init_data_store(ctx)?;
        let store: StoreArc = self.data_store.clone().expect("InitDataStore opened the data store");
        let location_meta = tidb_lightning_mydump::ParquetFileMeta::default();
        let is_auto_detecting = self.plan.format == "auto";
        let mut data_files = Vec::new();
        match file_name_key.find(['*', '[']) {
            None => {
                let mut reader = store.open(ctx, &file_name_key, None).map_err(|e| {
                    cant_read(&e.to_string(), "Please check the file location is correct")
                })?;
                let size = std::io::Seek::seek(&mut reader, std::io::SeekFrom::End(0));
                let _ = reader.close();
                let size = size.map_err(|e| cant_read(&e.to_string(), "failed to read file size by seek"))? as i64;
                self.detect_and_update_format(&file_name_key);
                let source_type = self.get_source_type();
                let ratio = estimate_format_size_expansion_ratio(ctx, &file_name_key, size, source_type, &store)?;
                let mut meta = tidb_lightning_mydump::SourceFileMeta {
                    path: String::from(&*file_name_key),
                    file_size: size,
                    compression: tidb_lightning_mydump::parse_compression_on_file_extension(&file_name_key),
                    r#type: source_type,
                    parquet_meta: location_meta.clone(),
                    ..Default::default()
                };
                meta.real_size = tidb_lightning_mydump::estimate_real_size_for_file(ctx, &meta, store.as_ref());
                meta.real_size = (meta.real_size as f64 * ratio) as i64;
                data_files.push(meta);
            }
            Some(idx) => {
                let prefix = if tidb_objstore::is_local(&u) { String::new() } else { file_name_key[..idx].to_owned() };
                let escaped = tidb_stringutil::escape_glob_question_mark(file_name_key.as_bytes());
                let mut all_files = Vec::new();
                let opt = tidb_objstore::storeapi::WalkOption { obj_prefix: prefix, skip_sub_dir: true, ..Default::default() };
                store
                    .walk_dir(ctx, Some(&opt), &mut |path, size| {
                        all_files.push(tidb_lightning_mydump::RawFile { path: path.to_owned(), size });
                        Ok(())
                    })
                    .map_err(|e| cant_read(&e.to_string(), "failed to walk dir"))?;
                // Go's `sync.Once` detects the format and its expansion ratio
                // from the first matched file.
                let matched: Vec<_> = all_files
                    .into_iter()
                    .filter(|f| super::file_match::filepath_match(escaped.as_bytes(), f.path.as_bytes()).unwrap_or(false))
                    .collect();
                let mut format_ratio = 1.0;
                let mut source_type = self.get_source_type();
                if let Some(first) = matched.first() {
                    self.detect_and_update_format(&first.path);
                    source_type = self.get_source_type();
                    format_ratio = estimate_format_size_expansion_ratio(ctx, &first.path, first.size, source_type, &store)?;
                }
                let estimator = CompressionEstimator::default();
                let processed = tidb_lightning_mydump::parallel_process(ctx, &matched, self.plan.thread_cnt * 2, |ctx, f| {
                    let mut meta = tidb_lightning_mydump::SourceFileMeta {
                        path: f.path.clone(),
                        file_size: f.size,
                        compression: tidb_lightning_mydump::parse_compression_on_file_extension(&f.path),
                        r#type: source_type,
                        parquet_meta: location_meta.clone(),
                        ..Default::default()
                    };
                    let compression_ratio = estimator.estimate(ctx, &meta, store.as_ref()).max(1.0);
                    meta.real_size = (compression_ratio * meta.file_size as f64) as i64;
                    meta.real_size = (meta.real_size as f64 * format_ratio) as i64;
                    Ok(meta)
                })
                .map_err(plain)?;
                data_files.extend(processed);
            }
        }
        if self.plan.in_import_into && is_auto_detecting && self.plan.format != "csv" {
            self.plan.check_non_csv_format_options()?;
        }
        self.plan.total_file_size = data_files.iter().map(|f| f.file_size).sum();
        self.total_real_size = data_files.iter().map(|f| f.real_size).sum();
        self.data_files = data_files;
        Ok(())
    }

    /// Go `CalResourceParams`: the thread count, max node count and DistSQL
    /// scan concurrency from the data size, the CPUs of a target node, the
    /// sampled index KV ratio and the schedule tune factors. A failed sample
    /// is only logged, leaving the ratio 0.
    ///
    /// # Errors
    /// The node CPUs or the tune factors could not be read.
    pub fn cal_resource_params(&mut self, ctx: &tidb_gocontext::Context, ks_codec: &[u8]) -> Result<(), DriverError> {
        use tidb_log::{Field, Value};
        let start = std::time::Instant::now();
        let dxf_err = |e: tidb_dxf_handle::HandleError| match e {
            tidb_dxf_handle::HandleError::Storage(tidb_dxf_storage::StorageError::Terror(t)) => DriverError::from(t),
            e => DriverError::from(tidb_terror::MysqlError::unknown(e.to_string())),
        };
        let target_node_cpu_cnt = tidb_dxf_handle::get_cpu_count_of_node(ctx).map_err(dxf_err)?;
        let factors = tidb_dxf_handle::get_schedule_tune_factors(ctx, &self.plan.keyspace).map_err(dxf_err)?;
        let total_size = self.total_real_size;
        let table_info = self.table.meta().read().clone();
        let num_of_index_gen_kv = super::kv_encode::get_num_of_index_gen_kv(&table_info);
        let mut index_size_ratio = 0.0;
        if num_of_index_gen_kv > 0 {
            match self.sample_index_size_ratio(ctx, ks_codec) {
                Ok(ratio) => index_size_ratio = ratio,
                Err(e) => tidb_logutil::bg_logger().warn(
                    "meet error when sampling index size ratio",
                    &[Field::new("error", Value::Str(e.to_mysql_error().to_string()))],
                ),
            }
        }
        let cal = tidb_dxf_scheduler::ResourceCalc::new(total_size, target_node_cpu_cnt, index_size_ratio, &factors);
        self.plan.thread_cnt = usize::try_from(cal.calc_required_slots()).unwrap_or(0);
        self.plan.max_node_cnt = usize::try_from(cal.calc_max_node_count_for_import_into()).unwrap_or(0);
        self.plan.distsql_scan_concurrency = usize::try_from(tidb_dxf_scheduler::calc_dist_sql_concurrency(
            self.plan.thread_cnt as isize,
            self.plan.max_node_cnt as isize,
            target_node_cpu_cnt,
        ))
        .unwrap_or(0);
        tidb_logutil::bg_logger().info(
            "auto calculate resource related params",
            &[
                Field::new("db", Value::Str(self.plan.db_name.clone())),
                Field::new("table", Value::Str(table_info.name.original().to_owned())),
                Field::new("tableID", Value::I64(table_info.id)),
                Field::new("thread", Value::I64(self.plan.thread_cnt as i64)),
                Field::new("maxNode", Value::I64(self.plan.max_node_cnt as i64)),
                Field::new("distsqlScanConcurrency", Value::I64(self.plan.distsql_scan_concurrency as i64)),
                Field::new("targetNodeCPU", Value::I64(target_node_cpu_cnt as i64)),
                Field::new("totalFileSize", Value::Str(tidb_config_configtypes::bytes_size(self.plan.total_file_size as f64))),
                Field::new("totalRealSize", Value::Str(tidb_config_configtypes::bytes_size(total_size as f64))),
                Field::new("fileCount", Value::I64(self.data_files.len() as i64)),
                Field::new("numOfIndexGenKV", Value::I64(num_of_index_gen_kv as i64)),
                Field::new("indexSizeRatio", Value::F64(index_size_ratio)),
                Field::new("amplifyFactor", Value::F64(factors.amplify_factor)),
                Field::new("costTime", Value::Duration(start.elapsed().as_nanos() as i64)),
            ],
        );
        Ok(())
    }

    /// Go `detectAndUpdateFormat`.
    fn detect_and_update_format(&mut self, path: &str) {
        if self.plan.format == "auto" {
            self.plan.format = parse_file_type(path).to_owned();
            if let Some(parameters) = &mut self.plan.parameters {
                parameters.format = self.plan.format.clone();
            }
        }
    }

    /// Go `getSourceType`.
    #[must_use]
    pub fn get_source_type(&self) -> tidb_lightning_mydump::SourceType {
        match self.plan.format.as_str() {
            "parquet" => tidb_lightning_mydump::SourceType::Parquet,
            "delimited data" | "csv" => tidb_lightning_mydump::SourceType::CSV,
            _ => tidb_lightning_mydump::SourceType::SQL,
        }
    }

    /// Go `Close`.
    pub fn close(&self) {
        if let Some(store) = &self.data_store {
            store.close();
        }
        if let Some(store) = &self.global_sort_store {
            store.close();
        }
    }
}
