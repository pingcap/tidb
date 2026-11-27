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

//! Go `pkg/planner/core/planbuilder.go`'s analyze section and the plan it
//! builds (`common_plans.go`'s `Analyze`, `AnalyzeColumnsTask`,
//! `AnalyzeIndexTask`, `AnalyzeInfo`, `V2AnalyzeOptions`): which physical
//! tables, columns and indexes `ANALYZE` reads, with which options.

use std::collections::{BTreeMap, BTreeSet};

use tidb_infoschema::table_source::TableSource;
use tidb_model::{ColumnInfo, GoShared, IndexInfo, SchemaState, TableInfo};
use tidb_planner_core_base::PlanError;

use super::PlanBuilder;
use tidb_exprctx::Columns;

/// Go `ast.AnalyzeOptionType`, in Go's order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AnalyzeOptionType {
    /// Go `AnalyzeOptNumBuckets`.
    NumBuckets,
    /// Go `AnalyzeOptNumTopN`.
    NumTopN,
    /// Go `AnalyzeOptCMSketchDepth`.
    CmSketchDepth,
    /// Go `AnalyzeOptCMSketchWidth`.
    CmSketchWidth,
    /// Go `AnalyzeOptNumSamples`.
    NumSamples,
    /// Go `AnalyzeOptSampleRate`: the bits of an `f64`.
    SampleRate,
}

impl AnalyzeOptionType {
    /// Go `ast.AnalyzeOptionString`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NumBuckets => "BUCKETS",
            Self::NumTopN => "TOPN",
            Self::CmSketchDepth => "CMSKETCH DEPTH",
            Self::CmSketchWidth => "CMSKETCH WIDTH",
            Self::NumSamples => "SAMPLES",
            Self::SampleRate => "SAMPLERATE",
        }
    }

    const ALL: [Self; 6] = [
        Self::NumBuckets,
        Self::NumTopN,
        Self::CmSketchDepth,
        Self::CmSketchWidth,
        Self::NumSamples,
        Self::SampleRate,
    ];
}

/// Go `map[ast.AnalyzeOptionType]uint64`.
pub type AnalyzeOpts = BTreeMap<AnalyzeOptionType, u64>;

/// Go `ast.ColumnChoice`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ColumnChoice {
    /// Go `DefaultChoice`.
    #[default]
    Default,
    /// Go `AllColumns`.
    All,
    /// Go `PredicateColumns`.
    Predicate,
    /// Go `ColumnList`.
    List,
}

impl ColumnChoice {
    /// Go `ColumnChoice.String()`, the `mysql.analyze_options.column_choice`
    /// value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Default => "DEFAULT",
            Self::All => "ALL",
            Self::Predicate => "PREDICATE",
            Self::List => "LIST",
        }
    }
}

/// Go `statistics.NonPartitionTableID`.
pub const NON_PARTITION_TABLE_ID: i64 = -1;

/// Go `statistics.AnalyzeTableID`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AnalyzeTableID {
    /// Go `TableID`: the logical table.
    pub table_id: i64,
    /// Go `PartitionID`, [`NON_PARTITION_TABLE_ID`] for the table itself.
    pub partition_id: i64,
}

impl AnalyzeTableID {
    /// Go `IsPartitionTable`.
    #[must_use]
    pub const fn is_partition_table(self) -> bool {
        self.partition_id != NON_PARTITION_TABLE_ID
    }

    /// Go `GetStatisticsID`.
    #[must_use]
    pub const fn get_statistics_id(self) -> i64 {
        if self.is_partition_table() {
            self.partition_id
        } else {
            self.table_id
        }
    }
}

/// Go `V2AnalyzeOptions`.
#[derive(Clone, Debug)]
pub struct V2AnalyzeOptions {
    /// Go `PhyTableID`.
    pub phy_table_id: i64,
    /// Go `RawOpts`: the explicit and saved options, saved after ANALYZE.
    pub raw_opts: AnalyzeOpts,
    /// Go `FilledOpts`: [`Self::raw_opts`] with the defaults filled in.
    pub filled_opts: AnalyzeOpts,
    /// Go `ColChoice`.
    pub col_choice: ColumnChoice,
    /// Go `ColumnList`.
    pub column_list: Vec<GoShared<ColumnInfo>>,
    /// Go `IsPartition`.
    pub is_partition: bool,
}

/// Go `AnalyzeInfo`.
#[derive(Clone, Debug)]
pub struct AnalyzeInfo {
    /// Go `DBName`.
    pub db_name: String,
    /// Go `TableName`.
    pub table_name: String,
    /// Go `PartitionName`.
    pub partition_name: String,
    /// Go `TableID`.
    pub table_id: AnalyzeTableID,
    /// Go `StatsVersion`.
    pub stats_version: i64,
    /// Go `V2Options`.
    pub v2_options: Option<V2AnalyzeOptions>,
}

/// Go `AnalyzeColumnsTask`. Go's `HandleCols` is derived by the executor
/// from [`Self::tbl_info`] and [`Self::cols_info`].
#[derive(Clone, Debug)]
pub struct AnalyzeColumnsTask {
    /// Go `ColsInfo`.
    pub cols_info: Vec<GoShared<ColumnInfo>>,
    /// Go `SkipColsInfo`.
    pub skip_cols_info: Vec<GoShared<ColumnInfo>>,
    /// Go `TblInfo`.
    pub tbl_info: GoShared<TableInfo>,
    /// Go `Indexes`.
    pub indexes: Vec<GoShared<IndexInfo>>,
    /// Go `len(tbl.TableInfo.Columns) == len(execColsInfo)`.
    pub all_columns: bool,
    /// Go's embedded `AnalyzeInfo`.
    pub analyze_info: AnalyzeInfo,
}

/// Go `AnalyzeIndexTask`.
#[derive(Clone, Debug)]
pub struct AnalyzeIndexTask {
    /// Go `IndexInfo`.
    pub index_info: GoShared<IndexInfo>,
    /// Go `TblInfo`.
    pub tbl_info: GoShared<TableInfo>,
    /// Go's embedded `AnalyzeInfo`.
    pub analyze_info: AnalyzeInfo,
}

/// Go `Analyze`.
#[derive(Clone, Debug, Default)]
pub struct Analyze {
    /// Go `ColTasks`.
    pub col_tasks: Vec<AnalyzeColumnsTask>,
    /// Go `IdxTasks`.
    pub idx_tasks: Vec<AnalyzeIndexTask>,
    /// Go `Opts`.
    pub opts: AnalyzeOpts,
    /// Go `OptionsMap`: the options of each physical table.
    pub options_map: BTreeMap<i64, V2AnalyzeOptions>,
}

/// What Go's `buildAnalyze` reads from `SessionVars` and the analyze
/// globals.
#[derive(Clone, Debug)]
pub struct AnalyzePlanVars {
    /// Go `SessionVars.AnalyzeVersion`.
    pub analyze_version: i64,
    /// Go `PartitionPruneMode == Dynamic`.
    pub dynamic_prune: bool,
    /// Go `SessionVars.EnableDDLAnalyzeExecOpt`.
    pub enable_ddl_analyze_exec_opt: bool,
    /// Go `SessionVars.AnalyzeSkipColumnTypes`.
    pub analyze_skip_column_types: BTreeSet<String>,
    /// Go `vardef.PersistAnalyzeOptions`.
    pub persist_analyze_options: bool,
    /// Go `vardef.AnalyzeColumnOptions`.
    pub analyze_column_options: String,
}

impl Default for AnalyzePlanVars {
    fn default() -> Self {
        Self {
            analyze_version: 2,
            dynamic_prune: true,
            enable_ddl_analyze_exec_opt: false,
            analyze_skip_column_types: BTreeSet::new(),
            persist_analyze_options: true,
            analyze_column_options: tidb_vardef::tidb_vars::DEF_TIDB_ANALYZE_COLUMN_OPTIONS.to_owned(),
        }
    }
}

/// Go `vardef.MaxTiDBAnalyzeDefaultNumBuckets` / `...NumTopN`.
const MAX_ANALYZE_NUM_BUCKETS_TOPN: u64 = 100_000;
/// Go `CMSketchSizeLimit` (`kv.TxnEntrySizeLimit / binary.MaxVarintLen32`).
const CMSKETCH_SIZE_LIMIT: u64 = 6 * 1024 * 1024 / 5;

/// Go `analyzeOptionLimit`.
fn analyze_option_limit(opt: AnalyzeOptionType) -> u64 {
    match opt {
        AnalyzeOptionType::NumBuckets | AnalyzeOptionType::NumTopN => MAX_ANALYZE_NUM_BUCKETS_TOPN,
        AnalyzeOptionType::CmSketchWidth | AnalyzeOptionType::CmSketchDepth => CMSKETCH_SIZE_LIMIT,
        AnalyzeOptionType::NumSamples => 5_000_000,
        AnalyzeOptionType::SampleRate => 1f64.to_bits(),
    }
}

/// Go `AnalyzeOptionDefault`.
#[must_use]
pub fn analyze_option_default() -> AnalyzeOpts {
    BTreeMap::from([
        (
            AnalyzeOptionType::NumBuckets,
            tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.load(std::sync::atomic::Ordering::SeqCst),
        ),
        (
            AnalyzeOptionType::NumTopN,
            tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.load(std::sync::atomic::Ordering::SeqCst),
        ),
        (AnalyzeOptionType::CmSketchWidth, 2048),
        (AnalyzeOptionType::CmSketchDepth, 5),
        (AnalyzeOptionType::NumSamples, 0),
        (AnalyzeOptionType::SampleRate, (-1f64).to_bits()),
    ])
}

fn errorf(message: String) -> PlanError {
    PlanError::internal(message)
}

/// Go `handleAnalyzeOptions`: validates the options the statement names.
/// NDVRATE has no `analyzeOptionLimit`, so its limit is 0.
fn handle_analyze_options(opts: &[tidb_ast::AnalyzeOption]) -> Result<AnalyzeOpts, PlanError> {
    let mut opt_map = AnalyzeOpts::new();
    let (mut sample_num, mut sample_rate) = (0_u64, 0.0_f64);
    // Go `datumValue.GetUint64()` of the literal; a decimal literal reads as
    // its integer part's bits, which the limits then refuse.
    let uint64 = |value: &str| value.parse::<u64>().unwrap_or(0);
    for opt in opts {
        let opt_type = match opt.kind {
            tidb_ast::AnalyzeOptionKind::TopN => AnalyzeOptionType::NumTopN,
            tidb_ast::AnalyzeOptionKind::Buckets => AnalyzeOptionType::NumBuckets,
            tidb_ast::AnalyzeOptionKind::CmSketchDepth => AnalyzeOptionType::CmSketchDepth,
            tidb_ast::AnalyzeOptionKind::CmSketchWidth => AnalyzeOptionType::CmSketchWidth,
            tidb_ast::AnalyzeOptionKind::Samples => AnalyzeOptionType::NumSamples,
            tidb_ast::AnalyzeOptionKind::SampleRate => AnalyzeOptionType::SampleRate,
            tidb_ast::AnalyzeOptionKind::NdvRate => {
                return Err(errorf(
                    "Value of analyze option NDVRATE should be positive and not larger than 0".to_owned(),
                ));
            }
        };
        match opt_type {
            AnalyzeOptionType::NumTopN => {
                let v = uint64(&opt.value);
                if v > analyze_option_limit(opt_type) {
                    return Err(errorf(format!(
                        "Value of analyze option {} should not be larger than {}",
                        opt_type.as_str(),
                        analyze_option_limit(opt_type)
                    )));
                }
                opt_map.insert(opt_type, v);
            }
            AnalyzeOptionType::SampleRate => {
                let f_val = opt.value.parse::<f64>().map_err(|error| errorf(error.to_string()))?;
                let limit = f64::from_bits(analyze_option_limit(opt_type));
                if f_val <= 0.0 || f_val > limit {
                    return Err(errorf(format!(
                        "Value of analyze option {} should not larger than {limit:.6}, and should be greater than 0",
                        opt_type.as_str()
                    )));
                }
                sample_rate = f_val;
                opt_map.insert(opt_type, f_val.to_bits());
            }
            _ => {
                let v = uint64(&opt.value);
                if opt_type == AnalyzeOptionType::NumSamples {
                    sample_num = v;
                }
                if v == 0 || v > analyze_option_limit(opt_type) {
                    return Err(errorf(format!(
                        "Value of analyze option {} should be positive and not larger than {}",
                        opt_type.as_str(),
                        analyze_option_limit(opt_type)
                    )));
                }
                opt_map.insert(opt_type, v);
            }
        }
    }
    if sample_num > 0 && sample_rate > 0.0 {
        return Err(errorf(
            "You can only either set the value of the sample num or set the value of the sample rate. Don't set both of them"
                .to_owned(),
        ));
    }
    Ok(opt_map)
}

/// Go `fillAnalyzeOptions`.
#[must_use]
pub fn fill_analyze_options(opt_map: &AnalyzeOpts) -> AnalyzeOpts {
    analyze_option_default()
        .into_iter()
        .map(|(key, default_val)| (key, opt_map.get(&key).copied().unwrap_or(default_val)))
        .collect()
}

/// Go `mergeAnalyzeOptions`.
fn merge_analyze_options(stmt_opts: &AnalyzeOpts, saved_opts: &AnalyzeOpts) -> AnalyzeOpts {
    let mut merged = AnalyzeOpts::new();
    for opt_type in AnalyzeOptionType::ALL {
        if let Some(stmt_opt) = stmt_opts.get(&opt_type) {
            merged.insert(opt_type, *stmt_opt);
        } else if let Some(saved_opt) = saved_opts.get(&opt_type) {
            merged.insert(opt_type, *saved_opt);
        }
    }
    merged
}

/// Go `pickColumnList`.
fn pick_column_list(
    ast_col_choice: ColumnChoice,
    ast_col_list: &[GoShared<ColumnInfo>],
    tbl_saved_col_choice: ColumnChoice,
    tbl_saved_col_list: &[GoShared<ColumnInfo>],
) -> (ColumnChoice, Vec<GoShared<ColumnInfo>>) {
    if ast_col_choice != ColumnChoice::Default {
        return (ast_col_choice, ast_col_list.to_vec());
    }
    (tbl_saved_col_choice, tbl_saved_col_list.to_vec())
}

/// Go `GetPhysicalIDsAndPartitionNames`.
///
/// # Errors
///
/// A partition clause on a nonpartitioned table, or a partition name the
/// table has none of.
pub fn get_physical_ids_and_partition_names(
    tbl_info: &TableInfo,
    partition_names: &[String],
) -> Result<(Vec<i64>, Vec<String>), PlanError> {
    let Some(pi) = tbl_info.get_partition_info() else {
        if !partition_names.is_empty() {
            return Err(PlanError::from(
                tidb_dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED.gen_with_stack_by_args(&[]),
            ));
        }
        return Ok((vec![tbl_info.id], vec![String::new()]));
    };
    let definitions = pi.read().definitions.snapshot();
    if partition_names.is_empty() {
        return Ok((
            definitions.iter().map(|def| def.id).collect(),
            definitions.iter().map(|def| def.name.original().to_owned()).collect(),
        ));
    }
    let mut ids = Vec::with_capacity(partition_names.len());
    let mut names = Vec::with_capacity(partition_names.len());
    for name in partition_names {
        let Some(def) = definitions
            .iter()
            .find(|def| def.name.lowercase() == tidb_hack::go_to_lower(name))
        else {
            return Err(errorf(format!(
                "can not found the specified partition name {name} in the table definition"
            )));
        };
        ids.push(def.id);
        names.push(def.name.original().to_owned());
    }
    Ok((ids, names))
}

/// Go `calcOnceMap`.
#[derive(Default)]
struct CalcOnceMap {
    data: BTreeSet<i64>,
    calculated: bool,
}

/// Go's `resolve.TableNameW` as ANALYZE reads it: the name as written and
/// the table it resolves to.
struct TableNameW {
    /// Go `tbl.Schema`: the written or current database.
    schema: String,
    /// Go `tbl.Name`: the table name as written.
    name: String,
    table_info: GoShared<TableInfo>,
}

/// Go `tblInfo.Columns[offset]`.
fn column_at(table: &TableInfo, offset: i64) -> Option<GoShared<ColumnInfo>> {
    usize::try_from(offset).ok().and_then(|offset| table.columns.get(offset))
}

/// Go `addColumnsWithVirtualExprs`: adds the columns of `virtual_columns`'
/// virtual expressions to `cols`, following virtual columns among them. A
/// virtual generated column's expression reads the columns it depends on.
fn add_columns_with_virtual_exprs(table: &TableInfo, cols: &mut CalcOnceMap, virtual_columns: Vec<GoShared<ColumnInfo>>) {
    let mut pending = virtual_columns;
    while let Some(column) = pending.pop() {
        let column = column.read();
        for dependency in column.dependences.snapshot() {
            let Ok(dependency) = dependency.as_utf8() else { continue };
            let Some(base) = table
                .columns
                .iter_deref()
                .find(|candidate| candidate.read().name.lowercase() == tidb_hack::go_to_lower(dependency))
            else {
                continue;
            };
            let (id, is_virtual) = {
                let base = base.read();
                (base.id, base.is_virtual_generated())
            };
            if cols.data.insert(id) && is_virtual {
                pending.push(base.clone());
            }
        }
    }
}

/// Go `getColOffsetForAnalyze`.
fn get_col_offset_for_analyze(cols_info: &[GoShared<ColumnInfo>], col_id: i64) -> i64 {
    cols_info
        .iter()
        .position(|col| col.read().id == col_id)
        .map_or(-1, |position| position as i64)
}

/// Go `getColumnListFromSet`.
fn get_column_list_from_set(table: &TableInfo, col_set: &BTreeSet<i64>) -> Vec<GoShared<ColumnInfo>> {
    table
        .columns
        .iter_deref()
        .filter(|col| col_set.contains(&col.read().id))
        .collect()
}

/// Go `handleutil.IsSpecialGlobalIndex`.
fn is_special_global_index(index: &IndexInfo, table: &TableInfo) -> bool {
    tidb_stats_handle_util::is_special_global_index(index, table)
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    fn analyze_plan_vars(&self) -> AnalyzePlanVars {
        self.session_ctx.map(|session| session.analyze_plan_vars()).unwrap_or_default()
    }

    fn append_analyze_warning(&self, message: String) {
        self.ctx.append_warning(1105, &message);
    }

    /// Go `buildAnalyze`.
    pub(crate) fn build_analyze(&mut self, analyze: &tidb_ast::AnalyzeTableStmt) -> Result<Analyze, PlanError> {
        if analyze.no_write_to_binlog {
            return Err(PlanError::from(
                tidb_plannererrors::ERR_NOT_SUPPORTED_YET.gen_with_stack_by_args(&["[NO_WRITE_TO_BINLOG | LOCAL]".into()]),
            ));
        }
        let vars = self.analyze_plan_vars();
        let stats_version = vars.analyze_version;
        // Require INSERT and SELECT privilege for tables.
        self.require_insert_and_select_priv(analyze.tables.iter());
        let opts = handle_analyze_options(&analyze.options)?;
        let filled_opts = fill_analyze_options(&opts);
        let mut tables = Vec::with_capacity(analyze.tables.len());
        for path in &analyze.tables {
            tables.push(self.analyze_table_name(path)?);
        }
        if let tidb_ast::AnalyzeTarget::Index(_) = &analyze.target {
            // Go `buildAnalyzeIndex` / `buildAnalyzeAllIndex`.
            let tbl_info = tables[0].table_info.read().clone();
            let (physical_ids, _) = get_physical_ids_and_partition_names(&tbl_info, &analyze.partitions)?;
            if !self.analyze_version_matches_for_physical_ids(&tbl_info, &physical_ids, stats_version) {
                self.append_analyze_version_overwrite_warning();
            }
        }
        self.build_analyze_table(analyze, &tables, filled_opts, stats_version, &vars)
    }

    /// Go `resolveCtx.GetTableName(tbl)` for an `ANALYZE` target.
    fn analyze_table_name(&self, path: &[String]) -> Result<TableNameW, PlanError> {
        let (schema, name) = match path {
            [name] => (self.source.current_database().to_owned(), name.clone()),
            [schema, name] => (schema.clone(), name.clone()),
            _ => return Err(errorf(format!("`{}` does not name a table", path.join(".")))),
        };
        if schema.is_empty() {
            return Err(PlanError::from(tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[])));
        }
        let table = self
            .source
            .try_find_table(&tidb_hack::go_to_lower(&schema), &tidb_hack::go_to_lower(&name))
            .map_err(PlanError::from)?
            .and_then(|table| table.table_info.clone());
        let Some(table_info) = table else {
            return Err(PlanError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.gen_with_stack_by_args(&[schema.as_str().into(), name.as_str().into()]),
            ));
        };
        Ok(TableNameW { schema, name, table_info })
    }

    /// Go `buildAnalyzeTable`.
    fn build_analyze_table(
        &mut self,
        analyze: &tidb_ast::AnalyzeTableStmt,
        tables: &[TableNameW],
        opts: AnalyzeOpts,
        version: i64,
        vars: &AnalyzePlanVars,
    ) -> Result<Analyze, PlanError> {
        let mut p = Analyze { opts, ..Analyze::default() };
        let use_persisted_options = vars.persist_analyze_options;
        // Construct tasks for each table.
        for tbl in tables {
            let (is_view, is_sequence) = {
                let info = tbl.table_info.read();
                (info.is_view(), info.is_sequence())
            };
            if is_view {
                return Err(errorf(format!("analyze view {} is not supported now", tbl.name)));
            }
            if is_sequence {
                return Err(errorf(format!("analyze sequence {} is not supported now", tbl.name)));
            }
            // Go master `CheckMViewReadable`.
            crate::util::check_mview_readable(
                &crate::util::MViewSessionVars::of(self.ctx),
                &tbl.table_info.read(),
                &tbl.name,
            )?;
            let (physical_ids, partition_names) =
                get_physical_ids_and_partition_names(&tbl.table_info.read(), &analyze.partitions)?;
            self.build_analyze_full_sampling_task(
                analyze,
                &mut p,
                physical_ids,
                partition_names,
                tbl,
                &tables[0],
                version,
                use_persisted_options,
                vars,
            )?;
        }
        Ok(p)
    }

    /// Go `analyzeVersionMatchesForPhysicalIDs`.
    fn analyze_version_matches_for_physical_ids(&self, tbl_info: &TableInfo, physical_ids: &[i64], version: i64) -> bool {
        let Some(session) = self.session_ctx else {
            return true;
        };
        physical_ids
            .iter()
            .all(|physical_id| session.analyze_version_matches(*physical_id, tbl_info, version))
    }

    /// Go `appendAnalyzeVersionOverwriteWarning`.
    fn append_analyze_version_overwrite_warning(&self) {
        self.append_analyze_warning(
            "The analyze version from the session is not compatible with the existing statistics of the table. The session-selected version will overwrite the existing version instead".to_owned(),
        );
    }

    /// Go `buildAnalyzeFullSamplingTask`.
    #[allow(clippy::too_many_arguments)]
    fn build_analyze_full_sampling_task(
        &mut self,
        analyze: &tidb_ast::AnalyzeTableStmt,
        analyze_plan: &mut Analyze,
        mut physical_ids: Vec<i64>,
        mut partition_names: Vec<String>,
        tbl: &TableNameW,
        first_table_name: &TableNameW,
        version: i64,
        persist_opts: bool,
        vars: &AnalyzePlanVars,
    ) -> Result<(), PlanError> {
        let is_analyze_table = analyze.partitions.is_empty();
        let (index_flag, index_names) = match &analyze.target {
            tidb_ast::AnalyzeTarget::Index(names) => (true, names.as_slice()),
            _ => (false, &[][..]),
        };
        let all_special_global_index = self.check_is_all_special_global_index(analyze, tbl)?;
        let ast_opts = handle_analyze_options(&analyze.options)?;
        // Get all column info which need to be analyzed.
        let (ast_col_choice, column_names) = match &analyze.target {
            tidb_ast::AnalyzeTarget::AllColumns => (ColumnChoice::All, &[][..]),
            tidb_ast::AnalyzeTarget::PredicateColumns => (ColumnChoice::Predicate, &[][..]),
            tidb_ast::AnalyzeTarget::Columns(names) => (ColumnChoice::List, names.as_slice()),
            // Go `buildAnalyze` never reads `HistogramOperation`: the
            // statement keeps the default choice and only its names count.
            tidb_ast::AnalyzeTarget::Histogram { columns, .. } => (ColumnChoice::Default, columns.as_slice()),
            _ => (ColumnChoice::Default, &[][..]),
        };
        let ast_col_list = get_analyze_column_list(column_names, tbl)?;

        let mut predicate_cols = CalcOnceMap::default();
        let mut must_analyzed_cols = CalcOnceMap::default();
        let is_partitioned = tbl.table_info.read().get_partition_info().is_some();
        let mut target_physical_ids_version_match =
            self.analyze_version_matches_for_physical_ids(&tbl.table_info.read(), &physical_ids, version);
        if !is_analyze_table && vars.dynamic_prune && is_partitioned {
            // Dynamic partition analyze later merges all partitions into
            // table-level stats, so the rewrite decision follows the
            // table-level stats.
            let table_version_matches = self
                .session_ctx
                .is_none_or(|session| session.analyze_version_matches_for_table(&tbl.table_info.read(), version));
            if !table_version_matches {
                (physical_ids, partition_names) = get_physical_ids_and_partition_names(&tbl.table_info.read(), &[])?;
                self.append_analyze_warning(
                    "The analyze version from the session is not compatible with the existing statistics of the table. TiDB will analyze all partitions to rewrite the table statistics with the session-selected version".to_owned(),
                );
                target_physical_ids_version_match = false;
            }
        }
        // If the statistics of the table is version 1, all columns must be
        // analyzed to overwrite the old statistics.
        let must_all_columns = !target_physical_ids_version_match;

        let (ast_cols_info, _) = self.get_full_analyze_columns_info(
            tbl,
            ast_col_choice,
            &ast_col_list,
            &mut predicate_cols,
            &mut must_analyzed_cols,
            must_all_columns,
            true,
            vars,
        )?;
        let (options_map, cols_info_map) = self.gen_v2_analyze_options(
            persist_opts,
            tbl,
            is_analyze_table,
            &physical_ids,
            &ast_opts,
            ast_col_choice,
            &ast_col_list,
            &mut predicate_cols,
            &mut must_analyzed_cols,
            must_all_columns,
            vars,
        )?;
        analyze_plan.options_map.extend(options_map.clone());

        let mut special_global_indexes = Vec::new();
        let need_analyze_cols = !(index_flag && all_special_global_index);
        let tbl_info = tbl.table_info.read().clone();
        if need_analyze_cols {
            if index_flag {
                self.append_analyze_warning("The version 2 would collect all statistics not only the selected indexes".to_owned());
            }
            // Build tasks for each partition.
            for (i, physical_id) in physical_ids.iter().copied().enumerate() {
                let id = if physical_id == tbl_info.id { NON_PARTITION_TABLE_ID } else { physical_id };
                let info = AnalyzeInfo {
                    db_name: tbl.schema.clone(),
                    table_name: tbl.name.clone(),
                    partition_name: partition_names[i].clone(),
                    table_id: AnalyzeTableID { table_id: tbl_info.id, partition_id: id },
                    stats_version: version,
                    v2_options: options_map.get(&physical_id).cloned(),
                };
                let exec_cols_info = cols_info_map.get(&physical_id).cloned().unwrap_or_else(|| ast_cols_info.clone());
                let (exec_cols_info, skip_cols_info) =
                    self.filter_skip_column_types(exec_cols_info, tbl, &mut must_analyzed_cols, vars);
                let all_columns = tbl_info.columns.len() == exec_cols_info.len();
                let (indexes, independent_indexes, special) =
                    self.get_modified_indexes_info_for_analyze(&tbl_info, all_columns, &exec_cols_info, vars);
                special_global_indexes = special;
                analyze_plan.col_tasks.push(AnalyzeColumnsTask {
                    cols_info: exec_cols_info,
                    skip_cols_info,
                    tbl_info: tbl.table_info.clone(),
                    indexes,
                    all_columns,
                    analyze_info: info.clone(),
                });
                for index_info in independent_indexes {
                    analyze_plan.idx_tasks.push(AnalyzeIndexTask {
                        index_info,
                        tbl_info: tbl.table_info.clone(),
                        analyze_info: info.clone(),
                    });
                }
            }
        }
        if is_analyze_table {
            if need_analyze_cols {
                // Non-global indexes are covered by the loop; the global ones
                // are handled here.
                for index_info in special_global_indexes {
                    analyze_plan.idx_tasks.extend(generate_index_tasks(&index_info, tbl, first_table_name, version));
                }
            } else {
                // `analyze table t index idx1[, idx2]` over global indexes.
                for idx_name in index_names {
                    let Some(idx) = tbl_info.find_index_by_name(&tidb_hack::go_to_lower(idx_name)) else {
                        continue;
                    };
                    if !is_special_global_index(&idx.read(), &tbl_info) {
                        continue;
                    }
                    analyze_plan.idx_tasks.extend(generate_index_tasks(&idx, tbl, first_table_name, version));
                }
            }
        }
        Ok(())
    }

    /// Go `checkIsAllSpecialGlobalIndex`.
    fn check_is_all_special_global_index(&self, analyze: &tidb_ast::AnalyzeTableStmt, tbl: &TableNameW) -> Result<bool, PlanError> {
        let tidb_ast::AnalyzeTarget::Index(index_names) = &analyze.target else {
            // Go's `else` branch over an empty `IndexNames`.
            return Ok(true);
        };
        let is_analyze_table = analyze.partitions.is_empty();
        let table = tbl.table_info.read();
        if index_names.is_empty() {
            for idx in table.indices.iter_deref() {
                let idx = idx.read();
                if idx.state != SchemaState::PUBLIC {
                    continue;
                }
                if !is_special_global_index(&idx, &table) {
                    return Ok(false);
                }
                if !is_analyze_table {
                    return Err(errorf(format!(
                        "Analyze global index '{}' can't work with analyze specified partitions",
                        idx.name.original()
                    )));
                }
            }
        } else {
            for idx_name in index_names {
                let idx = table.find_index_by_name(&tidb_hack::go_to_lower(idx_name));
                let Some(idx) = idx.filter(|idx| idx.read().state == SchemaState::PUBLIC) else {
                    return Err(PlanError::from(
                        tidb_plannererrors::ERR_ANALYZE_MISS_INDEX
                            .gen_with_stack_by_args(&[idx_name.as_str().into(), tbl.name.as_str().into()]),
                    ));
                };
                let idx = idx.read();
                if !is_special_global_index(&idx, &table) {
                    return Ok(false);
                }
                if !is_analyze_table {
                    return Err(errorf(format!(
                        "Analyze global index '{}' can't work with analyze specified partitions",
                        idx.name.original()
                    )));
                }
            }
        }
        Ok(true)
    }

    /// Go `getFullAnalyzeColumnsInfo`: the columns to analyze, and the list
    /// to save in `mysql.analyze_options` (only for a column list).
    #[allow(clippy::too_many_arguments)]
    fn get_full_analyze_columns_info(
        &self,
        tbl: &TableNameW,
        column_choice: ColumnChoice,
        specified_cols: &[GoShared<ColumnInfo>],
        predicate_cols: &mut CalcOnceMap,
        must_analyzed_cols: &mut CalcOnceMap,
        must_all_columns: bool,
        warning: bool,
        vars: &AnalyzePlanVars,
    ) -> Result<(Vec<GoShared<ColumnInfo>>, Vec<GoShared<ColumnInfo>>), PlanError> {
        let table = tbl.table_info.read().clone();
        let all = || table.columns.iter_deref().collect::<Vec<_>>();
        if must_all_columns && warning && matches!(column_choice, ColumnChoice::Predicate | ColumnChoice::List) {
            self.append_analyze_warning(format!(
                "Table {}.{} has version 1 statistics so all the columns must be analyzed to overwrite the current statistics",
                tidb_hack::go_to_lower(&tbl.schema),
                tidb_hack::go_to_lower(&tbl.name)
            ));
        }
        match column_choice {
            ColumnChoice::Default => match vars.analyze_column_options.to_ascii_uppercase().as_str() {
                "ALL" => Ok((all(), Vec::new())),
                "PREDICATE" => Ok((
                    self.get_columns_based_on_predicate_columns(tbl, predicate_cols, must_analyzed_cols, must_all_columns)?,
                    Vec::new(),
                )),
                _ => Ok((all(), Vec::new())),
            },
            ColumnChoice::All => Ok((all(), Vec::new())),
            ColumnChoice::Predicate => Ok((
                self.get_columns_based_on_predicate_columns(tbl, predicate_cols, must_analyzed_cols, must_all_columns)?,
                Vec::new(),
            )),
            ColumnChoice::List => {
                let mut col_set = specified_cols.iter().map(|col| col.read().id).collect::<BTreeSet<_>>();
                let must_analyzed = self.get_must_analyzed_columns(tbl, must_analyzed_cols, vars).clone();
                if warning {
                    let missing = must_analyzed.difference(&col_set).copied().collect::<BTreeSet<_>>();
                    if !missing.is_empty() {
                        let missing_names = table
                            .columns
                            .iter_deref()
                            .filter_map(|col| {
                                let col = col.read();
                                missing.contains(&col.id).then(|| col.name.original().to_owned())
                            })
                            .collect::<Vec<_>>();
                        self.append_analyze_warning(format!(
                            "Columns {} are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
                            missing_names.join(",")
                        ));
                    }
                }
                col_set.extend(must_analyzed);
                let col_list = get_column_list_from_set(&table, &col_set);
                if must_all_columns {
                    return Ok((all(), col_list));
                }
                Ok((col_list.clone(), col_list))
            }
        }
    }

    /// Go `getColumnsBasedOnPredicateColumns`.
    fn get_columns_based_on_predicate_columns(
        &self,
        tbl: &TableNameW,
        predicate_cols: &mut CalcOnceMap,
        must_analyzed_cols: &mut CalcOnceMap,
        rewrite_all_stats_needed: bool,
    ) -> Result<Vec<GoShared<ColumnInfo>>, PlanError> {
        let table = tbl.table_info.read().clone();
        if rewrite_all_stats_needed {
            return Ok(table.columns.iter_deref().collect());
        }
        let mut col_set = self.get_predicate_columns(tbl, predicate_cols)?.clone();
        let vars = self.analyze_plan_vars();
        col_set.extend(self.get_must_analyzed_columns(tbl, must_analyzed_cols, &vars).iter().copied());
        Ok(get_column_list_from_set(&table, &col_set))
    }

    /// Go `getMustAnalyzedColumns`: the indexed columns (and those their
    /// virtual columns read) and the integer primary key.
    fn get_must_analyzed_columns<'m>(
        &self,
        tbl: &TableNameW,
        cols: &'m mut CalcOnceMap,
        vars: &AnalyzePlanVars,
    ) -> &'m BTreeSet<i64> {
        if cols.calculated {
            return &cols.data;
        }
        let table = tbl.table_info.read().clone();
        cols.data = BTreeSet::new();
        if !table.indices.is_empty() {
            let mut virtual_columns = Vec::new();
            for idx in table.indices.iter_deref() {
                let idx = idx.read();
                // A public index can always be analyzed; a write-reorg one
                // only under `EnableDDLAnalyzeExecOpt`.
                let index_state_analyzable = idx.state == SchemaState::PUBLIC
                    || (idx.state == SchemaState::WRITE_REORGANIZATION && vars.enable_ddl_analyze_exec_opt);
                if idx.mv_index || idx.is_columnar_index() || !index_state_analyzable {
                    continue;
                }
                for idx_col in idx.columns.iter_deref() {
                    let Some(col_info) = column_at(&table, idx_col.read().offset) else { continue };
                    cols.data.insert(col_info.read().id);
                    if col_info.read().is_virtual_generated() {
                        virtual_columns.push(col_info);
                    }
                }
            }
            add_columns_with_virtual_exprs(&table, cols, virtual_columns);
        }
        if table.pk_is_handle {
            if let Some(pk_col) = table.get_pk_col_info() {
                cols.data.insert(pk_col.read().id);
            }
        }
        cols.calculated = true;
        &cols.data
    }

    /// Go `getPredicateColumns`.
    fn get_predicate_columns<'m>(&self, tbl: &TableNameW, cols: &'m mut CalcOnceMap) -> Result<&'m BTreeSet<i64>, PlanError> {
        if cols.calculated {
            return Ok(&cols.data);
        }
        let table = tbl.table_info.read().clone();
        cols.data = BTreeSet::new();
        let col_list = match self.session_ctx {
            Some(session) => session.get_predicate_columns(table.id)?,
            None => Vec::new(),
        };
        if col_list.is_empty() {
            self.append_analyze_warning(format!(
                "No predicate column has been collected yet for table {}.{}, so only indexes and the columns composing the indexes will be analyzed",
                tidb_hack::go_to_lower(&tbl.schema),
                tidb_hack::go_to_lower(&tbl.name)
            ));
        } else {
            let mut virtual_columns = Vec::new();
            for id in col_list {
                let Some(column_info) = table.columns.iter_deref().find(|col| col.read().id == id) else {
                    continue;
                };
                cols.data.insert(id);
                if column_info.read().is_virtual_generated() {
                    virtual_columns.push(column_info);
                }
            }
            add_columns_with_virtual_exprs(&table, cols, virtual_columns);
        }
        cols.calculated = true;
        Ok(&cols.data)
    }

    /// Go `getModifiedIndexesInfoForAnalyze`: the column task's indexes
    /// (offsets into `cols_info` when not every column is analyzed), the
    /// multi-valued indexes analyzed on their own, and the special global
    /// indexes.
    fn get_modified_indexes_info_for_analyze(
        &self,
        tbl_info: &TableInfo,
        all_columns: bool,
        cols_info: &[GoShared<ColumnInfo>],
        vars: &AnalyzePlanVars,
    ) -> (Vec<GoShared<IndexInfo>>, Vec<GoShared<IndexInfo>>, Vec<GoShared<IndexInfo>>) {
        let mut idxs_info = Vec::new();
        let mut independent_idxs_info = Vec::new();
        let mut special_global_idxs_info = Vec::new();
        for origin_idx in tbl_info.indices.iter_deref() {
            let origin = origin_idx.read();
            let index_state_analyzable = origin.state == SchemaState::PUBLIC
                || (origin.state == SchemaState::WRITE_REORGANIZATION && vars.enable_ddl_analyze_exec_opt);
            if !index_state_analyzable {
                continue;
            }
            if is_special_global_index(&origin, tbl_info) {
                special_global_idxs_info.push(origin_idx.clone());
                continue;
            }
            if origin.mv_index {
                independent_idxs_info.push(origin_idx.clone());
                continue;
            }
            if origin.is_columnar_index() {
                self.append_analyze_warning(format!(
                    "analyzing columnar index is not supported, skip {}",
                    origin.name.lowercase()
                ));
                continue;
            }
            if all_columns {
                idxs_info.push(origin_idx.clone());
                continue;
            }
            // Offsets follow `cols_info`, which the samples are decoded by;
            // Go's `idx.Clone()` copies the columns, leaving the table's.
            let idx = origin.clone_like_go();
            for idx_col in idx.columns.iter_deref() {
                let col_id = column_at(tbl_info, idx_col.read().offset).map_or(-1, |col| col.read().id);
                idx_col.write().offset = get_col_offset_for_analyze(cols_info, col_id);
            }
            idxs_info.push(GoShared::new(idx));
        }
        (idxs_info, independent_idxs_info, special_global_idxs_info)
    }

    /// Go `filterSkipColumnTypes`: drops the columns whose types
    /// `tidb_analyze_skip_column_types` names unless an index needs them,
    /// vector columns, and generated columns reading a dropped column.
    fn filter_skip_column_types(
        &self,
        origin: Vec<GoShared<ColumnInfo>>,
        tbl: &TableNameW,
        must_analyzed_cols: &mut CalcOnceMap,
        vars: &AnalyzePlanVars,
    ) -> (Vec<GoShared<ColumnInfo>>, Vec<GoShared<ColumnInfo>>) {
        let skip_types = &vars.analyze_skip_column_types;
        let must_analyze = self.get_must_analyzed_columns(tbl, must_analyzed_cols, vars).clone();
        let mut result = Vec::new();
        let mut skip_col = Vec::new();
        for col_info in origin {
            let (vector, skip, keep) = {
                let col = col_info.read();
                (
                    col.field_type.code() == tidb_datatype::FieldTypeCode::VectorFloat32,
                    skip_types.contains(tidb_datatype::type_to_str(col.field_type.code(), col.field_type.charset_name())),
                    must_analyze.contains(&col.id),
                )
            };
            // Vector columns are skipped whatever skipTypes says, since their
            // statistics are useless for current TiDB; a column an index needs
            // is kept.
            if (skip || vector) && !keep {
                skip_col.push(col_info);
                continue;
            }
            result.push(col_info);
        }
        let mut skip_col_names = skip_col
            .iter()
            .map(|col| col.read().name.lowercase().to_owned())
            .collect::<BTreeSet<_>>();
        // A generated column can only depend on generated columns defined
        // before it, so adding each skipped one to the names also skips the
        // generated columns that depend on it.
        let mut filtered_result = Vec::with_capacity(result.len());
        for col_info in result {
            let should_skip = {
                let col = col_info.read();
                if col.is_generated() {
                    let keep = must_analyze.contains(&col.id);
                    // Stored generated columns that must be analyzed are
                    // decoded from storage.
                    let decodable_from_storage = col.generated_stored && keep;
                    !decodable_from_storage
                        && col.dependences.snapshot().iter().any(|dep| {
                            dep.as_utf8().is_ok_and(|dep| skip_col_names.contains(&tidb_hack::go_to_lower(dep)))
                        })
                } else {
                    false
                }
            };
            if should_skip {
                skip_col_names.insert(col_info.read().name.lowercase().to_owned());
                skip_col.push(col_info);
            } else {
                filtered_result.push(col_info);
            }
        }
        (filtered_result, skip_col)
    }

    /// Go `genV2AnalyzeOptions`.
    #[allow(clippy::too_many_arguments)]
    fn gen_v2_analyze_options(
        &self,
        persist: bool,
        tbl: &TableNameW,
        is_analyze_table: bool,
        physical_ids: &[i64],
        ast_opts: &AnalyzeOpts,
        ast_col_choice: ColumnChoice,
        ast_col_list: &[GoShared<ColumnInfo>],
        predicate_cols: &mut CalcOnceMap,
        must_analyzed_cols: &mut CalcOnceMap,
        must_all_columns: bool,
        vars: &AnalyzePlanVars,
    ) -> Result<(BTreeMap<i64, V2AnalyzeOptions>, BTreeMap<i64, Vec<GoShared<ColumnInfo>>>), PlanError> {
        let mut options_map = BTreeMap::new();
        let mut cols_info_map = BTreeMap::new();
        if !persist {
            return Ok((options_map, cols_info_map));
        }
        let (mut ast_opts, mut ast_col_choice, mut ast_col_list) = (ast_opts.clone(), ast_col_choice, ast_col_list.to_vec());
        if !is_analyze_table && vars.dynamic_prune && (!ast_opts.is_empty() || ast_col_choice != ColumnChoice::Default) {
            ast_opts = AnalyzeOpts::new();
            ast_col_choice = ColumnChoice::Default;
            ast_col_list = Vec::new();
            self.append_analyze_warning("Ignore columns and options when analyze partition in dynamic mode".to_owned());
        }
        let table_id = tbl.table_info.read().id;
        // Get the analyze options which are saved in mysql.analyze_options.
        let (tbl_saved_opts, tbl_saved_col_choice, tbl_saved_col_list) = self.get_saved_analyze_opts(table_id, tbl)?;
        let mut tbl_opts = tbl_saved_opts.clone();
        let mut tbl_col_choice = tbl_saved_col_choice;
        let mut tbl_col_list = tbl_saved_col_list.clone();
        if is_analyze_table {
            tbl_opts = merge_analyze_options(&ast_opts, &tbl_saved_opts);
            (tbl_col_choice, tbl_col_list) =
                pick_column_list(ast_col_choice, &ast_col_list, tbl_saved_col_choice, &tbl_saved_col_list);
        }
        let tbl_filled_opts = fill_analyze_options(&tbl_opts);
        let (tbl_cols_info, tbl_col_list) = self.get_full_analyze_columns_info(
            tbl,
            tbl_col_choice,
            &tbl_col_list,
            predicate_cols,
            must_analyzed_cols,
            must_all_columns,
            false,
            vars,
        )?;
        options_map.insert(
            table_id,
            V2AnalyzeOptions {
                phy_table_id: table_id,
                raw_opts: tbl_opts.clone(),
                filled_opts: tbl_filled_opts.clone(),
                col_choice: tbl_col_choice,
                column_list: tbl_col_list.clone(),
                is_partition: false,
            },
        );
        cols_info_map.insert(table_id, tbl_cols_info.clone());
        for physical_id in physical_ids.iter().copied() {
            if physical_id == table_id {
                continue;
            }
            // In dynamic mode the partitions use the table's options.
            if vars.dynamic_prune {
                options_map.insert(
                    physical_id,
                    V2AnalyzeOptions {
                        phy_table_id: physical_id,
                        raw_opts: tbl_opts.clone(),
                        filled_opts: tbl_filled_opts.clone(),
                        col_choice: tbl_col_choice,
                        column_list: tbl_col_list.clone(),
                        is_partition: true,
                    },
                );
                cols_info_map.insert(physical_id, tbl_cols_info.clone());
                continue;
            }
            let (par_saved_opts, par_saved_col_choice, par_saved_col_list) = self.get_saved_analyze_opts(physical_id, tbl)?;
            // Merge partition level options with table level options first.
            let saved_opts = merge_analyze_options(&par_saved_opts, &tbl_saved_opts);
            let (saved_col_choice, saved_col_list) =
                pick_column_list(par_saved_col_choice, &par_saved_col_list, tbl_saved_col_choice, &tbl_saved_col_list);
            // Then merge statement level options.
            let merged_opts = merge_analyze_options(&ast_opts, &saved_opts);
            let filled_merged_opts = fill_analyze_options(&merged_opts);
            let (final_col_choice, merged_col_list) =
                pick_column_list(ast_col_choice, &ast_col_list, saved_col_choice, &saved_col_list);
            let (final_cols_info, final_col_list) = self.get_full_analyze_columns_info(
                tbl,
                final_col_choice,
                &merged_col_list,
                predicate_cols,
                must_analyzed_cols,
                must_all_columns,
                false,
                vars,
            )?;
            options_map.insert(
                physical_id,
                V2AnalyzeOptions {
                    phy_table_id: physical_id,
                    raw_opts: merged_opts,
                    filled_opts: filled_merged_opts,
                    col_choice: final_col_choice,
                    column_list: final_col_list,
                    is_partition: false,
                },
            );
            cols_info_map.insert(physical_id, final_cols_info);
        }
        Ok((options_map, cols_info_map))
    }

    /// Go `getSavedAnalyzeOpts`: the options saved in
    /// `mysql.analyze_options` for a physical table.
    fn get_saved_analyze_opts(
        &self,
        physical_id: i64,
        tbl: &TableNameW,
    ) -> Result<(AnalyzeOpts, ColumnChoice, Vec<GoShared<ColumnInfo>>), PlanError> {
        let mut analyze_options = AnalyzeOpts::new();
        let Some(session) = self.session_ctx else {
            return Ok((analyze_options, ColumnChoice::Default, Vec::new()));
        };
        let rows = session.exec_restricted_sql(
            "select sample_num,sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id = %?",
            &[tidb_sqlescape::SqlArg::Signed(physical_id)],
        )?;
        let Some(row) = rows.first() else {
            return Ok((analyze_options, ColumnChoice::Default, Vec::new()));
        };
        let int64 = |index: usize| row.get(index).map_or(0, tidb_datatype::Datum::get_int64);
        let sample_num = int64(0);
        if sample_num > 0 {
            analyze_options.insert(AnalyzeOptionType::NumSamples, sample_num as u64);
        }
        let sample_rate = match row.get(1) {
            Some(tidb_datatype::Datum::Real(value)) => *value,
            _ => 0.0,
        };
        if sample_rate > 0.0 {
            analyze_options.insert(AnalyzeOptionType::SampleRate, sample_rate.to_bits());
        }
        let buckets = int64(2);
        if buckets > 0 {
            analyze_options.insert(AnalyzeOptionType::NumBuckets, buckets as u64);
        }
        let topn = int64(3);
        if topn >= 0 {
            analyze_options.insert(AnalyzeOptionType::NumTopN, topn as u64);
        }
        let col_type: &[u8] = match row.get(4) {
            Some(tidb_datatype::Datum::Enum(value, _)) => value.name().as_bytes(),
            _ => b"",
        };
        match col_type {
            b"ALL" => Ok((analyze_options, ColumnChoice::All, tbl.table_info.read().columns.iter_deref().collect())),
            b"LIST" => {
                let col_id_strs = row.get(5).and_then(tidb_datatype::Datum::as_raw_bytes).unwrap_or_default();
                let table = tbl.table_info.read();
                let col_list = String::from_utf8_lossy(col_id_strs)
                    .split(',')
                    .filter_map(|col_id_str| {
                        let col_id = col_id_str.parse::<i64>().unwrap_or(0);
                        table.columns.iter_deref().find(|col| col.read().id == col_id)
                    })
                    .collect();
                Ok((analyze_options, ColumnChoice::List, col_list))
            }
            b"PREDICATE" => Ok((analyze_options, ColumnChoice::Predicate, Vec::new())),
            _ => Ok((analyze_options, ColumnChoice::Default, Vec::new())),
        }
    }
}

/// Go `getAnalyzeColumnList`.
fn get_analyze_column_list(specified_columns: &[String], tbl: &TableNameW) -> Result<Vec<GoShared<ColumnInfo>>, PlanError> {
    let table = tbl.table_info.read();
    specified_columns
        .iter()
        .map(|col_name| {
            table
                .columns
                .iter_deref()
                .find(|col| col.read().name.lowercase() == tidb_hack::go_to_lower(col_name))
                .ok_or_else(|| {
                    PlanError::from(
                        tidb_plannererrors::ERR_ANALYZE_MISS_COLUMN
                            .gen_with_stack_by_args(&[col_name.as_str().into(), table.name.original().into()]),
                    )
                })
        })
        .collect()
}

/// Go `generateIndexTasks` for a special global index: one task on the
/// logical table, named after the statement's first table
/// (`as.TableNames[0]`).
fn generate_index_tasks(
    idx: &GoShared<IndexInfo>,
    tbl: &TableNameW,
    first_table_name: &TableNameW,
    version: i64,
) -> Vec<AnalyzeIndexTask> {
    let table_id = tbl.table_info.read().id;
    vec![AnalyzeIndexTask {
        index_info: idx.clone(),
        tbl_info: tbl.table_info.clone(),
        analyze_info: AnalyzeInfo {
            db_name: first_table_name.schema.clone(),
            table_name: first_table_name.name.clone(),
            partition_name: String::new(),
            table_id: AnalyzeTableID { table_id, partition_id: NON_PARTITION_TABLE_ID },
            stats_version: version,
            v2_options: None,
        },
    }]
}

#[cfg(test)]
mod tests {
    use tidb_ast::CiString;
    use tidb_model::partition::{PartitionDefinition, PartitionInfo};
    use tidb_model::{GoShared, TableInfo};

    use super::{
        fill_analyze_options, get_physical_ids_and_partition_names, handle_analyze_options, merge_analyze_options,
        AnalyzeOptionType, AnalyzeOpts, CMSKETCH_SIZE_LIMIT,
    };

    fn option(value: &str, kind: tidb_ast::AnalyzeOptionKind) -> tidb_ast::AnalyzeOption {
        tidb_ast::AnalyzeOption { value: value.to_owned(), kind }
    }

    /// Go `mergeAnalyzeOptions` then `fillAnalyzeOptions`: a statement option
    /// wins, a saved one fills in, the defaults fill the rest.
    #[test]
    fn statement_options_merge_over_saved_before_defaults() {
        let saved = AnalyzeOpts::from([
            (AnalyzeOptionType::NumBuckets, 7),
            (AnalyzeOptionType::NumTopN, 0),
            (AnalyzeOptionType::SampleRate, 0.25f64.to_bits()),
        ]);
        let statement = AnalyzeOpts::from([(AnalyzeOptionType::NumBuckets, 11), (AnalyzeOptionType::NumSamples, 9)]);
        let merged = merge_analyze_options(&statement, &saved);
        assert_eq!(merged.get(&AnalyzeOptionType::NumBuckets), Some(&11));
        assert_eq!(merged.get(&AnalyzeOptionType::NumTopN), Some(&0));
        assert_eq!(merged.get(&AnalyzeOptionType::NumSamples), Some(&9));
        assert_eq!(merged.get(&AnalyzeOptionType::SampleRate), Some(&0.25f64.to_bits()));
        let filled = fill_analyze_options(&AnalyzeOpts::new());
        assert_eq!(filled.get(&AnalyzeOptionType::CmSketchWidth), Some(&2048));
        assert_eq!(filled.get(&AnalyzeOptionType::SampleRate), Some(&(-1f64).to_bits()));
    }

    /// Go `analyzeOptionLimit`'s CMSketch bound, `kv.TxnEntrySizeLimit /
    /// binary.MaxVarintLen32`.
    #[test]
    fn legacy_cms_options_keep_go_size_limit() {
        assert_eq!(CMSKETCH_SIZE_LIMIT, 6 * 1024 * 1024 / 5);
        for kind in [tidb_ast::AnalyzeOptionKind::CmSketchWidth, tidb_ast::AnalyzeOptionKind::CmSketchDepth] {
            handle_analyze_options(&[option(&CMSKETCH_SIZE_LIMIT.to_string(), kind)]).expect("the boundary is accepted");
            let error = handle_analyze_options(&[option(&(CMSKETCH_SIZE_LIMIT + 1).to_string(), kind)])
                .expect_err("one above the boundary is refused");
            assert!(error.message().contains(&CMSKETCH_SIZE_LIMIT.to_string()));
        }
        assert_eq!(
            handle_analyze_options(&[
                option("10", tidb_ast::AnalyzeOptionKind::Samples),
                option("0.5", tidb_ast::AnalyzeOptionKind::SampleRate),
            ])
            .unwrap_err()
            .message(),
            "You can only either set the value of the sample num or set the value of the sample rate. Don't set both of them"
        );
    }

    /// Go `GetPhysicalIDsAndPartitionNames`: every partition in definition
    /// order, or the named ones in the order named.
    #[test]
    fn physical_ids_follow_definition_and_requested_order() {
        let table = TableInfo {
            id: 10,
            name: CiString::new("t"),
            partition: Some(GoShared::new(PartitionInfo {
                enable: true,
                definitions: vec![
                    PartitionDefinition { id: 11, name: CiString::new("p0"), ..PartitionDefinition::default() },
                    PartitionDefinition { id: 12, name: CiString::new("p1"), ..PartitionDefinition::default() },
                ]
                .into(),
                ..PartitionInfo::default()
            })),
            ..TableInfo::default()
        };
        assert_eq!(
            get_physical_ids_and_partition_names(&table, &[]).unwrap(),
            (vec![11, 12], vec!["p0".to_owned(), "p1".to_owned()])
        );
        assert_eq!(
            get_physical_ids_and_partition_names(&table, &["P1".to_owned(), "p0".to_owned()]).unwrap().0,
            vec![12, 11]
        );
        assert_eq!(
            get_physical_ids_and_partition_names(&table, &["missing".to_owned()]).unwrap_err().message(),
            "can not found the specified partition name missing in the table definition"
        );
        let plain = TableInfo { id: 20, name: CiString::new("plain"), ..TableInfo::default() };
        assert_eq!(get_physical_ids_and_partition_names(&plain, &[]).unwrap(), (vec![20], vec![String::new()]));
        assert_eq!(
            get_physical_ids_and_partition_names(&plain, &["p0".to_owned()]).unwrap_err().message(),
            "Partition management on a not partitioned table is not possible"
        );
    }
}
