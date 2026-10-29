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

//! Go `pkg/planner/core/planbuilder.go`'s SHOW output schemas:
//! `buildShowSchema`, `convert2OutputSchemasAndNames`, `buildColumnWithName`
//! and the fixed schemas `buildShowSchema` delegates to.

use tidb_ast::{Expr, ShowStmt, ShowStmtType};
use tidb_charset::Collation;
use tidb_datatype::{FieldNameMetadata, IdentifierMetadata};
use tidb_datatype::{FieldName, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::column::Column;
use tidb_expr::schema::Schema;

/// Go `metadef.InformationSchemaName`, the `DBName` of every
/// `buildColumnWithName` field.
const INFORMATION_SCHEMA: &str = "INFORMATION_SCHEMA";

/// Go `mysql.MaxDatabaseNameLength`, `MaxTableNameLength` and
/// `MaxColumnNameLength`.
const MAX_NAME_LENGTH: i64 = 64;

/// Go `columnsWithNames`: columns and their names, appended in step.
#[derive(Default)]
struct ColumnsWithNames {
    cols: Vec<Column>,
    names: Vec<FieldName>,
}

impl ColumnsWithNames {
    /// Go `columnsWithNames.Append(col, name)`.
    fn append(&mut self, (col, name): (Column, FieldName)) {
        self.cols.push(col);
        self.names.push(name);
    }

    /// Go `col2Schema()` with its `names`.
    fn into_schema(self) -> (Schema, Vec<FieldName>) {
        (Schema::new(self.cols), self.names)
    }
}

/// Go `types.DefaultCharsetForType`.
fn default_collation_for_type(tp: FieldTypeCode) -> Collation {
    match tp {
        FieldTypeCode::VarString
        | FieldTypeCode::String
        | FieldTypeCode::Varchar
        | FieldTypeCode::Enum
        | FieldTypeCode::Set
        | FieldTypeCode::Json => Collation::Utf8Mb4Bin,
        _ => Collation::Binary,
    }
}

/// Go `buildColumnWithName(tableName, name, tp, size)`: a zero `FieldType`
/// with the type, its default charset (utf8mb4 for VARCHAR and BLOB), the
/// given length, and `UnsignedFlag` on everything but VARCHAR and BLOB.
fn build_column_with_name(
    table_name: &str,
    name: &str,
    tp: FieldTypeCode,
    size: i64,
) -> (Column, FieldName) {
    let (collation, flags) = if matches!(tp, FieldTypeCode::Varchar | FieldTypeCode::Blob) {
        (Collation::Utf8Mb4Bin, 0)
    } else {
        (default_collation_for_type(tp), FieldTypeFlags::UNSIGNED)
    };
    let mut field_type = FieldType::new(tp);
    field_type.set_collation(collation);
    field_type.set_flen(size);
    field_type.set_decimal(0);
    field_type.set_flags(flags);
    let name = FieldName::new(FieldNameMetadata {
        database: IdentifierMetadata::new(INFORMATION_SCHEMA),
        table: IdentifierMetadata::new(table_name),
        column: IdentifierMetadata::new(name),
        ..FieldNameMetadata::default()
    });
    (Column::new(0, field_type), name)
}

fn fixed_schema(table_name: &str, columns: &[(&str, FieldTypeCode, i64)]) -> (Schema, Vec<FieldName>) {
    let mut schema = ColumnsWithNames::default();
    for &(name, tp, size) in columns {
        schema.append(build_column_with_name(table_name, name, tp, size));
    }
    schema.into_schema()
}

/// Go `buildShowProcedureSchema`.
fn build_show_procedure_schema() -> (Schema, Vec<FieldName>) {
    use FieldTypeCode::{Blob, Datetime, Varchar};
    fixed_schema(
        "ROUTINES",
        &[
            ("Db", Varchar, 128),
            ("Name", Varchar, 128),
            ("Type", Varchar, 128),
            ("Definer", Varchar, 128),
            ("Modified", Datetime, 19),
            ("Created", Datetime, 19),
            ("Security_type", Varchar, 128),
            ("Comment", Blob, 196_605),
            ("character_set_client", Varchar, 32),
            ("collation_connection", Varchar, 32),
            ("Database Collation", Varchar, 32),
        ],
    )
}

/// Go `buildShowTriggerSchema`.
fn build_show_trigger_schema() -> (Schema, Vec<FieldName>) {
    use FieldTypeCode::{Blob, Datetime, Varchar};
    fixed_schema(
        "TRIGGERS",
        &[
            ("Trigger", Varchar, 128),
            ("Event", Varchar, 128),
            ("Table", Varchar, 128),
            ("Statement", Blob, 196_605),
            ("Timing", Varchar, 128),
            ("Created", Datetime, 19),
            ("sql_mode", Blob, 8192),
            ("Definer", Varchar, 128),
            ("character_set_client", Varchar, 32),
            ("collation_connection", Varchar, 32),
            ("Database Collation", Varchar, 32),
        ],
    )
}

/// Go `buildShowEventsSchema`.
fn build_show_events_schema() -> (Schema, Vec<FieldName>) {
    use FieldTypeCode::{Datetime, Int24, Varchar};
    fixed_schema(
        "EVENTS",
        &[
            ("Db", Varchar, 128),
            ("Name", Varchar, 128),
            ("Time zone", Varchar, 32),
            ("Definer", Varchar, 128),
            ("Type", Varchar, 128),
            ("Execute At", Datetime, 19),
            ("Interval Value", Varchar, 128),
            ("Interval Field", Varchar, 128),
            ("Starts", Datetime, 19),
            ("Ends", Datetime, 19),
            ("Status", Varchar, 32),
            ("Originator", Int24, 4),
            ("character_set_client", Varchar, 32),
            ("collation_connection", Varchar, 32),
            ("Database Collation", Varchar, 32),
        ],
    )
}

/// Go `buildShowWarningsSchema`.
fn build_show_warnings_schema() -> (Schema, Vec<FieldName>) {
    use FieldTypeCode::{Long, Varchar};
    fixed_schema(
        "WARNINGS",
        &[("Level", Varchar, 64), ("Code", Long, 19), ("Message", Varchar, 64)],
    )
}

/// Go `buildTableRegionsSchema`.
pub fn build_table_regions_schema() -> (Schema, Vec<FieldName>) {
    use FieldTypeCode::{LongLong, Tiny, Varchar};
    fixed_schema(
        "",
        &[
            ("REGION_ID", LongLong, 4),
            ("START_KEY", Varchar, 64),
            ("END_KEY", Varchar, 64),
            ("LEADER_ID", LongLong, 4),
            ("LEADER_STORE_ID", LongLong, 4),
            ("PEERS", Varchar, 64),
            ("SCATTERING", Tiny, 1),
            ("WRITTEN_BYTES", LongLong, 4),
            ("READ_BYTES", LongLong, 4),
            ("APPROXIMATE_SIZE(MB)", LongLong, 4),
            ("APPROXIMATE_KEYS", LongLong, 4),
            ("SCHEDULING_CONSTRAINTS", Varchar, 256),
            ("SCHEDULING_STATE", Varchar, 16),
        ],
    )
}

/// Go `buildTableDistributionSchema`.
fn build_table_distribution_schema() -> (Schema, Vec<FieldName>) {
    use FieldTypeCode::{LongLong, Varchar};
    fixed_schema(
        "",
        &[
            ("PARTITION_NAME", Varchar, 4),
            ("STORE_ID", LongLong, 4),
            ("STORE_TYPE", Varchar, 64),
            ("REGION_LEADER_COUNT", LongLong, 4),
            ("REGION_PEER_COUNT", LongLong, 4),
            ("REGION_WRITE_BYTES", LongLong, 4),
            ("REGION_WRITE_KEYS", LongLong, 4),
            ("REGION_WRITE_QUERY", LongLong, 4),
            ("REGION_LEADER_READ_BYTES", LongLong, 4),
            ("REGION_LEADER_READ_KEYS", LongLong, 4),
            ("REGION_LEADER_READ_QUERY", LongLong, 4),
            ("REGION_PEER_READ_BYTES", LongLong, 4),
            ("REGION_PEER_READ_KEYS", LongLong, 4),
            ("REGION_PEER_READ_QUERY", LongLong, 4),
        ],
    )
}

/// Go `buildShowNextRowID`.
pub fn build_show_next_row_id() -> (Schema, Vec<FieldName>) {
    use FieldTypeCode::{LongLong, Varchar};
    fixed_schema(
        "",
        &[
            ("DB_NAME", Varchar, MAX_NAME_LENGTH),
            ("TABLE_NAME", Varchar, MAX_NAME_LENGTH),
            ("COLUMN_NAME", Varchar, MAX_NAME_LENGTH),
            ("NEXT_GLOBAL_ROW_ID", LongLong, 4),
            ("ID_TYPE", Varchar, 15),
        ],
    )
}

/// Go `buildShowDDLJobsFields`.
pub fn build_show_ddl_jobs_fields() -> (Schema, Vec<FieldName>) {
    use FieldTypeCode::{Datetime, LongLong, Varchar};
    fixed_schema(
        "",
        &[
            ("JOB_ID", LongLong, 4),
            ("DB_NAME", Varchar, 64),
            ("TABLE_NAME", Varchar, 64),
            ("JOB_TYPE", Varchar, 64),
            ("SCHEMA_STATE", Varchar, 64),
            ("SCHEMA_ID", LongLong, 4),
            ("TABLE_ID", LongLong, 4),
            ("ROW_COUNT", LongLong, 4),
            ("CREATE_TIME", Datetime, 19),
            ("START_TIME", Datetime, 19),
            ("END_TIME", Datetime, 19),
            ("STATE", Varchar, 64),
            ("COMMENTS", Varchar, 65_535),
        ],
    )
}

/// Go `importIntoSchemaNames` / `ImportIntoSchemaFTypes`.
const IMPORT_INTO_SCHEMA: &[(&str, FieldTypeCode)] = {
    use FieldTypeCode::{LongLong, String, Timestamp};
    &[
        ("Job_ID", LongLong),
        ("Group_Key", String),
        ("Data_Source", String),
        ("Target_Table", String),
        ("Table_ID", LongLong),
        ("Phase", String),
        ("Status", String),
        ("Source_File_Size", String),
        ("Imported_Rows", LongLong),
        ("Result_Message", String),
        ("Create_Time", Timestamp),
        ("Start_Time", Timestamp),
        ("End_Time", Timestamp),
        ("Created_By", String),
        ("Last_Update_Time", Timestamp),
        ("Cur_Step", String),
        ("Cur_Step_Processed_Size", String),
        ("Cur_Step_Total_Size", String),
        ("Cur_Step_Progress_Pct", String),
        ("Cur_Step_Speed", String),
        ("Cur_Step_ETA", String),
    ]
};

/// Go `showImportGroupsNames` / `showImportGroupsFTypes`.
const SHOW_IMPORT_GROUPS_SCHEMA: &[(&str, FieldTypeCode)] = {
    use FieldTypeCode::{LongLong, String, Timestamp};
    &[
        ("Group_Key", String),
        ("Total_Jobs", LongLong),
        ("Pending", LongLong),
        ("Running", LongLong),
        ("Completed", LongLong),
        ("Failed", LongLong),
        ("Cancelled", LongLong),
        ("First_Job_Create_Time", Timestamp),
        ("Last_Job_Update_Time", Timestamp),
    ]
};

/// Go `distributionJobsSchemaNames` / `distributionJobsSchedulerFTypes`.
const DISTRIBUTION_JOBS_SCHEMA: &[(&str, FieldTypeCode)] = {
    use FieldTypeCode::{LongLong, String, Timestamp};
    &[
        ("Job_ID", LongLong),
        ("Database", String),
        ("Table", String),
        ("Partition_List", String),
        ("Engine", String),
        ("Rule", String),
        ("Status", String),
        ("Timeout", String),
        ("Create_Time", Timestamp),
        ("Start_Time", Timestamp),
        ("Finish_Time", Timestamp),
    ]
};

/// Go `table.ColDescFieldNames(full)`.
#[must_use]
pub fn col_desc_field_names(full: bool) -> &'static [&'static str] {
    if full {
        &["Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"]
    } else {
        &["Field", "Type", "Null", "Key", "Default", "Extra"]
    }
}

/// Go `extractPatternLikeOrIlikeName`: the pattern's text when it is a
/// literal string, otherwise empty.
fn extract_pattern_like_or_ilike_name(pattern: Option<&Expr>) -> &str {
    match pattern {
        Some(Expr::String(value)) => value,
        _ => "",
    }
}

/// Go `auth.UserIdentity.String()`: the unquoted `user@host`. The
/// preprocessor has already replaced `CURRENT_USER` with the session's
/// authenticated account.
fn user_identity_string(user: &tidb_ast::UserSpec) -> String {
    format!("{}@{}", user.user, user.host)
}

/// Go `convert2OutputSchemasAndNames(names, ftypes, flags)`: VARCHAR unless
/// a type is given, with the type's default length, decimal and charset.
#[must_use]
pub fn convert2_output_schemas_and_names(
    names: &[&str],
    ftypes: &[FieldTypeCode],
    flags: &[u32],
) -> (Schema, Vec<FieldName>) {
    let mut cols = Vec::with_capacity(names.len());
    let mut output_names = Vec::with_capacity(names.len());
    for (index, name) in names.iter().enumerate() {
        output_names.push(FieldName::new(FieldNameMetadata {
            column: IdentifierMetadata::new(*name),
            ..FieldNameMetadata::default()
        }));
        let tp = match ftypes.get(index) {
            Some(tp) if *tp != FieldTypeCode::Unspecified => *tp,
            _ => FieldTypeCode::Varchar,
        };
        let mut field_type = FieldType::new(tp);
        let (flen, decimal) = tp.default_field_length_and_decimal();
        field_type.set_flen(i64::from(flen));
        field_type.set_decimal(i64::from(decimal));
        field_type.set_collation(default_collation_for_type(tp));
        if let Some(flag) = flags.get(index) {
            field_type.set_flags(*flag);
        }
        cols.push(Column::new(0, field_type));
    }
    (Schema::new(cols), output_names)
}

fn typed_schema(columns: &[(&str, FieldTypeCode)]) -> (Schema, Vec<FieldName>) {
    let (names, ftypes): (Vec<&str>, Vec<FieldTypeCode>) = columns.iter().copied().unzip();
    convert2_output_schemas_and_names(&names, &ftypes, &[])
}

/// Go `buildShowSchema(s, isView, isSequence)`.
#[must_use]
pub fn build_show_schema(s: &ShowStmt, is_view: bool, is_sequence: bool) -> (Schema, Vec<FieldName>) {
    use FieldTypeCode::{
        Datetime, Double, Json, Long, LongLong, String as Str, Tiny, Varchar,
    };
    use ShowStmtType as T;
    let typed = |columns: &[(&str, FieldTypeCode)]| typed_schema(columns);
    let untyped = |names: &[&str]| convert2_output_schemas_and_names(names, &[], &[]);
    match s.tp {
        T::BinlogStatus | T::MasterStatus => typed(&[
            ("File", Varchar),
            ("Position", LongLong),
            ("Binlog_Do_DB", Varchar),
            ("Binlog_Ignore_DB", Varchar),
            ("Executed_Gtid_Set", Varchar),
        ]),
        T::ProcedureStatus | T::FunctionStatus => build_show_procedure_schema(),
        T::Triggers => build_show_trigger_schema(),
        T::Events => build_show_events_schema(),
        T::Warnings | T::Errors if s.count_warnings_or_errors => typed(&[("Count", Long)]),
        T::Warnings | T::Errors => build_show_warnings_schema(),
        T::Regions => build_table_regions_schema(),
        T::Distributions => build_table_distribution_schema(),
        T::Engines => untyped(&["Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"]),
        T::Config => untyped(&["Type", "Instance", "Name", "Value"]),
        T::Databases => {
            let pattern = extract_pattern_like_or_ilike_name(s.pattern.as_ref());
            let field = if pattern.is_empty() {
                "Database".to_owned()
            } else {
                format!("Database ({pattern})")
            };
            untyped(&[&field])
        }
        T::OpenTables => typed(&[
            ("Database", Varchar),
            ("Table", Varchar),
            ("In_use", Long),
            ("Name_locked", Long),
        ]),
        T::Tables => {
            let pattern = extract_pattern_like_or_ilike_name(s.pattern.as_ref());
            let mut field = format!("Tables_in_{}", s.db_name);
            if !pattern.is_empty() {
                field = format!("{field} ({pattern})");
            }
            if s.full {
                untyped(&[&field, "Table_type"])
            } else {
                untyped(&[&field])
            }
        }
        T::TableStatus => typed(&[
            ("Name", Varchar),
            ("Engine", Varchar),
            ("Version", LongLong),
            ("Row_format", Varchar),
            ("Rows", LongLong),
            ("Avg_row_length", LongLong),
            ("Data_length", LongLong),
            ("Max_data_length", LongLong),
            ("Index_length", LongLong),
            ("Data_free", LongLong),
            ("Auto_increment", LongLong),
            ("Create_time", Datetime),
            ("Update_time", Datetime),
            ("Check_time", Datetime),
            ("Collation", Varchar),
            ("Checksum", Varchar),
            ("Create_options", Varchar),
            ("Comment", Varchar),
        ]),
        T::Columns => untyped(col_desc_field_names(s.full)),
        T::Charset => typed(&[
            ("Charset", Varchar),
            ("Description", Varchar),
            ("Default collation", Varchar),
            ("Maxlen", LongLong),
        ]),
        T::Variables | T::Status => untyped(&["Variable_name", "Value"]),
        T::Collation => convert2_output_schemas_and_names(
            &["Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"],
            &[Varchar, Varchar, LongLong, Varchar, Varchar, LongLong, Varchar],
            &[0, 0, FieldTypeFlags::UNSIGNED | FieldTypeFlags::NOT_NULL, 0, 0, 0, 0],
        ),
        T::CreateTable | T::CreateSequence => {
            if is_sequence {
                untyped(&["Sequence", "Create Sequence"])
            } else if is_view {
                untyped(&["View", "Create View", "character_set_client", "collation_connection"])
            } else {
                untyped(&["Table", "Create Table"])
            }
        }
        T::CreatePlacementPolicy => untyped(&["Policy", "Create Policy"]),
        T::CreateResourceGroup => untyped(&["Resource_Group", "Create Resource Group"]),
        T::CreateUser => match &s.user {
            Some(user) => untyped(&[&format!("CREATE USER for {}", user_identity_string(user))]),
            None => untyped(&[]),
        },
        T::CreateView => {
            untyped(&["View", "Create View", "character_set_client", "collation_connection"])
        }
        T::CreateDatabase => untyped(&["Database", "Create Database"]),
        T::Grants => match &s.user {
            Some(user) => untyped(&[&format!("Grants for {}", user_identity_string(user))]),
            // Go: "Don't know the name yet, so just say "user"".
            None => untyped(&["Grants for User"]),
        },
        T::Index => typed(&[
            ("Table", Varchar),
            ("Non_unique", LongLong),
            ("Key_name", Varchar),
            ("Seq_in_index", LongLong),
            ("Column_name", Varchar),
            ("Collation", Varchar),
            ("Cardinality", LongLong),
            ("Sub_part", LongLong),
            ("Packed", Varchar),
            ("Null", Varchar),
            ("Index_type", Varchar),
            ("Comment", Varchar),
            ("Index_comment", Varchar),
            ("Visible", Varchar),
            ("Expression", Varchar),
            ("Clustered", Varchar),
            ("Global", Varchar),
        ]),
        T::Plugins => typed(&[
            ("Name", Varchar),
            ("Status", Varchar),
            ("Type", Varchar),
            ("Library", Varchar),
            ("License", Varchar),
            ("Version", Varchar),
        ]),
        T::ProcessList => typed(&[
            ("Id", LongLong),
            ("User", Varchar),
            ("Host", Varchar),
            ("db", Varchar),
            ("Command", Varchar),
            ("Time", Long),
            ("State", Varchar),
            ("Info", Str),
        ]),
        T::StatsMeta => typed(&[
            ("Db_name", Varchar),
            ("Table_name", Varchar),
            ("Partition_name", Varchar),
            ("Update_time", Datetime),
            ("Modify_count", LongLong),
            ("Row_count", LongLong),
            ("Last_analyze_time", Datetime),
        ]),
        T::StatsExtended => typed(&[
            ("Db_name", Varchar),
            ("Table_name", Varchar),
            ("Stats_name", Varchar),
            ("Column_names", Varchar),
            ("Stats_type", Varchar),
            ("Stats_val", Varchar),
            ("Last_update_version", LongLong),
        ]),
        T::StatsHistograms => typed(&[
            ("Db_name", Varchar),
            ("Table_name", Varchar),
            ("Partition_name", Varchar),
            ("Column_name", Varchar),
            ("Is_index", Tiny),
            ("Update_time", Datetime),
            ("Distinct_count", LongLong),
            ("Null_count", LongLong),
            ("Avg_col_size", Double),
            ("Correlation", Double),
            ("Load_status", Varchar),
            ("Total_mem_usage", LongLong),
            ("Hist_mem_usage", LongLong),
            ("Topn_mem_usage", LongLong),
            ("Cms_mem_usage", LongLong),
        ]),
        T::StatsBuckets => typed(&[
            ("Db_name", Varchar),
            ("Table_name", Varchar),
            ("Partition_name", Varchar),
            ("Column_name", Varchar),
            ("Is_index", Tiny),
            ("Bucket_id", LongLong),
            ("Count", LongLong),
            ("Repeats", LongLong),
            ("Lower_Bound", Varchar),
            ("Upper_Bound", Varchar),
            ("Ndv", LongLong),
        ]),
        T::StatsTopN => typed(&[
            ("Db_name", Varchar),
            ("Table_name", Varchar),
            ("Partition_name", Varchar),
            ("Column_name", Varchar),
            ("Is_index", Tiny),
            ("Value", Varchar),
            ("Count", LongLong),
        ]),
        T::StatsHealthy => typed(&[
            ("Db_name", Varchar),
            ("Table_name", Varchar),
            ("Partition_name", Varchar),
            ("Healthy", LongLong),
        ]),
        T::HistogramsInFlight => typed(&[("HistogramsInFlight", LongLong)]),
        T::ColumnStatsUsage => typed(&[
            ("Db_name", Varchar),
            ("Table_name", Varchar),
            ("Partition_name", Varchar),
            ("Column_name", Varchar),
            ("Last_used_at", Datetime),
            ("Last_analyzed_at", Datetime),
        ]),
        T::StatsLocked => typed(&[
            ("Db_name", Varchar),
            ("Table_name", Varchar),
            ("Partition_name", Varchar),
            ("Status", Varchar),
        ]),
        T::Profiles => typed(&[("Query_ID", Long), ("Duration", Double), ("Query", Varchar)]),
        T::Privileges => typed(&[("Privilege", Varchar), ("Context", Varchar), ("Comment", Varchar)]),
        T::Bindings => typed(&[
            ("Original_sql", Varchar),
            ("Bind_sql", Varchar),
            ("Default_db", Varchar),
            ("Status", Varchar),
            ("Create_time", Datetime),
            ("Update_time", Datetime),
            ("Charset", Varchar),
            ("Collation", Varchar),
            ("Source", Varchar),
            ("Sql_digest", Varchar),
            ("Plan_digest", Varchar),
        ]),
        T::BindingCacheStatus => typed(&[
            ("bindings_in_cache", LongLong),
            ("bindings_in_table", LongLong),
            ("memory_usage", Varchar),
            ("memory_quota", Varchar),
        ]),
        T::AnalyzeStatus => typed(&[
            ("Table_schema", Varchar),
            ("Table_name", Varchar),
            ("Partition_name", Varchar),
            ("Job_info", Varchar),
            ("Processed_rows", LongLong),
            ("Start_time", Datetime),
            ("End_time", Datetime),
            ("State", Varchar),
            ("Fail_reason", Varchar),
            ("Instance", Varchar),
            ("Process_ID", LongLong),
            ("Remaining_seconds", Varchar),
            ("Progress", Double),
            ("Estimated_total_rows", LongLong),
        ]),
        T::Builtins => typed(&[("Supported_builtin_functions", Varchar)]),
        T::Backups | T::Restores => typed(&[
            ("Id", LongLong),
            ("Destination", Varchar),
            ("State", Varchar),
            ("Progress", Double),
            ("Queue_time", Datetime),
            ("Execution_time", Datetime),
            ("Finish_time", Datetime),
            ("Connection", LongLong),
            ("Message", Varchar),
        ]),
        T::PlacementLabels => typed(&[("Key", Varchar), ("Values", Json)]),
        T::Placement
        | T::PlacementForDatabase
        | T::PlacementForTable
        | T::PlacementForPartition => typed(&[
            ("Target", Varchar),
            ("Placement", Varchar),
            ("Scheduling_State", Varchar),
        ]),
        T::SessionStates => typed(&[("Session_states", Json), ("Session_token", Json)]),
        T::ImportJobs => typed(IMPORT_INTO_SCHEMA),
        T::ImportGroups => typed(SHOW_IMPORT_GROUPS_SCHEMA),
        T::DistributionJobs => typed(DISTRIBUTION_JOBS_SCHEMA),
        T::Affinity => typed(&[
            ("Db_name", Varchar),
            ("Table_name", Varchar),
            ("Partition_name", Varchar),
            ("Leader_store_id", LongLong),
            ("Voter_store_ids", Varchar),
            ("Status", Varchar),
            ("Region_count", LongLong),
            ("Affinity_region_count", LongLong),
        ]),
        // Go's switch has no case for these; its `names` stay empty.
        T::None
        | T::CreateImport
        | T::CreateProcedure
        | T::MaskingPolicies
        | T::Profile
        | T::TableNextRowId
        | T::ReplicaStatus
        | T::Imports => untyped(&[]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column_names(names: &[FieldName]) -> Vec<String> {
        names.iter().map(|name| name.names.column.original.clone()).collect()
    }

    /// Go `buildShowSchema`'s ShowTables arm: the database and a literal
    /// LIKE pattern name the column; FULL adds `Table_type`.
    #[test]
    fn show_tables_names_its_column_after_database_and_pattern() {
        let mut show = ShowStmt::new(ShowStmtType::Tables);
        show.db_name = "test".to_owned();
        show.pattern = Some(Expr::String("t%".to_owned()));
        show.full = true;
        let (schema, names) = build_show_schema(&show, false, false);
        assert_eq!(column_names(&names), ["Tables_in_test (t%)", "Table_type"]);
        assert_eq!(schema.columns.len(), 2);
    }

    /// Go `convert2OutputSchemasAndNames` for SHOW COLLATION: `Id` is
    /// BIGINT(20) UNSIGNED NOT NULL, the rest VARCHAR with Go's defaults.
    #[test]
    fn show_collation_types_follow_go_defaults_and_flags() {
        let (schema, names) = build_show_schema(&ShowStmt::new(ShowStmtType::Collation), false, false);
        assert_eq!(names.len(), 7);
        let id = schema.columns[2].ret_type.as_ref().unwrap();
        assert_eq!(id.code(), FieldTypeCode::LongLong);
        assert_eq!(id.flen(), 20);
        assert_eq!(id.flags(), FieldTypeFlags::UNSIGNED | FieldTypeFlags::NOT_NULL);
        let name = schema.columns[0].ret_type.as_ref().unwrap();
        assert_eq!(name.code(), FieldTypeCode::Varchar);
        assert_eq!(name.collation(), Collation::Utf8Mb4Bin);
    }

    /// Go `buildShowWarningsSchema` and the COUNT(*) form.
    #[test]
    fn show_warnings_schema_and_its_count_form() {
        let mut show = ShowStmt::new(ShowStmtType::Warnings);
        let (schema, names) = build_show_schema(&show, false, false);
        assert_eq!(column_names(&names), ["Level", "Code", "Message"]);
        let code = schema.columns[1].ret_type.as_ref().unwrap();
        assert_eq!((code.code(), code.flen(), code.flags()), (FieldTypeCode::Long, 19, FieldTypeFlags::UNSIGNED));
        assert_eq!(names[0].names.table.original, "WARNINGS");
        show.count_warnings_or_errors = true;
        let (_, names) = build_show_schema(&show, false, false);
        assert_eq!(column_names(&names), ["Count"]);
    }

    /// Go's SHOW CREATE TABLE header follows the object, not the keyword.
    #[test]
    fn show_create_table_header_follows_the_object_kind() {
        let show = ShowStmt::new(ShowStmtType::CreateTable);
        let names = |view, sequence| column_names(&build_show_schema(&show, view, sequence).1);
        assert_eq!(names(false, false), ["Table", "Create Table"]);
        assert_eq!(names(true, false), ["View", "Create View", "character_set_client", "collation_connection"]);
        assert_eq!(names(false, true), ["Sequence", "Create Sequence"]);
    }

    /// Go `resolveShowStmt` feeds `buildShowSchema`: the current database
    /// names SHOW TABLES' column, a table's schema wins over it, and
    /// `CURRENT_USER` prints as the authenticated account.
    #[test]
    fn resolve_show_stmt_fills_database_and_current_user() {
        let mut tables = ShowStmt::new(ShowStmtType::Tables);
        crate::preprocess::resolve_show_stmt(&mut tables, "test", None).unwrap();
        assert_eq!(column_names(&build_show_schema(&tables, false, false).1), ["Tables_in_test"]);

        let mut index = ShowStmt::new(ShowStmtType::Index);
        index.table = Some(vec!["d1".to_owned(), "t".to_owned()]);
        crate::preprocess::resolve_show_stmt(&mut index, "test", None).unwrap();
        assert_eq!(index.db_name, "d1");

        let mut columns = ShowStmt::new(ShowStmtType::Columns);
        columns.db_name = "d2".to_owned();
        columns.table = Some(vec!["t".to_owned()]);
        crate::preprocess::resolve_show_stmt(&mut columns, "test", None).unwrap();
        assert_eq!(columns.table, Some(vec!["d2".to_owned(), "t".to_owned()]));

        let mut grants = ShowStmt::new(ShowStmtType::Grants);
        grants.user = Some(tidb_ast::UserSpec {
            current_user: true,
            user: String::new(),
            host: String::new(),
        });
        crate::preprocess::resolve_show_stmt(&mut grants, "", Some(("u1", "%"))).unwrap();
        assert_eq!(column_names(&build_show_schema(&grants, false, false).1), ["Grants for u1@%"]);
    }
}
