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

//! Go `pkg/ddl/storage_class.go`, complete: the JSON schema, validation and
//! table/partition assignment logic for the `STORAGE_CLASS` table option
//! (which is syntax sugar over `ENGINE_ATTRIBUTE`'s `storage_class` field),
//! plus four small helpers this file leans on from `pkg/ddl/partition.go`:
//! `findColumnByName`, `getRangeValue`, `isPartExprUnsigned` and
//! `parseAndEvalBoolExpr`.
//!
//! This is a COMPLETE port of `storage_class.go`'s 21 functions (every
//! production symbol in the file), plus the four named `partition.go`
//! helpers -- it is NOT a port of `partition.go` itself, which is a
//! multi-thousand-line file with its own much larger unit. Both files remain
//! SEEDs of the enclosing `pkg/ddl` package.
//!
//! # Narrowings (named, so a reader can grep for them)
//!
//! - `// boundary:` `util/logutil`. Go logs one line (`logutil.BgLogger().
//!   Info(...)`) each time a table's or partition's storage-class tier is
//!   set. No logging sink is ported to this crate; [`set_storage_class_for_table`]
//!   and [`set_storage_class_tier_for_partition`] just perform the mutation.
//! - `// boundary:` `util/intest`. Go's `intest.Assert(len(namesIn) > 0)` in
//!   `isPartitionMatchNamesIn`/`isPartitionMatchValuesIn` is a debug-only
//!   invariant check (compiled out in release builds); this port's callers
//!   already only reach those functions with a non-empty scope, so the
//!   assertion is dropped rather than translated to a `debug_assert!`.
//! - `// boundary:` `ast.AlterTableSpec` is not ported in this workspace
//!   (only `ast.TableOption` is). Go's `CheckStorageClassConflictInAlterTableSpecs`
//!   takes `[]*ast.AlterTableSpec` and internally skips every spec whose
//!   `Tp != ast.AlterTableOption`. [`check_storage_class_conflict_in_alter_table_specs`]
//!   is narrowed to take the ALREADY-FILTERED `Options` slices of exactly
//!   those specs -- the filter is the caller's job until `AlterTableSpec`
//!   lands. Every case in the ported Go test (`TestCheckStorageClassConflictInAlterTableSpecs`)
//!   only ever uses `Tp: ast.AlterTableOption` specs, so no test coverage is
//!   lost by this narrowing.
//! - `// boundary:` `exprstatic.NewExprContext()`. `compareNumericRangePartitionValues`,
//!   `compareRangeColumnsPartitionValues` and `isPartExprUnsigned` each build
//!   one in Go and pass it as the `expression.BuildContext`/`EvalContext` for
//!   a purely CONSTANT (no live session) expression build+eval. This port
//!   uses [`tidb_expr::rewriter::NoResolver`] instead of porting
//!   `exprstatic::ExprContext` through a [`tidb_expr::rewriter::ColumnResolver`]
//!   adapter: `NoResolver` already reproduces the session-less UTC default
//!   Go's own `NewExprContext()` falls back to, and no ported test exercises
//!   session state (timezone, sql_mode, ...) through these three functions.
//! - `// boundary:` `tables.NewPartitionExprBuildCtx()` (`pkg/table/tables`,
//!   not ported). `isPartExprUnsigned` uses it only as a `ColumnResolver`
//!   with no observable behavior beyond name resolution against the table's
//!   own columns, which this port already gets from
//!   [`tidb_expr::simple_expr::BuildOptions::with_table_info`]; `NoResolver`
//!   stands in for the outer (session) context exactly as in the note above.
//! - `// boundary:` Go's `NewFunctionBase(ctx, ast.EQ/ast.GT, ...)` +
//!   `SetCharsetAndCollation` + `EvalInt` pair in `parseAndEvalBoolExpr`
//!   hand-builds two comparison `ScalarFunction` nodes under the column's own
//!   charset/collation. This port reaches the same "equal / greater-than
//!   under one collation" outcome through [`tidb_expr::compare_datums_with_collation`]
//!   instead, which is this crate's own tested utility for exactly that
//!   comparison and avoids re-deriving `ScalarFunction` collation plumbing
//!   for a helper NO ported test currently exercises (see below).
//! - `// boundary:` Go's case-folding (`strings.ToUpper`/`strings.ToLower`/
//!   `strings.EqualFold`) is Unicode-aware. This crate is not (yet) a direct
//!   dependency of `tidb-mysql`, whose `to_uppercase`/`to_lowercase` mirror
//!   Go's simple case fold exactly (see `tidb-ast`'s `CiString`); this file
//!   uses `str::to_uppercase`/`to_lowercase`/`eq_ignore_ascii_case` instead.
//!   Every storage-class tier name and partition keyword (`STANDARD`, `IA`,
//!   `MAXVALUE`, `DEFAULT`) is ASCII, so the two foldings agree for every
//!   value this file's own vocabulary can produce; only a caller-supplied
//!   `names_in`/`values_in` entry containing non-ASCII casing could observe
//!   the difference, and no ported test does.
//!
//! # Test coverage and labeling
//!
//! Source: `pkg/ddl/storage_class_test.go`, 9 Go test functions. Seven are
//! ported below, byte-exact on every expected value:
//! `TestBuildStorageClassSettingsFromJSON`, `TestBuildStorageClassForTable`,
//! `TestBuildStorageClassForPartitions`, `TestStorageClassString`,
//! `TestGetEngineAttributeFromStorageClassTableOptions`,
//! `TestCheckStorageClassConflictInAlterTableSpecs`,
//! `TestGetSimpleTableStorageClassForShowCreate`.
//!
//! Two are NOT ported, by name, with the reason recorded here rather than
//! silently dropped:
//!
//! - `TestStorageClassPartitionScopesUseNormalizedValues`
//! - `TestStorageClassPartitionScopesRejectInvalidLessThanValue`
//!
//! Both drive `ddl.BuildTableInfoFromAST` over full `CREATE TABLE` SQL text
//! (`pkg/ddl/table.go`, via `pkg/meta/metabuild`) to get a realistic
//! `*model.TableInfo` with parsed/normalized partition definitions, then
//! assert on `BuildStorageClassForPartitions`'s tier assignment. That
//! `CREATE TABLE` -> `TableInfo` metadata pipeline (column/partition
//! definition building, including `buildRangePartitionDefinitions`'s and
//! `buildListPartitionDefinitions`'s exact `LessThan`/`InValues` string
//! formatting) is an entire separate unit outside `storage_class.go` and the
//! four named `partition.go` helpers -- it is not in this task's dependency
//! list, and porting it here would silently expand the claimed unit. The
//! COMPARISON logic these two tests exercise beyond what
//! `TestBuildStorageClassForPartitions` already covers is
//! `compareRangeColumnsPartitionValues` (RANGE COLUMNS partitions, reached
//! only when `PartitionInfo.Columns` is non-empty) and `isPartExprUnsigned`
//! (reached only when `PartitionInfo.Expr` is non-empty) -- neither of which
//! any surviving `TestBuildStorageClassForPartitions` case exercises either,
//! since none of its directly-constructed `TableInfo`s populate
//! `Partition.Columns`/`Partition.Expr`. Both functions are still ported in
//! full below (`compare_range_columns_partition_values`,
//! `is_part_expr_unsigned`, `find_column_by_name`, `parse_and_eval_bool_expr`,
//! `get_range_value`), just without direct test coverage in this port; a
//! small supplementary regression test
//! (`is_part_expr_unsigned_reads_the_partition_columns_flag`) exercises
//! `is_part_expr_unsigned` directly against a hand-built `TableInfo`,
//! without going through `BuildTableInfoFromAST`.

use std::cmp::Ordering;

use serde_json::Value;

use tidb_ast::{CiString, PartitionType, TableOption};
use tidb_datatype::{unwrap_from_single_quotes, wrap_in_single_quotes, Collation, FieldType};
use tidb_expr::exprctx::SimplePlanColumnIdAllocator;
use tidb_expr::rewriter::NoResolver;
use tidb_expr::simple_expr::{parse_simple_expr, BuildOptions, ColumnInfoSource};
use tidb_expr::{compare_datums_with_collation, eval_expression_once, NoColumns};
use tidb_model::column::ColumnInfo;
use tidb_model::engine_attribute::{
    STORAGE_CLASS_TIER_DEFAULT, STORAGE_CLASS_TIER_IA, STORAGE_CLASS_TIER_STANDARD,
};
use tidb_model::{
    parse_engine_attribute_from_string, GoShared, GoSharedPointerSlice, GoSharedSlice,
    PartitionDefinition, StorageClassDef, StorageClassSettings, StorageClassTransitRule, TableInfo,
};

use crate::table_info_build::DdlAdmissionError;

/// Go `partition.go`'s unexported `partitionMaxValue`, needed by the
/// comparison helpers below.
const PARTITION_MAX_VALUE: &str = "MAXVALUE";

fn storage_class_invalid_spec(msg: impl Into<String>) -> DdlAdmissionError {
    DdlAdmissionError::with_code(
        tidb_error::tidb::errcode::ErrStorageClassInvalidSpec,
        msg.into(),
    )
}

fn not_allowed_type_in_partition(value: &str) -> DdlAdmissionError {
    DdlAdmissionError::with_code(
        tidb_error::tidb::errcode::ErrFieldTypeNotAllowedAsPartitionField,
        format!("not allowed type in partition: '{value}'"),
    )
}

fn err_engine_attribute_and_storage_class_conflict() -> DdlAdmissionError {
    storage_class_invalid_spec("can not specify 'ENGINE_ATTRIBUTE' and 'STORAGE_CLASS' together")
}

/// Go `BuildStorageClassSettingsFromJSON`: builds storage class settings from
/// a JSON object (`None` mirrors Go's nil `json.RawMessage`).
pub fn build_storage_class_settings_from_json(
    input: Option<&[u8]>,
) -> Result<StorageClassSettings, DdlAdmissionError> {
    let Some(input) = input else {
        return Ok(single_def_settings(StorageClassDef {
            tier: STORAGE_CLASS_TIER_DEFAULT.to_owned(),
            ..Default::default()
        }));
    };

    // Try parsing as a string. `serde_json::from_slice` already requires the
    // WHOLE input to be exactly one JSON value (matching Go's
    // `json.Unmarshal`), so a non-string top-level value or trailing garbage
    // both fail here and fall through.
    if let Ok(tier) = serde_json::from_slice::<String>(input) {
        let tier = tier.to_uppercase();
        // Go returns immediately whether `checkTier` succeeds or fails: the
        // string form never falls back to the object/list forms.
        check_tier(&tier)?;
        return Ok(single_def_settings(StorageClassDef {
            tier,
            ..Default::default()
        }));
    }

    // Try parsing as a single object.
    if let Ok(mut def) = decode_storage_class_def_single(input) {
        normalize_storage_class_def(&mut def);
        check_storage_class_def(&def)?;
        return Ok(single_def_settings(def));
    }

    // Try parsing as a list of objects. Go only attempts this branch when the
    // single-object decode failed with a `json.UnmarshalTypeError` (i.e. the
    // top-level JSON value was array-shaped); this port instead always tries
    // both attempts, which is observably equivalent -- a non-array top-level
    // value that could not decode as a single object also cannot decode as
    // an array (`decode_storage_class_def_list` fails on the very first
    // `Value::Array` match), so the net effect (no fallback for non-array
    // inputs) is the same without needing to classify the first failure.
    if let Ok(mut defs) = decode_storage_class_def_list(input) {
        normalize_storage_class_defs(&mut defs)?;
        return Ok(StorageClassSettings {
            defs: GoSharedPointerSlice::from_nullable(defs),
        });
    }

    let preview_len = input.len().min(192);
    Err(storage_class_invalid_spec(format!(
        "invalid storage class def: '{}'",
        String::from_utf8_lossy(&input[..preview_len])
    )))
}

fn single_def_settings(def: StorageClassDef) -> StorageClassSettings {
    StorageClassSettings {
        defs: GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(def))]),
    }
}

/// The JSON object field names `StorageClassDef` accepts. Go's
/// `decoder.DisallowUnknownFields()` rejects any other key.
const STORAGE_CLASS_DEF_FIELDS: [&str; 5] =
    ["tier", "names_in", "less_than", "values_in", "transitions"];
/// The JSON object field names `StorageClassTransitRule` accepts.
const TRANSIT_RULE_FIELDS: [&str; 3] = ["tier", "after_days", "after_seconds"];

fn decode_storage_class_def_single(input: &[u8]) -> Result<StorageClassDef, String> {
    let value: Value = serde_json::from_slice(input).map_err(|e| e.to_string())?;
    decode_storage_class_def_object(&value)
}

fn decode_storage_class_def_list(input: &[u8]) -> Result<Vec<Option<StorageClassDef>>, String> {
    let value: Value = serde_json::from_slice(input).map_err(|e| e.to_string())?;
    let Value::Array(items) = value else {
        return Err("invalid storage class def: expected a JSON array".to_owned());
    };
    items
        .iter()
        .map(|item| match item {
            Value::Null => Ok(None),
            other => decode_storage_class_def_object(other).map(Some),
        })
        .collect()
}

fn decode_storage_class_def_object(value: &Value) -> Result<StorageClassDef, String> {
    let Value::Object(map) = value else {
        return Err("invalid storage class def: expected a JSON object".to_owned());
    };
    for key in map.keys() {
        if !STORAGE_CLASS_DEF_FIELDS.contains(&key.as_str()) {
            return Err(format!("json: unknown field \"{key}\""));
        }
    }

    let tier = match map.get("tier") {
        Some(Value::String(s)) => s.clone(),
        None | Some(Value::Null) => String::new(),
        Some(_) => return Err("'tier' must be a JSON string".to_owned()),
    };
    let names_in = decode_string_array(map.get("names_in"), "names_in")?;
    let less_than = match map.get("less_than") {
        Some(Value::String(s)) => Some(s.clone()),
        None | Some(Value::Null) => None,
        Some(_) => return Err("'less_than' must be a JSON string".to_owned()),
    };
    let values_in = decode_string_array(map.get("values_in"), "values_in")?;
    let transitions = match map.get("transitions") {
        Some(Value::Array(items)) => items
            .iter()
            .map(decode_transit_rule)
            .collect::<Result<Vec<_>, _>>()?,
        None | Some(Value::Null) => Vec::new(),
        Some(_) => return Err("'transitions' must be a JSON array".to_owned()),
    };

    Ok(StorageClassDef {
        tier,
        names_in: GoSharedSlice::from_vec(names_in),
        less_than: less_than.map(GoShared::new),
        values_in: GoSharedSlice::from_vec(values_in),
        transitions: GoSharedSlice::from_vec(transitions),
    })
}

fn decode_string_array(value: Option<&Value>, field: &str) -> Result<Vec<String>, String> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| match item {
                Value::String(s) => Ok(s.clone()),
                // A JSON `null` array element leaves the Go string's zero
                // value (`""`) rather than erroring.
                Value::Null => Ok(String::new()),
                _ => Err(format!("'{field}' elements must be JSON strings")),
            })
            .collect(),
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(_) => Err(format!("'{field}' must be a JSON array")),
    }
}

fn decode_transit_rule(value: &Value) -> Result<StorageClassTransitRule, String> {
    let Value::Object(map) = value else {
        return Err("invalid transition rule: expected a JSON object".to_owned());
    };
    for key in map.keys() {
        if !TRANSIT_RULE_FIELDS.contains(&key.as_str()) {
            return Err(format!("json: unknown field \"{key}\""));
        }
    }
    let tier = match map.get("tier") {
        Some(Value::String(s)) => s.clone(),
        None | Some(Value::Null) => String::new(),
        Some(_) => return Err("'tier' must be a JSON string".to_owned()),
    };
    let after_days = decode_u64_field(map.get("after_days"), "after_days")?;
    let after_seconds = decode_u64_field(map.get("after_seconds"), "after_seconds")?;
    Ok(StorageClassTransitRule {
        tier,
        after_days,
        after_seconds,
    })
}

fn decode_u64_field(value: Option<&Value>, field: &str) -> Result<u64, String> {
    match value {
        Some(Value::Number(n)) => n
            .as_u64()
            .ok_or_else(|| format!("'{field}' must be a non-negative integer")),
        None | Some(Value::Null) => Ok(0),
        Some(_) => Err(format!("'{field}' must be a JSON number")),
    }
}

fn normalize_storage_class_defs(
    defs: &mut [Option<StorageClassDef>],
) -> Result<(), DdlAdmissionError> {
    for def in defs.iter_mut() {
        let Some(def) = def else {
            return Err(storage_class_invalid_spec(
                "storage class def must not be null",
            ));
        };
        normalize_storage_class_def(def);
        check_storage_class_def(def)?;
    }
    Ok(())
}

fn normalize_storage_class_def(def: &mut StorageClassDef) {
    def.tier = def.tier.to_uppercase();
    for i in 0..def.names_in.len() {
        let lowered = def.names_in.get(i).to_lowercase();
        def.names_in.set(i, lowered);
    }
    for i in 0..def.transitions.len() {
        def.transitions
            .update(i, |rule| rule.tier = rule.tier.to_uppercase());
    }
}

fn normalize_storage_class_tier(tier: &str) -> Result<String, DdlAdmissionError> {
    let tier = tier.to_uppercase();
    check_tier(&tier)?;
    Ok(tier)
}

fn build_engine_attribute_from_storage_class_tier(tier: &str) -> Result<String, DdlAdmissionError> {
    let tier = normalize_storage_class_tier(tier)?;
    let storage_class_json = tidb_model::serde_helpers::to_go_json(&tier)
        .map_err(|e| storage_class_invalid_spec(e.to_string()))?;
    let storage_class_json = String::from_utf8(storage_class_json)
        .map_err(|e| storage_class_invalid_spec(e.to_string()))?;
    Ok(format!("{{\"storage_class\":{storage_class_json}}}"))
}

/// Go `GetEngineAttributeFromStorageClassTableOptions`: returns the effective
/// engine attribute from table options, normalizing `STORAGE_CLASS` syntax
/// sugar into the underlying `ENGINE_ATTRIBUTE` JSON form when present.
pub fn get_engine_attribute_from_storage_class_table_options(
    options: &[TableOption],
) -> Result<(String, bool), DdlAdmissionError> {
    let mut last_opt: Option<&TableOption> = None;
    let mut has_engine_attribute = false;
    let mut has_storage_class = false;
    for opt in options {
        match opt {
            TableOption::EngineAttribute(_) => {
                has_engine_attribute = true;
                last_opt = Some(opt);
            }
            TableOption::StorageClass(_) => {
                has_storage_class = true;
                last_opt = Some(opt);
            }
            _ => {}
        }
    }
    let Some(last_opt) = last_opt else {
        return Ok((String::new(), false));
    };
    if has_engine_attribute && has_storage_class {
        return Err(err_engine_attribute_and_storage_class_conflict());
    }
    for opt in options {
        match opt {
            TableOption::EngineAttribute(value) => validate_engine_attribute_table_option(value)?,
            TableOption::StorageClass(value) => {
                normalize_storage_class_tier(value)?;
            }
            _ => {}
        }
    }
    match last_opt {
        TableOption::EngineAttribute(value) => Ok((value.clone(), true)),
        TableOption::StorageClass(value) => {
            Ok((build_engine_attribute_from_storage_class_tier(value)?, true))
        }
        _ => unreachable!("last_opt only ever holds EngineAttribute/StorageClass"),
    }
}

fn validate_engine_attribute_table_option(input: &str) -> Result<(), DdlAdmissionError> {
    let attr = parse_engine_attribute_from_string(input).map_err(|e| {
        DdlAdmissionError::with_code(
            tidb_error::tidb::errcode::ErrEngineAttributeInvalidFormat,
            format!("'{e}'"),
        )
    })?;
    let Some(storage_class) = attr.storage_class else {
        return Err(DdlAdmissionError::with_code(
            tidb_error::tidb::errcode::ErrEngineAttributeNotSupported,
            "ENGINE_ATTRIBUTE without a 'storage_class' field is not supported",
        ));
    };
    let raw = storage_class.get();
    build_storage_class_settings_from_json(Some(raw.as_bytes())).map(|_| ())
}

/// Go `CheckStorageClassConflictInAlterTableSpecs`: rejects `ALTER TABLE`
/// statements that mix raw `ENGINE_ATTRIBUTE` and `STORAGE_CLASS` syntax
/// sugar across alter specs.
///
/// Narrowed to take the `Options` slices of the alter specs whose
/// `Tp == ast.AlterTableOption` directly, since `ast.AlterTableSpec` is not
/// ported in this workspace -- see the module boundary note.
pub fn check_storage_class_conflict_in_alter_table_specs<'a, I>(
    alter_table_option_specs: I,
) -> Result<(), DdlAdmissionError>
where
    I: IntoIterator<Item = &'a [TableOption]>,
{
    let mut has_engine_attribute = false;
    let mut has_storage_class = false;
    for options in alter_table_option_specs {
        for opt in options {
            match opt {
                TableOption::EngineAttribute(_) => has_engine_attribute = true,
                TableOption::StorageClass(_) => has_storage_class = true,
                _ => {}
            }
        }
    }
    if has_engine_attribute && has_storage_class {
        return Err(err_engine_attribute_and_storage_class_conflict());
    }
    Ok(())
}

/// Go `GetSimpleTableStorageClassForShowCreate`: returns the table-level
/// storage class tier when it can be losslessly rendered as the
/// `STORAGE_CLASS` syntax sugar in `SHOW CREATE TABLE`.
pub fn get_simple_table_storage_class_for_show_create(
    tb_info: &TableInfo,
) -> Result<(String, bool), DdlAdmissionError> {
    if tb_info.engine_attribute.is_empty() {
        return Ok((String::new(), false));
    }

    let storage_class = match get_only_storage_class_engine_attribute(&tb_info.engine_attribute) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Ok((String::new(), false)),
        Err(e) => return Err(storage_class_invalid_spec(e.to_string())),
    };

    let settings = build_storage_class_settings_from_json(Some(&storage_class))?;
    if settings.defs.handles().len() != 1 {
        return Ok((String::new(), false));
    }
    let Some(def) = settings.defs.get(0) else {
        return Ok((String::new(), false));
    };
    let def = def.read();
    if !def.has_no_scope_def() || !def.transitions.is_empty() {
        return Ok((String::new(), false));
    }

    let tier = normalize_storage_class_tier(&def.tier)?;
    Ok((tier, true))
}

fn get_only_storage_class_engine_attribute(
    engine_attribute: &str,
) -> Result<Option<Vec<u8>>, serde_json::Error> {
    let fields: serde_json::Map<String, Value> = serde_json::from_str(engine_attribute)?;
    if fields.len() != 1 {
        return Ok(None);
    }
    match fields.get("storage_class") {
        Some(value) => Ok(Some(serde_json::to_vec(value)?)),
        None => Ok(None),
    }
}

const TIER_NAMES: [&str; 2] = [STORAGE_CLASS_TIER_STANDARD, STORAGE_CLASS_TIER_IA];

fn check_tier(tier: &str) -> Result<(), DdlAdmissionError> {
    if TIER_NAMES.contains(&tier) {
        Ok(())
    } else {
        Err(storage_class_invalid_spec(format!(
            "invalid storage class tier: {tier}"
        )))
    }
}

fn check_transitions(
    default_tier: &str,
    rules: &[StorageClassTransitRule],
) -> Result<(), DdlAdmissionError> {
    // Currently only STANDARD -> IA is reasonable.
    let ok = default_tier == STORAGE_CLASS_TIER_STANDARD
        && rules.len() == 1
        && rules[0].tier == STORAGE_CLASS_TIER_IA
        && rules[0].total_seconds() > 0;
    if !ok {
        return Err(storage_class_invalid_spec(
            "only transition from 'STANDARD' to 'IA' is allowed",
        ));
    }
    Ok(())
}

fn check_storage_class_def(def: &StorageClassDef) -> Result<(), DdlAdmissionError> {
    check_tier(&def.tier)?;

    if !def.transitions.is_empty() {
        check_transitions(&def.tier, &def.transitions.snapshot())?;
    }

    let mut scope_fields = 0;
    if !def.names_in.is_empty() {
        scope_fields += 1;
    }
    if def.less_than.is_some() {
        scope_fields += 1;
    }
    if !def.values_in.is_empty() {
        scope_fields += 1;
    }
    if scope_fields > 1 {
        return Err(storage_class_invalid_spec(
            "can not specify 'names_in', 'less_than', or 'values_in' together",
        ));
    }

    Ok(())
}

/// Go `setStorageClassForTable`. See the module boundary note: Go's
/// `logutil.BgLogger().Info(...)` call is dropped, unported.
fn set_storage_class_for_table(
    tb_info: &mut TableInfo,
    tier: &str,
    transitions: Vec<StorageClassTransitRule>,
) {
    tb_info.storage_class_tier = tier.to_owned();
    tb_info.storage_class_transitions = GoSharedSlice::from_vec(transitions);
}

/// Go `BuildStorageClassForTable`: builds the storage class tier for a
/// table. Go declares an `error` return that its body never populates (every
/// path returns `nil`); this port keeps the plain signature rather than a
/// `Result` that can never carry an `Err`.
pub fn build_storage_class_for_table(
    tb_info: &mut TableInfo,
    settings: Option<&StorageClassSettings>,
) {
    let Some(settings) = settings else {
        return;
    };

    for def in settings.defs.iter_deref() {
        let def = def.read();
        if def.has_no_scope_def() {
            set_storage_class_for_table(tb_info, &def.tier, def.transitions.snapshot());
            return;
        }
    }

    set_storage_class_for_table(tb_info, STORAGE_CLASS_TIER_DEFAULT, Vec::new());
}

/// Go `setStorageClassTierForPartition`. See the module boundary note on
/// dropped logging.
fn set_storage_class_tier_for_partition(
    partitions: &GoSharedSlice<PartitionDefinition>,
    index: usize,
    tier: &str,
    transitions: Vec<StorageClassTransitRule>,
) {
    partitions.update(index, |part| {
        part.storage_class_tier = tier.to_owned();
        part.storage_class_transitions = GoSharedSlice::from_vec(transitions);
    });
}

/// Go `BuildStorageClassForPartitions`: builds the storage class tier for
/// every partition in `partitions`.
///
/// `partitions` and `tb_info.partition`'s own definitions are typically the
/// SAME backing storage (a cloned [`GoSharedSlice`] header, exactly like Go's
/// slice-parameter aliasing), which is how a caller observes the mutation
/// this function performs.
pub fn build_storage_class_for_partitions(
    partitions: &GoSharedSlice<PartitionDefinition>,
    tb_info: &TableInfo,
    settings: Option<&StorageClassSettings>,
) -> Result<(), DdlAdmissionError> {
    let Some(settings) = settings else {
        return Ok(());
    };

    let defs: Vec<GoShared<StorageClassDef>> = settings.defs.iter_deref().collect();
    for def in &defs {
        check_storage_class_partition_scope(tb_info, partitions, &def.read())?;
    }

    let default_def = defs.iter().find(|d| d.read().has_no_scope_def()).cloned();

    'partitions: for index in 0..partitions.len() {
        let part = partitions.get(index);
        for def in &defs {
            let def = def.read();
            if def.has_no_scope_def() {
                continue;
            }

            if !def.names_in.is_empty()
                && is_partition_match_names_in(&part, &def.names_in.snapshot())
            {
                set_storage_class_tier_for_partition(
                    partitions,
                    index,
                    &def.tier,
                    def.transitions.snapshot(),
                );
                continue 'partitions;
            }

            if let Some(less_than) = &def.less_than {
                let less_than = less_than.read().clone();
                if is_partition_match_less_than(tb_info, &part, &less_than)? {
                    set_storage_class_tier_for_partition(
                        partitions,
                        index,
                        &def.tier,
                        def.transitions.snapshot(),
                    );
                    continue 'partitions;
                }
            }

            if !def.values_in.is_empty()
                && is_partition_match_values_in(&part, &def.values_in.snapshot())
            {
                set_storage_class_tier_for_partition(
                    partitions,
                    index,
                    &def.tier,
                    def.transitions.snapshot(),
                );
                continue 'partitions;
            }
        }

        if let Some(default_def) = &default_def {
            let default_def = default_def.read();
            set_storage_class_tier_for_partition(
                partitions,
                index,
                &default_def.tier,
                default_def.transitions.snapshot(),
            );
            continue;
        }
        set_storage_class_tier_for_partition(
            partitions,
            index,
            STORAGE_CLASS_TIER_DEFAULT,
            Vec::new(),
        );
    }

    Ok(())
}

fn check_storage_class_partition_scope(
    tb_info: &TableInfo,
    partitions: &GoSharedSlice<PartitionDefinition>,
    def: &StorageClassDef,
) -> Result<(), DdlAdmissionError> {
    let Some(partition_info) = tb_info.partition.as_ref() else {
        return Ok(());
    };
    let partition_info = partition_info.read();

    if !def.has_no_scope_def()
        && (partition_info.partition_type == PartitionType::HASH
            || partition_info.partition_type == PartitionType::KEY)
    {
        return Err(storage_class_invalid_spec(
            "partition-scoped storage_class does not support HASH or KEY partitions",
        ));
    }
    if def.less_than.is_some() {
        if partition_info.partition_type != PartitionType::RANGE {
            return Err(storage_class_invalid_spec(
                "'less_than' only supports RANGE partitions",
            ));
        }
        for part in partitions.snapshot() {
            if part.less_than.len() != 1 {
                return Err(storage_class_invalid_spec(
                    "'less_than' only supports single-column RANGE partitions",
                ));
            }
        }
    }
    if !def.values_in.is_empty() {
        if partition_info.partition_type != PartitionType::LIST {
            return Err(storage_class_invalid_spec(
                "'values_in' only supports LIST partitions",
            ));
        }
        for part in partitions.snapshot() {
            for part_values in part.in_values.snapshot() {
                if part_values.len() != 1 {
                    return Err(storage_class_invalid_spec(
                        "'values_in' only supports single-column LIST partitions",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn is_partition_match_names_in(part: &PartitionDefinition, names_in: &[String]) -> bool {
    names_in.iter().any(|name| part.name.lowercase() == name)
}

fn is_partition_match_less_than(
    tb_info: &TableInfo,
    part: &PartitionDefinition,
    less_than: &str,
) -> Result<bool, DdlAdmissionError> {
    if part.less_than.len() != 1 {
        return Ok(false);
    }
    partition_value_less_than_or_equal(tb_info, &part.less_than.get(0), less_than)
}

fn is_partition_match_values_in(part: &PartitionDefinition, values_in: &[String]) -> bool {
    for part_values in part.in_values.snapshot() {
        if part_values.len() != 1 {
            continue;
        }
        let value = part_values.get(0);
        if values_in.iter().any(|v| partition_value_equals(&value, v)) {
            return true;
        }
    }
    false
}

fn partition_value_equals(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    let left_keyword = is_partition_value_keyword(left);
    let right_keyword = is_partition_value_keyword(right);
    if left_keyword || right_keyword {
        if !left_keyword || !right_keyword {
            return false;
        }
        return left.eq_ignore_ascii_case(right);
    }
    unwrap_quotes(left) == unwrap_quotes(right)
}

fn unwrap_quotes(value: &str) -> String {
    String::from_utf8(unwrap_from_single_quotes(value.as_bytes()))
        .unwrap_or_else(|_| value.to_owned())
}

fn compare_range_partition_values(
    tb_info: &TableInfo,
    left: &str,
    right: &str,
) -> Result<i32, DdlAdmissionError> {
    let left_max_value = left.eq_ignore_ascii_case(PARTITION_MAX_VALUE);
    let right_max_value = right.eq_ignore_ascii_case(PARTITION_MAX_VALUE);
    match (left_max_value, right_max_value) {
        (true, true) => return Ok(0),
        (true, false) => return Ok(1),
        (false, true) => return Ok(-1),
        (false, false) => {}
    }

    let has_columns = tb_info
        .partition
        .as_ref()
        .is_some_and(|p| !p.read().columns.is_empty());
    if has_columns {
        compare_range_columns_partition_values(tb_info, left, right)
    } else {
        compare_numeric_range_partition_values(tb_info, left, right)
    }
}

/// Go `compareRangeColumnsPartitionValues`. See the module boundary notes:
/// no ported test reaches this function (it requires `Partition.Columns` to
/// be non-empty, which none of `TestBuildStorageClassForPartitions`'s
/// directly-constructed `TableInfo`s set), and the equal/greater-than
/// comparison is expressed through [`compare_datums_with_collation`] rather
/// than hand-built `eq`/`gt` `ScalarFunction` nodes.
fn compare_range_columns_partition_values(
    tb_info: &TableInfo,
    left: &str,
    right: &str,
) -> Result<i32, DdlAdmissionError> {
    let Some(partition_info) = tb_info.partition.as_ref() else {
        return Err(storage_class_invalid_spec(
            "'less_than' can not find RANGE COLUMNS partition column",
        ));
    };
    let partition_info = partition_info.read();
    if partition_info.columns.is_empty() {
        return Err(storage_class_invalid_spec(
            "'less_than' can not find RANGE COLUMNS partition column",
        ));
    }
    let col_name = partition_info.columns.get(0);

    let Some(col_info) = find_column_by_name(col_name.lowercase(), tb_info) else {
        return Err(storage_class_invalid_spec(
            "'less_than' can not find RANGE COLUMNS partition column",
        ));
    };

    let mut right_owned = right.to_owned();
    if !is_single_quoted_partition_value(&right_owned) {
        right_owned =
            String::from_utf8(wrap_in_single_quotes(right_owned.as_bytes())).unwrap_or(right_owned);
    }

    parse_and_eval_bool_expr(left, &right_owned, &col_info).map_err(|_| {
        storage_class_invalid_spec(format!("invalid 'less_than' value: {right_owned}"))
    })
}

fn compare_numeric_range_partition_values(
    tb_info: &TableInfo,
    left: &str,
    right: &str,
) -> Result<i32, DdlAdmissionError> {
    let expr_is_set = tb_info
        .partition
        .as_ref()
        .is_some_and(|p| !p.read().expr.is_empty());
    let unsigned = expr_is_set && is_part_expr_unsigned(tb_info);

    let left_value = get_range_value(left, unsigned).map_err(|_| {
        storage_class_invalid_spec(format!("invalid RANGE partition value: {left}"))
    })?;
    let right_value = get_range_value(right, unsigned)
        .map_err(|_| storage_class_invalid_spec(format!("invalid 'less_than' value: {right}")))?;

    Ok(match (left_value, right_value) {
        (RangeValue::UInt(l), RangeValue::UInt(r)) => compare_uint64(l, r),
        (RangeValue::Int(l), RangeValue::Int(r)) => compare_int64(l, r),
        // `unsigned` is the same flag both calls used, so both results are
        // always the same variant; this arm only exists so the match is
        // exhaustive.
        _ => unreachable!("get_range_value always returns the variant matching `unsigned`"),
    })
}

fn compare_uint64(left: u64, right: u64) -> i32 {
    match left.cmp(&right) {
        Ordering::Less => -1,
        Ordering::Greater => 1,
        Ordering::Equal => 0,
    }
}

fn compare_int64(left: i64, right: i64) -> i32 {
    match left.cmp(&right) {
        Ordering::Less => -1,
        Ordering::Greater => 1,
        Ordering::Equal => 0,
    }
}

fn is_single_quoted_partition_value(value: &str) -> bool {
    value.len() >= 2 && value.starts_with('\'') && value.ends_with('\'')
}

fn partition_value_less_than_or_equal(
    tb_info: &TableInfo,
    left: &str,
    right: &str,
) -> Result<bool, DdlAdmissionError> {
    Ok(compare_range_partition_values(tb_info, left, right)? <= 0)
}

fn is_partition_value_keyword(value: &str) -> bool {
    value.eq_ignore_ascii_case(PARTITION_MAX_VALUE) || value.eq_ignore_ascii_case("DEFAULT")
}

// ---------------------------------------------------------------------------
// Helpers ported from `pkg/ddl/partition.go`, as named by this file's task:
// `findColumnByName`, `getRangeValue`, `isPartExprUnsigned`,
// `parseAndEvalBoolExpr`.
// ---------------------------------------------------------------------------

/// Go `findColumnByName` (`partition.go`).
fn find_column_by_name(col_name: &str, tb_info: &TableInfo) -> Option<ColumnInfo> {
    tb_info
        .columns
        .iter_deref()
        .map(|c| c.read().clone())
        .find(|c| c.name.lowercase() == col_name)
}

enum RangeValue {
    Int(i64),
    UInt(u64),
}

/// Go `getRangeValue` (`partition.go`): gets an integer from a RANGE value
/// string, falling back to parsing and evaluating it as a constant
/// expression (e.g. `TO_SECONDS('2004-01-01')`).
///
/// The `bool` Go returns (whether the input was a constant EXPRESSION rather
/// than a literal integer) is dropped here: no caller in this file reads it.
fn get_range_value(value: &str, unsigned: bool) -> Result<RangeValue, DdlAdmissionError> {
    if unsigned {
        // Go's `strconv.ParseUint` rejects a leading sign entirely (unlike
        // Rust's `u64::from_str`, which accepts a leading `+`).
        if !value.starts_with('+') {
            if let Ok(v) = value.parse::<u64>() {
                return Ok(RangeValue::UInt(v));
            }
        }
    } else if let Ok(v) = value.parse::<i64>() {
        return Ok(RangeValue::Int(v));
    }

    let expr = parse_simple_expr(&NoResolver, value, &BuildOptions::new())
        .map_err(|_| not_allowed_type_in_partition(value))?;
    let datum = eval_expression_once(&expr, &NoColumns)
        .map_err(|_| not_allowed_type_in_partition(value))?;
    match (unsigned, datum) {
        (true, tidb_datatype::Datum::UInt(v)) => Ok(RangeValue::UInt(v)),
        (false, tidb_datatype::Datum::Int(v)) => Ok(RangeValue::Int(v)),
        _ => Err(not_allowed_type_in_partition(value)),
    }
}

/// The `model.ColumnInfo` fields [`ColumnInfoSource`] reads, borrowed from an
/// owned snapshot -- see `tidb_executor::ddl_copr::CopColumnInfo` for the
/// same pattern against the same trait.
struct PartitionExprColumnInfo<'a>(&'a ColumnInfo);

impl ColumnInfoSource for PartitionExprColumnInfo<'_> {
    fn column_name(&self) -> &CiString {
        &self.0.name
    }
    fn column_id(&self) -> i64 {
        self.0.id
    }
    fn column_offset(&self) -> i64 {
        self.0.offset
    }
    fn column_field_type(&self) -> &FieldType {
        &self.0.field_type
    }
    fn column_hidden(&self) -> bool {
        self.0.hidden
    }
}

/// Go `isPartExprUnsigned` (`partition.go`). See the module boundary note on
/// `tables.NewPartitionExprBuildCtx()`.
fn is_part_expr_unsigned(tb_info: &TableInfo) -> bool {
    let Some(partition) = tb_info.partition.as_ref() else {
        return false;
    };
    let expr_text = partition.read().expr.clone();

    let columns: Vec<ColumnInfo> = tb_info
        .columns
        .iter_deref()
        .map(|c| c.read().clone())
        .collect();
    let wrapped: Vec<PartitionExprColumnInfo<'_>> =
        columns.iter().map(PartitionExprColumnInfo).collect();

    let ids = SimplePlanColumnIdAllocator::new(0);
    let options =
        match BuildOptions::new().with_table_info(&NoResolver, &ids, "", &tb_info.name, &wrapped) {
            Ok(options) => options,
            Err(_) => return false,
        };
    let Ok(expr) = parse_simple_expr(&NoResolver, &expr_text, &options) else {
        return false;
    };
    expr.static_type()
        .is_some_and(|ft| ft.flags() & tidb_datatype::FieldTypeFlags::UNSIGNED != 0)
}

/// Go `parseAndEvalBoolExpr` (`partition.go`): compares `l` and `r`, both
/// cast to `col_info`'s type, returning `0` (equal), `1` (`l > r`) or `-1`
/// (`l < r`). See the module boundary note on `NewFunctionBase`.
fn parse_and_eval_bool_expr(
    l: &str,
    r: &str,
    col_info: &ColumnInfo,
) -> Result<i32, DdlAdmissionError> {
    let options = BuildOptions::new().with_cast_expr_to(col_info.field_type.clone());
    let lexpr = parse_simple_expr(&NoResolver, l, &options)
        .map_err(|e| storage_class_invalid_spec(e.to_string()))?;
    let rexpr = parse_simple_expr(&NoResolver, r, &options)
        .map_err(|e| storage_class_invalid_spec(e.to_string()))?;

    let l_val = eval_expression_once(&lexpr, &NoColumns)
        .map_err(|e| storage_class_invalid_spec(format!("{e:?}")))?;
    let r_val = eval_expression_once(&rexpr, &NoColumns)
        .map_err(|e| storage_class_invalid_spec(format!("{e:?}")))?;

    let collation = Collation::from_name(col_info.get_collate()).unwrap_or(Collation::Utf8Mb4Bin);
    let ordering = compare_datums_with_collation(&l_val, &r_val, collation)
        .map_err(|e| storage_class_invalid_spec(format!("{e:?}")))?;
    Ok(match ordering {
        Ordering::Equal => 0,
        Ordering::Greater => 1,
        Ordering::Less => -1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::CiString;
    use tidb_model::{PartitionInfo, TableInfo};

    fn def(tier: &str) -> StorageClassDef {
        StorageClassDef {
            tier: tier.to_owned(),
            ..Default::default()
        }
    }

    fn settings_of(defs: Vec<StorageClassDef>) -> StorageClassSettings {
        StorageClassSettings {
            defs: GoSharedPointerSlice::from_handles(
                defs.into_iter().map(|d| Some(GoShared::new(d))).collect(),
            ),
        }
    }

    fn assert_def_eq(
        got: &StorageClassDef,
        tier: &str,
        names_in: &[&str],
        less_than: Option<&str>,
        values_in: &[&str],
        transitions: &[(&str, u64)],
    ) {
        assert_eq!(got.tier, tier);
        assert_eq!(got.names_in.snapshot(), names_in);
        assert_eq!(
            got.less_than.as_ref().map(|v| v.read().clone()),
            less_than.map(str::to_owned)
        );
        assert_eq!(got.values_in.snapshot(), values_in);
        let got_transitions = got.transitions.snapshot();
        assert_eq!(got_transitions.len(), transitions.len());
        for (rule, (tier, after_days)) in got_transitions.iter().zip(transitions) {
            assert_eq!(&rule.tier, tier);
            assert_eq!(rule.after_days, *after_days);
        }
    }

    /// `(tier, names_in, less_than, values_in, transitions)` expected from one
    /// decoded `StorageClassDef`, used only by
    /// [`build_storage_class_settings_from_json_cases`].
    type ExpectedDef<'a> = (
        &'a str,
        &'a [&'a str],
        Option<&'a str>,
        &'a [&'a str],
        &'a [(&'a str, u64)],
    );

    /// Source: `TestBuildStorageClassSettingsFromJSON`.
    #[test]
    fn build_storage_class_settings_from_json_cases() {
        let ok = |input: &str, expect: Vec<ExpectedDef<'_>>| {
            let settings = build_storage_class_settings_from_json(Some(input.as_bytes()))
                .unwrap_or_else(|e| panic!("input {input:?} should succeed, got {e}"));
            let got_defs = settings.defs.handles();
            assert_eq!(got_defs.len(), expect.len(), "input: {input}");
            for (handle, (tier, names_in, less_than, values_in, transitions)) in
                got_defs.iter().zip(expect)
            {
                let def = handle.as_ref().expect("no null defs expected").read();
                assert_def_eq(&def, tier, names_in, less_than, values_in, transitions);
            }
        };
        let err = |input: &str| {
            assert!(
                build_storage_class_settings_from_json(Some(input.as_bytes())).is_err(),
                "input {input:?} should fail"
            );
        };

        ok(r#""STANDARD""#, vec![("STANDARD", &[], None, &[], &[])]);
        err(r#""INVALID""#);
        ok(
            r#"{"tier": "STANDARD"}"#,
            vec![("STANDARD", &[], None, &[], &[])],
        );
        ok(
            r#"{"tier": "STANDARD", "names_in": ["part1", "part2"]}"#,
            vec![("STANDARD", &["part1", "part2"], None, &[], &[])],
        );
        ok(
            r#"{"tier": "STANDARD", "less_than": "100"}"#,
            vec![("STANDARD", &[], Some("100"), &[], &[])],
        );
        ok(
            r#"{"tier": "STANDARD", "values_in": ["100", "200"]}"#,
            vec![("STANDARD", &[], None, &["100", "200"], &[])],
        );
        err(r#"{"tier": "STANDARD", "names_in": ["part1", "part2"], "values_in": ["100", "200"]}"#);
        err(r#"{"tier": "STANDARD", "unknown": "100"}"#);
        err(r#"{"tier": "STANDARD", "names_in": ["part1", "part2""#);
        err(r#"{"tier":"STANDARD"} {"tier":"IA"}"#);
        ok(
            r#"[{"tier": "IA", "names_in": ["part1", "part2"]}, {"tier": "STANDARD"}]"#,
            vec![
                ("IA", &["part1", "part2"], None, &[], &[]),
                ("STANDARD", &[], None, &[], &[]),
            ],
        );
        ok(
            r#"[{"tier": "ia", "names_in": ["Part1"]}, {"tier": "standard", "transitions": [{"tier": "ia", "after_days": 30}]}]"#,
            vec![
                ("IA", &["part1"], None, &[], &[]),
                ("STANDARD", &[], None, &[], &[("IA", 30)]),
            ],
        );
        err(r#"[{"tier": "STANDARD", "unknown": "100"}]"#);
        err(r#"[null]"#);
        err(r#"[{"tier": "STANDARD"}, null]"#);
        ok(
            r#"{"tier": "STANDARD", "transitions": [{"tier": "IA", "after_days": 30}]}"#,
            vec![("STANDARD", &[], None, &[], &[("IA", 30)])],
        );
        err(
            r#"{"tier": "STANDARD", "transitions": [{"tier": "IA", "after_days": 30}, {"tier": "IA", "after_days": 60}]}"#,
        );
        err(r#"{"tier": "IA", "transitions": [{"tier": "STANDARD", "after_days": 30}]}"#);
        err(
            r#"{"tier": "STANDARD", "transitions": [{"tier": "IA", "after_days": 15}, {"tier": "STANDARD", "after_days": 30}]}"#,
        );
        err(r#"{"tier": "STANDARD", "transitions": [{"tier": "IA", "after_days": 0}]}"#);

        // Go: `BuildStorageClassSettingsFromJSON(nil)`.
        let default_settings = build_storage_class_settings_from_json(None).unwrap();
        let handles = default_settings.defs.handles();
        assert_eq!(handles.len(), 1);
        assert_eq!(handles[0].as_ref().unwrap().read().tier, "STANDARD");
    }

    /// Source: `TestBuildStorageClassForTable`.
    #[test]
    fn build_storage_class_for_table_cases() {
        let cases: Vec<(Option<StorageClassSettings>, &str)> = vec![
            (None, ""),
            (Some(settings_of(vec![def("IA")])), "IA"),
            (
                Some(settings_of(vec![StorageClassDef {
                    tier: "IA".to_owned(),
                    names_in: GoSharedSlice::from_vec(vec!["part1".to_owned()]),
                    ..Default::default()
                }])),
                "STANDARD",
            ),
            (
                Some(settings_of(vec![
                    StorageClassDef {
                        tier: "STANDARD".to_owned(),
                        names_in: GoSharedSlice::from_vec(vec!["part1".to_owned()]),
                        ..Default::default()
                    },
                    StorageClassDef {
                        tier: "STANDARD".to_owned(),
                        names_in: GoSharedSlice::from_vec(vec!["part2".to_owned()]),
                        ..Default::default()
                    },
                    def("IA"),
                ])),
                "IA",
            ),
        ];

        for (settings, expected) in cases {
            let mut tb_info = TableInfo::default();
            build_storage_class_for_table(&mut tb_info, settings.as_ref());
            assert_eq!(tb_info.storage_class_tier, expected);
        }
    }

    fn part(name: &str) -> PartitionDefinition {
        PartitionDefinition {
            name: CiString::new(name),
            ..Default::default()
        }
    }

    fn part_with_less_than(name: &str, less_than: &str) -> PartitionDefinition {
        PartitionDefinition {
            name: CiString::new(name),
            less_than: GoSharedSlice::from_vec(vec![less_than.to_owned()]),
            ..Default::default()
        }
    }

    fn part_with_in_values(name: &str, values: &[&str]) -> PartitionDefinition {
        PartitionDefinition {
            name: CiString::new(name),
            in_values: GoSharedSlice::from_vec(
                values
                    .iter()
                    .map(|v| GoSharedSlice::from_vec(vec![(*v).to_owned()]))
                    .collect(),
            ),
            ..Default::default()
        }
    }

    fn def_names_in(tier: &str, names: &[&str]) -> StorageClassDef {
        StorageClassDef {
            tier: tier.to_owned(),
            names_in: GoSharedSlice::from_vec(names.iter().map(|s| (*s).to_owned()).collect()),
            ..Default::default()
        }
    }

    fn def_less_than(tier: &str, less_than: &str) -> StorageClassDef {
        StorageClassDef {
            tier: tier.to_owned(),
            less_than: Some(GoShared::new(less_than.to_owned())),
            ..Default::default()
        }
    }

    fn def_values_in(tier: &str, values: &[&str]) -> StorageClassDef {
        StorageClassDef {
            tier: tier.to_owned(),
            values_in: GoSharedSlice::from_vec(values.iter().map(|s| (*s).to_owned()).collect()),
            ..Default::default()
        }
    }

    /// Runs `BuildStorageClassForPartitions` and returns the resulting tiers,
    /// or `None` on error -- mirroring the Go table's `expected == nil` ==
    /// "expect an error" convention.
    fn run_partitions(
        settings: Option<StorageClassSettings>,
        partitions: Vec<PartitionDefinition>,
        partition_type: PartitionType,
    ) -> Option<Vec<String>> {
        let definitions = GoSharedSlice::from_vec(partitions);
        let tb_info = TableInfo {
            partition: Some(GoShared::new(PartitionInfo {
                definitions: definitions.clone(),
                partition_type,
                ..Default::default()
            })),
            ..Default::default()
        };
        match build_storage_class_for_partitions(&definitions, &tb_info, settings.as_ref()) {
            Ok(()) => Some(
                definitions
                    .snapshot()
                    .into_iter()
                    .map(|p| p.storage_class_tier)
                    .collect(),
            ),
            Err(_) => None,
        }
    }

    /// Source: `TestBuildStorageClassForPartitions`.
    #[test]
    fn build_storage_class_for_partitions_cases() {
        // no storage class settings
        assert_eq!(
            run_partitions(
                None,
                vec![part("part1"), part("part2")],
                PartitionType::NONE
            ),
            Some(vec![String::new(), String::new()])
        );

        // no scope definition
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![def("IA")])),
                vec![part("part1"), part("part2")],
                PartitionType::NONE,
            ),
            Some(vec!["IA".to_owned(), "IA".to_owned()])
        );

        // no scope definition on hash partition
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![def("IA")])),
                vec![part("part1"), part("part2")],
                PartitionType::HASH,
            ),
            Some(vec!["IA".to_owned(), "IA".to_owned()])
        );

        // names_in scope definition
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![def_names_in("IA", &["part1"])])),
                vec![part("part1"), part("part2")],
                PartitionType::NONE,
            ),
            Some(vec!["IA".to_owned(), "STANDARD".to_owned()])
        );

        // names_in invalid on hash partition
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![def_names_in("IA", &["part1"])])),
                vec![part("part1"), part("part2")],
                PartitionType::HASH,
            ),
            None
        );

        // names_in invalid on key partition
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![def_names_in("IA", &["part1"])])),
                vec![part("part1"), part("part2")],
                PartitionType::KEY,
            ),
            None
        );

        // partition scopes override no-scope default
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![
                    def_names_in("STANDARD", &["part1"]),
                    def("IA"),
                    def_names_in("STANDARD", &["part2"]),
                ])),
                vec![part("part1"), part("part2")],
                PartitionType::NONE,
            ),
            Some(vec!["STANDARD".to_owned(), "STANDARD".to_owned()])
        );

        // partition scope wins when no-scope default appears first
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![
                    def("STANDARD"),
                    def_names_in("IA", &["part1"])
                ])),
                vec![part("part1"), part("part2")],
                PartitionType::NONE,
            ),
            Some(vec!["IA".to_owned(), "STANDARD".to_owned()])
        );

        // less_than scope definition
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![def_less_than("IA", "200")])),
                vec![
                    part_with_less_than("part1", "100"),
                    part_with_less_than("part2", "200"),
                    part_with_less_than("part3", "1000"),
                ],
                PartitionType::RANGE,
            ),
            Some(vec![
                "IA".to_owned(),
                "IA".to_owned(),
                "STANDARD".to_owned()
            ])
        );

        // values_in scope definition
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![def_values_in("IA", &["2", "3"])])),
                vec![
                    part_with_in_values("part1", &["1", "2"]),
                    part_with_in_values("part2", &["3"]),
                    part_with_in_values("part3", &["4"]),
                ],
                PartitionType::LIST,
            ),
            Some(vec![
                "IA".to_owned(),
                "IA".to_owned(),
                "STANDARD".to_owned()
            ])
        );

        // less_than maxvalue includes literal and maxvalue upper bounds
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![def_less_than("IA", "MAXVALUE")])),
                vec![
                    part_with_less_than("part1", "'MAXVALUE'"),
                    part_with_less_than("part2", "MAXVALUE"),
                ],
                PartitionType::RANGE,
            ),
            Some(vec!["IA".to_owned(), "IA".to_owned()])
        );

        // values_in keyword does not match quoted literal
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![def_values_in("IA", &["DEFAULT"])])),
                vec![
                    part_with_in_values("part1", &["'DEFAULT'"]),
                    part_with_in_values("part2", &["DEFAULT"]),
                ],
                PartitionType::LIST,
            ),
            Some(vec!["STANDARD".to_owned(), "IA".to_owned()])
        );

        // less_than invalid on list partition (InValues instead of LessThan)
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![def_less_than("IA", "200")])),
                vec![part_with_in_values("part1", &["1"])],
                PartitionType::LIST,
            ),
            None
        );

        // less_than invalid on multi-column range partition
        let multi_column = PartitionDefinition {
            name: CiString::new("part1"),
            less_than: GoSharedSlice::from_vec(vec!["100".to_owned(), "200".to_owned()]),
            ..Default::default()
        };
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![def_less_than("IA", "200")])),
                vec![multi_column],
                PartitionType::RANGE,
            ),
            None
        );

        // less_than invalid numeric range value
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![def_less_than("IA", "abc")])),
                vec![part_with_less_than("part1", "100")],
                PartitionType::RANGE,
            ),
            None
        );

        // values_in invalid on range partition
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![def_values_in("IA", &["1"])])),
                vec![part_with_less_than("part1", "100")],
                PartitionType::RANGE,
            ),
            None
        );

        // values_in invalid on multi-column list partition
        let multi_column_list = PartitionDefinition {
            name: CiString::new("part1"),
            in_values: GoSharedSlice::from_vec(vec![GoSharedSlice::from_vec(vec![
                "1".to_owned(),
                "2".to_owned(),
            ])]),
            ..Default::default()
        };
        assert_eq!(
            run_partitions(
                Some(settings_of(vec![def_values_in("IA", &["1"])])),
                vec![multi_column_list],
                PartitionType::LIST,
            ),
            None
        );
    }

    /// Source: `TestStorageClassString`. `TableInfo::storage_class_string` is
    /// an already-ported `tidb-model` method (`pkg/meta/model/table.go`); this
    /// re-asserts the Go test's own cases at the call site the Go test file
    /// lives beside, without re-porting the method itself.
    #[test]
    fn storage_class_string_cases() {
        let ti = TableInfo {
            storage_class_tier: "STANDARD".to_owned(),
            ..Default::default()
        };
        assert_eq!(ti.storage_class_string(), "STANDARD");

        let ti = TableInfo {
            storage_class_tier: "IA".to_owned(),
            ..Default::default()
        };
        assert_eq!(ti.storage_class_string(), "IA");

        let ti = TableInfo {
            storage_class_tier: "STANDARD".to_owned(),
            storage_class_transitions: GoSharedSlice::from_vec(vec![StorageClassTransitRule {
                tier: "IA".to_owned(),
                after_days: 30,
                after_seconds: 0,
            }]),
            ..Default::default()
        };
        assert_eq!(
            ti.storage_class_string(),
            r#"{"tier":"STANDARD","transitions":[{"tier":"IA","after_days":30}]}"#
        );
    }

    fn opt_engine_attribute(value: &str) -> TableOption {
        TableOption::EngineAttribute(value.to_owned())
    }
    fn opt_storage_class(value: &str) -> TableOption {
        TableOption::StorageClass(value.to_owned())
    }

    /// Source: `TestGetEngineAttributeFromStorageClassTableOptions`.
    #[test]
    fn get_engine_attribute_from_storage_class_table_options_cases() {
        let ok = |options: Vec<TableOption>, expected_json: &str, expected_found: bool| {
            let (got, found) =
                get_engine_attribute_from_storage_class_table_options(&options).unwrap();
            assert_eq!(found, expected_found);
            let got_value: Value = serde_json::from_str(&got).unwrap();
            let expected_value: Value = serde_json::from_str(expected_json).unwrap();
            assert_eq!(got_value, expected_value);
        };
        let err = |options: Vec<TableOption>| {
            assert!(get_engine_attribute_from_storage_class_table_options(&options).is_err());
        };

        ok(
            vec![opt_storage_class("ia")],
            r#"{"storage_class":"IA"}"#,
            true,
        );
        ok(
            vec![opt_engine_attribute(r#"{"storage_class":"STANDARD"}"#)],
            r#"{"storage_class":"STANDARD"}"#,
            true,
        );
        ok(
            vec![
                opt_engine_attribute(r#"{"storage_class":"STANDARD"}"#),
                opt_engine_attribute(r#"{"storage_class":"IA"}"#),
            ],
            r#"{"storage_class":"IA"}"#,
            true,
        );
        err(vec![
            opt_engine_attribute("{"),
            opt_engine_attribute(r#"{"storage_class":"IA"}"#),
        ]);
        err(vec![opt_engine_attribute(r#"{"key":"value"}"#)]);
        err(vec![
            opt_engine_attribute(r#"{"storage_class":"STANDARD"}"#),
            opt_storage_class("IA"),
        ]);
        err(vec![
            opt_storage_class("IA"),
            opt_engine_attribute(r#"{"storage_class":"STANDARD"}"#),
        ]);
        err(vec![opt_storage_class("cold")]);
        err(vec![opt_storage_class("cold"), opt_storage_class("IA")]);
    }

    /// Source: `TestCheckStorageClassConflictInAlterTableSpecs`. The Go
    /// `ast.AlterTableSpec` wrapper is narrowed away -- see the module
    /// boundary note -- so each case is expressed directly as the `Options`
    /// slices of the (here, always `AlterTableOption`) alter specs.
    #[test]
    fn check_storage_class_conflict_in_alter_table_specs_cases() {
        // same spec conflict
        let specs: Vec<Vec<TableOption>> = vec![vec![
            opt_engine_attribute(r#"{"storage_class":"STANDARD"}"#),
            opt_storage_class("IA"),
        ]];
        assert!(
            check_storage_class_conflict_in_alter_table_specs(specs.iter().map(Vec::as_slice))
                .is_err()
        );

        // separate specs conflict
        let specs: Vec<Vec<TableOption>> = vec![
            vec![opt_engine_attribute(r#"{"storage_class":"STANDARD"}"#)],
            vec![opt_storage_class("IA")],
        ];
        assert!(
            check_storage_class_conflict_in_alter_table_specs(specs.iter().map(Vec::as_slice))
                .is_err()
        );

        // single form
        let specs: Vec<Vec<TableOption>> = vec![vec![opt_storage_class("IA")]];
        assert!(
            check_storage_class_conflict_in_alter_table_specs(specs.iter().map(Vec::as_slice))
                .is_ok()
        );
    }

    /// Source: `TestGetSimpleTableStorageClassForShowCreate`.
    #[test]
    fn get_simple_table_storage_class_for_show_create_cases() {
        let cases: Vec<(&str, &str, bool)> = vec![
            (r#"{"storage_class":"IA"}"#, "IA", true),
            (r#"{"storage_class":{"tier":"ia"}}"#, "IA", true),
            (r#"{"storage_class":[{"tier":"ia"}]}"#, "IA", true),
            (
                r#"{"storage_class":{"tier":"STANDARD","transitions":[{"tier":"IA","after_days":30}]}}"#,
                "",
                false,
            ),
            (
                r#"{"storage_class":{"tier":"IA","names_in":["p0"]}}"#,
                "",
                false,
            ),
            (r#"{"storage_class":"IA","future_field":true}"#, "", false),
        ];

        for (engine_attribute, expected, expected_ok) in cases {
            let tb_info = TableInfo {
                engine_attribute: engine_attribute.to_owned(),
                ..Default::default()
            };
            let (got, ok) = get_simple_table_storage_class_for_show_create(&tb_info).unwrap();
            assert_eq!(ok, expected_ok, "input: {engine_attribute}");
            assert_eq!(got, expected, "input: {engine_attribute}");
        }
    }

    /// Supplementary regression test for `is_part_expr_unsigned`
    /// (`partition.go`'s `isPartExprUnsigned`), which no surviving ported Go
    /// test exercises -- see the module doc's test-coverage section. Builds a
    /// `TableInfo` directly (rather than through `BuildTableInfoFromAST`, out
    /// of scope here) with one `BIGINT UNSIGNED` column and a partition
    /// expression that is just that column's name.
    #[test]
    fn is_part_expr_unsigned_reads_the_partition_columns_flag() {
        use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
        use tidb_model::GoSharedPointerSlice;

        let mut unsigned_bigint = FieldType::new(FieldTypeCode::LongLong);
        unsigned_bigint.set_raw_flags(u64::from(FieldTypeFlags::UNSIGNED));
        let signed_int = FieldType::new(FieldTypeCode::Long);

        let unsigned_col = ColumnInfo {
            name: CiString::new("id"),
            offset: 0,
            field_type: unsigned_bigint,
            ..zero_column_info()
        };
        let signed_col = ColumnInfo {
            name: CiString::new("id"),
            offset: 0,
            field_type: signed_int,
            ..zero_column_info()
        };

        let unsigned_tb_info = TableInfo {
            name: CiString::new("t"),
            columns: GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(unsigned_col))]),
            partition: Some(GoShared::new(PartitionInfo {
                expr: "id".to_owned(),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert!(is_part_expr_unsigned(&unsigned_tb_info));

        let signed_tb_info = TableInfo {
            name: CiString::new("t"),
            columns: GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(signed_col))]),
            partition: Some(GoShared::new(PartitionInfo {
                expr: "id".to_owned(),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert!(!is_part_expr_unsigned(&signed_tb_info));

        // No `Partition.Expr` at all -> Go never reaches its own build/parse
        // path and effectively treats it as not unsigned via a fully
        // separate guard in this file's own callers; this function itself
        // is not called by them in that case.
        let no_partition = TableInfo::default();
        assert!(!is_part_expr_unsigned(&no_partition));
    }

    fn zero_column_info() -> ColumnInfo {
        ColumnInfo {
            id: 0,
            name: CiString::default(),
            offset: 0,
            origin_default_value: Default::default(),
            origin_default_value_bit: Default::default(),
            default_value: Default::default(),
            default_value_bit: Default::default(),
            default_is_expr: false,
            generated_expr_string: String::new(),
            generated_stored: false,
            dependences: Default::default(),
            field_type: FieldType::new(tidb_datatype::FieldTypeCode::Unspecified),
            changing_field_type: None,
            state: Default::default(),
            comment: String::new(),
            hidden: false,
            ..Default::default()
        }
    }
}
