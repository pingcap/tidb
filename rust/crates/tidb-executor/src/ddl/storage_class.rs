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

//! Go `pkg/ddl/storage_class.go` and the metadata half of
//! `pkg/ddl/engine_attribute.go`: a table's `ENGINE_ATTRIBUTE` JSON, its
//! `STORAGE_CLASS = tier` sugar, and the storage-class tier they resolve to
//! for the table and for each partition.

use serde_json::{Map, Value};
use tidb_ast::TableOption;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_error::tidb::errcode::{
    ErrEngineAttributeInvalidFormat, ErrEngineAttributeNotSupported, ErrStorageClassInvalidSpec,
};
use tidb_model::engine_attribute::{
    STORAGE_CLASS_TIER_DEFAULT, STORAGE_CLASS_TIER_IA, STORAGE_CLASS_TIER_STANDARD,
};
use tidb_model::serde_helpers::go_json_field_matches;
use tidb_model::StorageClassTransitRule;

use crate::partition_routing::{PartitionDef, PartitionKind};
use crate::DriverError;

/// A storage-class tier and its transitions, the pair Go stores as
/// `StorageClassTier` and `StorageClassTransitions` on a table and on each
/// partition definition.
pub type StorageClass = (String, Vec<StorageClassTransitRule>);

/// Go `model.StorageClassDef`, decoded from the written JSON.
#[derive(Clone, Debug, Default)]
pub struct StorageClassDef {
    tier: String,
    names_in: Vec<String>,
    less_than: Option<String>,
    values_in: Vec<String>,
    transitions: Vec<StorageClassTransitRule>,
}

impl StorageClassDef {
    fn has_no_scope_def(&self) -> bool {
        self.names_in.is_empty() && self.less_than.is_none() && self.values_in.is_empty()
    }
}

fn invalid_spec(message: impl std::fmt::Display) -> DriverError {
    DriverError::DdlCoded {
        errno: ErrStorageClassInvalidSpec,
        message: format!("Invalid storage class: {message}"),
    }
}

fn invalid_format(error: &str) -> DriverError {
    DriverError::DdlCoded {
        errno: ErrEngineAttributeInvalidFormat,
        message: format!("Invalid engine attribute format: '{error}'"),
    }
}

/// Go `model.ParseEngineAttributeFromString` wrapped in
/// `ErrEngineAttributeInvalidFormat`, which every DDL caller does.
fn parse_engine_attribute(input: &str) -> Result<tidb_model::EngineAttribute, DriverError> {
    tidb_model::parse_engine_attribute_from_string(input).map_err(|error| invalid_format(&error))
}

/// Go `BuildStorageClassSettingsFromJSON`; `None` is a nil `json.RawMessage`.
///
/// Go tries the value as a tier string, then as one definition object, then
/// as a list of definitions. Every decoding failure of the two object forms
/// ends in the same `invalid storage class def` message, so the attempts
/// collapse into one match on the JSON value's kind here.
pub fn build_storage_class_settings_from_json(
    input: Option<&[u8]>,
) -> Result<Vec<StorageClassDef>, DriverError> {
    let Some(input) = input else {
        return Ok(vec![StorageClassDef {
            tier: STORAGE_CLASS_TIER_DEFAULT.to_owned(),
            ..StorageClassDef::default()
        }]);
    };
    let invalid_def = || {
        // `%-.192s` keeps the first 192 runes.
        let text = String::from_utf8_lossy(input)
            .chars()
            .take(192)
            .collect::<String>();
        invalid_spec(format!("invalid storage class def: '{text}'"))
    };
    let Ok(value) = serde_json::from_slice::<Value>(input) else {
        return Err(invalid_def());
    };
    let tier_only = |tier: &str| {
        let tier = tidb_hack::go_to_upper(tier);
        check_tier(&tier)?;
        Ok(vec![StorageClassDef {
            tier,
            ..StorageClassDef::default()
        }])
    };
    match value {
        // `json.Unmarshal` into a string accepts a JSON string, and `null`
        // leaves the string empty.
        Value::String(tier) => tier_only(&tier),
        Value::Null => tier_only(""),
        Value::Object(fields) => {
            let mut def = decode_def(&fields).ok_or_else(invalid_def)?;
            normalize_storage_class_def(&mut def);
            check_storage_class_def(&def)?;
            Ok(vec![def])
        }
        Value::Array(items) => {
            // A `null` element decodes to a nil `*StorageClassDef`, which
            // `normalizeStorageClassDefs` refuses in list order.
            let defs = items
                .iter()
                .map(|item| match item {
                    Value::Null => Some(None),
                    Value::Object(fields) => decode_def(fields).map(Some),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
                .ok_or_else(invalid_def)?;
            defs.into_iter()
                .map(|def| {
                    let mut def =
                        def.ok_or_else(|| invalid_spec("storage class def must not be null"))?;
                    normalize_storage_class_def(&mut def);
                    check_storage_class_def(&def)?;
                    Ok(def)
                })
                .collect()
        }
        Value::Bool(_) | Value::Number(_) => Err(invalid_def()),
    }
}

/// Go `decodeStorageClassDef`: `encoding/json` with `DisallowUnknownFields`,
/// so an unknown member or a value of the wrong JSON kind fails the decode.
fn decode_def(fields: &Map<String, Value>) -> Option<StorageClassDef> {
    let mut def = StorageClassDef::default();
    for (key, value) in fields {
        if go_json_field_matches(key, "tier") {
            decode_string(value, &mut def.tier)?;
        } else if go_json_field_matches(key, "names_in") {
            def.names_in = decode_strings(value)?;
        } else if go_json_field_matches(key, "less_than") {
            def.less_than = match value {
                Value::Null => None,
                Value::String(text) => Some(text.clone()),
                _ => return None,
            };
        } else if go_json_field_matches(key, "values_in") {
            def.values_in = decode_strings(value)?;
        } else if go_json_field_matches(key, "transitions") {
            def.transitions = match value {
                Value::Null => Vec::new(),
                Value::Array(items) => {
                    items.iter().map(decode_transition).collect::<Option<_>>()?
                }
                _ => return None,
            };
        } else {
            return None;
        }
    }
    Some(def)
}

/// A JSON `null` leaves a Go string untouched.
fn decode_string(value: &Value, destination: &mut String) -> Option<()> {
    match value {
        Value::Null => {}
        Value::String(text) => text.clone_into(destination),
        _ => return None,
    }
    Some(())
}

fn decode_strings(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::Null => Some(Vec::new()),
        Value::Array(items) => items
            .iter()
            .map(|item| {
                let mut text = String::new();
                decode_string(item, &mut text).map(|()| text)
            })
            .collect(),
        _ => None,
    }
}

fn decode_transition(value: &Value) -> Option<StorageClassTransitRule> {
    let mut rule = StorageClassTransitRule::default();
    let fields = match value {
        Value::Null => return Some(rule),
        Value::Object(fields) => fields,
        _ => return None,
    };
    // A Go `uint` takes only an unsigned integer literal.
    let decode_uint = |value: &Value, destination: &mut u64| -> Option<()> {
        match value {
            Value::Null => {}
            Value::Number(number) => *destination = number.as_u64()?,
            _ => return None,
        }
        Some(())
    };
    for (key, value) in fields {
        if go_json_field_matches(key, "tier") {
            decode_string(value, &mut rule.tier)?;
        } else if go_json_field_matches(key, "after_days") {
            decode_uint(value, &mut rule.after_days)?;
        } else if go_json_field_matches(key, "after_seconds") {
            decode_uint(value, &mut rule.after_seconds)?;
        } else {
            return None;
        }
    }
    Some(rule)
}

/// Go `normalizeStorageClassDef`.
fn normalize_storage_class_def(def: &mut StorageClassDef) {
    def.tier = tidb_hack::go_to_upper(&def.tier);
    for name in &mut def.names_in {
        *name = tidb_hack::go_to_lower(&*name);
    }
    for rule in &mut def.transitions {
        rule.tier = tidb_hack::go_to_upper(&rule.tier);
    }
}

/// Go `normalizeStorageClassTier`.
fn normalize_storage_class_tier(tier: &str) -> Result<String, DriverError> {
    let tier = tidb_hack::go_to_upper(tier);
    check_tier(&tier)?;
    Ok(tier)
}

/// Go `checkTier`.
fn check_tier(tier: &str) -> Result<(), DriverError> {
    if tier == STORAGE_CLASS_TIER_STANDARD || tier == STORAGE_CLASS_TIER_IA {
        return Ok(());
    }
    Err(invalid_spec(format!("invalid storage class tier: {tier}")))
}

/// Go `checkTransitions`: only one `STANDARD` -> `IA` step after a positive
/// delay.
fn check_transitions(
    default_tier: &str,
    rules: &[StorageClassTransitRule],
) -> Result<(), DriverError> {
    let allowed = default_tier == STORAGE_CLASS_TIER_STANDARD
        && rules.len() == 1
        && rules[0].tier == STORAGE_CLASS_TIER_IA
        && rules[0].total_seconds() > 0;
    if !allowed {
        return Err(invalid_spec(
            "only transition from 'STANDARD' to 'IA' is allowed",
        ));
    }
    Ok(())
}

/// Go `checkStorageClassDef`.
fn check_storage_class_def(def: &StorageClassDef) -> Result<(), DriverError> {
    check_tier(&def.tier)?;
    if !def.transitions.is_empty() {
        check_transitions(&def.tier, &def.transitions)?;
    }
    let scopes = usize::from(!def.names_in.is_empty())
        + usize::from(def.less_than.is_some())
        + usize::from(!def.values_in.is_empty());
    if scopes > 1 {
        return Err(invalid_spec(
            "can not specify 'names_in', 'less_than', or 'values_in' together",
        ));
    }
    Ok(())
}

fn engine_attribute_and_storage_class_conflict() -> DriverError {
    invalid_spec("can not specify 'ENGINE_ATTRIBUTE' and 'STORAGE_CLASS' together")
}

/// Go `GetEngineAttributeFromStorageClassTableOptions`: the effective
/// `ENGINE_ATTRIBUTE` of one option list, with `STORAGE_CLASS = tier`
/// rewritten into its JSON form. Every written value is validated, not only
/// the last one, which is the one that takes effect.
pub fn engine_attribute_from_table_options(
    options: &[TableOption],
) -> Result<Option<String>, DriverError> {
    let mut last = None;
    let (mut has_engine_attribute, mut has_storage_class) = (false, false);
    for option in options {
        match option {
            TableOption::EngineAttribute(_) => {
                has_engine_attribute = true;
                last = Some(option);
            }
            TableOption::StorageClass(_) => {
                has_storage_class = true;
                last = Some(option);
            }
            _ => {}
        }
    }
    let Some(last) = last else {
        return Ok(None);
    };
    if has_engine_attribute && has_storage_class {
        return Err(engine_attribute_and_storage_class_conflict());
    }
    for option in options {
        match option {
            TableOption::EngineAttribute(input) => validate_engine_attribute_table_option(input)?,
            TableOption::StorageClass(tier) => {
                normalize_storage_class_tier(tier)?;
            }
            _ => {}
        }
    }
    match last {
        TableOption::EngineAttribute(input) => Ok(Some(input.clone())),
        // Go `buildEngineAttributeFromStorageClassTier`: `json.Marshal` of
        // an `EngineAttribute` holding the tier as a JSON string. A checked
        // tier is plain ASCII, so it needs no escaping.
        TableOption::StorageClass(tier) => Ok(Some(format!(
            r#"{{"storage_class":"{}"}}"#,
            normalize_storage_class_tier(tier)?
        ))),
        _ => unreachable!("only the two storage-class options are recorded"),
    }
}

/// Go `validateEngineAttributeTableOption`.
fn validate_engine_attribute_table_option(input: &str) -> Result<(), DriverError> {
    let attribute = parse_engine_attribute(input)?;
    let Some(storage_class) = attribute.storage_class else {
        return Err(DriverError::DdlCoded {
            errno: ErrEngineAttributeNotSupported,
            message: "Storage engine does not support ENGINE_ATTRIBUTE.".to_owned(),
        });
    };
    build_storage_class_settings_from_json(Some(&storage_class.bytes()))?;
    Ok(())
}

/// Go `CheckStorageClassConflictInAlterTableSpecs`: one ALTER may not mix
/// `ENGINE_ATTRIBUTE` and `STORAGE_CLASS` across its specifications.
pub fn check_storage_class_conflict_in_alter_specs<'a>(
    options: impl IntoIterator<Item = &'a TableOption>,
) -> Result<(), DriverError> {
    let (mut has_engine_attribute, mut has_storage_class) = (false, false);
    for option in options {
        match option {
            TableOption::EngineAttribute(_) => has_engine_attribute = true,
            TableOption::StorageClass(_) => has_storage_class = true,
            _ => {}
        }
    }
    if has_engine_attribute && has_storage_class {
        return Err(engine_attribute_and_storage_class_conflict());
    }
    Ok(())
}

/// Go `getStorageClassSettingsFromTableInfo`: `None` when the attribute
/// names no storage class.
pub fn storage_class_settings_of(
    engine_attribute: &str,
) -> Result<Option<Vec<StorageClassDef>>, DriverError> {
    let attribute = parse_engine_attribute(engine_attribute)?;
    let Some(storage_class) = attribute.storage_class else {
        return Ok(None);
    };
    build_storage_class_settings_from_json(Some(&storage_class.bytes())).map(Some)
}

/// Go `GetSimpleTableStorageClassForShowCreate`: the table-level tier when
/// `SHOW CREATE TABLE` can print the attribute as `STORAGE_CLASS='tier'`
/// without losing anything.
pub fn simple_table_storage_class_for_show_create(
    engine_attribute: &str,
) -> Result<Option<String>, DriverError> {
    if engine_attribute.is_empty() {
        return Ok(None);
    }
    // Go `getOnlyStorageClassEngineAttribute`: a map of raw members, so the
    // attribute qualifies only when `storage_class` is its single key.
    let fields = serde_json::from_str::<Map<String, Value>>(engine_attribute)
        .map_err(|error| invalid_format(&error.to_string()))?;
    let Some(storage_class) = fields.get("storage_class").filter(|_| fields.len() == 1) else {
        return Ok(None);
    };
    let raw = serde_json::to_vec(storage_class).unwrap_or_default();
    let defs = build_storage_class_settings_from_json(Some(&raw))?;
    let [def] = defs.as_slice() else {
        return Ok(None);
    };
    if !def.has_no_scope_def() || !def.transitions.is_empty() {
        return Ok(None);
    }
    normalize_storage_class_tier(&def.tier).map(Some)
}

/// Go `BuildStorageClassForTable`: the first definition without a scope,
/// else the default tier.
pub fn build_storage_class_for_table(defs: &[StorageClassDef]) -> StorageClass {
    defs.iter().find(|def| def.has_no_scope_def()).map_or_else(
        || (STORAGE_CLASS_TIER_DEFAULT.to_owned(), Vec::new()),
        |def| (def.tier.clone(), def.transitions.clone()),
    )
}

/// Go `rebuildStorageClassForPartitions` over every definition of
/// `partition`. An attribute that names no storage class leaves them unset.
pub fn rebuild_storage_class_for_partitions(
    engine_attribute: &str,
    partition: &mut crate::partition_routing::PartitionSpec,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    if let Some(classes) = partition_storage_classes(engine_attribute, partition, ctx)? {
        for (definition, class) in partition.definitions.iter_mut().zip(classes) {
            definition.storage_class = class;
        }
    }
    Ok(())
}

/// The classes [`rebuild_storage_class_for_partitions`] assigns, one per
/// definition, or `None` when the attribute names no storage class.
pub fn partition_storage_classes(
    engine_attribute: &str,
    partition: &crate::partition_routing::PartitionSpec,
    ctx: &crate::StmtContext,
) -> Result<Option<Vec<StorageClass>>, DriverError> {
    let Some(settings) = storage_class_settings_of(engine_attribute)? else {
        return Ok(None);
    };
    let definitions = partition.definitions.iter().collect::<Vec<_>>();
    build_storage_class_for_partitions(&settings, &partition.kind, &definitions, ctx).map(Some)
}

/// The partition method as `checkStorageClassPartitionScope` reads it from
/// `tbInfo.Partition.Type`, which does not tell the COLUMNS forms apart.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PartitionMethod {
    HashOrKey,
    Range,
    List,
    None,
}

fn partition_method(kind: &PartitionKind) -> PartitionMethod {
    match kind {
        PartitionKind::Hash | PartitionKind::Key => PartitionMethod::HashOrKey,
        PartitionKind::Range { .. } | PartitionKind::RangeColumns { .. } => PartitionMethod::Range,
        PartitionKind::List { .. } | PartitionKind::ListColumns { .. } => PartitionMethod::List,
        PartitionKind::None => PartitionMethod::None,
    }
}

/// Go `BuildStorageClassForPartitions`: the class of every definition in
/// `partitions`, in order. `kind` supplies the method and, for `less_than`,
/// the RANGE expression's signedness or the RANGE COLUMNS column type.
pub fn build_storage_class_for_partitions(
    defs: &[StorageClassDef],
    kind: &PartitionKind,
    partitions: &[&PartitionDef],
    ctx: &crate::StmtContext,
) -> Result<Vec<StorageClass>, DriverError> {
    for def in defs {
        check_storage_class_partition_scope(kind, partitions, def)?;
    }
    let default_def = defs.iter().find(|def| def.has_no_scope_def());
    let mut classes = Vec::with_capacity(partitions.len());
    'partitions: for partition in partitions {
        for def in defs.iter().filter(|def| !def.has_no_scope_def()) {
            let matched = (!def.names_in.is_empty()
                && def
                    .names_in
                    .contains(&tidb_hack::go_to_lower(&partition.name)))
                || match &def.less_than {
                    Some(less_than) => {
                        is_partition_match_less_than(kind, partition, less_than, ctx)?
                    }
                    None => false,
                }
                || (!def.values_in.is_empty()
                    && is_partition_match_values_in(partition, &def.values_in));
            if matched {
                classes.push((def.tier.clone(), def.transitions.clone()));
                continue 'partitions;
            }
        }
        classes.push(default_def.map_or_else(
            || (STORAGE_CLASS_TIER_DEFAULT.to_owned(), Vec::new()),
            |def| (def.tier.clone(), def.transitions.clone()),
        ));
    }
    Ok(classes)
}

/// Go `checkStorageClassPartitionScope`.
fn check_storage_class_partition_scope(
    kind: &PartitionKind,
    partitions: &[&PartitionDef],
    def: &StorageClassDef,
) -> Result<(), DriverError> {
    let method = partition_method(kind);
    if !def.has_no_scope_def() && method == PartitionMethod::HashOrKey {
        return Err(invalid_spec(
            "partition-scoped storage_class does not support HASH or KEY partitions",
        ));
    }
    if def.less_than.is_some() {
        if method != PartitionMethod::Range {
            return Err(invalid_spec("'less_than' only supports RANGE partitions"));
        }
        if partitions
            .iter()
            .any(|partition| partition.less_than.len() != 1)
        {
            return Err(invalid_spec(
                "'less_than' only supports single-column RANGE partitions",
            ));
        }
    }
    if !def.values_in.is_empty() {
        if method != PartitionMethod::List {
            return Err(invalid_spec("'values_in' only supports LIST partitions"));
        }
        if partitions
            .iter()
            .any(|partition| partition.in_values.iter().any(|values| values.len() != 1))
        {
            return Err(invalid_spec(
                "'values_in' only supports single-column LIST partitions",
            ));
        }
    }
    Ok(())
}

/// Go `isPartitionMatchLessThan`: the partition's upper bound is at most the
/// written `less_than`.
fn is_partition_match_less_than(
    kind: &PartitionKind,
    partition: &PartitionDef,
    less_than: &str,
    ctx: &crate::StmtContext,
) -> Result<bool, DriverError> {
    let [bound] = partition.less_than.as_slice() else {
        return Ok(false);
    };
    Ok(compare_range_partition_values(kind, bound, less_than, ctx)? <= 0)
}

/// Go `isPartitionMatchValuesIn`.
fn is_partition_match_values_in(partition: &PartitionDef, values_in: &[String]) -> bool {
    partition
        .in_values
        .iter()
        .filter_map(|values| match values.as_slice() {
            [value] => Some(value),
            _ => None,
        })
        .any(|value| {
            values_in
                .iter()
                .any(|candidate| partition_value_equals(value, candidate))
        })
}

/// Go `isPartitionValueKeyword`.
fn is_partition_value_keyword(value: &str) -> bool {
    value.eq_ignore_ascii_case(super::table_partition::PARTITION_MAX_VALUE)
        || value.eq_ignore_ascii_case("DEFAULT")
}

/// Go `partitionValueEquals`: the stored value text against a written one,
/// keywords case-insensitively and literals with their quotes removed.
fn partition_value_equals(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    let (left_keyword, right_keyword) = (
        is_partition_value_keyword(left),
        is_partition_value_keyword(right),
    );
    if left_keyword || right_keyword {
        return left_keyword && right_keyword && left.eq_ignore_ascii_case(right);
    }
    super::table_partition::unwrap_from_single_quotes(left)
        == super::table_partition::unwrap_from_single_quotes(right)
}

/// Go `compareRangePartitionValues`, which only ever answers whether
/// `left` is above `right`: `MAXVALUE` is above everything else, and the
/// value comparisons report 1 for above and 0 otherwise.
fn compare_range_partition_values(
    kind: &PartitionKind,
    left: &str,
    right: &str,
    ctx: &crate::StmtContext,
) -> Result<i32, DriverError> {
    let max_value = super::table_partition::PARTITION_MAX_VALUE;
    match (
        left.eq_ignore_ascii_case(max_value),
        right.eq_ignore_ascii_case(max_value),
    ) {
        (true, true) => return Ok(0),
        (true, false) => return Ok(1),
        (false, true) => return Ok(-1),
        (false, false) => {}
    }
    match kind {
        PartitionKind::RangeColumns { field_types, .. } => {
            let Some(column) = field_types.first() else {
                return Err(invalid_spec(
                    "'less_than' can not find RANGE COLUMNS partition column",
                ));
            };
            compare_range_columns_partition_values(column, left, right, ctx)
        }
        PartitionKind::Range { unsigned, .. } => {
            compare_numeric_range_partition_values(*unsigned, left, right, ctx)
        }
        _ => compare_numeric_range_partition_values(false, left, right, ctx),
    }
}

/// Go `compareRangeColumnsPartitionValues` through `parseAndEvalBoolExpr`:
/// both texts cast to the partition column's type and compared with `>` under
/// its collation. A NULL comparison evaluates to 0.
fn compare_range_columns_partition_values(
    column: &FieldType,
    left: &str,
    right: &str,
    ctx: &crate::StmtContext,
) -> Result<i32, DriverError> {
    let right = if right.len() >= 2 && right.starts_with('\'') && right.ends_with('\'') {
        right.to_owned()
    } else {
        // Go `driver.WrapInSingleQuotes`.
        format!("'{}'", right.replace('\\', "\\\\").replace('\'', "''"))
    };
    let invalid = || invalid_spec(format!("invalid 'less_than' value: {right}"));
    let cast = |text: &str| -> Option<Datum> {
        let expr = tidb_model::generated_expr::parse_expression(text).ok()?;
        let value = super::table_partition_list::eval_column_value(&expr, ctx).ok()?;
        // Go casts with `WithCastExprTo`; a lossy cast only warns.
        value
            .convert_to_in(
                column,
                ctx.ddl_default_conversion_flags(),
                &ctx.session_zone(),
            )
            .ok()
            .map(|converted| converted.value)
    };
    let (Some(left), Some(right_value)) = (cast(left), cast(&right)) else {
        return Err(invalid());
    };
    if matches!(left, Datum::Null) || matches!(right_value, Datum::Null) {
        return Ok(0);
    }
    let order = tidb_expr::compare_datums_with_collation(&left, &right_value, column.collation())
        .map_err(|_| invalid())?;
    Ok(i32::from(order == std::cmp::Ordering::Greater))
}

/// Go `compareNumericRangePartitionValues` with `getRangeValue`: each text
/// is an integer literal, or else a constant expression evaluated as an
/// integer.
fn compare_numeric_range_partition_values(
    unsigned: bool,
    left: &str,
    right: &str,
    ctx: &crate::StmtContext,
) -> Result<i32, DriverError> {
    let left_value = get_range_value(left, unsigned, ctx)
        .ok_or_else(|| invalid_spec(format!("invalid RANGE partition value: {left}")))?;
    let right_value = get_range_value(right, unsigned, ctx)
        .ok_or_else(|| invalid_spec(format!("invalid 'less_than' value: {right}")))?;
    let order = if unsigned {
        (left_value as u64).cmp(&(right_value as u64))
    } else {
        left_value.cmp(&right_value)
    };
    Ok(order as i32)
}

/// Go `getRangeValue`, as the 64-bit pattern its caller compares signed or
/// unsigned.
fn get_range_value(text: &str, unsigned: bool, ctx: &crate::StmtContext) -> Option<i64> {
    if unsigned {
        if let Ok(value) = text.parse::<u64>() {
            return Some(value as i64);
        }
    } else if let Ok(value) = text.parse::<i64>() {
        return Some(value);
    }
    let expr = tidb_model::generated_expr::parse_expression(text).ok()?;
    let value = super::table_partition_list::eval_column_value(&expr, ctx).ok()?;
    let mut target = FieldType::new(FieldTypeCode::LongLong);
    if unsigned {
        target.add_flags(FieldTypeFlags::UNSIGNED);
    }
    // Go `EvalInt`: a NULL result is not a range value.
    match value
        .convert_to_in(
            &target,
            ctx.ddl_default_conversion_flags(),
            &ctx.session_zone(),
        )
        .ok()?
        .value
    {
        Datum::Int(value) => Some(value),
        Datum::UInt(value) => Some(value as i64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(input: &str) -> Result<Vec<StorageClassDef>, String> {
        build_storage_class_settings_from_json(Some(input.as_bytes()))
            .map_err(|error| error.to_mysql_error().message)
    }

    #[test]
    fn settings_follow_go_decoding() {
        assert_eq!(settings(r#""ia""#).unwrap()[0].tier, "IA");
        assert_eq!(
            settings(r#""AI""#).unwrap_err(),
            "Invalid storage class: invalid storage class tier: AI"
        );
        assert_eq!(
            settings("null").unwrap_err(),
            "Invalid storage class: invalid storage class tier: "
        );
        assert_eq!(
            settings(r#"{"tier":"IA","bogus":1}"#).unwrap_err(),
            r#"Invalid storage class: invalid storage class def: '{"tier":"IA","bogus":1}'"#
        );
        assert_eq!(
            settings(r#"[{"tier":"IA"}, null]"#).unwrap_err(),
            "Invalid storage class: storage class def must not be null"
        );
        assert_eq!(
            settings(r#"{"tier":"IA","transitions":[{"tier":"IA","after_days":30}]}"#).unwrap_err(),
            "Invalid storage class: only transition from 'STANDARD' to 'IA' is allowed"
        );
        assert_eq!(
            settings(r#"{"tier":"IA","names_in":["p0"],"values_in":["1"]}"#).unwrap_err(),
            "Invalid storage class: can not specify 'names_in', 'less_than', or 'values_in' together"
        );
        let defs = settings(r#"{"TIER":"standard","names_in":["P0"]}"#).unwrap();
        assert_eq!(
            (defs[0].tier.as_str(), defs[0].names_in.clone()),
            ("STANDARD", vec!["p0".to_owned()])
        );
    }

    #[test]
    fn options_and_show_create_follow_go() {
        let attribute = |options: Vec<TableOption>| {
            engine_attribute_from_table_options(&options)
                .map_err(|error| error.to_mysql_error().message)
        };
        assert_eq!(
            attribute(vec![TableOption::StorageClass("ia".into())]).unwrap(),
            Some(r#"{"storage_class":"IA"}"#.to_owned())
        );
        assert_eq!(
            attribute(vec![
                TableOption::EngineAttribute("{".into()),
                TableOption::EngineAttribute(r#"{"storage_class": "IA"}"#.into()),
            ])
            .unwrap_err(),
            "Invalid engine attribute format: 'unexpected end of JSON input'"
        );
        assert_eq!(
            attribute(vec![TableOption::EngineAttribute("{}".into())]).unwrap_err(),
            "Storage engine does not support ENGINE_ATTRIBUTE."
        );
        assert_eq!(
            simple_table_storage_class_for_show_create(r#"{"storage_class": "ia"}"#).unwrap(),
            Some("IA".to_owned())
        );
        assert_eq!(
            simple_table_storage_class_for_show_create(
                r#"{"storage_class": {"tier":"STANDARD", "transitions":[{"tier":"IA", "after_days":30}]}}"#
            )
            .unwrap(),
            None
        );
    }
}
