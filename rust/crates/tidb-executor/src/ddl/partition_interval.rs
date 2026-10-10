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

//! Go's RANGE INTERVAL partitioning (`pkg/ddl/partition.go`):
//! `generatePartitionDefinitionsFromInterval`, `GeneratePartDefsFromInterval`
//! and `comparePartitionDefinitions`. INTERVAL is syntactic sugar: it expands
//! into ordinary RANGE definitions, which the CREATE TABLE battery then
//! checks like written ones.

use tidb_ast::{
    Expr, PartitionDefinition, PartitionDefinitionClause, PartitionValue, TablePartitioning,
};
use tidb_datatype::{Datum, EvalType, FieldType};

use super::DriverError;

/// `mysql.PartitionCountLimit`.
const PARTITION_COUNT_LIMIT: usize = 8192;

/// Go `ErrGeneralUnsupportedDDL` ("Unsupported %s").
fn unsupported(message: impl std::fmt::Display) -> DriverError {
    DriverError::DdlCoded {
        errno: 8200,
        message: format!("Unsupported {message}"),
    }
}

fn parse(text: &str) -> Result<Expr, DriverError> {
    tidb_model::generated_expr::parse_expression(text)
        .map_err(|error| DriverError::Parse(error.message))
}

fn restore(expr: &Expr) -> String {
    expr.restore_with_flags(tidb_ast::RestoreFlags::DEFAULT)
}

fn eval(expr: &Expr, ctx: &crate::StmtContext) -> Result<Datum, DriverError> {
    super::table_partition_list::eval_column_value(expr, ctx)
}

/// Go `Datum.ConvertTo` into the RANGE COLUMNS column, when there is one.
fn convert(
    value: Datum,
    column: Option<&FieldType>,
    ctx: &crate::StmtContext,
) -> Result<Datum, DriverError> {
    let Some(column) = column else {
        return Ok(value);
    };
    value
        .convert_to_in(
            column,
            ctx.ddl_default_conversion_flags(),
            &ctx.session_zone(),
        )
        .map(|converted| converted.value)
        .map_err(|error| DriverError::Parse(error.to_string()))
}

fn compare(left: &Datum, right: &Datum) -> Result<std::cmp::Ordering, DriverError> {
    tidb_expr::compare_datums_with_collation(left, right, tidb_datatype::Collation::Binary)
        .map_err(|error| DriverError::Parse(format!("{error:?}")))
}

fn value_string(value: &Datum) -> Result<String, DriverError> {
    value
        .sql_string()
        .map_err(|error| DriverError::Parse(format!("{error:?}")))
}

/// Go `TimeUnitType.String()` for the units INTERVAL partitioning accepts;
/// `None` is `TimeUnitInvalid`, a plain integer step.
fn time_unit(unit: Option<&str>) -> Result<Option<&'static str>, DriverError> {
    let Some(unit) = unit else {
        return Ok(None);
    };
    const UNITS: [&str; 8] = [
        "YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND",
    ];
    UNITS
        .iter()
        .find(|candidate| candidate.eq_ignore_ascii_case(unit))
        .map(|unit| Some(*unit))
        .ok_or_else(|| {
            unsupported(
                "INTERVAL partitioning, only supports YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE and SECOND as time unit",
            )
        })
}

/// Which INTERVAL statement generates the definitions
/// (`GeneratePartDefsFromInterval`'s `tp`).
pub(super) enum IntervalStatement<'a> {
    /// CREATE TABLE / PARTITION BY: FIRST through LAST.
    Create,
    /// `ALTER TABLE ... FIRST PARTITION LESS THAN (expr)`.
    DropFirst(&'a Expr),
    /// `ALTER TABLE ... LAST PARTITION LESS THAN (expr)`.
    AddLast(&'a Expr),
}

/// The INTERVAL a table's definitions are generated from.
pub(super) struct Interval<'a> {
    pub step: &'a Expr,
    pub unit: Option<&'a str>,
    pub first: &'a Expr,
    pub last: &'a Expr,
}

/// Go `GeneratePartDefsFromInterval`: the RANGE definitions from `start`
/// in INTERVAL steps up to and including the statement's end value, named
/// `P_LT_<bound>`. `existing` counts the table's definitions toward the
/// partition limit.
pub(super) fn generate_from_interval(
    interval: &Interval<'_>,
    statement: IntervalStatement<'_>,
    column: Option<&FieldType>,
    existing: usize,
    ctx: &crate::StmtContext,
) -> Result<Vec<PartitionDefinition>, DriverError> {
    let step_text = restore(interval.step);
    let step_value = String::from_utf8_lossy(&tidb_datatype::unwrap_from_single_quotes(
        step_text.as_bytes(),
    ))
    .into_owned();
    if !step_value.starts_with(|first: char| ('1'..='9').contains(&first)) {
        return Err(unsupported("INTERVAL, should be a positive number"));
    }
    let unit = time_unit(interval.unit)?;
    let (start, last, skip_start) = match statement {
        IntervalStatement::Create => (interval.first, interval.last, false),
        IntervalStatement::DropFirst(end) => (interval.first, end, false),
        IntervalStatement::AddLast(end) => (interval.last, end, true),
    };
    let last_value = convert(eval(last, ctx)?, column, ctx)?;
    let start_text = restore(start);
    let mut definitions = Vec::new();
    for ordinal in 0..PARTITION_COUNT_LIMIT {
        let current = if ordinal == 0 {
            if skip_start {
                // The current LAST partition already exists.
                continue;
            }
            start.clone()
        } else {
            match unit {
                None => parse(&format!("({start_text}) + {ordinal} * ({step_text})"))?,
                Some(unit) => parse(&format!(
                    "DATE_ADD({start_text}, INTERVAL ({ordinal} * ({step_text})) {unit})"
                ))?,
            }
        };
        let value = convert(eval(&current, ctx)?, column, ctx)?;
        let order = compare(&value, &last_value)?;
        if order == std::cmp::Ordering::Greater {
            let mut message = format!(
                "INTERVAL: expr ({}) not matching FIRST + n INTERVALs ({start_text} + n * {step_value}",
                value_string(&last_value)?
            );
            if let Some(unit) = unit {
                message.push(' ');
                message.push_str(unit);
            }
            message.push(')');
            return Err(unsupported(message));
        }
        let text = value_string(&value)?;
        if text.is_empty() || text.starts_with('\'') {
            return Err(unsupported(
                "INTERVAL partitioning: Error when generating partition values",
            ));
        }
        let bound = if unit.is_some() {
            parse(&format!(
                "'{}'",
                text.replace('\\', "\\\\").replace('\'', "''")
            ))?
        } else {
            parse(&text)?
        };
        definitions.push(PartitionDefinition {
            name: format!("P_LT_{text}"),
            clause: PartitionDefinitionClause::LessThan(vec![PartitionValue::Expr(bound)]),
            options: Vec::new(),
            sub_partitions: Vec::new(),
        });
        if order == std::cmp::Ordering::Equal {
            break;
        }
        if ordinal == PARTITION_COUNT_LIMIT - 1 {
            return Err(DriverError::PartitionTooMany);
        }
    }
    if existing + definitions.len() > PARTITION_COUNT_LIMIT {
        return Err(DriverError::PartitionTooMany);
    }
    Ok(definitions)
}

/// Go `getLowerBoundInt` for one column.
fn lower_bound_int(column: &FieldType) -> i64 {
    if column.has_flag(tidb_datatype::FieldTypeFlags::UNSIGNED) {
        return 0;
    }
    tidb_datatype::integer_signed_lower_bound(column.code()).min(0)
}

/// Go `generatePartitionDefinitionsFromInterval` for CREATE TABLE and
/// `ALTER TABLE ... PARTITION BY`: the clause's INTERVAL expanded into the
/// definitions the rest of the battery checks. Written definitions are
/// allowed beside the INTERVAL and must match the generated ones; they are
/// kept so their names survive.
///
/// `column` is the RANGE COLUMNS column's type; `unsigned` is
/// `isPartExprUnsigned` for the expression form.
pub(super) fn expand_interval(
    partitioning: &TablePartitioning,
    column: Option<&FieldType>,
    unsigned: bool,
    ctx: &crate::StmtContext,
) -> Result<Vec<PartitionDefinition>, DriverError> {
    let method = &partitioning.method;
    let interval = method
        .interval
        .as_deref()
        .expect("the caller dispatched on INTERVAL");
    if method.kind != tidb_ast::PartitionType::RANGE {
        return Err(unsupported(
            "INTERVAL partitioning, only allowed on RANGE partitioning",
        ));
    }
    if method.columns.len() > 1 {
        return Err(unsupported(
            "INTERVAL partitioning, does not allow RANGE COLUMNS with more than one column",
        ));
    }
    if let Some(column) = column {
        if !matches!(column.eval_type(), EvalType::Int | EvalType::Datetime) {
            return Err(unsupported(
                "INTERVAL partitioning, only supports Date, Datetime and INT types",
            ));
        }
    }
    let (Some(first), Some(last)) = (&interval.first_range_end, &interval.last_range_end) else {
        return Err(unsupported(
            "INTERVAL partitioning, currently requires FIRST and LAST partitions to be defined",
        ));
    };
    let unit = time_unit(interval.unit.as_deref())?;
    match column {
        Some(column) => {
            super::table_partition_list::fold_range_column_value(first, column, ctx)?;
            super::table_partition_list::fold_range_column_value(last, column, ctx)?;
        }
        None => {
            let mode = super::table_partition::PartitionBuildMode::Create;
            super::table_partition_range::fold_range_bound(
                first,
                "FIRST PARTITION",
                unsigned,
                ctx,
                mode,
            )?;
            super::table_partition_range::fold_range_bound(
                last,
                "LAST PARTITION",
                unsigned,
                ctx,
                mode,
            )?;
        }
    }
    let mut definitions = Vec::new();
    if interval.null_partition {
        let bound = if column.is_some() && unit.is_some() {
            parse("'0000-01-01'")?
        } else {
            let lowest = match column {
                Some(column) => lower_bound_int(column),
                None if unsigned => 0,
                None => i64::MIN,
            };
            parse(&lowest.to_string())?
        };
        definitions.push(PartitionDefinition {
            name: "P_NULL".to_owned(),
            clause: PartitionDefinitionClause::LessThan(vec![PartitionValue::Expr(bound)]),
            options: Vec::new(),
            sub_partitions: Vec::new(),
        });
    }
    let generated = generate_from_interval(
        &Interval {
            step: &interval.expr,
            unit: interval.unit.as_deref(),
            first,
            last,
        },
        IntervalStatement::Create,
        column,
        definitions.len(),
        ctx,
    )?;
    definitions.extend(generated);
    if interval.maxvalue_partition {
        definitions.push(PartitionDefinition {
            name: "P_MAXVALUE".to_owned(),
            clause: PartitionDefinitionClause::LessThan(vec![PartitionValue::MaxValue]),
            options: Vec::new(),
            sub_partitions: Vec::new(),
        });
    }
    if partitioning.definitions.is_empty() {
        return Ok(definitions);
    }
    compare_partition_definitions(&definitions, &partitioning.definitions, ctx)?;
    Ok(partitioning.definitions.clone())
}

/// Go `comparePartitionDefinitions`: written definitions beside an INTERVAL
/// must be the generated ones, names aside.
fn compare_partition_definitions(
    generated: &[PartitionDefinition],
    defined: &[PartitionDefinition],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    if generated.len() != defined.len() {
        // Go passes the counts as extra arguments to a one-verb format.
        return Err(unsupported(format!(
            "number of partitions generated != partition defined (%d != %d)%!(EXTRA int={}, int={})",
            generated.len(),
            defined.len()
        )));
    }
    for (generated, defined) in generated.iter().zip(defined) {
        let name = &defined.name;
        if !defined.sub_partitions.is_empty() {
            return Err(unsupported(format!(
                "partition {name} does have unsupported subpartitions"
            )));
        }
        if !defined.options.is_empty() {
            return Err(unsupported(format!(
                "partition {name} does have unsupported options"
            )));
        }
        let PartitionDefinitionClause::LessThan(defined_values) = &defined.clause else {
            return Err(unsupported(format!(
                "partition {name} does not have the right type for LESS THAN"
            )));
        };
        let PartitionDefinitionClause::LessThan(generated_values) = &generated.clause else {
            unreachable!("INTERVAL generates LESS THAN definitions")
        };
        match (&defined_values[0], &generated_values[0]) {
            (PartitionValue::MaxValue, PartitionValue::MaxValue) => continue,
            (PartitionValue::MaxValue, _) | (_, PartitionValue::MaxValue) => {
                return Err(unsupported(format!(
                    "partition {name} differs between generated and defined for MAXVALUE"
                )))
            }
            (PartitionValue::Expr(defined), PartitionValue::Expr(generated)) => {
                let equal = eval(
                    &parse(&format!(
                        "({}) = ({})",
                        restore(defined),
                        restore(generated)
                    ))?,
                    ctx,
                )?;
                if !matches!(equal, Datum::Int(1)) {
                    return Err(unsupported(format!(
                        "partition {name} differs between generated and defined for expression"
                    )));
                }
            }
            _ => {
                return Err(unsupported(format!(
                    "partition {name} differs between generated and defined for expression"
                )))
            }
        }
    }
    Ok(())
}

/// What Go `getPartitionIntervalFromTable` recovers from a table's RANGE
/// definitions.
pub(super) struct TableInterval {
    pub step: Expr,
    pub unit: Option<&'static str>,
    pub first: Expr,
    pub last: Expr,
    pub null_partition: bool,
    pub maxvalue_partition: bool,
    /// The RANGE COLUMNS column, when the table has one.
    pub column: Option<FieldType>,
}

impl TableInterval {
    pub(super) fn interval(&self) -> Interval<'_> {
        Interval {
            step: &self.step,
            unit: self.unit,
            first: &self.first,
            last: &self.last,
        }
    }
}

/// Go `getPartitionIntervalFromTable`: whether the table's definitions are
/// what an INTERVAL would have generated, and if so which one. `None` for
/// anything else, as Go answers nil.
pub(super) fn interval_from_table(
    partition: &crate::partition_routing::PartitionSpec,
    ctx: &crate::StmtContext,
) -> Option<TableInterval> {
    use crate::partition_routing::PartitionKind;
    let (column, unsigned) = match &partition.kind {
        PartitionKind::Range { unsigned, .. } => (None, *unsigned),
        PartitionKind::RangeColumns { field_types, .. } if field_types.len() == 1 => {
            (Some(field_types[0].clone()), false)
        }
        _ => return None,
    };
    let definitions = &partition.definitions;
    if definitions.len() < 2 {
        return None;
    }
    let (is_int, min_value) = match &column {
        Some(column) if column.eval_type() == EvalType::Int => {
            (true, lower_bound_int(column).to_string())
        }
        Some(column) if column.eval_type() == EvalType::Datetime => {
            (false, "0000-01-01".to_owned())
        }
        Some(_) => return None,
        None if unsigned => (true, "0".to_owned()),
        None => (true, i64::MIN.to_string()),
    };
    let bound = |ordinal: usize| {
        definitions[ordinal]
            .less_than
            .first()
            .map(|text| super::table_partition::unwrap_from_single_quotes(text))
    };
    let mut start = 0;
    let mut end = definitions.len() - 1;
    let mut null_partition = false;
    let mut maxvalue_partition = false;
    let mut first = bound(start)?;
    if first.eq_ignore_ascii_case(&min_value) {
        null_partition = true;
        start += 1;
        first = bound(start)?;
    }
    let mut last = bound(end)?;
    if last.eq_ignore_ascii_case(super::table_partition::PARTITION_MAX_VALUE) {
        maxvalue_partition = true;
        end -= 1;
        last = bound(end)?;
    }
    if start >= end {
        return None;
    }
    let steps = (end - start) as i64;
    let eval_int = |text: String| -> Option<i64> {
        match eval(&parse(&text).ok()?, ctx).ok()? {
            Datum::Int(value) => Some(value),
            Datum::UInt(value) => i64::try_from(value).ok(),
            _ => None,
        }
    };
    let (step, unit, first_expr, last_expr) = if is_int {
        let step = eval_int(format!("(({last}) - ({first})) DIV {steps}"))?;
        if step < 1 {
            return None;
        }
        let literal = |text: &str| -> Option<Expr> {
            if min_value == "0" {
                text.parse::<u64>().ok()?;
            } else {
                text.parse::<i64>().ok()?;
            }
            parse(text).ok()
        };
        (
            parse(&step.to_string()).ok()?,
            None,
            literal(&first)?,
            literal(&last)?,
        )
    } else {
        let seconds = eval_int(format!("TIMESTAMPDIFF(SECOND, '{first}', '{last}')"))?;
        if seconds < 1 {
            return None;
        }
        const DAY: i64 = 24 * 60 * 60;
        let step = seconds / steps;
        let (step, unit) = if step < 28 * DAY {
            (step, "SECOND")
        } else if steps <= 3 {
            (step / (28 * DAY), "MONTH")
        } else {
            (step / (30 * DAY), "MONTH")
        };
        let quoted = |text: &str| {
            parse(&format!(
                "'{}'",
                text.replace('\\', "\\\\").replace('\'', "''")
            ))
        };
        (
            parse(&step.to_string()).ok()?,
            Some(unit),
            quoted(&first).ok()?,
            quoted(&last).ok()?,
        )
    };
    let found = TableInterval {
        step,
        unit,
        first: first_expr,
        last: last_expr,
        null_partition,
        maxvalue_partition,
        column,
    };
    // Go regenerates the definitions and keeps the guess only when every
    // bound matches (`comparePartitionAstAndModel`).
    let mut generated = Vec::new();
    if null_partition {
        generated.push(None);
    }
    for definition in generate_from_interval(
        &found.interval(),
        IntervalStatement::Create,
        found.column.as_ref(),
        0,
        ctx,
    )
    .ok()?
    {
        let PartitionDefinitionClause::LessThan(values) = definition.clause else {
            return None;
        };
        let PartitionValue::Expr(expr) = &values[0] else {
            return None;
        };
        generated.push(Some(
            convert(eval(expr, ctx).ok()?, found.column.as_ref(), ctx).ok()?,
        ));
    }
    if maxvalue_partition {
        generated.push(None);
    }
    if generated.len() != definitions.len() {
        return None;
    }
    for (ordinal, value) in generated.iter().enumerate() {
        let Some(value) = value else {
            continue;
        };
        let stored = bound(ordinal)?;
        let stored = if found.column.is_some() {
            let literal = format!("'{}'", stored.replace('\\', "\\\\").replace('\'', "''"));
            convert(
                eval(&parse(&literal).ok()?, ctx).ok()?,
                found.column.as_ref(),
                ctx,
            )
            .ok()?
        } else {
            match stored.parse::<i64>() {
                Ok(parsed) => Datum::Int(parsed),
                Err(_) => Datum::UInt(stored.parse::<u64>().ok()?),
            }
        };
        if compare(&stored, value).ok()? != std::cmp::Ordering::Equal {
            return None;
        }
    }
    Some(found)
}
