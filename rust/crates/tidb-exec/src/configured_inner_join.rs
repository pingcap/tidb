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

//! Lazy execution of the configured two-relation INNER/CROSS join milestone.
//!
//! The planner owns binding and both physical scans. This module consumes those
//! plans without reconstructing SQL, opens them through the same-snapshot real
//! TiKV authority, and joins decoded rows in stable left-major/right-minor
//! order. The right input is materialized only as demanded: a bounded caller
//! never causes an additional logical child row to be pulled after its output
//! budget is satisfied. Right-row retention can nevertheless grow to the full
//! right input; memory quotas, spill, and an explicit build-side bound remain
//! outside this milestone.

use std::sync::Arc;

use tidb_datatype::{Datum, DatumKind};
use tidb_distsql::{CancelHandle, QueryTransport, TimestampSource};
use tidb_planner::{
    configured_join_plan::{ConfiguredJoinPlan, FullSchemaColumn},
    configured_relation_tree::RelationSide,
    join_condition::JoinSide,
    read_only_scan::{ConfiguredColumnKind, ConfiguredTable},
};
use tidb_protocol::ColumnInfo;

use crate::{
    distsql_recordset::{DistSqlRecordSet, DistSqlRecordSetError},
    real_tikv_multi_read::{RealTiKvMultiQuery, RealTiKvMultiReadError},
    real_tikv_read::RealTiKvMultiReadSession,
    recordset_lifecycle::RecordSetLifecycle,
};

/// Failure while opening or consuming the configured join result.
#[derive(Debug)]
pub enum ConfiguredInnerJoinError {
    /// The shared-snapshot multi-read authority rejected the statement.
    MultiRead(RealTiKvMultiReadError),
    /// The planner/runtime contract is internally inconsistent.
    InvalidPlan(String),
    /// A source row did not have its planner-promised FullSchema width.
    InvalidRowWidth {
        /// Zero-based relation (`0` is left, `1` is right).
        relation: usize,
        /// Configured source width.
        expected: usize,
        /// Decoded row width.
        actual: usize,
    },
    /// A join key operand did not decode as one of the scalar kinds this
    /// milestone's key comparison understands (int/uint/double/string), or
    /// the pair straddled two kinds outside the admitted signed/unsigned
    /// integer cross-comparison.
    InvalidJoinKey {
        /// Zero-based relation (`0` is left, `1` is right).
        relation: usize,
        /// Source-column offset containing the invalid key.
        offset: usize,
        /// Actual decoded datum kind.
        kind: DatumKind,
    },
    /// One lazy source failed while rows were being consumed or released.
    Source {
        /// Zero-based relation (`0` is left, `1` is right).
        relation: usize,
        /// Existing DistSQL record-set error.
        source: DistSqlRecordSetError,
    },
    /// The caller's shared statement cancellation fired.
    Cancelled,
}

impl std::fmt::Display for ConfiguredInnerJoinError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MultiRead(source) => write!(formatter, "configured join read failed: {source}"),
            Self::InvalidPlan(message) => {
                write!(formatter, "invalid configured join plan: {message}")
            }
            Self::InvalidRowWidth {
                relation,
                expected,
                actual,
            } => write!(
                formatter,
                "relation {relation} row has width {actual}, expected {expected}"
            ),
            Self::InvalidJoinKey {
                relation,
                offset,
                kind,
            } => write!(
                formatter,
                "relation {relation} join key at offset {offset} decoded as {kind:?}"
            ),
            Self::Source { relation, source } => {
                write!(formatter, "relation {relation} result failed: {source}")
            }
            Self::Cancelled => formatter.write_str("configured join cancelled by caller"),
        }
    }
}

impl std::error::Error for ConfiguredInnerJoinError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MultiRead(source) => Some(source),
            Self::Source { source, .. } => Some(source),
            Self::InvalidPlan(_)
            | Self::InvalidRowWidth { .. }
            | Self::InvalidJoinKey { .. }
            | Self::Cancelled => None,
        }
    }
}

impl From<RealTiKvMultiReadError> for ConfiguredInnerJoinError {
    fn from(source: RealTiKvMultiReadError) -> Self {
        Self::MultiRead(source)
    }
}

impl<T, S> RealTiKvMultiReadSession<T, S>
where
    T: QueryTransport,
    T::Response: 'static,
    S: TimestampSource,
{
    /// Executes one planner-bound configured join with a fresh cancellation owner.
    pub fn execute_configured_inner_join(
        &mut self,
        plan: ConfiguredJoinPlan,
    ) -> Result<ConfiguredInnerJoinRecordSet, ConfiguredInnerJoinError> {
        self.execute_configured_inner_join_with_cancellation(
            plan,
            Arc::new(CancelHandle::default()),
        )
    }

    /// Executes one planner-bound configured join under caller cancellation.
    ///
    /// The two physical plans are passed directly to the supplied-plan
    /// multi-read seam. They therefore share one snapshot and retain the
    /// planner's exact range and pushed-down Selection decisions.
    pub fn execute_configured_inner_join_with_cancellation(
        &mut self,
        plan: ConfiguredJoinPlan,
        cancellation: Arc<CancelHandle>,
    ) -> Result<ConfiguredInnerJoinRecordSet, ConfiguredInnerJoinError> {
        let layout = JoinLayout::from_plan(&plan, self.configured_tables())?;
        let plans = [plan.left_scan().clone(), plan.right_scan().clone()];
        let query = self.execute_plans_with_cancellation(plans, Arc::clone(&cancellation))?;
        ConfiguredInnerJoinRecordSet::from_query(layout, query, cancellation)
    }
}

/// Lazy, single-owner result set for a configured INNER/CROSS join.
///
/// Laziness bounds child pulls by caller demand, not retained memory: sparse
/// matches can require retaining the complete right input for replay.
pub struct ConfiguredInnerJoinRecordSet {
    sources: Option<[DistSqlRecordSet; 2]>,
    layout: JoinLayout,
    cancellation: Arc<CancelHandle>,
    snapshot_ts: Option<u64>,
    right_rows: Vec<Vec<Datum>>,
    right_eof: bool,
    current_left: Option<Vec<Datum>>,
    right_cursor: usize,
    exhausted: bool,
    lifecycle: RecordSetLifecycle,
}

impl ConfiguredInnerJoinRecordSet {
    fn from_query(
        layout: JoinLayout,
        query: RealTiKvMultiQuery,
        cancellation: Arc<CancelHandle>,
    ) -> Result<Self, ConfiguredInnerJoinError> {
        let snapshot_ts = query.snapshot_ts();
        let Some(relations) = query.into_relations() else {
            return Ok(Self {
                sources: None,
                layout,
                cancellation,
                snapshot_ts,
                right_rows: Vec::new(),
                right_eof: true,
                current_left: None,
                right_cursor: 0,
                exhausted: true,
                lifecycle: RecordSetLifecycle::default(),
            });
        };
        let mut sources = relations.map(|query| query.into_record_set());
        for (relation, expected) in layout.widths.into_iter().enumerate() {
            let actual = sources[relation].columns().len();
            if actual != expected {
                cancellation.cancel();
                close_sources(&mut sources);
                return Err(ConfiguredInnerJoinError::InvalidPlan(format!(
                    "relation {relation} metadata has width {actual}, expected {expected}",
                )));
            }
        }
        Ok(Self {
            sources: Some(sources),
            layout,
            cancellation,
            snapshot_ts,
            right_rows: Vec::new(),
            right_eof: false,
            current_left: None,
            right_cursor: 0,
            exhausted: false,
            lifecycle: RecordSetLifecycle::default(),
        })
    }

    /// Returns MySQL metadata in exact query projection order.
    #[must_use]
    pub fn columns(&self) -> &[ColumnInfo] {
        &self.layout.columns
    }

    /// Returns the one snapshot shared by both scans, or `None` for local empty.
    #[must_use]
    pub const fn snapshot_ts(&self) -> Option<u64> {
        self.snapshot_ts
    }

    /// Returns whether caller cancellation has fired.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Cancels both physical inputs through their shared statement authority.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Pulls at most `required_rows` joined rows without reading past that bound.
    pub fn next_batch(
        &mut self,
        required_rows: usize,
    ) -> Result<Vec<Vec<Datum>>, ConfiguredInnerJoinError> {
        self.next_full_batch(required_rows).map(|rows| {
            rows.into_iter()
                .map(|row| self.project_full_row(&row))
                .collect()
        })
    }

    /// Pulls unprojected `[left..., right...]` rows for an internal terminal
    /// operator.
    ///
    /// The public record set intentionally retains its projected-row contract.
    /// ORDER BY keys, however, are bound against the planner FullSchema and can
    /// name a hidden `USING` column or an otherwise unprojected input column.
    /// Keeping this stream private to `tidb-exec` lets the ordered adapter
    /// apply its limit before this one projection boundary without opening a
    /// second join or cancellation authority.
    pub(crate) fn next_full_batch(
        &mut self,
        required_rows: usize,
    ) -> Result<Vec<Vec<Datum>>, ConfiguredInnerJoinError> {
        self.lifecycle.mark_advanced();
        if required_rows == 0 || self.exhausted {
            return Ok(Vec::new());
        }
        if self.cancellation.is_cancelled() {
            return Err(self.abort(ConfiguredInnerJoinError::Cancelled));
        }

        let mut output = Vec::with_capacity(required_rows);
        while output.len() < required_rows {
            if self.cancellation.is_cancelled() {
                return Err(self.abort(ConfiguredInnerJoinError::Cancelled));
            }
            if self.current_left.is_none() {
                match self.pull_row(0)? {
                    Some(row) => {
                        self.current_left = Some(row);
                        self.right_cursor = 0;
                    }
                    None => {
                        self.exhausted = true;
                        break;
                    }
                }
            }

            if self.right_cursor < self.right_rows.len() {
                let matched = {
                    let left = self.current_left.as_ref().expect("left row established");
                    let right = &self.right_rows[self.right_cursor];
                    self.layout.rows_match(left, right)
                };
                self.right_cursor += 1;
                match matched {
                    Ok(true) => {
                        let left = self.current_left.as_ref().expect("left row established");
                        let right = &self.right_rows[self.right_cursor - 1];
                        output.push(self.layout.full_row(left, right));
                    }
                    Ok(false) => {}
                    Err(error) => return Err(self.abort(error)),
                }
                continue;
            }

            if !self.right_eof {
                match self.pull_row(1)? {
                    Some(row) => self.right_rows.push(row),
                    None => self.right_eof = true,
                }
                continue;
            }

            if self.right_rows.is_empty() {
                self.current_left = None;
                self.exhausted = true;
                break;
            }
            self.current_left = None;
            self.right_cursor = 0;
        }
        Ok(output)
    }

    /// Projects one internally streamed full row into exact query metadata
    /// order. `next_full_batch` constructs these rows, and the ordered adapter
    /// retains them unchanged until after LIMIT/TopN has finished.
    pub(crate) fn project_full_row(&self, row: &[Datum]) -> Vec<Datum> {
        self.layout.project_full_row(row)
    }

    /// Returns the physical FullSchema width expected by the ordered adapter.
    #[must_use]
    pub(crate) const fn full_schema_width(&self) -> usize {
        self.layout.widths[0] + self.layout.widths[1]
    }

    /// Finishes both child result sets exactly once.
    pub fn finish(&mut self) -> Result<(), ConfiguredInnerJoinError> {
        if !self.lifecycle.begin_finish() {
            return Ok(());
        }
        self.apply_to_sources(DistSqlRecordSet::finish)
    }

    /// Closes both child result sets exactly once, finishing them first.
    pub fn close(&mut self) -> Result<(), ConfiguredInnerJoinError> {
        if !self.lifecycle.begin_close() {
            return Ok(());
        }
        let finish_error = self.finish().err();
        let close_error = self.apply_to_sources(DistSqlRecordSet::close).err();
        match finish_error.or(close_error) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Exposes the once-only lifecycle for connection adapters and focused tests.
    #[must_use]
    pub const fn lifecycle(&self) -> &RecordSetLifecycle {
        &self.lifecycle
    }

    fn pull_row(
        &mut self,
        relation: usize,
    ) -> Result<Option<Vec<Datum>>, ConfiguredInnerJoinError> {
        if self.cancellation.is_cancelled() {
            return Err(self.abort(ConfiguredInnerJoinError::Cancelled));
        }
        let next = self
            .sources
            .as_mut()
            .expect("nonempty join retains both sources")[relation]
            .next_batch(1);
        let mut rows = match next {
            Ok(rows) => rows,
            Err(source) => {
                return Err(self.abort(ConfiguredInnerJoinError::Source { relation, source }));
            }
        };
        let Some(row) = rows.pop() else {
            return Ok(None);
        };
        let expected = self.layout.widths[relation];
        if row.len() != expected {
            let actual = row.len();
            return Err(self.abort(ConfiguredInnerJoinError::InvalidRowWidth {
                relation,
                expected,
                actual,
            }));
        }
        Ok(Some(row))
    }

    fn abort(&mut self, error: ConfiguredInnerJoinError) -> ConfiguredInnerJoinError {
        self.cancellation.cancel();
        let _ = self.close();
        error
    }

    fn apply_to_sources(
        &mut self,
        operation: fn(&mut DistSqlRecordSet) -> Result<(), DistSqlRecordSetError>,
    ) -> Result<(), ConfiguredInnerJoinError> {
        let Some(sources) = self.sources.as_mut() else {
            return Ok(());
        };
        let mut first_error = None;
        for (relation, source) in sources.iter_mut().enumerate() {
            if let Err(source) = operation(source) {
                first_error.get_or_insert(ConfiguredInnerJoinError::Source { relation, source });
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// Builds the exact MySQL projection metadata for a configured join without
/// opening any physical read. Local-empty terminal operators reuse this seam
/// so their result metadata cannot drift from an opened join record set.
pub fn configured_join_columns(
    plan: &ConfiguredJoinPlan,
    tables: [&ConfiguredTable; 2],
) -> Result<Vec<ColumnInfo>, ConfiguredInnerJoinError> {
    Ok(JoinLayout::from_plan(plan, tables)?.columns)
}

impl Drop for ConfiguredInnerJoinRecordSet {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[derive(Clone, Copy)]
enum ProjectionSlot {
    Left(usize),
    Right(usize),
}

struct JoinLayout {
    widths: [usize; 2],
    equality: Option<(usize, usize)>,
    projections: Vec<ProjectionSlot>,
    columns: Vec<ColumnInfo>,
}

impl JoinLayout {
    fn from_plan(
        plan: &ConfiguredJoinPlan,
        tables: [&ConfiguredTable; 2],
    ) -> Result<Self, ConfiguredInnerJoinError> {
        let widths = [tables[0].columns().len(), tables[1].columns().len()];
        if plan.left_scan().projected_columns().len() != widths[0]
            || plan.right_scan().projected_columns().len() != widths[1]
        {
            return Err(ConfiguredInnerJoinError::InvalidPlan(
                "configured join scans must project every FullSchema column".to_owned(),
            ));
        }
        if plan.full_schema().len() != widths[0] + widths[1] {
            return Err(ConfiguredInnerJoinError::InvalidPlan(
                "FullSchema width does not match configured inputs".to_owned(),
            ));
        }
        for (full_offset, column) in plan.full_schema().iter().enumerate() {
            validate_full_schema_column(column, full_offset, widths, tables)?;
        }
        if plan
            .visible_full_offsets()
            .iter()
            .any(|offset| *offset >= plan.full_schema().len())
        {
            return Err(ConfiguredInnerJoinError::InvalidPlan(
                "visible join schema contains an invalid FullSchema offset".to_owned(),
            ));
        }

        let equality = match plan.equality() {
            Some(equality) => {
                if equality.left().side() != JoinSide::Left
                    || equality.right().side() != JoinSide::Right
                    || equality.left().side_index() >= widths[0]
                    || equality.right().side_index() >= widths[1]
                {
                    return Err(ConfiguredInnerJoinError::InvalidPlan(
                        "join equality is not a valid left-to-right key".to_owned(),
                    ));
                }
                Some((equality.left().side_index(), equality.right().side_index()))
            }
            None => None,
        };

        let mut projections = Vec::with_capacity(plan.projections().len());
        let mut columns = Vec::with_capacity(plan.projections().len());
        for projection in plan.projections() {
            let full_column = plan
                .full_schema()
                .get(projection.full_offset())
                .ok_or_else(|| {
                    ConfiguredInnerJoinError::InvalidPlan(
                        "projection contains an invalid FullSchema offset".to_owned(),
                    )
                })?;
            projections.push(projection_slot(full_column));
            columns.push(protocol_column(
                full_column,
                projection.output_name(),
                tables,
            ));
        }
        Ok(Self {
            widths,
            equality,
            projections,
            columns,
        })
    }

    fn rows_match(
        &self,
        left: &[Datum],
        right: &[Datum],
    ) -> Result<bool, ConfiguredInnerJoinError> {
        let Some((left_offset, right_offset)) = self.equality else {
            return Ok(true);
        };
        join_key_eq(&left[left_offset], &right[right_offset]).ok_or_else(|| {
            // Neither operand decoded as one of the scalar kinds this
            // milestone's key comparison understands (int/uint/double/
            // string). Attribute the failure to whichever side is not a
            // recognized numeric/string key kind, preferring the left side
            // when both are unrecognized.
            let (relation, offset, kind) = match &left[left_offset] {
                Datum::Int(_)
                | Datum::UInt(_)
                | Datum::Real(_)
                | Datum::String(_)
                | Datum::Bytes(_) => (1, right_offset, right[right_offset].kind()),
                datum => (0, left_offset, datum.kind()),
            };
            ConfiguredInnerJoinError::InvalidJoinKey {
                relation,
                offset,
                kind,
            }
        })
    }

    fn full_row(&self, left: &[Datum], right: &[Datum]) -> Vec<Datum> {
        let mut row = Vec::with_capacity(left.len() + right.len());
        row.extend(left.iter().cloned());
        row.extend(right.iter().cloned());
        row
    }

    fn project_full_row(&self, row: &[Datum]) -> Vec<Datum> {
        self.projections
            .iter()
            .map(|slot| match *slot {
                ProjectionSlot::Left(offset) => row[offset].clone(),
                ProjectionSlot::Right(offset) => row[self.widths[0] + offset].clone(),
            })
            .collect()
    }
}

/// Evaluates one non-null SQL `=` join key comparison, or `None` if neither
/// operand decoded as a scalar kind this milestone's key comparison
/// understands.
///
/// Per Go `types.CompareInt` (`pkg/types/compare.go`), a signed and an
/// unsigned integer are never compared by reinterpreting one side's bit
/// pattern as the other's domain: an unsigned value above `i64::MAX`, or a
/// negative signed value, can never equal a value from the other domain.
/// `2^63` unsigned and `-1` signed are therefore correctly unequal to every
/// value on the other side, not just to each other by coincidence of bit
/// pattern.
fn join_key_eq(left: &Datum, right: &Datum) -> Option<bool> {
    match (left, right) {
        (Datum::Int(left), Datum::Int(right)) => Some(left == right),
        (Datum::UInt(left), Datum::UInt(right)) => Some(left == right),
        (Datum::Int(signed), Datum::UInt(unsigned))
        | (Datum::UInt(unsigned), Datum::Int(signed)) => {
            Some(*signed >= 0 && *unsigned <= i64::MAX as u64 && *signed == *unsigned as i64)
        }
        (Datum::Real(left), Datum::Real(right)) => Some(left == right),
        (Datum::String(left), Datum::String(right)) => Some(left.bytes() == right.bytes()),
        (Datum::Bytes(left), Datum::Bytes(right)) => Some(left == right),
        _ => None,
    }
}

fn validate_full_schema_column(
    column: &FullSchemaColumn,
    expected_full_offset: usize,
    widths: [usize; 2],
    tables: [&ConfiguredTable; 2],
) -> Result<(), ConfiguredInnerJoinError> {
    let relation = match column.side() {
        RelationSide::Left => 0,
        RelationSide::Right => 1,
    };
    let expected_offset = match relation {
        0 => column.side_offset(),
        _ => widths[0] + column.side_offset(),
    };
    let configured_column = tables[relation]
        .columns()
        .get(column.side_offset())
        .ok_or_else(|| {
            ConfiguredInnerJoinError::InvalidPlan(
                "FullSchema source offset is outside its configured relation".to_owned(),
            )
        })?;
    if column.full_offset() != expected_full_offset
        || expected_offset != expected_full_offset
        || column.table_id() != tables[relation].table_id()
        || column.column_id() != configured_column.id()
        || column.name() != configured_column.name()
    {
        return Err(ConfiguredInnerJoinError::InvalidPlan(
            "FullSchema column does not match its configured source".to_owned(),
        ));
    }
    Ok(())
}

fn projection_slot(column: &FullSchemaColumn) -> ProjectionSlot {
    match column.side() {
        RelationSide::Left => ProjectionSlot::Left(column.side_offset()),
        RelationSide::Right => ProjectionSlot::Right(column.side_offset()),
    }
}

fn protocol_column(
    column: &FullSchemaColumn,
    output_name: &str,
    tables: [&ConfiguredTable; 2],
) -> ColumnInfo {
    let relation = match column.side() {
        RelationSide::Left => 0,
        RelationSide::Right => 1,
    };
    let table = tables[relation];
    let configured_column = &table.columns()[column.side_offset()];
    // Mirrors the single-scan `protocol_columns` result metadata (Go
    // `column.ConvertColumnInfo`): each output column carries its own source
    // column's type/charset/length/unsigned flag rather than an assumed
    // signed `LONGLONG`, so a joined `DOUBLE`, `CHAR`, or `BIGINT UNSIGNED`
    // column reports correctly over the wire.
    let scalar = configured_column.scalar_type();
    ColumnInfo {
        schema: table.schema().to_owned(),
        table: column.qualifier().to_owned(),
        org_table: table.table().to_owned(),
        name: output_name.to_owned(),
        org_name: configured_column.name().to_owned(),
        column_length: scalar.result_column_length() as u32,
        charset: scalar.result_charset_id() as u16,
        flag: match configured_column.kind() {
            ConfiguredColumnKind::ClusteredPrimaryKey => 0x0003,
            ConfiguredColumnKind::StoredNotNull => 0x0001,
        } | if scalar.is_unsigned() { 0x0020 } else { 0 },
        decimal: scalar.result_decimal(),
        type_code: scalar.result_type_code() as u8,
        default_value: None,
    }
}

fn close_sources(sources: &mut [DistSqlRecordSet; 2]) {
    for source in sources {
        let _ = source.close();
    }
}
