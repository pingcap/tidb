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

//! The wire forms of a statement's results that the node's connection path
//! shares: prepared-statement parameters bound to datums, result-set column
//! metadata (Go `ConvertColumnInfo`), and the materialized and record-set
//! result sources the connection layer drains.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, UNSPECIFIED_LENGTH};
use tidb_exec::{
    col_names_to_result_fields, convert_result_field, ResultFieldMetadata, ResultFieldTypeMetadata,
};
use tidb_protocol::ColumnInfo;

use crate::SessionRecordSet;

use crate::domain::resultset_source::{ResultSetSource, StatementStatus};
use crate::domain::wire_status::WireStatus;

/// Binds one `COM_STMT_EXECUTE` parameter list to the driver's datum domain.
///
/// Go `ExecBinaryParam` builds one datum per parameter kind: a signed width
/// becomes an Int, an unsigned one a UInt, FLOAT and DOUBLE their own real
/// domains, DECIMAL is parsed from its digits, and a NULL parameter is a NULL
/// datum. A string-family parameter is `types.NewDatum(string)` -- a string
/// datum in the default collation, the same datum a quoted literal in the
/// text protocol produces -- and only the BLOB family is `NewBytesDatum`.
/// Binding every string parameter as binary bytes made `c_last = ?` a
/// binary-collation comparison against a `utf8mb4` column, which this tier
/// evaluates locally after fetching the whole scanned range instead of
/// pushing it to the coprocessor (TPC-C ORDER_STATUS by customer name).
pub fn prepared_parameters(
    values: &[tidb_protocol::PreparedValue],
) -> Vec<tidb_datatype::Datum> {
    values
        .iter()
        .map(|value| match value {
            tidb_protocol::PreparedValue::SignedLongLong(value) => {
                tidb_datatype::Datum::Int(*value)
            }
            tidb_protocol::PreparedValue::UnsignedLongLong(value) => {
                tidb_datatype::Datum::UInt(*value)
            }
            tidb_protocol::PreparedValue::String(bytes) => {
                tidb_datatype::Datum::new_string(bytes.clone())
            }
            tidb_protocol::PreparedValue::Bytes(bytes) => {
                tidb_datatype::Datum::new_bytes(bytes.clone())
            }
            tidb_protocol::PreparedValue::Float(value) => {
                tidb_datatype::Datum::Float32(f64::from(*value))
            }
            tidb_protocol::PreparedValue::Double(value) => tidb_datatype::Datum::Real(*value),
            tidb_protocol::PreparedValue::Decimal(digits) => {
                match std::str::from_utf8(digits) {
                    Ok(text) => {
                        tidb_datatype::Datum::Decimal(tidb_datatype::Decimal::from_literal(text))
                    }
                    // Go's own FromString reports truncation and keeps
                    // what it read; digits that are not even text cannot
                    // be a number at all.
                    Err(_) => tidb_datatype::Datum::Null,
                }
            }
            tidb_protocol::PreparedValue::Null => tidb_datatype::Datum::Null,
            // Go parses the rendered text into a Time or Duration datum.
            // This tier keeps temporal values as their formatted text --
            // the same documented divergence the temporal casts and the
            // date/time builtins carry -- so the text IS the value here, and
            // it is the string datum a quoted temporal literal would be.
            tidb_protocol::PreparedValue::Temporal(text) => {
                tidb_datatype::Datum::new_string(text.clone())
            }
        })
        .collect()
}

/// Converts a `(name, FieldType)` output schema with no plan names to
/// protocol column metadata: no schema or table, and `org_name` mirroring the
/// display name.
pub fn select_columns(columns: &[(String, FieldType)]) -> Vec<ColumnInfo> {
    columns
        .iter()
        .map(|(name, field_type)| {
            convert_result_field(&ResultFieldMetadata {
                schema: String::new(),
                table: String::new(),
                org_table: String::new(),
                name: name.clone(),
                org_name: name.clone(),
                empty_org_name: false,
                default_value: None,
                field_type: result_field_type(field_type),
            })
        })
        .collect()
}

/// Go `tidbResultSet.Columns()`: `ConvertColumnInfo` over
/// `recordSet.Fields()`, which `colNames2ResultFields` derives from the
/// statement's output names and current database. A computed column has no
/// table and an empty `OrgName`. Without one name per column the result has
/// no fields to convert and takes [`select_columns`]' shape.
pub fn result_set_columns(
    columns: &[(String, FieldType)],
    (output_names, current_db): (&[tidb_datatype::FieldNameMetadata], &str),
) -> Vec<ColumnInfo> {
    if output_names.len() != columns.len() {
        return select_columns(columns);
    }
    let schema = columns
        .iter()
        .map(|(_, field_type)| result_field_type(field_type))
        .collect::<Vec<_>>();
    col_names_to_result_fields(&schema, output_names, current_db)
        .iter()
        .zip(columns)
        .map(|(field, (name, _))| {
            // The display name is the one the executor resolved for the
            // result, which the integration results pin.
            let mut field = field.as_result_field();
            name.clone_into(&mut field.name);
            convert_result_field(&field)
        })
        .collect()
}

fn result_field_type(field_type: &FieldType) -> ResultFieldTypeMetadata {
    ResultFieldTypeMetadata {
        code: field_type.code(),
        // Only the low 16 bits are MySQL wire column flags.
        flags: (field_type.flags() & u32::from(u16::MAX)) as u16,
        flen: length_to_option(field_type.flen()).map(|flen| flen as u32),
        decimal: length_to_option(field_type.decimal()).map(|decimal| decimal as u8),
        collation: field_type.collation(),
    }
}

const fn length_to_option(length: i64) -> Option<i64> {
    if length == UNSPECIFIED_LENGTH {
        None
    } else {
        Some(length)
    }
}

/// The one-column `affected_rows` result set that carries a write outcome on
/// the text path (see the module documentation for why not an OK packet).
pub fn affected_rows_source(count: u64) -> MaterializedResultSetSource {
    let field_type = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);
    MaterializedResultSetSource::new(
        select_columns(&[("affected_rows".to_owned(), field_type)]),
        vec![vec![Datum::UInt(count)]],
    )
}

/// A prepared SELECT backed by the session-owned executor record set.
/// Keeping the session borrow in this source lets the wire writer pull native
/// chunks without first collecting every row into nested vectors.
pub struct PreparedRecordSet<'a> {
    state: SessionRecordSet<'a>,
    field_types: Vec<FieldType>,
}

impl ResultSetSource for PreparedRecordSet<'_> {
    fn statement_status(&self) -> Option<StatementStatus<'_>> {
        let session = self.state.session();
        Some(StatementStatus {
            warnings: session.wire_warning_count(),
            status: WireStatus::of_session(session),
            affected_rows: session.statement_affected_rows(),
            last_insert_id: session.statement_insert_id(),
            info: session.statement_message().as_bytes(),
        })
    }

    fn new_chunk(&self) -> Option<Chunk> {
        Some(self.state.new_chunk())
    }

    fn field_types(&self) -> &[FieldType] {
        &self.field_types
    }

    fn next_chunk(&mut self, chunk: &mut Chunk) -> Result<(), tidb_executor::MysqlError> {
        self.state
            .next(chunk)
            .map_err(|error| error.to_mysql_error())
    }

    fn next_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Vec<Vec<Datum>>, tidb_executor::MysqlError> {
        let mut chunk = self.state.new_chunk();
        chunk.set_required_rows(max_rows as isize, max_rows);
        self.next_chunk(&mut chunk)?;
        Ok((0..chunk.num_rows())
            .map(|index| chunk.get_row(index).get_datum_row(&self.field_types))
            .collect())
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, tidb_executor::MysqlError> {
        Ok(result_set_columns(
            self.state.columns(),
            self.state.output_names(),
        ))
    }

    fn finish(&mut self) -> Result<(), tidb_executor::MysqlError> {
        self.state.finish().map_err(|error| error.to_mysql_error())
    }

    fn close(&mut self) -> Result<(), tidb_executor::MysqlError> {
        self.state.close().map_err(|error| error.to_mysql_error())
    }

    fn add_write_sql_resp_duration(&mut self, duration: std::time::Duration) {
        self.state.add_write_sql_resp_duration(duration);
    }

    fn add_result_chunk_cells(&mut self, cells: i64) {
        self.state.add_result_chunk_cells(cells);
    }
}

/// A fully materialized result-set source: the pipeline returns complete row
/// vectors, so batching only replays them to the incremental wire writer.
pub struct MaterializedResultSetSource {
    columns: Vec<ColumnInfo>,
    rows: std::vec::IntoIter<Vec<Datum>>,
}

impl MaterializedResultSetSource {
    /// Wraps already-computed columns and rows.
    #[must_use]
    pub fn new(columns: Vec<ColumnInfo>, rows: Vec<Vec<Datum>>) -> Self {
        Self {
            columns,
            rows: rows.into_iter(),
        }
    }
}

impl ResultSetSource for MaterializedResultSetSource {
    fn next_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Vec<Vec<Datum>>, tidb_executor::MysqlError> {
        Ok(self.rows.by_ref().take(max_rows.max(1)).collect())
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, tidb_executor::MysqlError> {
        Ok(self.columns.clone())
    }

    fn finish(&mut self) -> Result<(), tidb_executor::MysqlError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), tidb_executor::MysqlError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `ExecBinaryParam`'s string-family arm is `types.NewDatum(string)`:
    /// a string datum in the default collation, exactly what a quoted SQL
    /// literal produces. Binding it as `Datum::Bytes` (the binary collation)
    /// instead made a prepared `WHERE string_col = ?` incomparable to the
    /// coprocessor's own describable form, forcing the whole scanned range
    /// back for a local filter (TPC-C ORDER_STATUS's `c_last = ?` lookup by
    /// customer name). This must fail before the `prepared_parameters` fix
    /// and pass after it.
    #[test]
    fn a_binary_protocol_string_parameter_binds_the_same_datum_kind_as_a_text_literal() {
        let bound =
            prepared_parameters(&[tidb_protocol::PreparedValue::String(b"BARBARBAR".to_vec())]);
        assert_eq!(bound.len(), 1);
        assert!(
            matches!(bound[0], Datum::String(_)),
            "a string-family binary parameter must bind a string datum, not {:?}",
            bound[0]
        );
        assert_eq!(
            bound[0],
            Datum::new_string(b"BARBARBAR".to_vec()),
            "the collation must be TiDB's default (utf8mb4_bin), matching a literal"
        );

        // The BLOB family stays a bytes datum, as Go's `NewBytesDatum` arm is.
        let blob = prepared_parameters(&[tidb_protocol::PreparedValue::Bytes(b"raw".to_vec())]);
        assert_eq!(blob[0], Datum::new_bytes(b"raw".to_vec()));
    }
}
