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

//! Connection routing for incremental result-set responses.

use tidb_datatype::{Datum, PackedTime, TimeType};
use tidb_protocol::{
    is_binary_datetime_result_type, is_binary_decimal_result_type, is_binary_duration_result_type,
    is_binary_string_result_type, BinaryDateTimeType, BinaryResultCell, BinaryResultSetStream,
    ResultSetOptions, TYPE_DOUBLE, TYPE_FLOAT, TYPE_INT24, TYPE_LONG, TYPE_LONGLONG, TYPE_SHORT,
    TYPE_TINY, TYPE_YEAR,
};

/// Maps one decoded `Datum` to the binary cell its column type dumps, following
/// TiDB's `DumpBinaryRow` switch on `columns[i].Type`. Returns `None` when the
/// datum and column type disagree (a caller-surfaced error, never silent).
pub(crate) fn datum_to_binary_cell(datum: Datum, type_code: u8) -> Option<BinaryResultCell> {
    match datum {
        // NULL is type-agnostic: it writes no value bytes and only sets the
        // row's null-bitmap bit, so any result column admits it (a nullable
        // aggregate such as SUM over an empty group yields one).
        Datum::Null => Some(BinaryResultCell::Null),
        // GetInt64 feeds the fixed-width integer cases; the cell width matches
        // the `dump.Uint*` the column type selects.
        Datum::Int(value) => integer_cell(value, type_code),
        // TypeLonglong reads GetUint64; an unsigned value reuses the same
        // little-endian widths by bit reinterpretation.
        Datum::UInt(value) => integer_cell(value as i64, type_code),
        // TypeFloat dumps Float32bits(GetFloat32); TypeDouble dumps
        // Float64bits(GetFloat64). A generic real narrows for a FLOAT column,
        // while the source-distinct Float32 datum is already that width.
        Datum::Real(value) => match type_code {
            TYPE_FLOAT => Some(BinaryResultCell::Float(value as f32)),
            TYPE_DOUBLE => Some(BinaryResultCell::Double(value)),
            _ => None,
        },
        Datum::Float32(value) if type_code == TYPE_FLOAT => {
            Some(BinaryResultCell::Float(value as f32))
        }
        // TypeNewDecimal dumps LengthEncodedString(GetMyDecimal(i).String()); the
        // encoder stringifies the decimal, so the cell carries the value itself.
        Datum::Decimal(value) if is_binary_decimal_result_type(type_code) => {
            Some(BinaryResultCell::NewDecimal(value))
        }
        Datum::String(value) if is_binary_string_result_type(type_code) => {
            Some(BinaryResultCell::String(value.into_bytes()))
        }
        Datum::Bytes(value) if is_binary_string_result_type(type_code) => {
            Some(BinaryResultCell::String(value))
        }
        // `TypeBit` sits in Go's string arm and dumps `row.GetBytes(i)`, which
        // for a BIT column is the stored big-endian payload.
        Datum::Bit(value) | Datum::BinaryLiteral(value)
            if is_binary_string_result_type(type_code) =>
        {
            Some(BinaryResultCell::String(value.into_bytes()))
        }
        // `TypeEnum`/`TypeSet`/`TypeJSON`/`TypeTiDBVectorFloat32` each stringify
        // and then take the same length-encoded-string exit as the string arm.
        // Go's `Enum.String()`/`Set.String()` are both `return e.Name`.
        Datum::Enum(value, _) if is_binary_string_result_type(type_code) => {
            Some(BinaryResultCell::String(value.name().as_bytes().to_vec()))
        }
        Datum::Set(value, _) if is_binary_string_result_type(type_code) => {
            Some(BinaryResultCell::String(value.name().as_bytes().to_vec()))
        }
        Datum::Json(value) if is_binary_string_result_type(type_code) => {
            Some(BinaryResultCell::String(value.to_string().into_bytes()))
        }
        Datum::VectorFloat32(value) if is_binary_string_result_type(type_code) => {
            Some(BinaryResultCell::String(value.to_string().into_bytes()))
        }
        // `dump.BinaryDateTime(buffer, row.GetTime(i))`. The body shape follows
        // the value's own field type, so the cell carries it alongside the
        // packed calendar fields.
        Datum::Time(value) if is_binary_datetime_result_type(type_code) => {
            let packed = PackedTime::from_raw(value.to_packed_uint().ok()?);
            let kind = match value.kind() {
                TimeType::Date => BinaryDateTimeType::Date,
                TimeType::DateTime => BinaryDateTimeType::Datetime,
                TimeType::Timestamp => BinaryDateTimeType::Timestamp,
            };
            Some(BinaryResultCell::Datetime(packed, kind))
        }
        // `dump.BinaryTime(row.GetDuration(i, 0).Duration)` takes the signed
        // nanosecond span; the fsp travels in the column metadata, not the body.
        Datum::Duration(value) if is_binary_duration_result_type(type_code) => {
            Some(BinaryResultCell::Duration(value.nanoseconds()))
        }
        _ => None,
    }
}

fn integer_cell(value: i64, type_code: u8) -> Option<BinaryResultCell> {
    match type_code {
        TYPE_TINY => Some(BinaryResultCell::Tiny(value)),
        TYPE_SHORT | TYPE_YEAR => Some(BinaryResultCell::Short(value)),
        TYPE_INT24 | TYPE_LONG => Some(BinaryResultCell::Long(value)),
        TYPE_LONGLONG => Some(BinaryResultCell::LongLong(value)),
        _ => None,
    }
}

use crate::resultset_source::ResultSetSource;
use crate::resultset_writer::{
    write_result_set_tracked, FramedResultSetSink, ResultSetSink, ResultSetWriteError,
    ResultSetWriteOutcome,
};

/// Complete outcome of a connection-owned streaming response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionResultSetResponse {
    /// Complete MySQL frames beginning at server packet sequence one.
    pub framed: Vec<u8>,
    /// Incremental packet/row accounting.
    pub outcome: ResultSetWriteOutcome,
}

/// Writes one lazy result set and closes it exactly once on every path.
pub fn write_connection_result_set<S: ResultSetSource>(
    source: &mut S,
    options: ResultSetOptions,
    batch_size: usize,
) -> Result<ConnectionResultSetResponse, ResultSetWriteError> {
    let mut sink = FramedResultSetSink::new(1);
    let outcome = write_connection_result_set_to_sink(source, &mut sink, options, batch_size)?;
    Ok(ConnectionResultSetResponse {
        framed: sink.into_framed(),
        outcome,
    })
}

/// Streams one lazy result set into a caller-owned connection sink.
pub fn write_connection_result_set_to_sink<S: ResultSetSource, W: ResultSetSink>(
    source: &mut S,
    sink: &mut W,
    options: ResultSetOptions,
    batch_size: usize,
) -> Result<ResultSetWriteOutcome, ResultSetWriteError> {
    let result = write_result_set_tracked(source, sink, options, batch_size);
    let finish_result = match &result {
        Err(error) if !error.finish_attempted => source.finish(),
        _ => Ok(()),
    };
    let close_result = source.close();

    match (result, finish_result, close_result) {
        (Err(error), _, _) => Err(error.error),
        (Ok(_), Err(message), _) | (Ok(_), Ok(()), Err(message)) => Err(ResultSetWriteError {
            message,
            retryable: false,
            bytes_escaped: sink.packets_written() > 0,
        }),
        (Ok(outcome), Ok(()), Ok(())) => Ok(outcome),
    }
}

/// Streams one prepared signed-`BIGINT` result using MySQL binary rows and
/// preserves the ordinary connection-owned finish/close lifecycle.
pub fn write_connection_binary_result_set_to_sink<S: ResultSetSource, W: ResultSetSink>(
    source: &mut S,
    sink: &mut W,
    options: ResultSetOptions,
    batch_size: usize,
) -> Result<ResultSetWriteOutcome, ResultSetWriteError> {
    let result = write_binary_result_set_tracked(source, sink, options, batch_size);
    let finish_result = match &result {
        Err(error) if !error.finish_attempted => source.finish(),
        _ => Ok(()),
    };
    let close_result = source.close();

    match (result, finish_result, close_result) {
        (Err(error), _, _) => Err(error.error),
        (Ok(_), Err(message), _) | (Ok(_), Ok(()), Err(message)) => Err(ResultSetWriteError {
            message,
            retryable: false,
            bytes_escaped: sink.packets_written() > 0,
        }),
        (Ok(outcome), Ok(()), Ok(())) => Ok(outcome),
    }
}

struct BinaryTrackedError {
    error: ResultSetWriteError,
    finish_attempted: bool,
}

fn write_binary_result_set_tracked<S: ResultSetSource, W: ResultSetSink>(
    source: &mut S,
    sink: &mut W,
    options: ResultSetOptions,
    batch_size: usize,
) -> Result<ResultSetWriteOutcome, BinaryTrackedError> {
    let mut batch = source
        .next_batch(batch_size.max(1))
        .map_err(|message| BinaryTrackedError {
            error: ResultSetWriteError {
                message,
                retryable: true,
                bytes_escaped: false,
            },
            finish_attempted: false,
        })?;
    let columns = source
        .columns()
        .map_err(|message| binary_failure(message, sink, false))?;
    let mut stream = BinaryResultSetStream::new(columns.clone(), options)
        .map_err(|error| binary_failure(error.to_string(), sink, false))?;
    let metadata_packets = stream
        .metadata_packets()
        .map_err(|error| binary_failure(error.to_string(), sink, false))?;
    let metadata_refs = metadata_packets
        .iter()
        .map(Vec::as_slice)
        .collect::<Vec<_>>();
    write_binary_payloads(sink, &metadata_refs, false)?;

    let mut rows_written = 0;
    let mut pending_rows = Vec::new();
    loop {
        if batch.is_empty() {
            break;
        }
        for row in batch {
            // One Datum -> one binary cell, dispatched by the column type exactly
            // as Go's DumpBinaryRow switches on `columns[i].Type`: an integer
            // column picks the matching fixed width, a string column takes its
            // raw bytes.
            let cells = row
                .into_iter()
                .zip(&columns)
                .enumerate()
                .map(|(column, (datum, metadata))| {
                    datum_to_binary_cell(datum, metadata.type_code).ok_or_else(|| {
                        format!(
                            "prepared binary result column {column} datum does not match type {}",
                            metadata.type_code
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|message| binary_failure(message, sink, false))?;
            let payload = stream
                .row_packet_owned(cells)
                .map_err(|error| binary_failure(error.to_string(), sink, false))?;
            pending_rows.push(payload);
            rows_written += 1;
        }
        let next_batch = match source.next_batch(batch_size.max(1)) {
            Ok(next_batch) => next_batch,
            Err(message) => {
                let pending_refs = pending_rows
                    .iter()
                    .map(Vec::as_slice)
                    .collect::<Vec<_>>();
                write_binary_payloads(sink, &pending_refs, false)?;
                return Err(binary_failure(message, sink, false));
            }
        };
        if next_batch.is_empty() {
            break;
        }
        let pending_refs = pending_rows
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<_>>();
        write_binary_payloads(sink, &pending_refs, false)?;
        pending_rows.clear();
        batch = next_batch;
    }

    if let Err(message) = source.finish() {
        let pending_refs = pending_rows
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<_>>();
        write_binary_payloads(sink, &pending_refs, false)?;
        return Err(binary_failure(message, sink, true));
    }
    let terminal = stream
        .finish_packet()
        .map_err(|error| binary_failure(error.to_string(), sink, true))?;
    let mut final_refs = pending_rows
        .iter()
        .map(Vec::as_slice)
        .collect::<Vec<_>>();
    final_refs.push(&terminal);
    write_binary_payloads(sink, &final_refs, true)?;
    flush_binary_payload(sink, true)?;
    Ok(ResultSetWriteOutcome {
        rows_written,
        packets_written: sink.packets_written(),
    })
}

fn binary_failure<W: ResultSetSink>(
    message: String,
    sink: &W,
    finish_attempted: bool,
) -> BinaryTrackedError {
    BinaryTrackedError {
        error: ResultSetWriteError {
            message,
            retryable: false,
            bytes_escaped: sink.packets_written() > 0,
        },
        finish_attempted,
    }
}

fn write_binary_payloads<W: ResultSetSink>(
    sink: &mut W,
    payloads: &[&[u8]],
    finish_attempted: bool,
) -> Result<(), BinaryTrackedError> {
    sink.write_payloads(payloads).map_err(|error| BinaryTrackedError {
        error: ResultSetWriteError {
            message: error.message,
            retryable: false,
            bytes_escaped: sink.packets_written() > 0 || error.bytes_escaped,
        },
        finish_attempted,
    })
}

fn flush_binary_payload<W: ResultSetSink>(
    sink: &mut W,
    finish_attempted: bool,
) -> Result<(), BinaryTrackedError> {
    sink.flush().map_err(|error| BinaryTrackedError {
        error: ResultSetWriteError {
            message: error.message,
            retryable: false,
            bytes_escaped: sink.packets_written() > 0 || error.bytes_escaped,
        },
        finish_attempted,
    })
}

#[cfg(test)]
mod tests {
    use super::datum_to_binary_cell;
    use tidb_datatype::{
        BinaryJSON, CoreTime, Datum, MySqlDuration, MysqlEnum, MysqlSet, Time, TimeType,
        VectorFloat32,
    };
    use tidb_protocol::encode_binary_result_row;

    /// The fixture was captured from production `column.DumpBinaryRow` over
    /// real chunk rows at the accepted source boundary. This asserts the
    /// whole `Datum -> cell -> bytes` wiring, not just the encoder, so a column
    /// type that reaches the writer as the wrong cell is caught here rather
    /// than on the wire.
    fn go_row(name: &str) -> Vec<u8> {
        let fixture = include_str!("../../../difftests/gobinaryrow/go_binary_rows.txt");
        for line in fixture.lines() {
            if let Some(hex) = line
                .strip_prefix(name)
                .and_then(|rest| rest.strip_prefix(' '))
            {
                return (0..hex.len())
                    .step_by(2)
                    .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).expect("hex"))
                    .collect();
            }
        }
        panic!("fixture row {name}");
    }

    fn row(datum: Datum, type_code: u8) -> Vec<u8> {
        let cell = datum_to_binary_cell(datum, type_code).expect("column type admits this datum");
        encode_binary_result_row(&[cell])
    }

    #[test]
    fn temporal_datums_reach_the_wire_as_go_dumps_them() {
        let datetime = |core, kind, fsp| Datum::Time(Time::new(core, kind, fsp).expect("time"));
        assert_eq!(
            row(
                datetime(
                    CoreTime::from_date(2017, 1, 5, 23, 59, 59, 575_601),
                    TimeType::DateTime,
                    6
                ),
                tidb_protocol::TYPE_DATETIME
            ),
            go_row("datetime_micros")
        );
        assert_eq!(
            row(
                datetime(
                    CoreTime::from_date(2017, 1, 5, 23, 59, 59, 0),
                    TimeType::DateTime,
                    0
                ),
                tidb_protocol::TYPE_DATETIME
            ),
            go_row("datetime_seconds")
        );
        assert_eq!(
            row(
                datetime(CoreTime::default(), TimeType::DateTime, 0),
                tidb_protocol::TYPE_DATETIME
            ),
            go_row("datetime_zero")
        );
        assert_eq!(
            row(
                datetime(
                    CoreTime::from_date(2020, 6, 15, 12, 34, 56, 1),
                    TimeType::Timestamp,
                    6
                ),
                tidb_protocol::TYPE_TIMESTAMP
            ),
            go_row("timestamp_micros")
        );
        assert_eq!(
            row(
                datetime(
                    CoreTime::from_date(2020, 6, 15, 0, 0, 0, 0),
                    TimeType::Date,
                    0
                ),
                tidb_protocol::TYPE_DATE
            ),
            go_row("date_plain")
        );
    }

    #[test]
    fn duration_datums_reach_the_wire_as_go_dumps_them() {
        let duration = |nanoseconds| {
            Datum::Duration(MySqlDuration::from_nanoseconds(nanoseconds, 6).expect("duration"))
        };
        assert_eq!(
            row(duration(0), tidb_protocol::TYPE_DURATION),
            go_row("duration_zero")
        );
        assert_eq!(
            row(
                duration((26 * 3600 + 3 * 60 + 4) * 1_000_000_000),
                tidb_protocol::TYPE_DURATION
            ),
            go_row("duration_1d2h3m4s")
        );
        assert_eq!(
            row(
                duration((3600 + 2 * 60 + 3) * 1_000_000_000 + 456_789_000),
                tidb_protocol::TYPE_DURATION
            ),
            go_row("duration_micros")
        );
        assert_eq!(
            row(
                duration(-((10 * 3600 + 20 * 60 + 30) * 1_000_000_000)),
                tidb_protocol::TYPE_DURATION
            ),
            go_row("duration_negative")
        );
    }

    #[test]
    fn blob_bit_enum_set_and_json_datums_reach_the_wire_as_go_dumps_them() {
        assert_eq!(
            row(
                Datum::Bytes(b"hello blob".to_vec()),
                tidb_protocol::TYPE_BLOB
            ),
            go_row("blob")
        );
        assert_eq!(
            row(
                Datum::Bytes(b"tiny".to_vec()),
                tidb_protocol::TYPE_TINY_BLOB
            ),
            go_row("tiny_blob")
        );
        assert_eq!(
            row(
                Datum::Bit(tidb_datatype::BinaryLiteral::from(vec![0x01, 0x02])),
                tidb_protocol::TYPE_BIT
            ),
            go_row("bit")
        );
        assert_eq!(
            row(
                Datum::Enum(
                    MysqlEnum::new("green", 2),
                    tidb_datatype::Collation::Utf8Mb4Bin
                ),
                tidb_protocol::TYPE_ENUM
            ),
            go_row("enum")
        );
        assert_eq!(
            row(
                Datum::Set(
                    MysqlSet::new("a,c", 5),
                    tidb_datatype::Collation::Utf8Mb4Bin
                ),
                tidb_protocol::TYPE_SET
            ),
            go_row("set")
        );
        assert_eq!(
            row(
                Datum::Json(BinaryJSON::parse(r#"{"a": [1, 2]}"#).expect("json")),
                tidb_protocol::TYPE_JSON
            ),
            go_row("json")
        );
    }

    #[test]
    fn float32_and_vector_datums_reach_their_dump_binary_row_bodies() {
        let float = 1.5_f32;
        let mut expected_float = vec![0, 0];
        expected_float.extend_from_slice(&float.to_bits().to_le_bytes());
        assert_eq!(
            row(
                Datum::new_float32_from_f64(f64::from(float)),
                tidb_protocol::TYPE_FLOAT
            ),
            expected_float
        );
        assert_eq!(
            row(
                Datum::new_vector_float32(VectorFloat32::parse("[1,2]").unwrap()),
                tidb_protocol::TYPE_TIDB_VECTOR_FLOAT32
            ),
            b"\0\0\x05[1,2]"
        );
    }
}
