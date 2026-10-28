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

//! Go aggregate.GetGroupKey: evaluate and encode a whole column at a time.

use super::*;

/// Walk logical rows in the same order as Go's `Chunk.GetRow`, resolving the
/// selection vector once for the whole column batch.  HashGroupKey receives a
/// column that has already been evaluated, so repeating row-wrapper creation
/// for every grouping column only adds selection/indexing overhead.
#[inline(always)]
pub(super) fn for_each_logical_row(
    chunk: &Chunk,
    rows: usize,
    mut visit: impl FnMut(usize, usize),
) {
    if let Some(selection) = chunk.sel() {
        debug_assert_eq!(selection.len(), rows);
        for (logical, &physical) in selection.iter().take(rows).enumerate() {
            visit(logical, physical);
        }
    } else {
        for logical in 0..rows {
            visit(logical, logical);
        }
    }
}

#[derive(Default)]
pub(super) struct GroupKeyBuffer {
    pub(super) encoded: Vec<Vec<u8>>,
    active_rows: usize,
    values: Vec<Datum>,
    // Only cop partial output needs evaluated grouping datums. Retain them
    // column-wise so a new group never evaluates its expressions twice.
    output_values: Vec<Vec<Datum>>,
}

impl GroupKeyBuffer {
    pub(super) fn prepare<C: Columns>(
        &mut self,
        ctx: &C,
        chunk: &Chunk,
        group_by: &[Expression],
        retain_values: bool,
    ) -> Result<(), ExecError> {
        let rows = chunk.num_rows();
        // Keep the backing Vec sized for reuse, but expose only this chunk's
        // logical length for accounting. Go's GetGroupKey returns
        // groupKey[:numRows] while retaining the backing capacity.
        self.active_rows = rows;
        self.encoded.resize_with(self.encoded.len().max(rows), || {
            Vec::with_capacity(10 * group_by.len())
        });
        for key in &mut self.encoded[..rows] {
            key.clear();
        }
        self.output_values
            .resize_with(if retain_values { group_by.len() } else { 0 }, Vec::new);
        for values in &mut self.output_values {
            values.clear();
        }
        let timezone = ctx.time_zone();
        let result = (|| {
            for (group_index, expr) in group_by.iter().enumerate() {
                let field_type = expr.static_type().ok_or_else(|| {
                    ExecError::internal("HashAgg group expression has no field type")
                })?;
                if !retain_values {
                    if let Some(index) = expr
                        .as_column()
                        .and_then(|column| usize::try_from(column.index).ok())
                        .filter(|index| *index < chunk.num_cols())
                    {
                        let column = chunk.column(index);
                        match field_type.eval_type() {
                            EvalType::Int if column.type_size() == 8 => {
                                {
                                    let raw = column.raw_cells();
                                    for_each_logical_row(chunk, rows, |logical, physical| {
                                        let key = &mut self.encoded[logical];
                                        if column.is_null(physical) {
                                            key.push(NIL_FLAG);
                                        } else {
                                            // Go ETInt hashes signed storage bits,
                                            // including unsigned SQL columns.
                                            let value = i64::from_ne_bytes(
                                                raw.get(physical)
                                                    .try_into()
                                                    .expect("ETInt group key cell is 8 bytes"),
                                            );
                                            key.push(VARINT_FLAG);
                                            encode_varint(key, value);
                                        }
                                    });
                                };
                                continue;
                            }
                            EvalType::Real if matches!(column.type_size(), 4 | 8) => {
                                {
                                    let raw = column.raw_cells();
                                    for_each_logical_row(chunk, rows, |logical, physical| {
                                        let key = &mut self.encoded[logical];
                                        if column.is_null(physical) {
                                            key.push(NIL_FLAG);
                                        } else {
                                            let cell = raw.get(physical);
                                            let value = match cell.len() {
                                                4 => f64::from(f32::from_ne_bytes(
                                                    cell.try_into().expect("Float cell is 4 bytes"),
                                                )),
                                                8 => f64::from_ne_bytes(
                                                    cell.try_into()
                                                        .expect("Double cell is 8 bytes"),
                                                ),
                                                _ => unreachable!("real group key width is 4 or 8"),
                                            };
                                            key.push(tidb_codec::FLOAT_FLAG);
                                            tidb_codec::encode_float(key, value);
                                        }
                                    });
                                };
                                continue;
                            }
                            EvalType::String if !field_type.is_hybrid() => {
                                let collator = field_type.runtime_collator();
                                {
                                    let raw = column.raw_cells();
                                    for_each_logical_row(chunk, rows, |logical, physical| {
                                        let key = &mut self.encoded[logical];
                                        if column.is_null(physical) {
                                            key.push(NIL_FLAG);
                                        } else {
                                            key.push(tidb_codec::COMPACT_BYTES_FLAG);
                                            let bytes = raw.get(physical);
                                            encode_compact_bytes(
                                                key,
                                                &collator.immutable_key(bytes),
                                            );
                                        }
                                    });
                                };
                                continue;
                            }
                            _ => {}
                        }
                    }
                }
                // Go's scalar EvalExpr fallback still completes one column
                // before encoding it or evaluating another expression.
                self.values.clear();
                for row in 0..rows {
                    self.values.push(expr.eval(ctx, chunk.get_row(row))?);
                }
                let key_type = group_key_type(field_type);
                for (value, key) in self.values.iter().zip(&mut self.encoded[..rows]) {
                    tidb_codec::append_hash_group_key_in_timezone(&timezone, value, &key_type, key)
                        .map_err(|error| ExecError::internal(error.to_string()))?;
                }
                if retain_values {
                    std::mem::swap(&mut self.values, &mut self.output_values[group_index]);
                }
            }
            Ok(())
        })();
        self.values.clear();
        result
    }

    pub(super) fn append_values(&self, row: usize, output: &mut Vec<Datum>) {
        output.extend(self.output_values.iter().map(|column| column[row].clone()));
    }

    pub(super) fn memory_usage(&self) -> usize {
        fn datum_bytes(values: &Vec<Datum>) -> usize {
            values.capacity() * std::mem::size_of::<Datum>()
                + values
                    .iter()
                    .map(|value| value.estimated_mem_usage() - std::mem::size_of::<Datum>())
                    .sum::<usize>()
        }
        self.encoded.capacity() * std::mem::size_of::<Vec<u8>>()
            + self.encoded[..self.active_rows]
                .iter()
                .map(Vec::capacity)
                .sum::<usize>()
            + datum_bytes(&self.values)
            + self.output_values.capacity() * std::mem::size_of::<Vec<Datum>>()
            + self.output_values.iter().map(datum_bytes).sum::<usize>()
    }
}

/// Go `aggregate.GetGroupKey`'s key type for one group item: an ENUM keys by
/// its value rather than its name (`EnumSetAsIntFlag`, issue #26885 -- the
/// invalid value 0 has the name '' a user-defined member may share), and a
/// DECIMAL by its own precision (`SetFlen(0)`, so `EncodeDecimal` never
/// fails on a value wider than the declared type).
fn group_key_type(
    field_type: &tidb_datatype::FieldType,
) -> std::borrow::Cow<'_, tidb_datatype::FieldType> {
    match field_type.code() {
        tidb_datatype::FieldTypeCode::Enum => {
            let mut key_type = field_type.clone();
            key_type.add_flags(tidb_datatype::FieldTypeFlags::ENUM_SET_AS_INT);
            std::borrow::Cow::Owned(key_type)
        }
        tidb_datatype::FieldTypeCode::NewDecimal => {
            let mut key_type = field_type.clone();
            key_type.set_flen(0);
            std::borrow::Cow::Owned(key_type)
        }
        _ => std::borrow::Cow::Borrowed(field_type),
    }
}
