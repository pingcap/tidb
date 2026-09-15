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

#[derive(Default)]
pub(super) struct GroupKeyBuffer {
    pub(super) encoded: Vec<Vec<u8>>,
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
                                for (logical, key) in self.encoded[..rows].iter_mut().enumerate() {
                                    let physical = chunk.get_row(logical).idx();
                                    if column.is_null(physical) {
                                        key.push(NIL_FLAG);
                                    } else {
                                        // Go ETInt hashes signed storage bits,
                                        // including unsigned SQL columns.
                                        key.push(VARINT_FLAG);
                                        encode_varint(key, column.get_int64(physical));
                                    }
                                }
                                continue;
                            }
                            EvalType::String if !field_type.is_hybrid() => {
                                let collator = field_type.runtime_collator();
                                for (logical, key) in self.encoded[..rows].iter_mut().enumerate() {
                                    let physical = chunk.get_row(logical).idx();
                                    if column.is_null(physical) {
                                        key.push(NIL_FLAG);
                                    } else {
                                        key.push(tidb_codec::COMPACT_BYTES_FLAG);
                                        let bytes = column.get_bytes(physical);
                                        encode_compact_bytes(key, &collator.immutable_key(&bytes));
                                    }
                                }
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
                for (value, key) in self.values.iter().zip(&mut self.encoded[..rows]) {
                    append_hash_agg_group_key_part(&timezone, expr, value, key)?;
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
            + self.encoded.iter().map(Vec::capacity).sum::<usize>()
            + datum_bytes(&self.values)
            + self.output_values.capacity() * std::mem::size_of::<Vec<Datum>>()
            + self.output_values.iter().map(datum_bytes).sum::<usize>()
    }
}
