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

//! Canonical live variance and standard-deviation partial state.
//!
//! Ordinary aggregation directly follows `func_varpop.go`'s count/sum/
//! intermediate-variance update and merge ordering. DISTINCT deliberately
//! owns a float-only set, matching the Go implementation instead of passing
//! through the executor's generic Datum tuple checker.

use std::collections::HashMap;
use std::mem::size_of;

use tidb_datatype::Datum;
use tidb_planner::aggregation_descriptor::AggregateKind;

use crate::ExecError;

#[derive(Clone, Debug, PartialEq)]
enum VariancePartial {
    Ordinary { count: i64, sum: f64, variance: f64 },
    Distinct(DistinctFloatSet),
}

/// One partial-state authority for VAR_POP, VAR_SAMP, STDDEV_POP, and
/// STDDEV_SAMP, in ordinary or DISTINCT mode.
#[derive(Clone, Debug, PartialEq)]
pub struct VarianceState {
    kind: AggregateKind,
    partial: VariancePartial,
}

impl VarianceState {
    /// Returns whether `kind` belongs to this state family.
    #[must_use]
    pub const fn supports(kind: AggregateKind) -> bool {
        matches!(
            kind,
            AggregateKind::VarPop
                | AggregateKind::VarSamp
                | AggregateKind::StddevPop
                | AggregateKind::StddevSamp
        )
    }

    /// Creates the source ordinary or float-DISTINCT partial state.
    pub fn new(kind: AggregateKind, distinct: bool) -> Result<Self, ExecError> {
        if !Self::supports(kind) {
            return Err(ExecError::Unsupported("variance aggregate kind"));
        }
        let partial = if distinct {
            VariancePartial::Distinct(DistinctFloatSet::default())
        } else {
            VariancePartial::Ordinary {
                count: 0,
                sum: 0.0,
                variance: 0.0,
            }
        };
        Ok(Self { kind, partial })
    }

    /// Restores the empty partial state while preserving kind and mode.
    pub fn reset(&mut self) {
        match &mut self.partial {
            VariancePartial::Ordinary {
                count,
                sum,
                variance,
            } => {
                *count = 0;
                *sum = 0.0;
                *variance = 0.0;
            }
            VariancePartial::Distinct(values) => values.clear(),
        }
    }

    /// Folds one already-resolved value. NULL is skipped like EvalReal's
    /// `isNull` branch; non-real coercion remains an explicit executor gap.
    pub fn update(&mut self, value: &Datum) -> Result<(), ExecError> {
        match value {
            Datum::Null => Ok(()),
            Datum::Real(value) => self.update_real(Some(*value)),
            _ => Err(ExecError::Unsupported("variance EvalReal coercion")),
        }
    }

    /// Folds a source-shaped optional float value.
    pub fn update_real(&mut self, value: Option<f64>) -> Result<(), ExecError> {
        let Some(value) = value else {
            return Ok(());
        };
        // SQL's typed EvalReal path does not expose a dependency-closed
        // non-finite contract here. Reject it rather than silently giving a
        // HashMap a different NaN identity from Go map[float64].
        if !value.is_finite() {
            return Err(ExecError::Unsupported("variance non-finite real"));
        }
        match &mut self.partial {
            VariancePartial::Ordinary {
                count,
                sum,
                variance,
            } => {
                *count = count.wrapping_add(1);
                *sum += value;
                if *count > 1 {
                    *variance = calculate_intermediate(*count, *sum, value, *variance);
                }
            }
            VariancePartial::Distinct(values) => {
                values.insert(value);
            }
        }
        Ok(())
    }

    /// Merges a source partial into this destination in Go argument order.
    pub fn merge_from(&mut self, source: &Self) -> Result<(), ExecError> {
        if self.kind != source.kind {
            return Err(ExecError::Unsupported("variance aggregate kind mismatch"));
        }
        match (&source.partial, &mut self.partial) {
            (
                VariancePartial::Ordinary {
                    count: source_count,
                    sum: source_sum,
                    variance: source_variance,
                },
                VariancePartial::Ordinary {
                    count: destination_count,
                    sum: destination_sum,
                    variance: destination_variance,
                },
            ) => {
                if *source_count == 0 {
                    return Ok(());
                }
                if *destination_count == 0 {
                    *destination_count = *source_count;
                    *destination_sum = *source_sum;
                    *destination_variance = *source_variance;
                    return Ok(());
                }
                *destination_variance = calculate_merge(
                    *source_count,
                    *destination_count,
                    *source_sum,
                    *destination_sum,
                    *source_variance,
                    *destination_variance,
                );
                *destination_count = destination_count.wrapping_add(*source_count);
                *destination_sum += source_sum;
                Ok(())
            }
            (VariancePartial::Distinct(source), VariancePartial::Distinct(destination)) => {
                destination.merge_from(source);
                Ok(())
            }
            _ => Err(ExecError::Unsupported("variance DISTINCT mode mismatch")),
        }
    }

    /// Finalizes the selected population/sample variance or standard deviation.
    #[must_use]
    pub fn result(&self) -> Option<f64> {
        let (count, variance) = match &self.partial {
            VariancePartial::Ordinary {
                count, variance, ..
            } => (*count, *variance),
            VariancePartial::Distinct(values) => values.variance(),
        };
        finalize(self.kind, count, variance)
    }

    /// Number of retained DISTINCT values, or zero for ordinary state.
    #[must_use]
    pub fn distinct_len(&self) -> usize {
        match &self.partial {
            VariancePartial::Distinct(values) => values.len(),
            VariancePartial::Ordinary { .. } => 0,
        }
    }

    /// Fixed width of Go's ordinary count/sum/variance tuple.
    #[must_use]
    pub const fn ordinary_partial_state_size() -> usize {
        size_of::<i64>() + size_of::<f64>() + size_of::<f64>()
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
struct DistinctFloatSet {
    // Finite f64 has an exact bit identity except that Go equality treats
    // -0.0 and +0.0 as the same map key. Canonicalizing zero preserves that.
    values: HashMap<u64, f64>,
}

impl DistinctFloatSet {
    fn insert(&mut self, value: f64) {
        let bits = if value == 0.0 { 0 } else { value.to_bits() };
        self.values.entry(bits).or_insert(value);
    }

    fn merge_from(&mut self, source: &Self) {
        for value in source.values.values().copied() {
            self.insert(value);
        }
    }

    fn variance(&self) -> (i64, f64) {
        let count = self.values.len() as i64;
        if count == 0 {
            return (0, 0.0);
        }
        let sum: f64 = self.values.values().copied().sum();
        let mean = sum / count as f64;
        let variance = self
            .values
            .values()
            .map(|value| {
                let difference = *value - mean;
                difference * difference
            })
            .sum();
        (count, variance)
    }

    fn clear(&mut self) {
        self.values.clear();
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

fn calculate_intermediate(count: i64, sum: f64, input: f64, variance: f64) -> f64 {
    let t = count as f64 * input - sum;
    let denominator = count.wrapping_mul(count.wrapping_sub(1)) as f64;
    variance + (t * t) / denominator
}

fn calculate_merge(
    source_count: i64,
    destination_count: i64,
    source_sum: f64,
    destination_sum: f64,
    source_variance: f64,
    destination_variance: f64,
) -> f64 {
    let source_count_f64 = source_count as f64;
    let destination_count_f64 = destination_count as f64;
    let t = (source_count_f64 / destination_count_f64) * destination_sum - source_sum;
    destination_variance
        + source_variance
        + ((destination_count_f64 / source_count_f64) / (destination_count_f64 + source_count_f64))
            * t
            * t
}

fn finalize(kind: AggregateKind, count: i64, variance: f64) -> Option<f64> {
    match kind {
        AggregateKind::VarPop if count != 0 => Some(variance / count as f64),
        AggregateKind::VarSamp if count > 1 => Some(variance / (count - 1) as f64),
        AggregateKind::StddevPop if count != 0 => Some((variance / count as f64).sqrt()),
        AggregateKind::StddevSamp if count > 1 => Some((variance / (count - 1) as f64).sqrt()),
        AggregateKind::VarPop
        | AggregateKind::VarSamp
        | AggregateKind::StddevPop
        | AggregateKind::StddevSamp => None,
        _ => unreachable!("VarianceState only accepts variance kinds"),
    }
}

/// Compatibility adapter for the former standalone VAR_POP leaf.
#[derive(Clone, Debug, PartialEq)]
pub struct VarPopState(VarianceState);

impl Default for VarPopState {
    fn default() -> Self {
        Self::new()
    }
}

impl VarPopState {
    /// Creates an empty ordinary VAR_POP compatibility state.
    #[must_use]
    pub fn new() -> Self {
        Self(VarianceState::new(AggregateKind::VarPop, false).expect("VAR_POP is supported"))
    }

    /// Clears the canonical partial state.
    pub fn reset(&mut self) {
        self.0.reset();
    }

    /// Folds source-shaped nullable finite floats.
    pub fn update(&mut self, values: &[Option<f64>]) {
        for value in values {
            self.0
                .update_real(*value)
                .expect("finite source VAR_POP fixture");
        }
    }

    /// Merges another ordinary VAR_POP compatibility state.
    pub fn merge_from(&mut self, source: &Self) {
        self.0
            .merge_from(&source.0)
            .expect("matching ordinary VAR_POP states");
    }

    /// Returns NULL for an empty input or population variance otherwise.
    #[must_use]
    pub fn result(&self) -> Option<f64> {
        self.0.result()
    }

    /// Returns the represented Go ordinary tuple width, excluding facade metadata.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        VarianceState::ordinary_partial_state_size()
    }
}

/// Former standalone sample-variance finalizer, now routed to the canonical state.
#[must_use]
pub fn sample_variance(count: i64, variance: f64) -> Option<f64> {
    finalize(AggregateKind::VarSamp, count, variance)
}

/// Former standalone population-standard-deviation finalizer.
#[must_use]
pub fn population_stddev(count: i64, variance: f64) -> Option<f64> {
    finalize(AggregateKind::StddevPop, count, variance)
}

/// Former standalone sample-standard-deviation finalizer.
#[must_use]
pub fn sample_stddev(count: i64, variance: f64) -> Option<f64> {
    finalize(AggregateKind::StddevSamp, count, variance)
}
