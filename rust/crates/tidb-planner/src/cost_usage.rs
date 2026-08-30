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

//! Cost-model trace primitives from `pkg/planner/util/costusage/cost_misc.go`.

use std::collections::HashMap;

/// Cost flag that forces a fresh cost calculation.
pub const COST_FLAG_RECALCULATE: u64 = 1;
/// Cost flag that asks the optimizer to use true cardinality.
pub const COST_FLAG_USE_TRUE_CARDINALITY: u64 = 1 << 1;
/// Cost flag that enables cost tracing.
pub const COST_FLAG_TRACE: u64 = 1 << 2;

/// A factor name and its source cost-model value.
#[derive(Clone, Debug, PartialEq)]
pub struct CostVer2Factor {
    name: String,
    value: f64,
}

impl CostVer2Factor {
    /// Creates a factor with a caller-owned name and value.
    #[must_use]
    pub fn new(name: impl Into<String>, value: f64) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }

    /// Returns the factor name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the factor value.
    #[must_use]
    pub const fn value(&self) -> f64 {
        self.value
    }
}

impl std::fmt::Display for CostVer2Factor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}({})",
            self.name,
            tidb_datatype::format_float_g_shortest(self.value)
        )
    }
}

/// The trace accumulated by one cost expression.
#[derive(Clone, Debug, PartialEq)]
pub struct CostTrace {
    factor_costs: HashMap<String, f64>,
    formula: String,
}

impl CostTrace {
    fn new() -> Self {
        Self {
            factor_costs: HashMap::new(),
            formula: String::new(),
        }
    }

    /// Returns the lazily-built formula.
    #[must_use]
    pub fn formula(&self) -> &str {
        &self.formula
    }

    /// Returns factor costs keyed by source factor name.
    #[must_use]
    pub const fn factor_costs(&self) -> &HashMap<String, f64> {
        &self.factor_costs
    }
}

/// Cost value plus an optional trace of the factors that produced it.
#[derive(Clone, Debug, PartialEq)]
pub struct CostVer2 {
    cost: f64,
    trace: Option<CostTrace>,
}

impl CostVer2 {
    /// Returns the non-negative display cost, matching Go's `max(cost, 0)`.
    #[must_use]
    pub fn value(&self) -> f64 {
        if self.cost.is_nan() {
            self.cost
        } else {
            self.cost.max(0.0)
        }
    }

    /// Returns the optional cost trace.
    #[must_use]
    pub const fn trace(&self) -> Option<&CostTrace> {
        self.trace.as_ref()
    }
}

/// Options controlling cost calculation and tracing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PlanCostOption {
    cost_flag: u64,
}

impl PlanCostOption {
    /// Creates options with no flags.
    #[must_use]
    pub const fn new() -> Self {
        Self { cost_flag: 0 }
    }

    /// Replaces the current flags.
    pub const fn with_cost_flag(&mut self, flag: u64) -> &mut Self {
        self.cost_flag = flag;
        self
    }

    /// Returns the configured flags.
    #[must_use]
    pub const fn cost_flag(self) -> u64 {
        self.cost_flag
    }
}

/// Reports whether one cost flag is set.
#[must_use]
pub const fn has_cost_flag(cost_flag: u64, flag: u64) -> bool {
    (cost_flag & flag) > 0
}

/// Reports whether tracing is enabled for an optional options value.
#[must_use]
pub const fn trace_cost(options: Option<&PlanCostOption>) -> bool {
    match options {
        Some(options) => has_cost_flag(options.cost_flag, COST_FLAG_TRACE),
        None => false,
    }
}

/// Creates a zero cost, optionally with an empty trace.
#[must_use]
pub fn new_zero_cost_ver2(trace: bool) -> CostVer2 {
    CostVer2 {
        cost: 0.0,
        trace: trace.then(CostTrace::new),
    }
}

/// Creates a cost and, when tracing is enabled, records its factor and formula.
#[must_use]
pub fn new_cost_ver2<F>(
    options: Option<&PlanCostOption>,
    factor: &CostVer2Factor,
    cost: f64,
    lazy_formula: F,
) -> CostVer2
where
    F: FnOnce() -> String,
{
    let trace = trace_cost(options).then(|| {
        let mut trace = CostTrace::new();
        trace.factor_costs.insert(factor.name.clone(), cost);
        trace.formula = lazy_formula();
        trace
    });
    CostVer2 { cost, trace }
}

/// Sums costs and merges factor traces/formulas in source argument order.
#[must_use]
pub fn sum_cost_ver2(costs: &[CostVer2]) -> CostVer2 {
    let mut result = CostVer2 {
        cost: 0.0,
        trace: None,
    };
    for cost in costs {
        result.cost += cost.cost;
        if let Some(trace) = &cost.trace {
            let result_trace = result.trace.get_or_insert_with(CostTrace::new);
            for (factor, factor_cost) in &trace.factor_costs {
                *result_trace
                    .factor_costs
                    .entry(factor.clone())
                    .or_insert(0.0) += factor_cost;
            }
            if !trace.formula.is_empty() {
                if !result_trace.formula.is_empty() {
                    result_trace.formula.push_str(" + ");
                }
                result_trace.formula.push('(');
                result_trace.formula.push_str(&trace.formula);
                result_trace.formula.push(')');
            }
        }
    }
    result
}

/// Divides a cost and each traced factor by a denominator.
#[must_use]
pub fn div_cost_ver2(cost: &CostVer2, denominator: f64) -> CostVer2 {
    let trace = cost.trace.as_ref().map(|source| {
        let mut trace = CostTrace::new();
        for (factor, factor_cost) in &source.factor_costs {
            trace
                .factor_costs
                .insert(factor.clone(), factor_cost / denominator);
        }
        trace.formula = format!(
            "({})/{}",
            source.formula,
            format_float_fixed_two(denominator)
        );
        trace
    });
    CostVer2 {
        cost: cost.cost / denominator,
        trace,
    }
}

/// Multiplies a cost and each traced factor by a scale.
#[must_use]
pub fn mul_cost_ver2(cost: &CostVer2, scale: f64) -> CostVer2 {
    let trace = cost.trace.as_ref().map(|source| {
        let mut trace = CostTrace::new();
        for (factor, factor_cost) in &source.factor_costs {
            trace
                .factor_costs
                .insert(factor.clone(), factor_cost * scale);
        }
        trace.formula = format!("({})*{}", source.formula, format_float_fixed_two(scale));
        trace
    });
    CostVer2 {
        cost: cost.cost * scale,
        trace,
    }
}

/// Adds a tie-breaker cost without changing the existing trace.
#[must_use]
pub fn add_cost_without_trace(mut cost: CostVer2, additional_cost: f64) -> CostVer2 {
    cost.cost += additional_cost;
    cost
}

fn format_float_fixed_two(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "+Inf".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_owned()
    } else {
        format!("{value:.2}")
    }
}

/// The source package's pre-defined untraced zero cost.
pub const ZERO_COST_VER2: CostVer2 = CostVer2 {
    cost: 0.0,
    trace: None,
};
