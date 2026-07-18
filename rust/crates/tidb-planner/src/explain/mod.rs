// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

//! Planner-owned EXPLAIN schemas and row/tree rendering.
//!
//! This is the Rust owner of the dependency-closed surface in
//! `pkg/planner/core/common_plans.go`.  The executor supplies already planned
//! operator metadata; it does not select columns or serialize a second plan.

use std::fmt;

use tidb_ast::{LoadDataFields, LoadDataLines};

/// `LineFieldsInfo` defaults and overrides shared by LOAD DATA/SELECT INTO.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LineFieldsInfo {
    /// Byte sequence separating adjacent input fields.
    pub fields_terminated_by: String,
    /// Optional byte sequence enclosing field contents.
    pub fields_enclosed_by: String,
    /// Byte sequence introducing escaped input characters.
    pub fields_escaped_by: String,
    /// Whether field enclosure is optional.
    pub fields_opt_enclosed: bool,
    /// Prefix required at the start of an input line.
    pub lines_starting_by: String,
    /// Byte sequence terminating an input line.
    pub lines_terminated_by: String,
}

impl Default for LineFieldsInfo {
    fn default() -> Self {
        Self {
            fields_terminated_by: "\t".to_owned(),
            fields_enclosed_by: String::new(),
            fields_escaped_by: "\\".to_owned(),
            fields_opt_enclosed: false,
            lines_starting_by: String::new(),
            lines_terminated_by: "\n".to_owned(),
        }
    }
}

/// Exact translation of Go's `NewLineFieldsInfo` override order.
#[must_use]
pub fn new_line_fields_info(
    fields: Option<&LoadDataFields>,
    lines: Option<&LoadDataLines>,
) -> LineFieldsInfo {
    let mut info = LineFieldsInfo::default();
    if let Some(fields) = fields {
        if let Some(value) = &fields.terminated {
            info.fields_terminated_by.clone_from(value);
        }
        if let Some(value) = &fields.enclosed {
            info.fields_enclosed_by.clone_from(value);
        }
        if let Some(value) = &fields.escaped {
            info.fields_escaped_by.clone_from(value);
        }
        info.fields_opt_enclosed = fields.optionally_enclosed;
    }
    if let Some(lines) = lines {
        if let Some(value) = &lines.starting {
            info.lines_starting_by.clone_from(value);
        }
        if let Some(value) = &lines.terminated {
            info.lines_terminated_by.clone_from(value);
        }
    }
    info
}

/// Valid format spellings from `pkg/types/explain_format.go`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExplainFormat {
    /// Compact row format with operator names instead of numbered IDs.
    Brief,
    /// Graphviz DOT output.
    Dot,
    /// Optimizer-hint output.
    Hint,
    /// MySQL-compatible JSON output.
    Json,
    /// Traditional tabular row output.
    Row,
    /// Tabular output including estimated cost.
    Verbose,
    /// Cost output evaluated with runtime cardinalities.
    TrueCardCost,
    /// Encoded binary-plan output.
    Binary,
    /// TiDB-specific JSON output.
    TidbJson,
    /// Tabular output including the cost formula.
    CostTrace,
    /// Compact output used for plan-cache diagnostics.
    PlanCache,
    /// Text tree output without row estimates.
    PlanTree,
}

impl ExplainFormat {
    /// Validates a format case-insensitively. `traditional` normalizes to ROW,
    /// exactly where Go's `Explain.prepareSchema` performs the conversion.
    pub fn parse(value: &str) -> Result<Self, ExplainError> {
        match value.to_ascii_lowercase().as_str() {
            "brief" => Ok(Self::Brief),
            "dot" => Ok(Self::Dot),
            "hint" => Ok(Self::Hint),
            "json" => Ok(Self::Json),
            "row" | "traditional" => Ok(Self::Row),
            "verbose" => Ok(Self::Verbose),
            "true_card_cost" => Ok(Self::TrueCardCost),
            "binary" => Ok(Self::Binary),
            "tidb_json" => Ok(Self::TidbJson),
            "cost_trace" => Ok(Self::CostTrace),
            "plan_cache" => Ok(Self::PlanCache),
            "plan_tree" => Ok(Self::PlanTree),
            _ => Err(ExplainError::UnsupportedFormat {
                format: value.to_owned(),
                analyze: false,
            }),
        }
    }

    #[must_use]
    /// Returns the canonical lowercase spelling accepted by TiDB.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Brief => "brief",
            Self::Dot => "dot",
            Self::Hint => "hint",
            Self::Json => "json",
            Self::Row => "row",
            Self::Verbose => "verbose",
            Self::TrueCardCost => "true_card_cost",
            Self::Binary => "binary",
            Self::TidbJson => "tidb_json",
            Self::CostTrace => "cost_trace",
            Self::PlanCache => "plan_cache",
            Self::PlanTree => "plan_tree",
        }
    }
}

/// Statement-context fields mutated by Go before any operator ExplainInfo call.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExplainContext {
    /// Whether the statement is currently being planned for EXPLAIN.
    pub in_explain_stmt: bool,
    /// Canonical EXPLAIN format exposed to planner operators.
    pub explain_format: String,
}

/// Result field names selected by `Explain.prepareSchema`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExplainSchema {
    /// Ordered names of the columns returned by EXPLAIN.
    pub field_names: Vec<&'static str>,
}

/// A flattened operator's task identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExplainTask {
    /// Execution in the TiDB root task.
    Root,
    /// Execution in a coprocessor task.
    Cop {
        /// Coprocessor request kind, such as `cop` or `mpp`.
        request: String,
        /// Storage engine serving the request.
        store: String,
    },
}

impl fmt::Display for ExplainTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Root => f.write_str("root"),
            Self::Cop { request, store } => write!(f, "{request}[{store}]"),
        }
    }
}

/// Planner metadata consumed by the source row renderer.
#[derive(Clone, Debug, PartialEq)]
pub struct ExplainOperator {
    /// Physical operator name.
    pub operator: String,
    /// Numeric plan-node identifier.
    pub id: i32,
    /// Suffix appended to the displayed operator identifier.
    pub label: String,
    /// Optimizer-estimated output row count.
    pub estimated_rows: Option<f64>,
    /// Task in which the operator executes.
    pub task: ExplainTask,
    /// Table, index, partition, or other accessed object description.
    pub access_object: String,
    /// Operator-specific planning details.
    pub operator_info: String,
    /// Child operators in display order.
    pub children: Vec<ExplainOperator>,
}

impl ExplainOperator {
    /// Creates a root-task operator with empty metadata and no children.
    #[must_use]
    pub fn new(operator: impl Into<String>, id: i32) -> Self {
        Self {
            operator: operator.into(),
            id,
            label: String::new(),
            estimated_rows: None,
            task: ExplainTask::Root,
            access_object: String::new(),
            operator_info: String::new(),
            children: Vec::new(),
        }
    }

    /// Sets the optimizer-estimated output row count.
    #[must_use]
    pub fn with_estimated_rows(mut self, rows: f64) -> Self {
        self.estimated_rows = Some(rows);
        self
    }

    /// Sets the operator's execution task.
    #[must_use]
    pub fn with_task(mut self, task: ExplainTask) -> Self {
        self.task = task;
        self
    }

    /// Sets the accessed-object description.
    #[must_use]
    pub fn with_access_object(mut self, value: impl Into<String>) -> Self {
        self.access_object = value.into();
        self
    }

    /// Sets the operator-specific information column.
    #[must_use]
    pub fn with_operator_info(mut self, value: impl Into<String>) -> Self {
        self.operator_info = value.into();
        self
    }

    /// Replaces the operator's children in display order.
    #[must_use]
    pub fn with_children(mut self, children: impl IntoIterator<Item = Self>) -> Self {
        self.children = children.into_iter().collect();
        self
    }

    fn explain_id(&self, brief: bool) -> String {
        let id = if brief {
            self.operator.clone()
        } else {
            format!("{}_{}", self.operator, self.id)
        };
        format!("{id}{}", self.label)
    }
}

/// Planner-owned explain request and rendered rows.
#[derive(Clone, Debug, PartialEq)]
pub struct Explain {
    /// Requested output format.
    pub format: ExplainFormat,
    /// Whether child execution should collect runtime statistics.
    pub analyze: bool,
    /// Whether runtime statistics were requested independently of analyze.
    pub runtime_stats: bool,
    /// Root operator of the explained plan.
    pub target: ExplainOperator,
    /// Materialized EXPLAIN rows from the most recent rendering.
    pub rows: Vec<Vec<String>>,
}

impl Explain {
    /// Creates an EXPLAIN request for a planned operator tree.
    #[must_use]
    pub fn new(format: ExplainFormat, analyze: bool, target: ExplainOperator) -> Self {
        Self {
            format,
            analyze,
            runtime_stats: false,
            target,
            rows: Vec::new(),
        }
    }

    /// Selects the exact Go field-name schema and initializes statement context.
    pub fn prepare_schema(
        &self,
        context: &mut ExplainContext,
    ) -> Result<ExplainSchema, ExplainError> {
        context.in_explain_stmt = true;
        if context.explain_format.is_empty() {
            context.explain_format = self.format.as_str().to_owned();
        }
        let runtime = self.analyze || self.runtime_stats;
        let names = match (self.format, runtime) {
            (ExplainFormat::Row | ExplainFormat::Brief | ExplainFormat::PlanCache, false) => {
                vec!["id", "estRows", "task", "access object", "operator info"]
            }
            (ExplainFormat::PlanTree, false) => {
                vec!["id", "task", "access object", "operator info"]
            }
            (ExplainFormat::Verbose, false) => vec![
                "id",
                "estRows",
                "estCost",
                "task",
                "access object",
                "operator info",
            ],
            (ExplainFormat::Verbose, true) => vec![
                "id",
                "estRows",
                "estCost",
                "actRows",
                "task",
                "access object",
                "execution info",
                "operator info",
                "memory",
                "disk",
            ],
            (ExplainFormat::TrueCardCost, _) => vec![
                "id",
                "estRows",
                "estCost",
                "costFormula",
                "actRows",
                "task",
                "access object",
                "execution info",
                "operator info",
                "memory",
                "disk",
            ],
            (ExplainFormat::CostTrace, false) => vec![
                "id",
                "estRows",
                "estCost",
                "costFormula",
                "task",
                "access object",
                "operator info",
            ],
            (ExplainFormat::CostTrace, true) => vec![
                "id",
                "estRows",
                "estCost",
                "costFormula",
                "actRows",
                "task",
                "access object",
                "execution info",
                "operator info",
                "memory",
                "disk",
            ],
            (ExplainFormat::Row | ExplainFormat::Brief | ExplainFormat::PlanCache, true) => vec![
                "id",
                "estRows",
                "actRows",
                "task",
                "access object",
                "execution info",
                "operator info",
                "memory",
                "disk",
            ],
            (ExplainFormat::Dot, _) => vec!["dot contents"],
            (ExplainFormat::Hint, _) => vec!["hint"],
            (ExplainFormat::Binary, _) => vec!["binary plan"],
            (ExplainFormat::TidbJson, _) => vec!["TiDB_JSON"],
            (ExplainFormat::Json, _) => {
                return Err(ExplainError::UnsupportedFormat {
                    format: self.format.as_str().to_owned(),
                    analyze: self.analyze,
                });
            }
            (ExplainFormat::PlanTree, true) => {
                return Err(ExplainError::UnsupportedFormat {
                    format: self.format.as_str().to_owned(),
                    analyze: true,
                });
            }
        };
        Ok(ExplainSchema { field_names: names })
    }

    /// Renders the non-analyze ROW/BRIEF/PLAN_CACHE/PLAN_TREE source surface.
    pub fn render_result(
        &mut self,
        context: &mut ExplainContext,
    ) -> Result<&[Vec<String>], ExplainError> {
        self.prepare_schema(context)?;
        if self.analyze || self.runtime_stats {
            return Err(ExplainError::RuntimeStatsUnavailable);
        }
        if !matches!(
            self.format,
            ExplainFormat::Row
                | ExplainFormat::Brief
                | ExplainFormat::PlanCache
                | ExplainFormat::PlanTree
        ) {
            return Err(ExplainError::RendererUnavailable(self.format));
        }
        self.rows.clear();
        let mut ancestors_have_more = Vec::new();
        render_operator(
            &self.target,
            self.format,
            true,
            &mut ancestors_have_more,
            &mut self.rows,
        );
        Ok(&self.rows)
    }
}

fn render_operator(
    operator: &ExplainOperator,
    format: ExplainFormat,
    is_last: bool,
    ancestors_have_more: &mut Vec<bool>,
    rows: &mut Vec<Vec<String>>,
) {
    let brief = matches!(format, ExplainFormat::Brief | ExplainFormat::PlanTree);
    let mut id = String::new();
    if !ancestors_have_more.is_empty() {
        for has_more in ancestors_have_more
            .iter()
            .take(ancestors_have_more.len() - 1)
        {
            id.push_str(if *has_more { "│ " } else { "  " });
        }
        id.push_str(if is_last { "└─" } else { "├─" });
    }
    id.push_str(&operator.explain_id(brief));

    let mut row = vec![id];
    if format != ExplainFormat::PlanTree {
        row.push(operator.estimated_rows.map_or_else(
            || "N/A".to_owned(),
            |estimated_rows| format!("{estimated_rows:.2}"),
        ));
    }
    row.extend([
        operator.task.to_string(),
        operator.access_object.clone(),
        operator.operator_info.clone(),
    ]);
    rows.push(row);

    let child_count = operator.children.len();
    for (index, child) in operator.children.iter().enumerate() {
        let child_is_last = index + 1 == child_count;
        ancestors_have_more.push(!child_is_last);
        render_operator(child, format, child_is_last, ancestors_have_more, rows);
        ancestors_have_more.pop();
    }
}

/// EXPLAIN validation/rendering error text follows Go's user-visible wording.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExplainError {
    /// The requested format or format/analyze combination is unsupported.
    UnsupportedFormat {
        /// User-requested canonical format spelling.
        format: String,
        /// Whether the request was EXPLAIN ANALYZE.
        analyze: bool,
    },
    /// The format is valid but its renderer has not been translated.
    RendererUnavailable(ExplainFormat),
    /// Runtime-statistics rendering has not been translated.
    RuntimeStatsUnavailable,
}

impl fmt::Display for ExplainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedFormat {
                format,
                analyze: true,
            } => {
                write!(
                    f,
                    "explain format '{format}' with analyze is not supported now"
                )
            }
            Self::UnsupportedFormat {
                format,
                analyze: false,
            } => {
                write!(f, "explain format '{format}' is not supported now")
            }
            Self::RendererUnavailable(format) => write!(
                f,
                "explain format '{}' renderer is not translated",
                format.as_str()
            ),
            Self::RuntimeStatsUnavailable => {
                f.write_str("explain analyze runtime statistics are not translated")
            }
        }
    }
}

impl std::error::Error for ExplainError {}
