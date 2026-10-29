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

//! Go `pkg/planner/indexadvisor/options.go`: the advisor's options, kept in
//! `mysql.tidb_kernel_options` and overridable per `RECOMMEND INDEX RUN`.

use std::collections::BTreeMap;
use std::time::Duration;

use tidb_ast::{Expr, RecommendIndexOption};

use crate::{advisor_error, Result};

/// Go `OptModule`.
pub(crate) const OPT_MODULE: &str = "index_advisor";
/// Go `OptMaxNumIndex`.
pub(crate) const OPT_MAX_NUM_INDEX: &str = "max_num_index";
/// Go `OptMaxIndexColumns`.
pub(crate) const OPT_MAX_INDEX_COLUMNS: &str = "max_index_columns";
/// Go `OptMaxNumQuery`.
pub(crate) const OPT_MAX_NUM_QUERY: &str = "max_num_query";
/// Go `OptTimeout`.
pub(crate) const OPT_TIMEOUT: &str = "timeout";

/// Go `AllOptions`.
pub const ALL_OPTIONS: [&str; 4] = [
    OPT_MAX_NUM_INDEX,
    OPT_MAX_INDEX_COLUMNS,
    OPT_MAX_NUM_QUERY,
    OPT_TIMEOUT,
];

/// Go `Option`.
#[derive(Clone, Debug)]
pub(crate) struct AdvisorOption {
    pub(crate) max_num_indexes: usize,
    pub(crate) max_index_width: usize,
    pub(crate) max_num_query: usize,
    pub(crate) timeout: Duration,
    pub(crate) specified_sqls: Vec<String>,
}

/// Go `description`.
pub fn description(option: &str) -> &'static str {
    match option {
        OPT_MAX_NUM_INDEX => "The maximum number of indexes to recommend for a table.",
        OPT_MAX_INDEX_COLUMNS => "The maximum number of columns in an index.",
        OPT_MAX_NUM_QUERY => "The maximum number of queries to recommend indexes.",
        OPT_TIMEOUT => "The timeout of index advisor.",
        _ => "",
    }
}

/// Go `defaultVal`.
pub(crate) fn default_val(option: &str) -> &'static str {
    match option {
        OPT_MAX_NUM_INDEX => "5",
        OPT_MAX_INDEX_COLUMNS => "3",
        OPT_MAX_NUM_QUERY => "1000",
        OPT_TIMEOUT => "30s",
        _ => "",
    }
}

/// The Go type name `%T` prints for the value a literal holds.
fn go_value_type(value: &Expr) -> &'static str {
    match value {
        Expr::Int(_) | Expr::Bool(_) => "int64",
        Expr::Decimal(_) => "*types.MyDecimal",
        Expr::Float(_) => "float64",
        Expr::Hex(_) => "types.BinaryLiteral",
        Expr::Bit(_) => "types.BitLiteral",
        Expr::Null => "<nil>",
        _ => "string",
    }
}

/// Go `intVal`.
fn int_val(value: &Expr) -> Result<i64> {
    match value {
        Expr::Int(text) => Ok(text
            .parse::<i64>()
            .or_else(|_| text.parse::<u64>().map(|big| big as i64))
            .unwrap_or(0)),
        Expr::Bool(flag) => Ok(i64::from(*flag)),
        other => Err(advisor_error(format!(
            "invalid value type {}",
            go_value_type(other)
        ))),
    }
}

/// Go `optionVal`: the text an option stores, or the refusal.
pub(crate) fn option_val(option: &str, value: &Expr) -> Result<String> {
    match option {
        OPT_MAX_NUM_INDEX | OPT_MAX_INDEX_COLUMNS | OPT_MAX_NUM_QUERY => {
            let number = int_val(value)?;
            if number <= 0 {
                return Err(advisor_error(format!(
                    "invalid value {number} for {option}"
                )));
            }
            Ok(number.to_string())
        }
        OPT_TIMEOUT => {
            let text = match value {
                Expr::String(text) | Expr::RawString(text) => text,
                other => {
                    return Err(advisor_error(format!(
                        "invalid value type {} for {option}, expected a duration string",
                        go_value_type(other)
                    )))
                }
            };
            let nanos = tidb_config::configtypes::parse_go_duration(text).map_err(advisor_error)?;
            if nanos < 0 {
                return Err(advisor_error(format!(
                    "invalid value {} for {option}",
                    tidb_config::configtypes::format_go_duration(nanos)
                )));
            }
            Ok(text.clone())
        }
        _ => Err(advisor_error(format!("unknown option {option}"))),
    }
}

/// Go `fillOption`, given the stored values (`GetOptions`).
pub(crate) fn fill_option(
    mut values: BTreeMap<String, String>,
    user_options: &[RecommendIndexOption],
    specified_sqls: Vec<String>,
) -> Result<AdvisorOption> {
    for user in user_options {
        let value = option_val(&user.name, &user.value)?;
        values.insert(user.name.clone(), value);
    }
    let number = |option: &str| -> usize {
        values
            .get(option)
            .and_then(|text| text.parse::<i64>().ok())
            .map_or(0, |parsed| usize::try_from(parsed).unwrap_or(0))
    };
    let timeout_text = values
        .get(OPT_TIMEOUT)
        .map_or("", String::as_str)
        .to_owned();
    let nanos =
        tidb_config::configtypes::parse_go_duration(&timeout_text).map_err(advisor_error)?;
    Ok(AdvisorOption {
        max_num_indexes: number(OPT_MAX_NUM_INDEX),
        max_index_width: number(OPT_MAX_INDEX_COLUMNS),
        max_num_query: number(OPT_MAX_NUM_QUERY),
        timeout: Duration::from_nanos(u64::try_from(nanos).unwrap_or(0)),
        specified_sqls,
    })
}
