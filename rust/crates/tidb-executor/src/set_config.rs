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

//! Go `pkg/executor/set_config.go`: `SET CONFIG` posts one config item to
//! the TiKV, PD and TiFlash nodes it selects; a node's failure is a warning.

use tidb_datatype::{Datum, EvalType};
use tidb_domain::cluster_topology::ClusterServer;
use tidb_expr::expression::Expression;
use tidb_planner::common_plans::SetConfig;

use crate::DriverError;

/// Go `SetConfigExec.doRequest`'s `httpHandler`: one POST of a body to a
/// URL, answering the status code, Go's `resp.Status` text and the body.
pub type HttpHandler<'a> = &'a dyn Fn(&str, String) -> Result<(u16, String, String), String>;

fn errorf(message: String) -> DriverError {
    DriverError::Mysql(tidb_terror::MysqlError::unknown(message))
}

/// Go `SetConfigExec`.
pub struct SetConfigExec {
    p: SetConfig,
    json_body: String,
}

impl SetConfigExec {
    /// Go `executorBuilder.buildSetConfig`.
    #[must_use]
    pub fn new(p: SetConfig) -> Self {
        Self { p, json_body: String::new() }
    }

    /// Go `SetConfigExec.Open`.
    ///
    /// # Errors
    /// Go's refusals of the type, the instance, a TiFlash item and the value.
    pub fn open(&mut self, ctx: &dyn tidb_exprctx::context::Columns) -> Result<(), DriverError> {
        if !self.p.tp.is_empty() {
            self.p.tp = self.p.tp.to_lowercase();
            if !matches!(self.p.tp.as_str(), "tikv" | "tidb" | "pd" | "tiflash" | "tso" | "scheduling") {
                return Err(errorf(format!("unknown type {}", self.p.tp)));
            }
            if self.p.tp == "tidb" {
                return Err(errorf(
                    "TiDB doesn't support to change configs online, please use SQL variables".to_owned(),
                ));
            }
            if self.p.tp == "tso" || self.p.tp == "scheduling" {
                return Err(errorf(format!("{} doesn't support to change configs online", self.p.tp)));
            }
        }
        if !self.p.instance.is_empty() {
            self.p.instance = self.p.instance.to_lowercase();
            if !is_valid_instance(&self.p.instance) {
                return Err(errorf(format!("invalid instance {}", self.p.instance)));
            }
        }
        self.p.name = self.p.name.to_lowercase();

        if self.p.tp == "tiflash" {
            if !self.p.name.starts_with("raftstore-proxy.") {
                return Err(errorf(
                    "This command can only change config items begin with 'raftstore-proxy'. For other TiFlash config items, please update the config file directly. Your change to the config file will take effect immediately without a restart.".to_owned(),
                ));
            }
            self.p.name = self.p.name["raftstore-proxy.".len()..].to_owned();
        }

        self.json_body = convert_config_item_2_json(ctx, &self.p.name, &self.p.value)?;
        Ok(())
    }

    /// Go `SetConfigExec.Next` over `servers_info` (Go
    /// `infoschema.GetClusterServerInfo`): a node's failed request is a
    /// statement warning.
    ///
    /// # Errors
    /// A named instance missing from the cluster, or a TiDB or unknown node.
    pub fn next(
        &self,
        servers_info: Vec<ClusterServer>,
        scheme: &str,
        http_handler: HttpHandler<'_>,
        warnings: &mut Vec<String>,
    ) -> Result<(), DriverError> {
        let servers_info: Vec<_> = servers_info
            .into_iter()
            .filter(|server| self.p.tp.is_empty() || server.server_type == self.p.tp)
            .filter(|server| self.p.instance.is_empty() || server.address == self.p.instance)
            .collect();
        if !self.p.instance.is_empty() && servers_info.is_empty() {
            return Err(errorf(format!("instance {} is not found in this cluster", self.p.instance)));
        }
        for server_info in &servers_info {
            let url = match server_info.server_type.as_str() {
                "pd" => format!("{scheme}://{}/pd/api/v1/config", server_info.status_address),
                "tikv" | "tiflash" => format!("{scheme}://{}/config", server_info.status_address),
                "tidb" => {
                    return Err(errorf(
                        "TiDB doesn't support to change configs online, please use SQL variables".to_owned(),
                    ))
                }
                other => return Err(errorf(format!("Unknown server type {other}"))),
            };
            if let Err(error) = self.do_request(http_handler, &url) {
                warnings.push(error);
            }
        }
        Ok(())
    }

    /// Go `SetConfigExec.doRequest`.
    fn do_request(&self, http_handler: HttpHandler<'_>, url: &str) -> Result<(), String> {
        let (code, status, message) = http_handler(url, self.json_body.clone())?;
        if code == 200 {
            Ok(())
        } else if (400..600).contains(&code) {
            Err(format!("bad request to {url}: {message}"))
        } else {
            Err(format!("request {url} failed: {status}"))
        }
    }
}

/// Go `isValidInstance`: a `host:port` whose host resolves.
fn is_valid_instance(instance: &str) -> bool {
    let Some((host, port)) = split_host_port(instance) else {
        return false;
    };
    if port.is_empty() {
        return false;
    }
    std::net::ToSocketAddrs::to_socket_addrs(&(host, 0)).is_ok()
}

/// Go `net.SplitHostPort` for the shapes `isValidInstance` accepts.
fn split_host_port(instance: &str) -> Option<(&str, &str)> {
    if let Some(rest) = instance.strip_prefix('[') {
        let (host, port) = rest.split_once("]:")?;
        return Some((host, port));
    }
    let (host, port) = instance.rsplit_once(':')?;
    (!host.contains(':')).then_some((host, port))
}

/// Go `ConvertConfigItem2JSON`: `{"<key>":<value>}`, the value written by
/// its evaluation type.
///
/// # Errors
/// A NULL value, an evaluation error, or a value of another type.
pub fn convert_config_item_2_json(
    ctx: &dyn tidb_exprctx::context::Columns,
    key: &str,
    val: &Expression,
) -> Result<String, DriverError> {
    let field_type = val
        .static_type()
        .cloned()
        .unwrap_or_else(|| tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Null));
    let eval_type = field_type.eval_type();
    if !matches!(eval_type, EvalType::String | EvalType::Int | EvalType::Real | EvalType::Decimal) {
        return Err(errorf("unsupported config value type".to_owned()));
    }
    let value = val
        .eval(ctx, tidb_chunk::row::Row::empty())
        .map_err(|error| DriverError::Mysql(tidb_expr::EvalErrorSql::to_mysql_error(&error)))?;
    let text = match (&value, eval_type) {
        (Datum::Null, _) => return Err(errorf("can't set config to null".to_owned())),
        (_, EvalType::String) => tidb_go_strconv::quote_bytes(
            &value.to_bytes().map_err(|error| errorf(error.to_string()))?,
        ),
        (_, EvalType::Int) => {
            let i = value.get_int64();
            if field_type.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN) {
                (if i == 0 { "false" } else { "true" }).to_owned()
            } else {
                i.to_string()
            }
        }
        (Datum::Real(f) | Datum::Float32(f), EvalType::Real) => {
            tidb_goruntime::go_strconv::format_float(*f, b'g', -1, 64)
        }
        (Datum::Decimal(d), EvalType::Decimal) => d.to_string(),
        _ => return Err(errorf("unsupported config value type".to_owned())),
    };
    Ok(format!("{{\"{key}\":{text}}}"))
}

#[cfg(test)]
mod tests {
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::constant::Constant;

    use super::*;

    /// Go `TestSetClusterConfigJSONData` (`pkg/executor/set_test.go`); Go's
    /// `nil` expression case has no Rust counterpart.
    #[test]
    fn test_set_cluster_config_json_data() {
        let d = tidb_datatype::Decimal::from_f64(123.456).unwrap();
        let mut ty_bool = FieldType::new(FieldTypeCode::Tiny);
        ty_bool.add_flags(tidb_datatype::FieldTypeFlags::IS_BOOLEAN);
        let constant = |value: Datum, ret_type: FieldType| Expression::Constant(Constant::new(value, ret_type));
        let cases: Vec<(Expression, &str, bool)> = vec![
            (constant(Datum::Int(1), ty_bool.clone()), r#"{"k":true}"#, true),
            (constant(Datum::Int(0), ty_bool.clone()), r#"{"k":false}"#, true),
            (constant(Datum::Int(2333), FieldType::new(FieldTypeCode::Long)), r#"{"k":2333}"#, true),
            (constant(Datum::Real(23.33), FieldType::new(FieldTypeCode::Double)), r#"{"k":23.33}"#, true),
            (constant(Datum::new_string_datum("abcd"), FieldType::new(FieldTypeCode::String)), r#"{"k":"abcd"}"#, true),
            (constant(Datum::Decimal(d), FieldType::new(FieldTypeCode::NewDecimal)), r#"{"k":123.456}"#, true),
            (constant(Datum::Null, FieldType::new(FieldTypeCode::LongLong)), "", false),
            // unsupported type
            (constant(Datum::Null, FieldType::new(FieldTypeCode::Json)), "", false),
            (
                constant(
                    Datum::new_string_datum(r#"["no","no","lz4","lz4","lz4","zstd","zstd"]"#),
                    FieldType::new(FieldTypeCode::String),
                ),
                r#"{"k":"[\"no\",\"no\",\"lz4\",\"lz4\",\"lz4\",\"zstd\",\"zstd\"]"}"#,
                true,
            ),
        ];
        let ctx = crate::StmtContext::for_query();
        for (val, result, succ) in cases {
            let converted = convert_config_item_2_json(&ctx, "k", &val);
            if succ {
                assert_eq!(converted.unwrap(), result);
            } else {
                assert!(converted.is_err());
            }
        }
    }
}
