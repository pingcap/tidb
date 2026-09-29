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

use super::*;

#[test]
fn region_aggregate_matches_go_wire_fixture() {
    use prost::Message;
    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }
    fn diagnostic(s: &str) -> String {
        if let Some((prefix, message)) = s.split_once(']') {
            if prefix.starts_with('[') {
                return format!(
                    "[{}]{message}",
                    prefix.rsplit(':').next().unwrap().trim_start_matches('[')
                );
            }
        }
        s.to_owned()
    }
    fn hex(s: &[u8]) -> String {
        s.iter().map(|b| format!("{b:02x}")).collect()
    }
    let previous = tidb_datatype::new_collation_enabled();
    tidb_datatype::set_new_collation_enabled(true);
    use std::io::Read;
    let mut data = String::new();
    flate2::read::GzDecoder::new(
        include_bytes!("../../testdata/region-aggregate-go.tsv.gz").as_slice(),
    )
    .read_to_string(&mut data)
    .unwrap();
    assert_eq!(data.lines().count(), 3129);
    let mut differences = Vec::new();
    for line in data.lines() {
        let f: Vec<_> = line.split('\t').collect();
        let flags = f[1].parse().unwrap();
        let pb = tidb_proto::tipb::Aggregation::decode(unhex(f[2]).as_slice()).unwrap();
        let col = tidb_proto::tipb::ColumnInfo::decode(unhex(f[3]).as_slice()).unwrap();
        let leaves = f[4]
            .split(',')
            .map(|s| tidb_proto::tipb::Expr::decode(unhex(s).as_slice()).unwrap())
            .collect::<Vec<_>>();
        let mut ctx = RequestEvalContext::new(SessionTimeZone::utc(), 4, flags);
        ctx.column_types = vec![tidb_expr::distsql_builtin::pb_type_to_field_type(
            leaves[0].field_type.as_ref().unwrap(),
        )];
        let ctx = Arc::new(ctx);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || -> Result<String, (String, String)> {
                let mut agg = super::super::RegionAggregator::build(&pb, &[col], &ctx)
                    .map_err(|e| ("build_error".to_owned(), e))?;
                for leaf in leaves {
                    let value = tidb_expr::distsql_builtin::pb_to_expr(&leaf, &[])
                        .unwrap()
                        .eval(ctx.as_ref(), tidb_chunk::row::Row::empty())
                        .unwrap();
                    agg.update(&[value]).map_err(|e| ("error".to_owned(), e))?;
                }
                let rows = agg.finish();
                assert_eq!(rows.len(), 1);
                Ok(rows[0]
                    .iter()
                    .map(|v| match v {
                        Datum::Null => "null".to_owned(),
                        Datum::Real(v) | Datum::Float32(v) => format!("real:{:016x}", v.to_bits()),
                        v => format!("value:{}", hex(&v.sql_bytes().unwrap())),
                    })
                    .collect::<Vec<_>>()
                    .join(";"))
            },
        ));
        let (answer, error) = match result {
            Ok(Ok(v)) => (v, String::new()),
            Ok(Err(e)) => e,
            Err(_) => ("panic".to_owned(), String::new()),
        };
        let warnings = ctx.take_warnings();
        let w = warnings
            .iter()
            .map(|(c, m)| hex(format!("[{c}]{m}").as_bytes()))
            .collect::<Vec<_>>()
            .join(",");
        let expected_error = String::from_utf8(unhex(f[6])).unwrap();
        let expected_warnings = f[8]
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| hex(diagnostic(&String::from_utf8(unhex(s)).unwrap()).as_bytes()))
            .collect::<Vec<_>>()
            .join(",");
        if answer != f[5]
            || diagnostic(&error) != diagnostic(&expected_error)
            || warnings.len().to_string() != f[7]
            || w != expected_warnings
        {
            differences.push(format!(
                "{}/flags{}: got {answer} error={error}, warnings={w}; Go {} error={} warnings={}",
                f[0], f[1], f[5], expected_error, expected_warnings
            ));
        }
    }
    tidb_datatype::set_new_collation_enabled(previous);
    assert!(
        differences.is_empty(),
        "{} differences: {:#?}",
        differences.len(),
        &differences[..differences.len().min(20)]
    );
}
