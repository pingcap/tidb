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
use std::io::Read;
use tidb_proto::{KvrpcMutation, KvrpcOp};

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn hex(s: &[u8]) -> String {
    s.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn region_group_matches_go_coprocessor_responses() {
    let mut data = String::new();
    flate2::read::GzDecoder::new(
        include_bytes!("../../testdata/region-group-go.tsv.gz").as_slice(),
    )
    .read_to_string(&mut data)
    .unwrap();
    assert_eq!(data.lines().count(), 11232);
    let previous = tidb_datatype::new_collation_enabled();
    tidb_datatype::set_new_collation_enabled(true);
    let mut store = MvccStore::new();
    let mut loaded = std::collections::HashSet::new();
    let mut differences = Vec::new();
    for line in data.lines() {
        let f: Vec<_> = line.split('\t').collect();
        for kv in f[2].split(',') {
            let (key, value) = kv.split_once(':').unwrap();
            let key = unhex(key);
            if !loaded.insert(key.clone()) {
                continue;
            }
            store
                .prewrite(&crate::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value: unhex(value),
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..crate::mvcc_store::PrewriteReq::default()
                })
                .unwrap();
            store.commit(&[key], 10, 11).unwrap();
        }
        let request = coprocessor::Request::decode(unhex(f[1]).as_slice()).unwrap();
        let response = handle_cop_request(&mut store, &request);
        let mut error = response.other_error;
        let mut rows = Vec::new();
        let mut warnings = Vec::new();
        if error.is_empty() {
            let select = tipb::SelectResponse::decode(response.data.as_ref()).unwrap();
            if let Some(e) = select.error {
                error = format!("[{}]{}", e.code(), e.msg());
            }
            for chunk in select.chunks {
                rows.extend_from_slice(chunk.rows_data());
            }
            for w in select.warnings {
                warnings.push(hex(format!("[{}]{}", w.code(), w.msg()).as_bytes()));
            }
        }
        if hex(&rows) != f[3] || hex(error.as_bytes()) != f[4] || warnings.join(",") != f[5] {
            differences.push(format!(
                "{}: Rust {} error={error} warnings={warnings:?}; Go {} error={} warnings={}",
                f[0],
                hex(&rows),
                f[3],
                String::from_utf8(unhex(f[4])).unwrap(),
                f[5],
            ));
        }
    }
    tidb_datatype::set_new_collation_enabled(previous);
    assert!(
        differences.is_empty(),
        "{} differences: {:#?}",
        differences.len(),
        &differences[..differences.len().min(30)]
    );
}
