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

//! Canonical runtime observation serializer for the `pkg/util/chunk` receipt.

// Cargo also builds each top-level `tests/*.rs` file as an integration target.
// These items are live when imported by the fixture probe and intentionally
// unused in this standalone helper target.
#[allow(dead_code)]
const SOURCE_COMMIT: &str = "665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f";

#[allow(dead_code)]
pub(crate) fn emit(probe_id: &str, conclusion: &str, cases: &[(&str, &str, &str)]) {
    fn quoted_ascii(value: &str) -> String {
        assert!(value.is_ascii(), "receipt observations must stay ASCII");
        let mut encoded = String::with_capacity(value.len() + 2);
        encoded.push('"');
        for byte in value.bytes() {
            match byte {
                b'"' => encoded.push_str("\\\""),
                b'\\' => encoded.push_str("\\\\"),
                b'\x08' => encoded.push_str("\\b"),
                b'\x0c' => encoded.push_str("\\f"),
                b'\n' => encoded.push_str("\\n"),
                b'\r' => encoded.push_str("\\r"),
                b'\t' => encoded.push_str("\\t"),
                0x00..=0x1f => encoded.push_str(&format!("\\u{byte:04x}")),
                _ => encoded.push(char::from(byte)),
            }
        }
        encoded.push('"');
        encoded
    }

    let observations = cases
        .iter()
        .map(|(name, input, observed)| {
            format!(
                "{{\"input\":{},\"name\":{},\"observed\":{}}}",
                quoted_ascii(input),
                quoted_ascii(name),
                quoted_ascii(observed)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let payload = format!(
        "{{\"boundary_observations\":[{observations}],\"conclusion\":{},\"probe_id\":{},\"schema\":\"go-package-lockdown-runtime-observation-v1\",\"source_commit\":\"{SOURCE_COMMIT}\"}}",
        quoted_ascii(conclusion),
        quoted_ascii(probe_id)
    );
    println!("LOCKDOWN_OBSERVATION {payload}");
}
