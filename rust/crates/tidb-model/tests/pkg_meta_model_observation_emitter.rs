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

//! Canonical runtime observation serializer for the `pkg/meta/model` receipt.
//!
//! The named probe tests pass values they compute at runtime. Keeping the
//! machine marker and schema in this separate helper prevents a named probe
//! from satisfying the receipt with a hard-coded observation literal.

use std::collections::BTreeMap;

const SOURCE_COMMIT: &str = "bdab0016365e8b1d79b5b11f52ee6fdde90f4c46";

pub(crate) fn emit(probe_id: &str, conclusion: &str, cases: &[(&str, &str, &str)]) {
    let boundary_observations = cases
        .iter()
        .map(|(name, input, observed)| {
            BTreeMap::from([
                ("input", serde_json::Value::String((*input).to_owned())),
                ("name", serde_json::Value::String((*name).to_owned())),
                (
                    "observed",
                    serde_json::Value::String((*observed).to_owned()),
                ),
            ])
        })
        .collect::<Vec<_>>();
    let payload = BTreeMap::from([
        (
            "boundary_observations",
            serde_json::to_value(boundary_observations).expect("observations must serialize"),
        ),
        (
            "conclusion",
            serde_json::Value::String(conclusion.to_owned()),
        ),
        ("probe_id", serde_json::Value::String(probe_id.to_owned())),
        (
            "schema",
            serde_json::Value::String("go-package-lockdown-runtime-observation-v1".to_owned()),
        ),
        (
            "source_commit",
            serde_json::Value::String(SOURCE_COMMIT.to_owned()),
        ),
    ]);
    println!(
        "LOCKDOWN_OBSERVATION {}",
        serde_json::to_string(&payload).expect("observation must serialize")
    );
}
