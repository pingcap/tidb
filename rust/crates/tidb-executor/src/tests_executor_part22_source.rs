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

//! Ports of `pkg/executor.part22`: deterministic items 1261--1320 of the
//! upstream executor test enumeration that need no SQL stack; the SQL ones
//! run on the TestKit stack (`tidb-session` `unistore_cop`).

#[test]
fn worker_pool_preserves_the_go_submission_contract() {
    let values: Vec<usize> = (0..16).collect();
    let output = tidb_goruntime::worker_pool::map(
        values.iter().map(|value| {
            let value = *value;
            move || {
                if value % 3 == 0 {
                    std::thread::sleep(std::time::Duration::from_micros(50));
                }
                value * 2
            }
        }),
        2,
    );
    assert_eq!(
        output,
        values.iter().map(|value| value * 2).collect::<Vec<_>>()
    );
}
