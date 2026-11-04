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

//! Go `pkg/domain/optimize_trace.go`.

/// Go `GetOptimizerTraceDirName`: the optimizer trace directory, a path
/// relative to the external storage. `instance_id` is Go
/// `infosync.GetServerInfo()`'s `ID`; an empty one falls back to the process
/// ID.
#[must_use]
pub fn get_optimizer_trace_dir_name(instance_id: &str) -> String {
    let instance_id = if instance_id.is_empty() { std::process::id().to_string() } else { instance_id.to_owned() };
    format!("optimizer_trace/{instance_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_instance_id_falls_back_to_the_process_id() {
        assert_eq!(get_optimizer_trace_dir_name("tidb-1"), "optimizer_trace/tidb-1");
        assert_eq!(get_optimizer_trace_dir_name(""), format!("optimizer_trace/{}", std::process::id()));
    }
}
