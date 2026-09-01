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

//! Source-shaped public boundary checks for Go `pkg/util/cgroup`.
//!
//! The detailed fixture matrix lives beside the private parser owner because
//! the Go tests are package-internal. These checks keep the exported platform
//! fallbacks and signed quota result visible at the Rust package boundary.

use tidb_util::cgroup::{self, CpuQuotaStatus, Version};

#[cfg(not(target_os = "linux"))]
#[test]
fn unsupported_platform_matches_go_public_fallbacks() {
    let cpu = cgroup::get_cgroup_cpu().unwrap();
    assert!(cpu.num_cpu > 0);
    assert_eq!(cgroup::get_cpu_period_and_quota().unwrap(), (-1, -1));
    assert_eq!(
        cgroup::cpu_quota_to_gomaxprocs(1).unwrap(),
        (-1, CpuQuotaStatus::Undefined)
    );
    assert_eq!(
        cgroup::get_cgroup_memory_limit().unwrap(),
        (0, Version::Unknown)
    );
    assert_eq!(cgroup::get_memory_limit().unwrap(), 0);
    assert_eq!(cgroup::get_memory_usage().unwrap(), 0);
    assert_eq!(cgroup::get_memory_inactive_file_usage().unwrap(), 0);
    assert!(!cgroup::in_container());
}
