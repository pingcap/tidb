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

//! Go `node.go`: the managed-node record and the arithmetic that turns a
//! node's CPU, memory, and disk into the budget a task step may spend.

use crate::subtask::{Allocatable, StepResource};
use crate::task::TaskBase;

const GB: i64 = 1_000_000_000;

/// Go `ManagedNode`: a TiDB node that is managed by the framework.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManagedNode {
    /// The node ID, see `GenerateExecID`. It is named `host` in the meta table.
    pub id: String,
    /// The role of the node, either `""` or `"background"`. All managed nodes
    /// should have the same role.
    pub role: String,
    /// The node's CPU count.
    pub cpu_count: isize,
}

/// Go `NodeResource`: the resource of the node. Exported for test.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeResource {
    /// Total CPU cores.
    pub total_cpu: isize,
    /// Total memory, in bytes.
    pub total_mem: i64,
    /// Total disk, in bytes.
    pub total_disk: u64,
}

/// Go `NodeResourceForTest`, only used for test.
pub static NODE_RESOURCE_FOR_TEST: &NodeResource = &NodeResource {
    total_cpu: 32,
    total_mem: 32 * GB,
    total_disk: 100 * (GB as u64),
};

impl NodeResource {
    /// Go `NewNodeResource`.
    #[must_use]
    pub const fn new(total_cpu: isize, total_mem: i64, total_disk: u64) -> Self {
        Self {
            total_cpu,
            total_mem,
            total_disk,
        }
    }

    /// Go `LimitDXFResource`: the resource available to DXF under the given
    /// percentage limit.
    #[must_use]
    pub fn limit_dxf_resource(&self, limit: isize) -> Self {
        let usable_cpu = get_limited_dxf_cpu(self.total_cpu, limit);
        if usable_cpu == self.total_cpu || self.total_cpu <= 0 {
            return Self::new(self.total_cpu, self.total_mem, self.total_disk);
        }
        let usable_mem = (usable_cpu as f64 / self.total_cpu as f64 * self.total_mem as f64) as i64;
        // This feature is for premium-based clusters, in which only global sort
        // is supported and there is no local disk, so the disk is left as is.
        Self::new(usable_cpu, usable_mem, self.total_disk)
    }

    /// Go `GetStepResource`: the step resource according to the task's slots.
    #[must_use]
    pub fn get_step_resource(&self, task: &TaskBase) -> StepResource {
        let slots = task.get_runtime_slots();
        StepResource {
            cpu: Allocatable::new(slots as i64),
            // same proportion as CPU
            mem: Allocatable::new(
                (slots as f64 / self.total_cpu as f64 * self.total_mem as f64) as i64,
            ),
        }
    }

    /// Go `GetTaskDiskResource`: the disk available to a task.
    #[must_use]
    pub fn get_task_disk_resource(&self, task: &TaskBase, quota_hint: u64) -> u64 {
        let slots = task.get_runtime_slots();
        let available_disk = self.total_disk.min(quota_hint);
        (slots as f64 / self.total_cpu as f64 * available_disk as f64) as u64
    }
}

/// Go `getLimitedDXFCPU`: the CPU slots available to DXF under the given
/// percentage limit.
fn get_limited_dxf_cpu(total_cpu: isize, limit: isize) -> isize {
    if total_cpu <= 0 || limit >= 100 {
        return total_cpu;
    }
    // Using CEIL might make the real limit higher than the given limit. Since
    // DXF uses slots or CPU cores as its unit of resource, that is acceptable.
    let usable_cpu = (total_cpu as f64 * limit as f64 / 100.0).ceil() as isize;
    if usable_cpu < 1 {
        return 1;
    }
    usable_cpu.min(total_cpu)
}

/// go-units' `units.BytesSize`.
#[must_use]
pub(crate) fn step_resource_bytes_size(size: f64) -> String {
    const BINARY_ABBRS: [&str; 9] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];
    let mut size = size;
    let mut i = 0;
    while size >= 1024.0 && i < BINARY_ABBRS.len() - 1 {
        size /= 1024.0;
        i += 1;
    }
    format!("{}{}", format_g4(size), BINARY_ABBRS[i])
}

/// Go's `%.4g` verb: four significant digits, switching to `%e` when the
/// decimal exponent is below -4 or at least 4, and dropping trailing zeros
/// from the fraction.
fn format_g4(v: f64) -> String {
    const PREC: i32 = 4;
    if v == 0.0 {
        return "0".to_owned();
    }
    let exp = v.abs().log10().floor() as i32;
    if !(-4..PREC).contains(&exp) {
        let s = format!("{:.*e}", (PREC - 1) as usize, v);
        // Rust writes `1.234e9`; Go writes `1.234e+09`.
        let (mantissa, e) = s.split_once('e').unwrap_or((s.as_str(), "0"));
        let mantissa = trim_fraction(mantissa);
        let (sign, digits) = e.strip_prefix('-').map_or(('+', e), |d| ('-', d));
        return format!("{mantissa}e{sign}{digits:0>2}");
    }
    let decimals = usize::try_from((PREC - 1 - exp).max(0)).unwrap_or(0);
    trim_fraction(&format!("{v:.decimals$}")).to_owned()
}

/// Drops the trailing zeros, and then a bare trailing dot, from a decimal.
fn trim_fraction(s: &str) -> &str {
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.')
    } else {
        s
    }
}
