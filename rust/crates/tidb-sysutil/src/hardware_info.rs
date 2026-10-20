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

//! Go `hardware_info.go`.

use std::sync::RwLock;

use tidb_proto::diagnosticspb as pb;

use crate::{fmt2, host, item};

type MemoryCapacityFn = fn() -> Result<u64, String>;

/// Go's package-level `getMemoryCapacity`, replaced by
/// `RegisterGetMemoryCapacity` when TiDB decides it runs under a cgroup.
static GET_MEMORY_CAPACITY: RwLock<Option<MemoryCapacityFn>> = RwLock::new(None);

/// Go `RegisterGetMemoryCapacity`.
pub fn register_get_memory_capacity(f: MemoryCapacityFn) {
    *GET_MEMORY_CAPACITY
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(f);
}

/// Go `GetMemoryCapacity`: the registered reader, else the host's total
/// virtual memory.
pub fn get_memory_capacity() -> Result<u64, String> {
    let registered = *GET_MEMORY_CAPACITY
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match registered {
        None => Ok(host::virtual_memory()?.total),
        Some(f) => f(),
    }
}

/// Go `getHardwareInfo`.
pub(crate) fn get_hardware_info() -> Vec<pb::ServerInfoItem> {
    let mut results = Vec::new();
    // cpu
    if let Ok(infos) = host::cpu_info() {
        if let Some(first) = infos.first() {
            let physical_cores = host::cpu_counts(false).unwrap_or(i64::from(first.cores));
            results.push(item(
                "cpu",
                "cpu",
                vec![
                    ("cpu-arch", host::goarch().to_owned()),
                    ("cpu-logical-cores", host::num_cpu().to_string()),
                    ("cpu-physical-cores", physical_cores.to_string()),
                    ("cpu-frequency", format!("{}MHz", fmt2(first.mhz))),
                    ("cache", first.cache_size.to_string()),
                ],
            ));
        }
    }

    // memory
    if let Ok(total) = get_memory_capacity() {
        results.push(item(
            "memory",
            "memory",
            vec![("capacity", total.to_string())],
        ));
    }

    // disk
    if let Ok(parts) = host::disk_partitions() {
        for p in parts {
            let Some(name) = p.device.strip_prefix("/dev/") else {
                continue;
            };
            let Ok(usage) = host::disk_usage(&p.mountpoint) else {
                continue;
            };
            results.push(item(
                "disk",
                name,
                vec![
                    ("fstype", p.fstype.clone()),
                    ("opts", p.opts.join(",")),
                    ("path", p.mountpoint.clone()),
                    ("total", usage.total.to_string()),
                    ("free", usage.free.to_string()),
                    ("used", usage.used.to_string()),
                    ("free-percent", fmt2((100.0 - usage.used_percent) / 100.00)),
                    ("used-percent", fmt2(usage.used_percent / 100.00)),
                ],
            ));
        }
    }

    // network
    if let Ok(nics) = host::net_interfaces() {
        for nic in nics {
            let flag = |f: &'static str| {
                if nic.flags.contains(&f) {
                    "true".to_owned()
                } else {
                    "false".to_owned()
                }
            };
            results.push(item(
                "net",
                &nic.name,
                vec![
                    ("mac", nic.hardware_addr.clone()),
                    ("is-up", flag("up")),
                    ("is-broadcast", flag("broadcast")),
                    ("is-multicast", flag("multicast")),
                    ("is-loopback", flag("loopback")),
                    ("is-point-to-point", flag("pointtopoint")),
                    ("addresses", nic.addrs.join(",")),
                ],
            ));
        }
    }
    results
}
