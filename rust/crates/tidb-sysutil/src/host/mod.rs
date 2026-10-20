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

//! The host readings sysutil takes from `gopsutil/v3` v3.24.5 (cgo build),
//! plus Go runtime facts (`runtime.GOARCH`, `runtime.NumCPU`) and the
//! standard library's `net.Interfaces`.
//!
//! Only the calls sysutil makes are here, each with the fields sysutil reads
//! and the error paths that decide whether sysutil reports the item at all.
//! The text formats gopsutil parses on Linux (`/proc`) and Darwin
//! (`netstat`) live in `parse`, which compiles and is tested on every
//! platform; `linux` and `darwin` only read the files and call the system.

pub(crate) mod parse;

#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "macos")]
use darwin as platform;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux as platform;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod platform {
    //! gopsutil's `ErrNotImplementedError` for every other platform.
    use super::*;
    const NOT_IMPLEMENTED: &str = "not implemented yet";
    pub(super) fn cpu_info() -> Result<Vec<CpuInfo>, String> {
        Err(NOT_IMPLEMENTED.into())
    }
    pub(super) fn cpu_counts(_: bool) -> Result<i64, String> {
        Err(NOT_IMPLEMENTED.into())
    }
    pub(super) fn cpu_times() -> Result<Vec<CpuTimes>, String> {
        Err(NOT_IMPLEMENTED.into())
    }
    pub(super) fn virtual_memory() -> Result<VirtualMemory, String> {
        Err(NOT_IMPLEMENTED.into())
    }
    pub(super) fn swap_memory() -> Result<SwapMemory, String> {
        Err(NOT_IMPLEMENTED.into())
    }
    pub(super) fn disk_partitions() -> Result<Vec<Partition>, String> {
        Err(NOT_IMPLEMENTED.into())
    }
    pub(super) fn disk_io_counters() -> Result<Vec<DiskIoCounters>, String> {
        Err(NOT_IMPLEMENTED.into())
    }
    pub(super) fn net_interfaces() -> Result<Vec<NetInterface>, String> {
        Err(NOT_IMPLEMENTED.into())
    }
    pub(super) fn net_io_counters() -> Result<Vec<NetIoCounters>, String> {
        Err(NOT_IMPLEMENTED.into())
    }
    pub(super) fn load_avg() -> Result<LoadAvg, String> {
        Err(NOT_IMPLEMENTED.into())
    }
    pub(super) fn num_cpu() -> usize {
        std::thread::available_parallelism().map_or(1, usize::from)
    }
}

/// gopsutil `cpu.InfoStat`, the fields sysutil reads.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct CpuInfo {
    pub(crate) mhz: f64,
    pub(crate) cache_size: i32,
    pub(crate) cores: i32,
}

/// gopsutil `cpu.TimesStat` in seconds.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct CpuTimes {
    pub(crate) user: f64,
    pub(crate) system: f64,
    pub(crate) idle: f64,
    pub(crate) nice: f64,
    pub(crate) iowait: f64,
    pub(crate) irq: f64,
    pub(crate) softirq: f64,
    pub(crate) steal: f64,
    pub(crate) guest: f64,
    pub(crate) guest_nice: f64,
}

impl CpuTimes {
    /// gopsutil `TimesStat.Total`, summed in Go's order.
    pub(crate) fn total(&self) -> f64 {
        self.user
            + self.system
            + self.idle
            + self.nice
            + self.iowait
            + self.irq
            + self.softirq
            + self.steal
            + self.guest
            + self.guest_nice
    }
}

/// gopsutil `mem.VirtualMemoryStat`, the fields sysutil reads.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct VirtualMemory {
    pub(crate) total: u64,
    pub(crate) available: u64,
}

/// gopsutil `mem.SwapMemoryStat`, the fields sysutil reads.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct SwapMemory {
    pub(crate) total: u64,
    pub(crate) used: u64,
    pub(crate) free: u64,
}

/// gopsutil `disk.PartitionStat`.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Partition {
    pub(crate) device: String,
    pub(crate) mountpoint: String,
    pub(crate) fstype: String,
    pub(crate) opts: Vec<String>,
}

/// gopsutil `disk.UsageStat`, the fields sysutil reads.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct DiskUsage {
    pub(crate) total: u64,
    pub(crate) free: u64,
    pub(crate) used: u64,
    pub(crate) used_percent: f64,
}

/// gopsutil `disk.IOCountersStat`, the fields sysutil reads.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct DiskIoCounters {
    pub(crate) name: String,
    pub(crate) read_count: u64,
    pub(crate) merged_read_count: u64,
    pub(crate) write_count: u64,
    pub(crate) merged_write_count: u64,
    pub(crate) read_bytes: u64,
    pub(crate) write_bytes: u64,
}

/// gopsutil `net.InterfaceStat`, the fields sysutil reads.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct NetInterface {
    pub(crate) name: String,
    pub(crate) hardware_addr: String,
    pub(crate) flags: Vec<&'static str>,
    pub(crate) addrs: Vec<String>,
}

/// gopsutil `net.IOCountersStat`.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct NetIoCounters {
    pub(crate) name: String,
    pub(crate) bytes_sent: u64,
    pub(crate) bytes_recv: u64,
    pub(crate) packets_sent: u64,
    pub(crate) packets_recv: u64,
    pub(crate) errin: u64,
    pub(crate) errout: u64,
    pub(crate) dropin: u64,
    pub(crate) dropout: u64,
    pub(crate) fifoin: u64,
    pub(crate) fifoout: u64,
}

/// gopsutil `load.AvgStat`.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct LoadAvg {
    pub(crate) load1: f64,
    pub(crate) load5: f64,
    pub(crate) load15: f64,
}

/// gopsutil `cpu.Info`.
pub(crate) fn cpu_info() -> Result<Vec<CpuInfo>, String> {
    platform::cpu_info()
}

/// gopsutil `cpu.Counts`.
pub(crate) fn cpu_counts(logical: bool) -> Result<i64, String> {
    platform::cpu_counts(logical)
}

/// gopsutil `cpu.Times(false)`: the one aggregate entry.
pub(crate) fn cpu_times() -> Result<Vec<CpuTimes>, String> {
    platform::cpu_times()
}

/// gopsutil `mem.VirtualMemory`.
pub(crate) fn virtual_memory() -> Result<VirtualMemory, String> {
    platform::virtual_memory()
}

/// gopsutil `mem.SwapMemory`.
pub(crate) fn swap_memory() -> Result<SwapMemory, String> {
    platform::swap_memory()
}

/// gopsutil `disk.Partitions(true)`.
pub(crate) fn disk_partitions() -> Result<Vec<Partition>, String> {
    platform::disk_partitions()
}

/// gopsutil `disk.Usage` (`disk_unix.go`).
pub(crate) fn disk_usage(path: &str) -> Result<DiskUsage, String> {
    #[cfg(unix)]
    {
        let stat = nix::sys::statfs::statfs(path).map_err(|errno| errno.desc().to_owned())?;
        #[allow(clippy::useless_conversion)]
        let bsize = stat.block_size() as u64;
        #[allow(clippy::useless_conversion)]
        let (blocks, bfree, bavail) = (
            u64::from(stat.blocks()),
            u64::from(stat.blocks_free()),
            u64::from(stat.blocks_available()),
        );
        Ok(parse::disk_usage(bsize, blocks, bfree, bavail))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err("not implemented yet".into())
    }
}

/// gopsutil `disk.IOCounters()`, in the order the platform lists them (Go
/// returns a map, whose order is unspecified).
pub(crate) fn disk_io_counters() -> Result<Vec<DiskIoCounters>, String> {
    platform::disk_io_counters()
}

/// gopsutil `net.Interfaces`.
pub(crate) fn net_interfaces() -> Result<Vec<NetInterface>, String> {
    platform::net_interfaces()
}

/// gopsutil `net.IOCounters(true)`.
pub(crate) fn net_io_counters() -> Result<Vec<NetIoCounters>, String> {
    platform::net_io_counters()
}

/// gopsutil `load.Avg`.
pub(crate) fn load_avg() -> Result<LoadAvg, String> {
    platform::load_avg()
}

/// Go `runtime.NumCPU`: the CPUs this process may run on at startup.
pub(crate) fn num_cpu() -> usize {
    platform::num_cpu()
}

/// Go `runtime.GOARCH` for the architecture this binary targets.
pub(crate) fn goarch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "x86" => "386",
        "aarch64" => "arm64",
        "powerpc64" if cfg!(target_endian = "little") => "ppc64le",
        "powerpc64" => "ppc64",
        "loongarch64" => "loong64",
        "riscv64" => "riscv64",
        "s390x" => "s390x",
        "mips64" if cfg!(target_endian = "little") => "mips64le",
        other => other,
    }
}

/// Go `net.HardwareAddr.String`: lower-case hex bytes joined by colons.
pub(crate) fn hardware_addr_string(addr: &[u8]) -> String {
    addr.iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// Go `net.IP.String`: IPv4 and IPv4-mapped IPv6 addresses print dotted.
fn go_ip_string(ip: std::net::IpAddr) -> String {
    match ip {
        std::net::IpAddr::V4(v4) => v4.to_string(),
        std::net::IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4.to_string(),
            None => v6.to_string(),
        },
    }
}

/// Go `simpleMaskLength`: the prefix length of a canonical mask, else None.
fn simple_mask_length(mask: &[u8]) -> Option<u32> {
    let mut n = 0;
    for (i, &v) in mask.iter().enumerate() {
        if v == 0xff {
            n += 8;
            continue;
        }
        // found non-ff byte; count 1 bits
        let ones = v.leading_ones();
        if v << ones != 0 {
            return None;
        }
        if mask[i + 1..].iter().any(|&rest| rest != 0) {
            return None;
        }
        return Some(n + ones);
    }
    Some(n)
}

/// Go `net.IPNet.String`: `ip/prefix`, `ip/hexmask` for a non-canonical
/// mask, `<nil>` for an IPv4 mask on an IPv6 address.
pub(crate) fn ip_net_string(ip: std::net::IpAddr, mask: &[u8]) -> String {
    let is_v4 = match ip {
        std::net::IpAddr::V4(_) => true,
        std::net::IpAddr::V6(v6) => v6.to_ipv4_mapped().is_some(),
    };
    let mask = match mask.len() {
        4 if !is_v4 => return "<nil>".to_owned(),
        4 => mask,
        16 if is_v4 => &mask[12..],
        16 => mask,
        _ => return "<nil>".to_owned(),
    };
    match simple_mask_length(mask) {
        Some(length) => format!("{}/{length}", go_ip_string(ip)),
        None => format!("{}/{}", go_ip_string(ip), hex_string(mask)),
    }
}

fn hex_string(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Go `net.Interface.Flags` bits gopsutil names, in gopsutil's order.
pub(crate) fn interface_flag_names(
    up: bool,
    broadcast: bool,
    loopback: bool,
    point_to_point: bool,
    multicast: bool,
) -> Vec<&'static str> {
    let mut flags = Vec::new();
    if up {
        flags.push("up");
    }
    if broadcast {
        flags.push("broadcast");
    }
    if loopback {
        flags.push("loopback");
    }
    if point_to_point {
        flags.push("pointtopoint");
    }
    if multicast {
        flags.push("multicast");
    }
    flags
}
