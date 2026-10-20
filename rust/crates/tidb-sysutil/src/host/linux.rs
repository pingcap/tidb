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

//! gopsutil v3.24.5's Linux readers: the files they read under `HOST_PROC`
//! (default `/proc`), `HOST_SYS` (`/sys`) and `HOST_DEV` (`/dev`), parsed by
//! `parse`.

use std::path::{Path, PathBuf};

use super::{
    parse, CpuInfo, CpuTimes, DiskIoCounters, LoadAvg, NetInterface, NetIoCounters, Partition,
    SwapMemory, VirtualMemory,
};

/// gopsutil `common.GetEnv(key, default, combineWith...)`.
fn host_path(key: &str, default: &str, rel: &str) -> PathBuf {
    let base = std::env::var(key)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned());
    if rel.is_empty() {
        return PathBuf::from(base);
    }
    Path::new(&base).join(rel.trim_start_matches('/'))
}

fn host_proc(rel: &str) -> PathBuf {
    host_path("HOST_PROC", "/proc", rel)
}

fn host_sys(rel: &str) -> PathBuf {
    host_path("HOST_SYS", "/sys", rel)
}

fn host_dev(rel: &str) -> PathBuf {
    host_path("HOST_DEV", "/dev", rel)
}

fn read(path: &Path) -> Result<String, String> {
    std::fs::read(path)
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .map_err(|error| error.to_string())
}

pub(super) fn cpu_info() -> Result<Vec<CpuInfo>, String> {
    // gopsutil ignores a read failure here and reports no CPUs.
    let cpuinfo = read(&host_proc("cpuinfo")).unwrap_or_default();
    parse::linux_cpu_info(&cpuinfo, &|cpu| {
        let path = host_sys(&format!(
            "devices/system/cpu/cpu{cpu}/cpufreq/cpuinfo_max_freq"
        ));
        let content = read(&path).ok()?;
        parse::read_lines(&content)
            .first()
            .map(|line| (*line).to_owned())
    })
}

/// The entries of `dir` matching `cpu[0-9]*`, sorted as `filepath.Glob`.
fn cpu_dirs(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| {
            name.strip_prefix("cpu")
                .and_then(|rest| rest.bytes().next())
                .is_some_and(|b| b.is_ascii_digit())
        })
        .collect();
    names.sort();
    names.into_iter().map(|name| dir.join(name)).collect()
}

pub(super) fn cpu_counts(logical: bool) -> Result<i64, String> {
    if logical {
        let mut ret = 0;
        if let Ok(cpuinfo) = read(&host_proc("cpuinfo")) {
            for line in parse::read_lines(&cpuinfo) {
                let line = line.to_lowercase();
                if line.starts_with("processor") {
                    let value = line.find(':').map_or(&line[..], |i| &line[i + 1..]);
                    if parse::parse_int(value.trim()).is_ok() {
                        ret += 1;
                    }
                }
            }
        }
        if ret == 0 {
            let stat = read(&host_proc("stat"))?;
            for line in parse::read_lines(&stat) {
                let b = line.as_bytes();
                if b.len() >= 4 && line.starts_with("cpu") && b[3].is_ascii_digit() {
                    ret += 1;
                }
            }
        }
        return Ok(ret);
    }
    // physical cores
    let mut thread_siblings_lists = std::collections::HashSet::new();
    for file in ["core_cpus_list", "thread_siblings_list"] {
        for dir in cpu_dirs(&host_sys("devices/system/cpu")) {
            let Ok(content) = read(&dir.join("topology").join(file)) else {
                continue;
            };
            let lines = parse::read_lines(&content);
            if lines.len() != 1 {
                continue;
            }
            thread_siblings_lists.insert(lines[0].to_owned());
        }
        let ret = thread_siblings_lists.len();
        if ret != 0 {
            return Ok(ret as i64);
        }
    }
    let cpuinfo = read(&host_proc("cpuinfo"))?;
    Ok(parse::linux_physical_cores_from_cpuinfo(&cpuinfo))
}

pub(super) fn cpu_times() -> Result<Vec<CpuTimes>, String> {
    let stat = read(&host_proc("stat")).unwrap_or_default();
    let first = parse::read_lines(&stat)
        .first()
        .copied()
        .unwrap_or_default();
    Ok(parse::linux_parse_stat_line(first).into_iter().collect())
}

pub(super) fn virtual_memory() -> Result<VirtualMemory, String> {
    let meminfo = read(&host_proc("meminfo")).unwrap_or_default();
    let zoneinfo = read(&host_proc("zoneinfo")).ok();
    let pagesize = nix::unistd::sysconf(nix::unistd::SysconfVar::PAGE_SIZE)
        .ok()
        .flatten()
        .map_or(4096, |size| size as u64);
    parse::linux_virtual_memory(&meminfo, zoneinfo.as_deref(), pagesize)
}

pub(super) fn swap_memory() -> Result<SwapMemory, String> {
    let info = nix::sys::sysinfo::sysinfo().map_err(|errno| errno.desc().to_owned())?;
    let total = info.swap_total();
    let free = info.swap_free();
    Ok(SwapMemory {
        total,
        used: total.wrapping_sub(free),
        free,
    })
}

/// gopsutil `readMountFile`: `mountinfo`, or `mounts` when `mountinfo` is
/// missing.
fn read_mount_file(root: &Path) -> Result<(String, bool, String), String> {
    let filename = root.join("mountinfo");
    match std::fs::read(&filename) {
        Ok(bytes) => Ok((
            String::from_utf8_lossy(&bytes).into_owned(),
            false,
            filename.to_string_lossy().into_owned(),
        )),
        Err(_) => {
            let filename = root.join("mounts");
            let content = read(&filename)?;
            Ok((content, true, filename.to_string_lossy().into_owned()))
        }
    }
}

pub(super) fn disk_partitions() -> Result<Vec<Partition>, String> {
    let mut root = host_proc("1");
    let hpm_path = std::env::var("HOST_PROC_MOUNTINFO").unwrap_or_default();
    if !hpm_path.is_empty() {
        root = Path::new(&hpm_path)
            .parent()
            .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
    }
    let (content, use_mounts, filename) = match read_mount_file(&root) {
        Ok(found) => found,
        Err(error) => {
            if !hpm_path.is_empty() {
                // don't fallback with HOST_PROC_MOUNTINFO
                return Err(error);
            }
            read_mount_file(&host_proc("self"))?
        }
    };
    let lines = parse::read_lines(&content);
    parse::linux_partitions(&lines, use_mounts, &filename, &|d, block_device_id| {
        if let Some(rest) = d.device.strip_prefix("/dev/mapper/") {
            if let Ok(devpath) = std::fs::canonicalize(host_dev(&format!("mapper/{rest}"))) {
                d.device = devpath.to_string_lossy().into_owned();
            }
        }
        if d.device == "/dev/root" {
            if let Ok(devpath) =
                std::fs::read_link(host_sys(&format!("/dev/block/{block_device_id}")))
            {
                let base = devpath
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_default();
                d.device = d.device.replacen("root", &base, 1);
            }
        }
    })
}

pub(super) fn disk_io_counters() -> Result<Vec<DiskIoCounters>, String> {
    let diskstats = read(&host_proc("diskstats"))?;
    parse::linux_disk_io_counters(&diskstats)
}

pub(super) fn net_interfaces() -> Result<Vec<NetInterface>, String> {
    use nix::net::if_::InterfaceFlags;
    let addrs = nix::ifaddrs::getifaddrs().map_err(|errno| errno.desc().to_owned())?;
    // Go lists links in index order with their netlink addresses; glibc's
    // getifaddrs reads the same dumps, one AF_PACKET entry per link.
    let mut links: Vec<(u32, NetInterface)> = Vec::new();
    let mut ips: Vec<(String, String)> = Vec::new();
    for ifa in addrs {
        let Some(address) = ifa.address.as_ref() else {
            continue;
        };
        if let Some(link) = address.as_link_addr() {
            let flags = ifa.flags;
            let hw = link_hardware_addr(link.hatype(), link.halen(), link.addr());
            links.push((
                link.ifindex() as u32,
                NetInterface {
                    name: ifa.interface_name.clone(),
                    hardware_addr: super::hardware_addr_string(&hw),
                    flags: super::interface_flag_names(
                        flags.contains(InterfaceFlags::IFF_UP),
                        flags.contains(InterfaceFlags::IFF_BROADCAST),
                        flags.contains(InterfaceFlags::IFF_LOOPBACK),
                        flags.contains(InterfaceFlags::IFF_POINTOPOINT),
                        flags.contains(InterfaceFlags::IFF_MULTICAST),
                    ),
                    addrs: Vec::new(),
                },
            ));
        } else if let Some(v4) = address.as_sockaddr_in() {
            let mask = ifa
                .netmask
                .as_ref()
                .and_then(|mask| mask.as_sockaddr_in())
                .map(|mask| mask.ip().octets());
            if let Some(mask) = mask {
                ips.push((
                    ifa.interface_name.clone(),
                    super::ip_net_string(std::net::IpAddr::V4(v4.ip()), &mask),
                ));
            }
        } else if let Some(v6) = address.as_sockaddr_in6() {
            let mask = ifa
                .netmask
                .as_ref()
                .and_then(|mask| mask.as_sockaddr_in6())
                .map(|mask| mask.ip().octets());
            if let Some(mask) = mask {
                ips.push((
                    ifa.interface_name.clone(),
                    super::ip_net_string(std::net::IpAddr::V6(v6.ip()), &mask),
                ));
            }
        }
    }
    links.sort_by_key(|(index, _)| *index);
    Ok(links
        .into_iter()
        .map(|(_, mut nic)| {
            nic.addrs = ips
                .iter()
                .filter(|(name, _)| *name == nic.name)
                .map(|(_, addr)| addr.clone())
                .collect();
            nic
        })
        .collect())
}

/// Go `interfaceTable`'s `IFLA_ADDRESS` rule: no /32 or /128 tunnel
/// endpoint as a hardware address, and never an all-zero one. getifaddrs
/// carries at most the first six address bytes this way.
fn link_hardware_addr(hatype: u16, halen: usize, addr: Option<[u8; 6]>) -> Vec<u8> {
    // Go's sysARPHardware* constants.
    const IPV4_IPV4: u16 = 768;
    const IPV6_IPV6: u16 = 769;
    const IPV6_IPV4: u16 = 776;
    const GRE_IPV4: u16 = 778;
    const GRE_IPV6: u16 = 823;
    match halen {
        4 if matches!(hatype, IPV4_IPV4 | GRE_IPV4 | IPV6_IPV4) => return Vec::new(),
        16 if matches!(hatype, IPV6_IPV6 | GRE_IPV6) => return Vec::new(),
        _ => {}
    }
    let Some(addr) = addr else {
        return Vec::new();
    };
    let value = &addr[..halen.min(addr.len())];
    if value.iter().all(|b| *b == 0) {
        return Vec::new();
    }
    value.to_vec()
}

pub(super) fn net_io_counters() -> Result<Vec<NetIoCounters>, String> {
    let netdev = read(&host_proc("net/dev"))?;
    parse::linux_net_io_counters(&netdev)
}

pub(super) fn load_avg() -> Result<LoadAvg, String> {
    match read(&host_proc("loadavg")).and_then(|text| parse::linux_load_avg(&text)) {
        Ok(load) => Ok(load),
        Err(_) => {
            let info = nix::sys::sysinfo::sysinfo().map_err(|errno| errno.desc().to_owned())?;
            let (load1, load5, load15) = info.load_average();
            Ok(LoadAvg {
                load1,
                load5,
                load15,
            })
        }
    }
}

/// Go `runtime.NumCPU` on Linux: the CPUs in the startup affinity mask.
pub(super) fn num_cpu() -> usize {
    match nix::sched::sched_getaffinity(nix::unistd::Pid::from_raw(0)) {
        Ok(set) => {
            let n = (0..nix::sched::CpuSet::count())
                .filter(|cpu| set.is_set(*cpu).unwrap_or(false))
                .count();
            n.max(1)
        }
        Err(_) => 1,
    }
}
