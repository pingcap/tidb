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

//! Linux cgroup v1/v2 discovery used by TiDB's CPU and memory authorities.
//!
//! This is the native Rust boundary for Go `pkg/util/cgroup`. The Go-only
//! `runtime.GOMAXPROCS` mutation has no process-global Rust equivalent; this
//! module exposes the same quota decision through [`quota_parallelism`] so a
//! runtime builder can apply it before starting workers.

use std::io;

#[cfg(target_os = "macos")]
use std::process::Command;

#[cfg(any(target_os = "linux", test))]
use std::collections::HashSet;
#[cfg(any(target_os = "linux", test))]
use std::fs;
#[cfg(any(target_os = "linux", test))]
use std::io::ErrorKind;
#[cfg(any(target_os = "linux", test))]
use std::path::{Component, Path, PathBuf};

#[cfg(any(target_os = "linux", test))]
const PROC_CGROUP: &str = "/proc/self/cgroup";
#[cfg(any(target_os = "linux", test))]
const PROC_MOUNTINFO: &str = "/proc/self/mountinfo";

#[cfg(any(target_os = "linux", test))]
const V1_MEMORY_STAT: &str = "memory.stat";
#[cfg(any(target_os = "linux", test))]
const V2_MEMORY_STAT: &str = "memory.stat";
#[cfg(any(target_os = "linux", test))]
const V2_MEMORY_LIMIT: &str = "memory.max";
#[cfg(any(target_os = "linux", test))]
const V1_MEMORY_USAGE: &str = "memory.usage_in_bytes";
#[cfg(any(target_os = "linux", test))]
const V2_MEMORY_USAGE: &str = "memory.current";
#[cfg(any(target_os = "linux", test))]
const V1_CPU_QUOTA: &str = "cpu.cfs_quota_us";
#[cfg(any(target_os = "linux", test))]
const V1_CPU_PERIOD: &str = "cpu.cfs_period_us";
#[cfg(any(target_os = "linux", test))]
const V1_CPU_SYSTEM: &str = "cpuacct.usage_sys";
#[cfg(any(target_os = "linux", test))]
const V1_CPU_USER: &str = "cpuacct.usage_user";
#[cfg(any(target_os = "linux", test))]
const V2_CPU_MAX: &str = "cpu.max";
#[cfg(any(target_os = "linux", test))]
const V2_CPU_STAT: &str = "cpu.stat";

/// How a detected CPU quota affected the recommended parallelism.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CpuQuotaStatus {
    /// The platform has no usable CPU quota.
    Undefined,
    /// The detected quota determines the result.
    Used,
    /// The caller's minimum is larger than the detected quota.
    MinimumUsed,
}

/// CPU usage and quota for the current cgroup.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CpuUsage {
    /// System CPU usage reported by the cgroup control file.
    pub system_time: u64,
    /// User CPU usage reported by the cgroup control file.
    pub user_time: u64,
    /// CFS period in microseconds.
    pub period: i64,
    /// CFS quota in microseconds; `-1` means unlimited.
    pub quota: i64,
    /// Logical CPUs visible to the process.
    pub num_cpu: usize,
}

impl CpuUsage {
    /// Returns the maximum CPUs this cgroup can keep busy.
    pub fn cpu_shares(self) -> f64 {
        if self.period <= 0 || self.quota <= 0 {
            self.num_cpu as f64
        } else {
            self.quota as f64 / self.period as f64
        }
    }
}

/// Detected cgroup version.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Version {
    /// No memory controller was detected.
    #[default]
    Unknown,
    /// Legacy controller hierarchy.
    V1,
    /// Unified controller hierarchy.
    V2,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Clone, Debug)]
struct Mount {
    path: PathBuf,
    version: Version,
}

#[cfg(any(target_os = "linux", test))]
fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message.into())
}

#[cfg(any(target_os = "linux", test))]
fn rooted(root: &Path, path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    root.join(path.strip_prefix(Path::new("/")).unwrap_or(path))
}

#[cfg(any(target_os = "linux", test))]
fn controller_matches(field: &str, controller: &str) -> bool {
    if field == controller {
        return true;
    }
    let fields: HashSet<_> = field.split(',').collect();
    let controllers: Vec<_> = controller.split(',').collect();
    fields.len() >= 2
        && fields.len() >= controllers.len()
        && controllers.into_iter().all(|c| fields.contains(c))
}

#[cfg(any(target_os = "linux", test))]
fn detect_control_path(root: &Path, controller: &str) -> io::Result<PathBuf> {
    let content = fs::read_to_string(rooted(root, PROC_CGROUP))?;
    let mut unified = None;
    for line in content.lines() {
        let fields: Vec<_> = line.split(':').collect();
        if fields.len() < 3 {
            continue;
        }
        if fields[0] == "0" && fields[1].is_empty() {
            unified = Some(PathBuf::from(fields[2]));
        } else if controller_matches(fields[1], controller) {
            return Ok(PathBuf::from(fields[2..].join(":")));
        }
    }
    Ok(unified.unwrap_or_default())
}

#[cfg(any(target_os = "linux", test))]
fn lexical_relative(from: &Path, to: &Path) -> Option<PathBuf> {
    let from: Vec<_> = from.components().filter_map(normal_component).collect();
    let to: Vec<_> = to.components().filter_map(normal_component).collect();
    let common = from.iter().zip(&to).take_while(|(a, b)| a == b).count();
    let mut out = PathBuf::new();
    for _ in common..from.len() {
        out.push("..");
    }
    for component in &to[common..] {
        out.push(component);
    }
    Some(out)
}

#[cfg(any(target_os = "linux", test))]
fn normal_component(component: Component<'_>) -> Option<std::ffi::OsString> {
    match component {
        Component::Normal(value) => Some(value.to_os_string()),
        Component::ParentDir => Some("..".into()),
        _ => None,
    }
}

#[cfg(any(target_os = "linux", test))]
fn detect_mount_version(fields: &[&str], controller: &str) -> Option<Version> {
    let separator = fields.iter().position(|field| *field == "-")?;
    if fields.len() < separator + 4 {
        return None;
    }
    match fields[separator + 1] {
        "cgroup" if controller_matches(fields[separator + 3], controller) => Some(Version::V1),
        "cgroup2" => Some(Version::V2),
        _ => None,
    }
}

#[cfg(any(target_os = "linux", test))]
fn detect_mounts(root: &Path, cgroup_root: &Path, controller: &str) -> io::Result<Vec<Mount>> {
    let content = fs::read_to_string(rooted(root, PROC_MOUNTINFO))?;
    let mut v1 = None;
    let mut v2 = None;
    for line in content.lines() {
        let fields: Vec<_> = line.split_ascii_whitespace().collect();
        if fields.len() < 10 {
            continue;
        }
        match detect_mount_version(&fields, controller) {
            Some(Version::V1) => {
                let namespace_root = Path::new(fields[3]);
                if namespace_root.to_string_lossy().contains("..") {
                    continue;
                }
                if let Some(relative) = lexical_relative(namespace_root, cgroup_root) {
                    v1 = Some(PathBuf::from(fields[4]).join(relative));
                }
            }
            Some(Version::V2) => v2 = Some(PathBuf::from(fields[4])),
            _ => {}
        }
    }
    let mut mounts = Vec::new();
    if let Some(path) = v1 {
        mounts.push(Mount {
            path,
            version: Version::V1,
        });
    }
    if let Some(path) = v2 {
        mounts.push(Mount {
            path,
            version: Version::V2,
        });
    }
    if mounts.is_empty() {
        Err(invalid("failed to detect cgroup root mount and version"))
    } else {
        Ok(mounts)
    }
}

#[cfg(any(target_os = "linux", test))]
fn read_i64(path: &Path) -> io::Result<i64> {
    fs::read_to_string(path)?
        .trim()
        .parse()
        .map_err(|error| invalid(format!("invalid integer in {}: {error}", path.display())))
}

#[cfg(any(target_os = "linux", test))]
fn read_all_u64(path: &Path) -> io::Result<u64> {
    let value = fs::read_to_string(path)?;
    value
        .trim()
        .parse()
        .map_err(|error| invalid(format!("invalid integer in {}: {error}", path.display())))
}

#[cfg(any(target_os = "linux", test))]
fn read_control_u64(path: &Path) -> io::Result<u64> {
    let value = fs::read_to_string(path)?;
    let value = value
        .lines()
        .next()
        .ok_or_else(|| invalid(format!("no value found in {}", path.display())))?
        .trim();
    if value == "max" {
        Ok(i64::MAX as u64)
    } else {
        value
            .parse()
            .map_err(|error| invalid(format!("invalid integer in {}: {error}", path.display())))
    }
}

#[cfg(any(target_os = "linux", test))]
fn read_stat(path: &Path, key: &str) -> io::Result<u64> {
    let content = fs::read_to_string(path)?;
    for line in content.lines() {
        let mut fields = line.split_ascii_whitespace();
        if fields.next() == Some(key) {
            let value = fields
                .next()
                .ok_or_else(|| invalid(format!("missing value for {key} in {}", path.display())))?;
            if fields.next().is_none() {
                return value.parse().map_err(|error| {
                    invalid(format!(
                        "invalid value for {key} in {}: {error}",
                        path.display()
                    ))
                });
            }
        }
    }
    Err(invalid(format!(
        "failed to find expected memory stat {key:?} in {}",
        path.display()
    )))
}

#[cfg(any(target_os = "linux", test))]
fn cpu_quota_v1(root: &Path) -> io::Result<(i64, i64)> {
    Ok((
        read_i64(&root.join(V1_CPU_PERIOD))?,
        read_i64(&root.join(V1_CPU_QUOTA))?,
    ))
}

#[cfg(any(target_os = "linux", test))]
fn cpu_usage_v1(root: &Path) -> io::Result<(u64, u64)> {
    Ok((
        read_all_u64(&root.join(V1_CPU_SYSTEM))?,
        read_all_u64(&root.join(V1_CPU_USER))?,
    ))
}

#[cfg(any(target_os = "linux", test))]
fn cpu_quota_v2(root: &Path) -> io::Result<(i64, i64)> {
    let content = fs::read_to_string(root.join(V2_CPU_MAX))?;
    let fields: Vec<_> = content.split_ascii_whitespace().collect();
    if fields.is_empty() || fields.len() > 2 {
        return Err(invalid("unexpected cgroup v2 cpu.max format"));
    }
    let quota = if fields[0] == "max" {
        -1
    } else {
        fields[0]
            .parse()
            .map_err(|error| invalid(format!("invalid cgroup v2 CPU quota: {error}")))?
    };
    let period = if fields.len() == 2 {
        fields[1]
            .parse()
            .map_err(|error| invalid(format!("invalid cgroup v2 CPU period: {error}")))?
    } else {
        0
    };
    Ok((period, quota))
}

#[cfg(any(target_os = "linux", test))]
fn cpu_usage_v2(root: &Path) -> io::Result<(u64, u64)> {
    let content = fs::read_to_string(root.join(V2_CPU_STAT))?;
    let mut system = 0;
    let mut user = 0;
    for line in content.lines() {
        let fields: Vec<_> = line.split_ascii_whitespace().collect();
        if fields.len() != 2 {
            continue;
        }
        let destination = match fields[0] {
            "system_usec" => &mut system,
            "user_usec" => &mut user,
            _ => continue,
        };
        *destination = fields[1]
            .parse()
            .map_err(|error| invalid(format!("invalid cgroup v2 CPU usage: {error}")))?;
    }
    Ok((system, user))
}

#[cfg(any(target_os = "linux", test))]
fn cgroup_cpu_at(root: &Path, include_usage: bool) -> io::Result<CpuUsage> {
    let controller = if include_usage { "cpu,cpuacct" } else { "cpu" };
    let path = detect_control_path(root, controller)?;
    if path.as_os_str().is_empty() {
        return Err(invalid("no cpu controller detected"));
    }
    let mounts = detect_mounts(root, &path, controller)?;
    let v1 = mounts.iter().find(|mount| mount.version == Version::V1);
    let v2 = mounts.iter().find(|mount| mount.version == Version::V2);
    let v1_root = v1.map(|mount| rooted(root, &mount.path));
    let v2_root =
        v2.map(|mount| rooted(root, &mount.path).join(path.strip_prefix("/").unwrap_or(&path)));

    let (period, quota) = match v2_root.as_deref().map(cpu_quota_v2).transpose() {
        Ok(Some(value)) => value,
        Ok(None) | Err(_) => cpu_quota_v1(
            v1_root
                .as_deref()
                .ok_or_else(|| invalid("missing CPU cgroup"))?,
        )?,
    };
    let (system_time, user_time) = if include_usage {
        match v2_root.as_deref().map(cpu_usage_v2).transpose() {
            Ok(Some(value)) => value,
            Ok(None) | Err(_) => cpu_usage_v1(
                v1_root
                    .as_deref()
                    .ok_or_else(|| invalid("missing CPU accounting cgroup"))?,
            )?,
        }
    } else {
        (0, 0)
    };
    Ok(CpuUsage {
        system_time,
        user_time,
        period,
        quota,
        num_cpu: 0,
    })
}

#[cfg(any(target_os = "linux", test))]
fn memory_mounts(root: &Path) -> io::Result<Option<(PathBuf, Vec<Mount>)>> {
    let path = detect_control_path(root, "memory")?;
    if path.as_os_str().is_empty() {
        return Ok(None);
    }
    let mounts = detect_mounts(root, &path, "memory")?;
    Ok(Some((path, mounts)))
}

#[cfg(any(target_os = "linux", test))]
fn memory_value<F1, F2>(root: &Path, v1_read: F1, v2_read: F2) -> io::Result<(u64, Version)>
where
    F1: Fn(&Path) -> io::Result<u64>,
    F2: Fn(&Path) -> io::Result<u64>,
{
    let Some((path, mounts)) = memory_mounts(root)? else {
        return Ok((0, Version::Unknown));
    };
    let v1 = mounts.iter().find(|mount| mount.version == Version::V1);
    let v2 = mounts.iter().find(|mount| mount.version == Version::V2);
    let mut v1_error = None;
    if let Some(mount) = v1 {
        match v1_read(&rooted(root, &mount.path)) {
            Ok(value) => return Ok((value, Version::V1)),
            Err(error) => v1_error = Some(error),
        }
    }
    if let Some(mount) = v2 {
        let cgroup_root = rooted(root, &mount.path).join(path.strip_prefix("/").unwrap_or(&path));
        return v2_read(&cgroup_root).map(|value| (value, Version::V2));
    }
    Err(v1_error.unwrap_or_else(|| invalid("failed to read cgroup memory value")))
}

// Keep this separate from `memory_value`: the pinned Go implementation's
// hybrid fallback joins the v2 cgroup path to the v1 mount for memory.current.
// That path choice is observable and its package tests define this module's
// compatibility boundary.
#[cfg(any(target_os = "linux", test))]
fn memory_usage_at(root: &Path) -> io::Result<u64> {
    let Some((path, mounts)) = memory_mounts(root)? else {
        return Ok(0);
    };
    let v1 = mounts.iter().find(|mount| mount.version == Version::V1);
    let v2 = mounts.iter().find(|mount| mount.version == Version::V2);
    let mut v1_error = None;
    if let Some(mount) = v1 {
        let v1_root = rooted(root, &mount.path);
        match read_control_u64(&v1_root.join(V1_MEMORY_USAGE)) {
            Ok(value) => return Ok(value),
            Err(error) => v1_error = Some(error),
        }
        if v2.is_some() {
            return read_control_u64(
                &v1_root
                    .join(path.strip_prefix("/").unwrap_or(&path))
                    .join(V2_MEMORY_USAGE),
            );
        }
    }
    if let Some(mount) = v2 {
        return read_control_u64(
            &rooted(root, &mount.path)
                .join(path.strip_prefix("/").unwrap_or(&path))
                .join(V2_MEMORY_USAGE),
        );
    }
    Err(v1_error.unwrap_or_else(|| invalid("failed to read cgroup memory usage")))
}

/// Returns CPU usage and quota for the current process.
#[cfg(target_os = "linux")]
pub fn get_cgroup_cpu() -> io::Result<CpuUsage> {
    let mut usage = cgroup_cpu_at(Path::new("/"), true)?;
    usage.num_cpu = std::thread::available_parallelism()?.get();
    Ok(usage)
}

/// Returns the platform CPU count without a cgroup quota on non-Linux hosts.
#[cfg(not(target_os = "linux"))]
pub fn get_cgroup_cpu() -> io::Result<CpuUsage> {
    Ok(CpuUsage {
        num_cpu: std::thread::available_parallelism()?.get(),
        ..CpuUsage::default()
    })
}

/// Converts the current CPU quota to a worker count.
#[cfg(target_os = "linux")]
pub fn quota_parallelism(minimum: usize) -> io::Result<(usize, CpuQuotaStatus)> {
    Ok(parallelism_for_cpu_quota(get_cgroup_cpu()?, minimum))
}

/// Applies the source CPU-quota rounding and minimum rules to a usage sample.
pub fn parallelism_for_cpu_quota(usage: CpuUsage, minimum: usize) -> (usize, CpuQuotaStatus) {
    let detected = usage.cpu_shares().ceil() as usize;
    if minimum > 0 && detected < minimum {
        (minimum, CpuQuotaStatus::MinimumUsed)
    } else {
        (detected, CpuQuotaStatus::Used)
    }
}

/// Reports that cgroup CPU quotas are unavailable on non-Linux hosts.
#[cfg(not(target_os = "linux"))]
pub fn quota_parallelism(_minimum: usize) -> io::Result<(usize, CpuQuotaStatus)> {
    Ok((0, CpuQuotaStatus::Undefined))
}

/// Returns the worker-count decision made by Go `SetGOMAXPROCS` without
/// pretending Rust has a mutable process-global scheduler.
///
/// `None` means an explicit `GOMAXPROCS` environment value or an unsupported
/// platform leaves runtime construction to the caller.
pub fn runtime_parallelism_recommendation() -> io::Result<Option<(usize, CpuQuotaStatus)>> {
    if std::env::var_os("GOMAXPROCS").is_some() {
        return Ok(None);
    }
    let recommendation = quota_parallelism(1)?;
    if recommendation.1 == CpuQuotaStatus::Undefined {
        Ok(None)
    } else {
        Ok(Some(recommendation))
    }
}

/// Returns the current cgroup CPU period and quota.
#[cfg(target_os = "linux")]
pub fn get_cpu_period_and_quota() -> io::Result<(i64, i64)> {
    let usage = cgroup_cpu_at(Path::new("/"), false)?;
    Ok((usage.period, usage.quota))
}

/// Returns the source-defined unsupported values on non-Linux hosts.
#[cfg(not(target_os = "linux"))]
pub fn get_cpu_period_and_quota() -> io::Result<(i64, i64)> {
    Ok((-1, -1))
}

/// Returns the current process cgroup memory limit and its version.
#[cfg(target_os = "linux")]
pub fn get_cgroup_memory_limit() -> io::Result<(u64, Version)> {
    memory_value(
        Path::new("/"),
        |root| read_stat(&root.join(V1_MEMORY_STAT), "hierarchical_memory_limit"),
        |root| read_control_u64(&root.join(V2_MEMORY_LIMIT)),
    )
}

/// Reports no cgroup memory limit on non-Linux hosts.
#[cfg(not(target_os = "linux"))]
pub fn get_cgroup_memory_limit() -> io::Result<(u64, Version)> {
    Ok((0, Version::Unknown))
}

/// Returns the current process cgroup memory limit.
pub fn get_memory_limit() -> io::Result<u64> {
    get_cgroup_memory_limit().map(|(limit, _)| limit)
}

/// Reads physical RAM through the same operating-system boundary Go's
/// `gopsutil.VirtualMemory().Total` uses for the global memory limit.
#[cfg(target_os = "linux")]
fn host_memory_total() -> io::Result<u64> {
    let content = fs::read_to_string("/proc/meminfo")?;
    let kib = content
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "missing MemTotal"))?;
    kib.checked_mul(1024)
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "MemTotal overflows bytes"))
}

#[cfg(target_os = "macos")]
fn host_memory_total() -> io::Result<u64> {
    let output = Command::new("sysctl").args(["-n", "hw.memsize"]).output()?;
    if !output.status.success() {
        return Err(io::Error::other("sysctl hw.memsize failed"));
    }
    std::str::from_utf8(&output.stdout)
        .ok()
        .map(str::trim)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid hw.memsize"))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn host_memory_total() -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "host memory discovery is not available on this platform",
    ))
}

/// Returns the RAM total visible to TiDB: a finite cgroup limit wins over
/// physical RAM, exactly as Go's memory hook does.
pub fn effective_memory_limit() -> io::Result<u64> {
    let physical = host_memory_total()?;
    Ok(select_effective_memory_limit(
        physical,
        get_memory_limit().unwrap_or_default(),
    ))
}

fn select_effective_memory_limit(physical: u64, cgroup_limit: u64) -> u64 {
    if cgroup_limit > 0 && cgroup_limit < physical {
        cgroup_limit
    } else {
        physical
    }
}

/// Returns the current process cgroup memory usage.
#[cfg(target_os = "linux")]
pub fn get_memory_usage() -> io::Result<u64> {
    memory_usage_at(Path::new("/"))
}

/// Reports no cgroup memory usage on non-Linux hosts.
#[cfg(not(target_os = "linux"))]
pub fn get_memory_usage() -> io::Result<u64> {
    Ok(0)
}

/// Returns inactive file-backed memory for the current process cgroup.
#[cfg(target_os = "linux")]
pub fn get_memory_inactive_file_usage() -> io::Result<u64> {
    memory_value(
        Path::new("/"),
        |root| read_stat(&root.join(V1_MEMORY_STAT), "total_inactive_file"),
        |root| read_stat(&root.join(V2_MEMORY_STAT), "inactive_file"),
    )
    .map(|(usage, _)| usage)
}

/// Reports no inactive cgroup memory on non-Linux hosts.
#[cfg(not(target_os = "linux"))]
pub fn get_memory_inactive_file_usage() -> io::Result<u64> {
    Ok(0)
}

#[cfg(any(target_os = "linux", test))]
fn container_files_indicate_container(cgroup: &str, mountinfo: &str) -> bool {
    if ["docker", "kubepods", "containerd"]
        .iter()
        .any(|marker| cgroup.contains(marker))
    {
        return true;
    }
    mountinfo.lines().any(|line| {
        let fields: Vec<_> = line.split(' ').collect();
        fields.len() > 8 && fields[4] == "/" && fields[8] == "overlay"
    })
}

/// Returns whether the process appears to run in a container.
#[cfg(target_os = "linux")]
pub fn in_container() -> bool {
    let cgroup = fs::read_to_string(PROC_CGROUP).unwrap_or_default();
    let mountinfo = fs::read_to_string(PROC_MOUNTINFO).unwrap_or_default();
    container_files_indicate_container(&cgroup, &mountinfo)
}

/// Non-Linux hosts do not use Linux cgroups.
#[cfg(not(target_os = "linux"))]
pub fn in_container() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(root: &Path, path: &str, value: &str) {
        let path = rooted(root, path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, value).unwrap();
    }

    #[test]
    fn effective_limit_uses_the_smaller_finite_cgroup_value() {
        assert_eq!(select_effective_memory_limit(16, 0), 16);
        assert_eq!(select_effective_memory_limit(16, 32), 16);
        assert_eq!(select_effective_memory_limit(16, 8), 8);
    }

    fn v1_mount(controller: &str, namespace_root: &str, mount: &str) -> String {
        format!("29 23 0:26 {namespace_root} {mount} rw - cgroup cgroup rw,{controller}\n")
    }

    fn v2_mount(mount: &str) -> String {
        format!("29 23 0:26 / {mount} rw - cgroup2 cgroup rw\n")
    }

    #[test]
    fn controller_order_and_colon_paths_match_go() {
        assert!(controller_matches("rw,cpuacct,cpu", "cpu,cpuacct"));
        assert!(!controller_matches("cpu", "cpu,cpuacct"));
        let root = tempfile::tempdir().unwrap();
        write(
            root.path(),
            PROC_CGROUP,
            "11:cpuacct,cpu:/slice:name\n0::/unified\n",
        );
        assert_eq!(
            detect_control_path(root.path(), "cpu,cpuacct").unwrap(),
            Path::new("/slice:name")
        );
    }

    #[test]
    fn v1_namespace_and_v2_mounts_are_resolved() {
        let root = tempfile::tempdir().unwrap();
        write(
            root.path(),
            PROC_MOUNTINFO,
            &(v1_mount("cpu,cpuacct", "/kubepods", "/sys/fs/cgroup/cpu")
                + &v2_mount("/sys/fs/cgroup")),
        );
        let mounts = detect_mounts(root.path(), Path::new("/kubepods/pod"), "cpu,cpuacct").unwrap();
        assert_eq!(mounts[0].path, Path::new("/sys/fs/cgroup/cpu/pod"));
        assert_eq!(mounts[1].version, Version::V2);
    }

    #[test]
    fn v1_cpu_values_and_unlimited_share_calculation() {
        let root = tempfile::tempdir().unwrap();
        write(root.path(), PROC_CGROUP, "11:cpu,cpuacct:/pod\n");
        write(
            root.path(),
            PROC_MOUNTINFO,
            &v1_mount("cpu,cpuacct", "/", "/sys/fs/cgroup/cpu"),
        );
        write(
            root.path(),
            "/sys/fs/cgroup/cpu/pod/cpu.cfs_period_us",
            "100000\n",
        );
        write(
            root.path(),
            "/sys/fs/cgroup/cpu/pod/cpu.cfs_quota_us",
            "150000\n",
        );
        write(
            root.path(),
            "/sys/fs/cgroup/cpu/pod/cpuacct.usage_sys",
            "123\n",
        );
        write(
            root.path(),
            "/sys/fs/cgroup/cpu/pod/cpuacct.usage_user",
            "456\n",
        );
        let mut usage = cgroup_cpu_at(root.path(), true).unwrap();
        assert_eq!((usage.period, usage.quota), (100000, 150000));
        assert_eq!((usage.system_time, usage.user_time), (123, 456));
        assert_eq!(usage.cpu_shares(), 1.5);
        usage.quota = -1;
        usage.num_cpu = 8;
        assert_eq!(usage.cpu_shares(), 8.0);
        assert_eq!(
            parallelism_for_cpu_quota(
                CpuUsage {
                    period: 100_000,
                    quota: 150_001,
                    ..usage
                },
                1,
            ),
            (2, CpuQuotaStatus::Used)
        );
        assert_eq!(
            parallelism_for_cpu_quota(
                CpuUsage {
                    period: 100_000,
                    quota: 1,
                    ..usage
                },
                2,
            ),
            (2, CpuQuotaStatus::MinimumUsed)
        );
    }

    #[test]
    fn v2_cpu_limited_unlimited_and_malformed_values() {
        let root = tempfile::tempdir().unwrap();
        write(root.path(), PROC_CGROUP, "0::/machine/pod\n");
        write(root.path(), PROC_MOUNTINFO, &v2_mount("/sys/fs/cgroup"));
        let base = "/sys/fs/cgroup/machine/pod";
        write(root.path(), &format!("{base}/cpu.max"), "100 1000\n");
        write(
            root.path(),
            &format!("{base}/cpu.stat"),
            "user_usec 100\nsystem_usec 200\n",
        );
        let usage = cgroup_cpu_at(root.path(), true).unwrap();
        assert_eq!((usage.period, usage.quota), (1000, 100));
        assert_eq!((usage.system_time, usage.user_time), (200, 100));
        write(root.path(), &format!("{base}/cpu.max"), "max 1000\n");
        assert_eq!(cgroup_cpu_at(root.path(), true).unwrap().quota, -1);
        write(root.path(), &format!("{base}/cpu.max"), "1 2 3\n");
        assert_eq!(
            cgroup_cpu_at(root.path(), true).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }

    #[test]
    fn hybrid_cpu_prefers_v2_and_falls_back_to_v1_per_value() {
        let root = tempfile::tempdir().unwrap();
        write(root.path(), PROC_CGROUP, "11:cpu,cpuacct:/v1\n0::/v2\n");
        write(
            root.path(),
            PROC_MOUNTINFO,
            &(v1_mount("cpu,cpuacct", "/", "/sys/fs/cgroup/cpu") + &v2_mount("/sys/fs/cgroup")),
        );
        for (name, value) in [
            (V1_CPU_PERIOD, "67890"),
            (V1_CPU_QUOTA, "12345"),
            (V1_CPU_SYSTEM, "123"),
            (V1_CPU_USER, "456"),
        ] {
            write(root.path(), &format!("/sys/fs/cgroup/cpu/v1/{name}"), value);
        }
        let usage = cgroup_cpu_at(root.path(), true).unwrap();
        assert_eq!((usage.period, usage.quota), (67890, 12345));
        assert_eq!((usage.system_time, usage.user_time), (123, 456));
    }

    #[test]
    fn memory_v1_namespace_and_v2_numeric_max_values() {
        let v1 = tempfile::tempdir().unwrap();
        write(v1.path(), PROC_CGROUP, "5:memory:/kubepods/pod\n");
        write(
            v1.path(),
            PROC_MOUNTINFO,
            &v1_mount("memory", "/kubepods", "/sys/fs/cgroup/memory"),
        );
        write(
            v1.path(),
            "/sys/fs/cgroup/memory/pod/memory.stat",
            "hierarchical_memory_limit 4096\ntotal_inactive_file 512\n",
        );
        write(
            v1.path(),
            "/sys/fs/cgroup/memory/pod/memory.usage_in_bytes",
            "1024\n",
        );
        assert_eq!(
            memory_value(
                v1.path(),
                |p| read_stat(&p.join(V1_MEMORY_STAT), "hierarchical_memory_limit"),
                |p| read_control_u64(&p.join(V2_MEMORY_LIMIT))
            )
            .unwrap(),
            (4096, Version::V1)
        );
        assert_eq!(memory_usage_at(v1.path()).unwrap(), 1024);
        assert_eq!(
            memory_value(
                v1.path(),
                |p| read_stat(&p.join(V1_MEMORY_STAT), "total_inactive_file"),
                |p| read_stat(&p.join(V2_MEMORY_STAT), "inactive_file"),
            )
            .unwrap(),
            (512, Version::V1)
        );

        let v2 = tempfile::tempdir().unwrap();
        write(v2.path(), PROC_CGROUP, "0::/machine/pod\n");
        write(v2.path(), PROC_MOUNTINFO, &v2_mount("/sys/fs/cgroup"));
        write(v2.path(), "/sys/fs/cgroup/machine/pod/memory.max", "max\n");
        write(
            v2.path(),
            "/sys/fs/cgroup/machine/pod/memory.current",
            "2048\n",
        );
        write(
            v2.path(),
            "/sys/fs/cgroup/machine/pod/memory.stat",
            "inactive_file 256\n",
        );
        assert_eq!(
            memory_value(
                v2.path(),
                |p| read_stat(&p.join(V1_MEMORY_STAT), "hierarchical_memory_limit"),
                |p| read_control_u64(&p.join(V2_MEMORY_LIMIT))
            )
            .unwrap(),
            (i64::MAX as u64, Version::V2)
        );
        assert_eq!(memory_usage_at(v2.path()).unwrap(), 2048);
        assert_eq!(
            memory_value(
                v2.path(),
                |p| read_stat(&p.join(V1_MEMORY_STAT), "total_inactive_file"),
                |p| read_stat(&p.join(V2_MEMORY_STAT), "inactive_file"),
            )
            .unwrap(),
            (256, Version::V2)
        );
    }

    #[test]
    fn hybrid_memory_usage_preserves_the_pinned_mount_choice() {
        let root = tempfile::tempdir().unwrap();
        write(root.path(), PROC_CGROUP, "5:memory:/legacy\n0::/unified\n");
        write(
            root.path(),
            PROC_MOUNTINFO,
            &(v1_mount("memory", "/", "/sys/fs/cgroup/memory") + &v2_mount("/sys/fs/cgroup")),
        );
        // The v1 usage is absent. The pinned Go fallback reads memory.current
        // below the v1 mount joined with the v1 control path.
        write(
            root.path(),
            "/sys/fs/cgroup/memory/legacy/legacy/memory.current",
            "77\n",
        );
        write(root.path(), "/sys/fs/cgroup/legacy/memory.current", "88\n");
        assert_eq!(memory_usage_at(root.path()).unwrap(), 77);
    }

    #[test]
    fn missing_controller_is_zero_but_missing_stat_and_bad_values_error() {
        let root = tempfile::tempdir().unwrap();
        write(root.path(), PROC_CGROUP, "2:devices:/\n");
        assert_eq!(
            memory_value(root.path(), |_| unreachable!(), |_| unreachable!()).unwrap(),
            (0, Version::Unknown)
        );

        let stat = root.path().join("memory.stat");
        fs::write(&stat, "other 1\n").unwrap();
        assert_eq!(
            read_stat(&stat, "inactive_file").unwrap_err().kind(),
            ErrorKind::InvalidData
        );
        fs::write(&stat, "inactive_file nope\n").unwrap();
        assert_eq!(
            read_stat(&stat, "inactive_file").unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }

    #[test]
    fn container_markers_and_root_overlay_match_the_source_checks() {
        assert!(container_files_indicate_container("1:cpu:/docker/abc", ""));
        assert!(container_files_indicate_container(
            "0::/",
            "1 2 3 4 / 6 7 8 overlay rest"
        ));
        assert!(!container_files_indicate_container(
            "0::/user.slice",
            "1 2 3 4 /home 6 7 8 overlay rest"
        ));
    }
}
