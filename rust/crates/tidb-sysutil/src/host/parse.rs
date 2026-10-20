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

//! The gopsutil v3.24.5 text parsers sysutil depends on, free of any system
//! call so that every platform compiles and tests them: Linux's `/proc`
//! formats and Darwin's `netstat -ibdnW`.
//!
//! Where gopsutil would index past a malformed line and panic, these return
//! an error instead; sysutil drops the item either way.

// Each platform calls only its own parsers.
#![allow(dead_code)]

use super::{
    CpuInfo, CpuTimes, DiskIoCounters, DiskUsage, LoadAvg, NetIoCounters, Partition, VirtualMemory,
};

/// gopsutil `common.ReadLines` over already-read content: lines without
/// their `\n`; a final unterminated line counts when non-empty.
pub(crate) fn read_lines(content: &str) -> Vec<&str> {
    let mut lines: Vec<&str> = content.split_inclusive('\n').collect();
    // `strings.Trim(line, "\n")`.
    for line in &mut lines {
        *line = line.trim_matches('\n');
    }
    lines
}

/// Go `strconv.ParseUint(s, 10, 64)`: digits only, no sign.
pub(crate) fn parse_uint(s: &str) -> Result<u64, String> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("strconv.ParseUint: parsing {s:?}: invalid syntax"));
    }
    s.parse::<u64>()
        .map_err(|_| format!("strconv.ParseUint: parsing {s:?}: value out of range"))
}

/// Go `strconv.ParseInt(s, 10, 64)`.
pub(crate) fn parse_int(s: &str) -> Result<i64, String> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("strconv.ParseInt: parsing {s:?}: invalid syntax"));
    }
    s.parse::<i64>()
        .map_err(|_| format!("strconv.ParseInt: parsing {s:?}: value out of range"))
}

/// Go `strconv.ParseFloat(s, 64)`.
pub(crate) fn parse_float(s: &str) -> Result<f64, String> {
    tidb_datatype::go_strconv::parse_float64(s)
        .map_err(|_| format!("strconv.ParseFloat: parsing {s:?}: invalid syntax"))
}

/// gopsutil `disk.Usage`'s arithmetic over a `statfs` result.
pub(crate) fn disk_usage(bsize: u64, blocks: u64, bfree: u64, bavail: u64) -> DiskUsage {
    let total = blocks.wrapping_mul(bsize);
    let free = bavail.wrapping_mul(bsize);
    let used = blocks.wrapping_sub(bfree).wrapping_mul(bsize);
    let used_percent = if used.wrapping_add(free) == 0 {
        0.0
    } else {
        // gopsutil does not use Total for the percentage (issue #562).
        (used as f64 / used.wrapping_add(free) as f64) * 100.0
    };
    DiskUsage {
        total,
        free,
        used,
        used_percent,
    }
}

/// gopsutil Linux `cpu.Info` over `/proc/cpuinfo`. `max_freq(cpu)` reads
/// `cpufreq/cpuinfo_max_freq` for `finishCPUInfo`. Cores is always 1.
pub(crate) fn linux_cpu_info(
    cpuinfo: &str,
    max_freq: &dyn Fn(i32) -> Option<String>,
) -> Result<Vec<CpuInfo>, String> {
    // Go `finishCPUInfo`: the maximum clock overrides /proc's value.
    let finish = |c: &mut CpuInfo, cpu: i32| {
        let Some(line) = max_freq(cpu) else {
            return;
        };
        let Ok(value) = parse_float(&line) else {
            return;
        };
        c.mhz = value / 1000.0; // value is in kHz
        if c.mhz > 9999.0 {
            c.mhz /= 1000.0; // value in Hz
        }
    };
    let mut ret = Vec::new();
    let mut c = CpuInfo {
        cores: 1,
        ..CpuInfo::default()
    };
    let mut cpu: i32 = -1;
    for line in read_lines(cpuinfo) {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() < 2 {
            continue;
        }
        let key = fields[0].trim();
        let value = fields[1].trim();
        match key {
            "processor" | "cpu number" => {
                if cpu >= 0 {
                    finish(&mut c, cpu);
                    ret.push(c);
                }
                c = CpuInfo {
                    cores: 1,
                    ..CpuInfo::default()
                };
                let t = parse_int(value)?;
                cpu = t as i32;
            }
            "stepping" | "revision" | "CPU revision" => {
                let val = if key == "revision" {
                    value.split('.').next().unwrap_or_default()
                } else {
                    value
                };
                parse_int(val)?;
            }
            "cpu MHz" | "clock" | "cpu MHz dynamic" => {
                // treat this as the fallback value, thus we ignore error
                if let Ok(t) = parse_float(&value.replacen("MHz", "", 1)) {
                    c.mhz = t;
                }
            }
            "cache size" => {
                let t = parse_int(&value.replacen(" KB", "", 1))?;
                c.cache_size = t as i32;
            }
            _ => {}
        }
    }
    if cpu >= 0 {
        finish(&mut c, cpu);
        ret.push(c);
    }
    Ok(ret)
}

/// The `/proc/cpuinfo` fallback of gopsutil Linux `cpu.Counts(false)`: the
/// sum of `cpu cores` over distinct `physical id` sections.
pub(crate) fn linux_physical_cores_from_cpuinfo(cpuinfo: &str) -> i64 {
    let mut mapping = std::collections::HashMap::new();
    let mut current: std::collections::HashMap<&str, i64> = std::collections::HashMap::new();
    let lowered = cpuinfo.to_lowercase();
    for line in read_lines(&lowered) {
        let line = line.trim();
        if line.is_empty() {
            // new section
            if let (Some(id), Some(cores)) = (current.get("physical id"), current.get("cpu cores"))
            {
                mapping.insert(*id, *cores);
            }
            current.clear();
            continue;
        }
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() < 2 {
            continue;
        }
        let key = fields[0].trim();
        if key == "physical id" || key == "cpu cores" {
            let Ok(val) = parse_int(fields[1].trim()) else {
                continue;
            };
            current.insert(
                if key == "physical id" {
                    "physical id"
                } else {
                    "cpu cores"
                },
                val,
            );
        }
    }
    mapping.values().sum()
}

/// gopsutil Linux `parseStatLine` with `ClocksPerSec` = 100.
pub(crate) fn linux_parse_stat_line(line: &str) -> Result<CpuTimes, String> {
    const CLOCKS_PER_SEC: f64 = 100.0;
    let fields: Vec<&str> = line.split_whitespace().collect();
    if fields.len() < 8 {
        return Err("stat does not contain cpu info".into());
    }
    if !fields[0].starts_with("cpu") {
        return Err("not contain cpu".into());
    }
    let value =
        |index: usize| -> Result<f64, String> { Ok(parse_float(fields[index])? / CLOCKS_PER_SEC) };
    let mut ct = CpuTimes {
        user: value(1)?,
        nice: value(2)?,
        system: value(3)?,
        idle: value(4)?,
        iowait: value(5)?,
        irq: value(6)?,
        softirq: value(7)?,
        ..CpuTimes::default()
    };
    if fields.len() > 8 {
        // Linux >= 2.6.11
        ct.steal = value(8)?;
    }
    if fields.len() > 9 {
        // Linux >= 2.6.24
        ct.guest = value(9)?;
    }
    if fields.len() > 10 {
        // Linux >= 3.2.0
        ct.guest_nice = value(10)?;
    }
    Ok(ct)
}

/// The `/proc/meminfo` keys gopsutil parses as unsigned integers; any of
/// them failing to parse fails `mem.VirtualMemory`.
const MEMINFO_KEYS: &[&str] = &[
    "MemTotal",
    "MemFree",
    "MemAvailable",
    "Buffers",
    "Cached",
    "Active",
    "Inactive",
    "Active(anon)",
    "Inactive(anon)",
    "Active(file)",
    "Inactive(file)",
    "Unevictable",
    "Writeback",
    "WritebackTmp",
    "Dirty",
    "Shmem",
    "Slab",
    "SReclaimable",
    "SUnreclaim",
    "PageTables",
    "SwapCached",
    "CommitLimit",
    "Committed_AS",
    "HighTotal",
    "HighFree",
    "LowTotal",
    "LowFree",
    "SwapTotal",
    "SwapFree",
    "Mapped",
    "VmallocTotal",
    "VmallocUsed",
    "VmallocChunk",
    "HugePages_Total",
    "HugePages_Free",
    "HugePages_Rsvd",
    "HugePages_Surp",
    "Hugepagesize",
    "AnonHugePages",
];

/// gopsutil Linux `fillFromMeminfoWithContext`'s Total and Available, with
/// `calculateAvailVmem` over `zoneinfo` (None when unreadable) on kernels
/// without `MemAvailable`.
pub(crate) fn linux_virtual_memory(
    meminfo: &str,
    zoneinfo: Option<&str>,
    pagesize: u64,
) -> Result<VirtualMemory, String> {
    let (mut total, mut free, mut available, mut cached, mut sreclaimable) = (0, 0, 0, 0, 0);
    let (mut active_file_value, mut inactive_file_value) = (0u64, 0u64);
    let (mut memavail, mut active_file, mut inactive_file, mut s_reclaimable) =
        (false, false, false, false);
    for line in read_lines(meminfo) {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() != 2 {
            continue;
        }
        let key = fields[0].trim();
        if !MEMINFO_KEYS.contains(&key) {
            continue;
        }
        let value = fields[1].trim().replace(" kB", "");
        let t = parse_uint(&value)?;
        let bytes = t.wrapping_mul(1024);
        match key {
            "MemTotal" => total = bytes,
            "MemFree" => free = bytes,
            "MemAvailable" => {
                memavail = true;
                available = bytes;
            }
            "Cached" => cached = bytes,
            "Active(file)" => {
                active_file = true;
                active_file_value = bytes;
            }
            "Inactive(file)" => {
                inactive_file = true;
                inactive_file_value = bytes;
            }
            "SReclaimable" => {
                s_reclaimable = true;
                sreclaimable = bytes;
            }
            _ => {}
        }
    }
    cached = cached.wrapping_add(sreclaimable);
    if !memavail {
        available = if active_file && inactive_file && s_reclaimable {
            calculate_avail_vmem(
                zoneinfo,
                pagesize,
                free,
                cached,
                sreclaimable,
                active_file_value,
                inactive_file_value,
            )
        } else {
            cached.wrapping_add(free)
        };
    }
    Ok(VirtualMemory { total, available })
}

/// gopsutil Linux `calculateAvailVmem`.
fn calculate_avail_vmem(
    zoneinfo: Option<&str>,
    pagesize: u64,
    free: u64,
    cached: u64,
    sreclaimable: u64,
    active_file: u64,
    inactive_file: u64,
) -> u64 {
    let Some(zoneinfo) = zoneinfo else {
        return free.wrapping_add(cached); // fallback under kernel 2.6.13
    };
    let mut watermark_low: u64 = 0;
    for line in read_lines(zoneinfo) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        // Go indexes fields[0] and fields[1] unguarded; zoneinfo has no
        // blank or bare "low" lines.
        if fields.first().is_some_and(|f| f.starts_with("low")) {
            let low_value = fields.get(1).and_then(|v| parse_uint(v).ok()).unwrap_or(0);
            watermark_low = watermark_low.wrapping_add(low_value);
        }
    }
    watermark_low = watermark_low.wrapping_mul(pagesize);

    let mut avail_memory = free.wrapping_sub(watermark_low);
    let mut page_cache = active_file.wrapping_add(inactive_file);
    page_cache =
        page_cache.wrapping_sub(((page_cache / 2) as f64).min(watermark_low as f64) as u64);
    avail_memory = avail_memory.wrapping_add(page_cache);
    avail_memory = avail_memory.wrapping_add(
        sreclaimable.wrapping_sub(((sreclaimable / 2) as f64).min(watermark_low as f64) as u64),
    );
    avail_memory
}

/// Go `strconv.Unquote` of a double-quoted string, for gopsutil's
/// `unescapeFstab`: the unescaped text, or None when Go would fail.
fn unquote_double(s: &str) -> Option<String> {
    if !s.contains('\\') && !s.contains('"') && !s.contains('\n') {
        return Some(s.to_owned());
    }
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'"' || c == b'\n' {
            return None;
        }
        if c != b'\\' {
            out.push(c);
            i += 1;
            continue;
        }
        let e = *bytes.get(i + 1)?;
        i += 2;
        match e {
            b'a' => out.push(7),
            b'b' => out.push(8),
            b'f' => out.push(12),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'v' => out.push(11),
            b'\\' => out.push(b'\\'),
            b'"' => out.push(b'"'),
            b'x' | b'u' | b'U' => {
                let n = match e {
                    b'x' => 2,
                    b'u' => 4,
                    _ => 8,
                };
                let hex = std::str::from_utf8(bytes.get(i..i + n)?).ok()?;
                if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return None;
                }
                let v = u32::from_str_radix(hex, 16).ok()?;
                i += n;
                if e == b'x' {
                    out.push(v as u8);
                } else {
                    let ch = char::from_u32(v)?;
                    let mut buf = [0u8; 4];
                    out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                }
            }
            b'0'..=b'7' => {
                let digits = bytes.get(i - 1..i + 2)?;
                if !digits.iter().all(|b| (b'0'..=b'7').contains(b)) {
                    return None;
                }
                let v = digits
                    .iter()
                    .fold(0u32, |acc, b| acc * 8 + u32::from(b - b'0'));
                if v > 255 {
                    return None;
                }
                out.push(v as u8);
                i += 2;
            }
            _ => return None,
        }
    }
    Some(String::from_utf8_lossy(&out).into_owned())
}

/// gopsutil `unescapeFstab`.
pub(crate) fn unescape_fstab(path: &str) -> String {
    unquote_double(path).unwrap_or_else(|| path.to_owned())
}

/// gopsutil Linux `disk.Partitions(true)` over `mounts` (`use_mounts`) or
/// `mountinfo` lines. `resolve` applies the `/dev/mapper/` and `/dev/root`
/// device rewrites, given the mountinfo block device id.
pub(crate) fn linux_partitions(
    lines: &[&str],
    use_mounts: bool,
    filename: &str,
    resolve: &dyn Fn(&mut Partition, &str),
) -> Result<Vec<Partition>, String> {
    let mut ret = Vec::with_capacity(lines.len());
    for line in lines {
        let d = if use_mounts {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 4 {
                return Err(format!(
                    "found invalid mounts line in file {filename}: {line}"
                ));
            }
            Partition {
                device: fields[0].to_owned(),
                mountpoint: unescape_fstab(fields[1]),
                fstype: fields[2].to_owned(),
                opts: fields[3].split_whitespace().map(str::to_owned).collect(),
            }
        } else {
            let parts: Vec<&str> = line.split(" - ").collect();
            if parts.len() != 2 {
                return Err(format!(
                    "found invalid mountinfo line in file {filename}: {line} "
                ));
            }
            let fields: Vec<&str> = parts[0].split_whitespace().collect();
            let tail: Vec<&str> = parts[1].split_whitespace().collect();
            if fields.len() < 6 || tail.len() < 2 {
                return Err(format!(
                    "found invalid mountinfo line in file {filename}: {line} "
                ));
            }
            let block_device_id = fields[2];
            let mount_point = fields[4];
            let mut mount_opts: Vec<String> = fields[5].split(',').map(str::to_owned).collect();
            let root_dir = fields[3];
            if !root_dir.is_empty() && root_dir != "/" {
                mount_opts.push("bind".to_owned());
            }
            let mut d = Partition {
                device: tail[1].to_owned(),
                mountpoint: unescape_fstab(mount_point),
                fstype: tail[0].to_owned(),
                opts: mount_opts,
            };
            resolve(&mut d, block_device_id);
            d
        };
        ret.push(d);
    }
    Ok(ret)
}

/// gopsutil Linux `disk.IOCounters()` over `/proc/diskstats`, 512-byte
/// sectors.
pub(crate) fn linux_disk_io_counters(diskstats: &str) -> Result<Vec<DiskIoCounters>, String> {
    const SECTOR_SIZE: u64 = 512;
    let mut ret: Vec<DiskIoCounters> = Vec::new();
    for line in read_lines(diskstats) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 14 {
            continue;
        }
        let name = fields[2];
        let mut values = [0u64; 11];
        for (slot, field) in values.iter_mut().zip(&fields[3..14]) {
            *slot = parse_uint(field)?;
        }
        let [reads, merged_reads, rbytes, rtime, writes, merged_writes, wbytes, wtime, iops, iotime, weighted] =
            values;
        if reads == 0
            && merged_reads == 0
            && rbytes == 0
            && rtime == 0
            && writes == 0
            && merged_writes == 0
            && wbytes == 0
            && wtime == 0
            && iops == 0
            && iotime == 0
            && weighted == 0
        {
            continue;
        }
        let d = DiskIoCounters {
            name: name.to_owned(),
            read_count: reads,
            merged_read_count: merged_reads,
            write_count: writes,
            merged_write_count: merged_writes,
            read_bytes: rbytes.wrapping_mul(SECTOR_SIZE),
            write_bytes: wbytes.wrapping_mul(SECTOR_SIZE),
        };
        // Go's map keeps the last line for a repeated name.
        match ret.iter_mut().find(|existing| existing.name == d.name) {
            Some(existing) => *existing = d,
            None => ret.push(d),
        }
    }
    Ok(ret)
}

/// gopsutil Linux `net.IOCounters(true)` over `/proc/net/dev`.
pub(crate) fn linux_net_io_counters(netdev: &str) -> Result<Vec<NetIoCounters>, String> {
    let lines = read_lines(netdev);
    if lines.len() < 2 {
        return Err("short /proc/net/dev".into());
    }
    let mut ret = Vec::with_capacity(lines.len().saturating_sub(1));
    for line in &lines[2..] {
        let Some(separator_pos) = line.rfind(':') else {
            continue;
        };
        let interface_name = line[..separator_pos].trim();
        if interface_name.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line[separator_pos + 1..].split_whitespace().collect();
        if fields.len() < 13 {
            return Err(format!("short /proc/net/dev line {line:?}"));
        }
        ret.push(NetIoCounters {
            name: interface_name.to_owned(),
            bytes_recv: parse_uint(fields[0])?,
            packets_recv: parse_uint(fields[1])?,
            errin: parse_uint(fields[2])?,
            dropin: parse_uint(fields[3])?,
            fifoin: parse_uint(fields[4])?,
            bytes_sent: parse_uint(fields[8])?,
            packets_sent: parse_uint(fields[9])?,
            errout: parse_uint(fields[10])?,
            dropout: parse_uint(fields[11])?,
            fifoout: parse_uint(fields[12])?,
        });
    }
    Ok(ret)
}

/// gopsutil Linux `fileAvgWithContext` over `/proc/loadavg`.
pub(crate) fn linux_load_avg(loadavg: &str) -> Result<LoadAvg, String> {
    let values: Vec<&str> = loadavg.split_whitespace().collect();
    if values.len() < 3 {
        return Err("short /proc/loadavg".into());
    }
    Ok(LoadAvg {
        load1: parse_float(values[0])?,
        load5: parse_float(values[1])?,
        load15: parse_float(values[2])?,
    })
}

/// gopsutil Darwin `netstatInterface`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NetstatInterface {
    pub(crate) link_id: Option<u64>,
    pub(crate) stat: NetIoCounters,
}

/// gopsutil Darwin `parseNetstatLine`.
fn parse_netstat_line(line: &str) -> Result<NetstatInterface, String> {
    let columns: Vec<&str> = line.split_whitespace().collect();
    if columns.first() == Some(&"Name") {
        return Err("Can't parse header of netstat output".into());
    }
    let mut link_id = None;
    if let Some(link) = columns
        .get(2)
        .ok_or_else(|| {
            format!(
                "Line {line:?} do have an invalid number of columns {}",
                columns.len()
            )
        })?
        .strip_prefix("<Link#")
        .and_then(|rest| rest.strip_suffix('>'))
        .filter(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
    {
        link_id = Some(parse_uint(link)?);
    }
    let number_columns = columns.len();
    let base = if number_columns < 12 { 0 } else { 1 };
    if !(11..=13).contains(&number_columns) {
        return Err(format!(
            "Line {line:?} do have an invalid number of columns {number_columns}"
        ));
    }
    let mut vv = vec![
        columns[base + 3], // Ipkts == PacketsRecv
        columns[base + 4], // Ierrs == Errin
        columns[base + 5], // Ibytes == BytesRecv
        columns[base + 6], // Opkts == PacketsSent
        columns[base + 7], // Oerrs == Errout
        columns[base + 8], // Obytes == BytesSent
    ];
    if columns.len() == 12 {
        vv.push(columns[base + 10]);
    }
    let mut parsed = Vec::with_capacity(7);
    for target in vv {
        if target == "-" {
            parsed.push(0);
            continue;
        }
        parsed.push(parse_uint(target)?);
    }
    let mut stat = NetIoCounters {
        // remove the * that sometimes is on right on interface
        name: columns[0].trim_matches('*').to_owned(),
        packets_recv: parsed[0],
        errin: parsed[1],
        bytes_recv: parsed[2],
        packets_sent: parsed[3],
        errout: parsed[4],
        bytes_sent: parsed[5],
        ..NetIoCounters::default()
    };
    if parsed.len() == 7 {
        stat.dropout = parsed[6];
    }
    Ok(NetstatInterface { link_id, stat })
}

/// gopsutil Darwin `parseNetstatOutput`: every line after the header.
pub(crate) fn parse_netstat_output(output: &str) -> Result<Vec<NetstatInterface>, String> {
    let lines: Vec<&str> = output.trim_matches('\n').split('\n').collect();
    lines[1..]
        .iter()
        .map(|line| parse_netstat_line(line))
        .collect()
}

/// Runs a command with arguments, answering its combined output.
pub(crate) type CommandRunner<'a> = &'a dyn Fn(&str, &[&str]) -> Result<String, String>;

/// gopsutil Darwin `net.IOCounters(true)` given `netstat -ibdnW`'s output
/// and a runner for its truncated-name fallback (`ifconfig -l`, then
/// `netstat -ibdnWI<name>`).
pub(crate) fn darwin_net_io_counters(
    netstat_output: &str,
    run: CommandRunner<'_>,
) -> Result<Vec<NetIoCounters>, String> {
    let ns_interfaces = parse_netstat_output(netstat_output)?;
    // Go `newMapInterfaceNameUsage`.
    let mut usage: Vec<(&str, u32)> = Vec::new();
    for iface in &ns_interfaces {
        if iface.link_id.is_some() {
            match usage.iter_mut().find(|(name, _)| *name == iface.stat.name) {
                Some((_, count)) => *count += 1,
                None => usage.push((&iface.stat.name, 1)),
            }
        }
    }
    let not_truncated = usage.iter().filter(|(_, count)| *count == 1).count();
    let is_truncated = usage.iter().any(|(_, count)| *count > 1);
    let mut ret = vec![NetIoCounters::default(); not_truncated];
    let mut ret_index = 0;
    // Go assigns `ret[retIndex]` into a slice sized by the untruncated
    // names; an index past it would panic there and is an error here.
    let mut assign = |ret: &mut Vec<NetIoCounters>, stat: &NetIoCounters| -> Result<(), String> {
        let slot = ret
            .get_mut(ret_index)
            .ok_or_else(|| "index out of range".to_owned())?;
        *slot = stat.clone();
        ret_index += 1;
        Ok(())
    };
    if !is_truncated {
        for iface in &ns_interfaces {
            if iface.link_id.is_some() {
                assign(&mut ret, &iface.stat)?;
            }
        }
    } else {
        let out = run("ifconfig", &["-l"])?;
        for interface_name in out.trim_end_matches('\n').split_whitespace() {
            let mut truncated = true;
            for iface in &ns_interfaces {
                if iface.link_id.is_some() && iface.stat.name == interface_name {
                    assign(&mut ret, &iface.stat)?;
                    truncated = false;
                    break;
                }
            }
            if truncated {
                let out = run("netstat", &[&format!("-ibdnWI{interface_name}")])?;
                let parsed_ifaces = parse_netstat_output(&out)?;
                if parsed_ifaces.is_empty() {
                    continue;
                }
                if let Some(iface) = parsed_ifaces.iter().find(|iface| iface.link_id.is_some()) {
                    ret.push(iface.stat.clone());
                }
            }
        }
    }
    Ok(ret)
}

/// One Darwin `getifaddrs` entry: its raw socket addresses, `sa_len` bytes
/// each, as the routing messages Go's `net.Interfaces` parses carry them.
#[derive(Clone, Debug, Default)]
pub(crate) struct IfaddrsEntry {
    pub(crate) name: String,
    pub(crate) flags: u32,
    pub(crate) addr: Option<Vec<u8>>,
    pub(crate) netmask: Option<Vec<u8>>,
}

const DARWIN_AF_INET: u8 = 2;
const DARWIN_AF_LINK: u8 = 18;
const DARWIN_AF_INET6: u8 = 30;

/// routebsd `parseKernelLinkAddr` behind `parseLinkAddr`: the hardware
/// address bytes of a `sockaddr_dl`, or None when malformed.
fn darwin_link_addr(b: &[u8]) -> Option<Vec<u8>> {
    if b.len() < 8 {
        return None;
    }
    let b = &b[4..];
    let dont_care = |n: u8| if n == 0xff { 0 } else { usize::from(n) };
    let (nlen, alen, slen) = (dont_care(b[1]), dont_care(b[2]), dont_care(b[3]));
    if b.len() < 4 + nlen + alen + slen {
        return None;
    }
    Some(b[4 + nlen..4 + nlen + alen].to_vec())
}

/// A routebsd address as `interfaceAddrTable` uses it: IPv4 or IPv6
/// bytes; anything else is neither.
enum RouteAddr {
    Inet4([u8; 4]),
    Inet6([u8; 16]),
    Other,
}

/// routebsd's KAME rule: an interface- or link-local address carries
/// its zone in bytes 2..4 in kernel form, which the parser clears.
fn kame_clear(mut ip: [u8; 16]) -> [u8; 16] {
    if ip[0] == 0xfe && ip[1] & 0xc0 == 0x80
        || ip[0] == 0xff && (ip[1] & 0x0f == 0x01 || ip[1] & 0x0f == 0x02)
    {
        let id = u16::from_be_bytes([ip[2], ip[3]]);
        if id != 0 {
            ip[2] = 0;
            ip[3] = 0;
        }
    }
    ip
}

/// Go's `internal/routebsd` (go1.27) `parseAddrs` for one address of an
/// interface-address message, with `af` still `AF_UNSPEC` (no destination
/// precedes the netmask). Err is `errInvalidAddr`, which fails the fetch.
fn darwin_route_addr(b: &[u8]) -> Result<RouteAddr, ()> {
    let byte = |i: usize| b.get(i).copied().unwrap_or(0);
    // `parseInetAddr`: up to `sa_len` bytes of the address; a short
    // socket address leaves the rest zero.
    let inet = |off: usize, len: usize| -> Result<Vec<u8>, ()> {
        let sa_len = usize::from(byte(0));
        if b.len() < sa_len {
            return Err(());
        }
        let mut ip = vec![0u8; len];
        if sa_len != 0 {
            let n = (off + len).min(sa_len);
            for (k, slot) in ip.iter_mut().enumerate().take(n.saturating_sub(off)) {
                *slot = byte(off + k);
            }
        }
        Ok(ip)
    };
    match byte(1) {
        DARWIN_AF_LINK => {
            if b.len() < 8 {
                return Err(());
            }
            Ok(RouteAddr::Other)
        }
        DARWIN_AF_INET => {
            let ip = inet(4, 4)?;
            Ok(RouteAddr::Inet4([ip[0], ip[1], ip[2], ip[3]]))
        }
        DARWIN_AF_INET6 => {
            let mut ip = [0u8; 16];
            ip.copy_from_slice(&inet(8, 16)?);
            Ok(RouteAddr::Inet6(kame_clear(ip)))
        }
        _ => {
            // parseKernelInetAddr: the length counts bytes, rounded up to
            // the routing message alignment; the padding reads as zero.
            let roundup = |l: usize| if l == 0 { 4 } else { (l + 3) & !3 };
            let l = roundup(usize::from(byte(0)));
            let padded = |i: usize| byte(i);
            if byte(0) == 28 {
                let mut ip = [0u8; 16];
                for (k, slot) in ip.iter_mut().enumerate() {
                    *slot = padded(8 + k);
                }
                return Ok(RouteAddr::Inet6(ip));
            }
            if byte(0) == 16 {
                return Ok(RouteAddr::Inet4([
                    padded(4),
                    padded(5),
                    padded(6),
                    padded(7),
                ]));
            }
            let mut ip = [0u8; 4];
            if l - 1 < 4 {
                for (k, slot) in ip.iter_mut().enumerate().take(l - 1) {
                    *slot = padded(1 + k);
                }
            } else {
                for (k, slot) in ip.iter_mut().enumerate() {
                    *slot = padded(l - 4 + k);
                }
            }
            Ok(RouteAddr::Inet4(ip))
        }
    }
}

/// Go `net.Interfaces` plus each `Interface.Addrs`, on Darwin, over the
/// `getifaddrs` entries (one `AF_LINK` entry per interface, in routing
/// table order, then its addresses). Flags are `linkFlags`.
pub(crate) fn darwin_interfaces(entries: &[IfaddrsEntry]) -> Vec<super::NetInterface> {
    const IFF_UP: u32 = 0x1;
    const IFF_BROADCAST: u32 = 0x2;
    const IFF_LOOPBACK: u32 = 0x8;
    const IFF_POINTOPOINT: u32 = 0x10;
    const IFF_MULTICAST: u32 = 0x8000;
    let mut nics = Vec::new();
    for entry in entries {
        let Some(addr) = entry.addr.as_deref() else {
            continue;
        };
        if addr.get(1) != Some(&DARWIN_AF_LINK) {
            continue;
        }
        let hw = darwin_link_addr(addr).unwrap_or_default();
        // Go `interfaceAddrTable`: an address needs both an IP and a mask;
        // an unparsable address message loses the interface's addresses.
        let mut addrs = Some(Vec::new());
        for other in entries.iter().filter(|other| other.name == entry.name) {
            let Some(ifa) = other.addr.as_deref() else {
                continue;
            };
            if ifa.get(1) == Some(&DARWIN_AF_LINK) {
                continue;
            }
            let mask = match other.netmask.as_deref().map(darwin_route_addr) {
                None => None,
                Some(Err(())) => {
                    addrs = None;
                    break;
                }
                Some(Ok(RouteAddr::Inet4(mask))) => Some(mask.to_vec()),
                Some(Ok(RouteAddr::Inet6(mask))) => Some(mask.to_vec()),
                Some(Ok(RouteAddr::Other)) => None,
            };
            let ip = match darwin_route_addr(ifa) {
                Err(()) => {
                    addrs = None;
                    break;
                }
                Ok(RouteAddr::Inet4(ip)) => Some(std::net::IpAddr::from(ip)),
                Ok(RouteAddr::Inet6(ip)) => Some(std::net::IpAddr::from(ip)),
                Ok(RouteAddr::Other) => None,
            };
            if let (Some(ip), Some(mask), Some(list)) = (ip, mask, addrs.as_mut()) {
                list.push(super::ip_net_string(ip, &mask));
            }
        }
        let flags = entry.flags;
        nics.push(super::NetInterface {
            name: entry.name.clone(),
            hardware_addr: super::hardware_addr_string(&hw),
            flags: super::interface_flag_names(
                flags & IFF_UP != 0,
                flags & IFF_BROADCAST != 0,
                flags & IFF_LOOPBACK != 0,
                flags & IFF_POINTOPOINT != 0,
                flags & IFF_MULTICAST != 0,
            ),
            addrs: addrs.unwrap_or_default(),
        });
    }
    nics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_lines_keeps_a_final_unterminated_line() {
        assert_eq!(read_lines("a\nb\n"), vec!["a", "b"]);
        assert_eq!(read_lines("a\nb"), vec!["a", "b"]);
        assert!(read_lines("").is_empty());
    }

    #[test]
    fn go_integer_parsing() {
        assert!(parse_uint("+1").is_err());
        assert_eq!(parse_uint("18446744073709551615"), Ok(u64::MAX));
        assert!(parse_uint("18446744073709551616").is_err());
        assert_eq!(parse_int("+7"), Ok(7));
        assert_eq!(parse_int("-7"), Ok(-7));
        assert!(parse_int("7a").is_err());
    }

    #[test]
    fn cpuinfo_takes_cache_and_max_frequency() {
        let cpuinfo = "processor\t: 0\nvendor_id\t: GenuineIntel\ncpu MHz\t\t: 2400.000\n\
                       cache size\t: 36608 KB\n\nprocessor\t: 1\ncpu MHz\t\t: 2300.5\n\
                       cache size\t: 36608 KB\n";
        let infos = linux_cpu_info(cpuinfo, &|_| None).unwrap();
        assert_eq!(infos.len(), 2);
        assert_eq!(infos[0].mhz, 2400.0);
        assert_eq!(infos[0].cache_size, 36608);
        assert_eq!(infos[0].cores, 1);
        let infos =
            linux_cpu_info(cpuinfo, &|cpu| (cpu == 0).then(|| "3500000".to_owned())).unwrap();
        assert_eq!(infos[0].mhz, 3500.0);
        assert_eq!(infos[1].mhz, 2300.5);
        assert!(linux_cpu_info("processor : x\n", &|_| None).is_err());
        assert!(linux_cpu_info("processor : 0\ncache size : big\n", &|_| None).is_err());
        assert!(linux_cpu_info("", &|_| None).unwrap().is_empty());
    }

    #[test]
    fn physical_cores_sum_distinct_packages() {
        let cpuinfo = "processor : 0\nphysical id : 0\ncpu cores : 4\n\nprocessor : 1\n\
                       physical id : 0\ncpu cores : 4\n\nprocessor : 2\nphysical id : 1\n\
                       cpu cores : 4\n\n";
        assert_eq!(linux_physical_cores_from_cpuinfo(cpuinfo), 8);
    }

    #[test]
    fn stat_line_divides_by_clock_ticks() {
        let t = linux_parse_stat_line("cpu  100 200 300 400 500 600 700 800 900 1000").unwrap();
        assert_eq!(t.user, 1.0);
        assert_eq!(t.nice, 2.0);
        assert_eq!(t.system, 3.0);
        assert_eq!(t.idle, 4.0);
        assert_eq!(t.guest_nice, 10.0);
        assert_eq!(t.total(), 55.0);
        assert!(linux_parse_stat_line("cpu 1 2 3").is_err());
        assert!(linux_parse_stat_line("intr 1 2 3 4 5 6 7 8").is_err());
    }

    #[test]
    fn meminfo_prefers_mem_available() {
        let meminfo = "MemTotal:       16000 kB\nMemFree:         1000 kB\n\
                       MemAvailable:    8000 kB\nCached:          2000 kB\n";
        let vm = linux_virtual_memory(meminfo, None, 4096).unwrap();
        assert_eq!(vm.total, 16000 * 1024);
        assert_eq!(vm.available, 8000 * 1024);
        // Without MemAvailable or the file counters: Cached + SReclaimable + Free.
        let meminfo =
            "MemTotal: 16000 kB\nMemFree: 1000 kB\nCached: 2000 kB\nSReclaimable: 10 kB\n";
        let vm = linux_virtual_memory(meminfo, None, 4096).unwrap();
        assert_eq!(vm.available, (2000 + 10 + 1000) * 1024);
        assert!(linux_virtual_memory("MemTotal: x kB\n", None, 4096).is_err());
        // Unknown keys are not parsed.
        assert!(linux_virtual_memory("Foo: x kB\n", None, 4096).is_ok());
    }

    #[test]
    fn meminfo_estimates_available_from_zoneinfo() {
        let meminfo = "MemTotal: 16000 kB\nMemFree: 1000 kB\nCached: 2000 kB\n\
                       Active(file): 400 kB\nInactive(file): 600 kB\nSReclaimable: 100 kB\n";
        let zoneinfo = "Node 0, zone DMA\n  pages free 1\n        low      2\n        high 3\n";
        let vm = linux_virtual_memory(meminfo, Some(zoneinfo), 1024).unwrap();
        let low = 2 * 1024u64;
        let page_cache = 1000 * 1024 - (500 * 1024u64).min(low);
        let expected = 1000 * 1024 - low + page_cache + (100 * 1024 - (50 * 1024u64).min(low));
        assert_eq!(vm.available, expected);
    }

    #[test]
    fn unescape_fstab_follows_go_unquote() {
        assert_eq!(unescape_fstab(r"/mnt/a\040b"), "/mnt/a b");
        assert_eq!(unescape_fstab("/plain"), "/plain");
        assert_eq!(unescape_fstab(r"/bad\q"), r"/bad\q");
        assert_eq!(unescape_fstab(r"/x\x41"), "/xA");
    }

    #[test]
    fn mountinfo_and_mounts_partitions() {
        let mountinfo = [
            "22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw,errors=remount-ro",
            "23 22 8:1 /var/lib /mnt/bind rw shared:1 - ext4 /dev/sda1 rw",
        ];
        let parts = linux_partitions(&mountinfo, false, "mountinfo", &|_, _| {}).unwrap();
        assert_eq!(parts[0].device, "/dev/sda1");
        assert_eq!(parts[0].mountpoint, "/");
        assert_eq!(parts[0].fstype, "ext4");
        assert_eq!(parts[0].opts, vec!["rw", "relatime"]);
        assert_eq!(parts[1].opts, vec!["rw", "bind"]);
        assert!(linux_partitions(&["bad line"], false, "mountinfo", &|_, _| {}).is_err());
        let parts = linux_partitions(
            &["/dev/sdb1 /data\\040x xfs rw,noatime 0 0"],
            true,
            "mounts",
            &|_, _| {},
        )
        .unwrap();
        assert_eq!(parts[0].mountpoint, "/data x");
        assert_eq!(parts[0].opts, vec!["rw,noatime"]);
    }

    #[test]
    fn diskstats_skip_idle_devices() {
        let diskstats = "   8       0 sda 10 2 30 4 5 6 70 8 0 9 10\n\
                         7       0 loop0 0 0 0 0 0 0 0 0 0 0 0\n";
        let counters = linux_disk_io_counters(diskstats).unwrap();
        assert_eq!(counters.len(), 1);
        assert_eq!(counters[0].name, "sda");
        assert_eq!(counters[0].read_bytes, 30 * 512);
        assert_eq!(counters[0].write_bytes, 70 * 512);
        assert_eq!(counters[0].merged_write_count, 6);
    }

    #[test]
    fn net_dev_counters() {
        let netdev = "Inter-|   Receive                                                |  Transmit\n \
                      face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n    \
                      lo: 100 2 0 0 0 0 0 0 100 2 0 0 0 0 0 0\n  \
                      eth0: 5 6 7 8 9 0 0 0 10 11 12 13 14 0 0 0\n";
        let counters = linux_net_io_counters(netdev).unwrap();
        assert_eq!(counters.len(), 2);
        assert_eq!(counters[1].name, "eth0");
        assert_eq!(counters[1].bytes_recv, 5);
        assert_eq!(counters[1].fifoin, 9);
        assert_eq!(counters[1].bytes_sent, 10);
        assert_eq!(counters[1].fifoout, 14);
    }

    #[test]
    fn loadavg_fields() {
        let load = linux_load_avg("0.52 0.58 0.59 1/389 12345\n").unwrap();
        assert_eq!((load.load1, load.load5, load.load15), (0.52, 0.58, 0.59));
    }

    /// gopsutil's `TestParseNetstatLineHeader` and friends
    /// (`net_darwin_test.go`).
    #[test]
    fn netstat_lines() {
        assert!(parse_netstat_line(
            "Name  Mtu   Network       Address            Ipkts Ierrs     Ibytes    Opkts Oerrs     Obytes  Coll Drop"
        )
        .is_err());
        let line = parse_netstat_line(
            "lo0   16384 <Link#1>                        869107     0  169411755   869107     0  169411755     0   0",
        )
        .unwrap();
        assert_eq!(line.link_id, Some(1));
        assert_eq!(line.stat.name, "lo0");
        assert_eq!(line.stat.packets_recv, 869107);
        assert_eq!(line.stat.bytes_recv, 169411755);
        let line = parse_netstat_line(
            "en0   1500  <Link#4>    f4:5c:89:a0:dd:4e  10   0  20  30   0  40     0   5",
        )
        .unwrap();
        assert_eq!(line.link_id, Some(4));
        assert_eq!(line.stat.dropout, 5);
        assert_eq!(line.stat.bytes_sent, 40);
        let line = parse_netstat_line(
            "lo0   16384 127           127.0.0.1          869107     -  169411755   869107     -  169411755     -   -",
        )
        .unwrap();
        assert_eq!(line.link_id, None);
        assert_eq!(line.stat.errin, 0);
    }

    #[test]
    fn netstat_output_keeps_link_rows() {
        let output = "Name       Mtu   Network       Address            Ipkts Ierrs     Ibytes    Opkts Oerrs     Obytes  Coll Drop\n\
                      lo0        16384 <Link#1>                      1     0 2 3     0 4     0   0\n\
                      lo0        16384 127           127.0.0.1       1     - 2 3     - 4     -   -\n\
                      gif0*      1280  <Link#2>                             0     0          0        0     0          0     0   0\n";
        let counters = darwin_net_io_counters(output, &|_, _| Err("unexpected".into())).unwrap();
        assert_eq!(counters.len(), 2);
        assert_eq!(counters[0].name, "lo0");
        assert_eq!(counters[1].name, "gif0");
    }

    /// Entries captured from a macOS `getifaddrs`: a link entry, a KAME
    /// link-local address, and an IPv4 address whose netmask socket address
    /// is truncated to seven bytes.
    #[test]
    fn darwin_interfaces_follow_go_net_interfaces() {
        let entry = |addr: Vec<u8>, netmask: Option<Vec<u8>>| IfaddrsEntry {
            name: "en0".to_owned(),
            flags: 34915,
            addr: Some(addr),
            netmask,
        };
        let v6_mask = vec![
            28, 30, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
        ];
        let entries = vec![
            entry(
                vec![
                    20, 18, 14, 0, 6, 3, 6, 0, 101, 110, 48, 246, 197, 106, 224, 66, 11, 0, 0, 0,
                ],
                None,
            ),
            entry(
                vec![
                    28, 30, 0, 0, 0, 0, 0, 0, 254, 128, 0, 14, 0, 0, 0, 0, 20, 17, 33, 113, 54, 33,
                    127, 91, 14, 0, 0, 0,
                ],
                Some(v6_mask),
            ),
            entry(
                vec![16, 2, 0, 0, 10, 0, 0, 148, 0, 0, 0, 0, 0, 0, 0, 0],
                Some(vec![7, 2, 0, 0, 255, 255, 255]),
            ),
            // An address without a netmask is not reported.
            entry(
                vec![16, 2, 0, 0, 10, 0, 0, 149, 0, 0, 0, 0, 0, 0, 0, 0],
                None,
            ),
        ];
        let nics = darwin_interfaces(&entries);
        assert_eq!(nics.len(), 1);
        assert_eq!(nics[0].name, "en0");
        assert_eq!(nics[0].hardware_addr, "f6:c5:6a:e0:42:0b");
        assert_eq!(nics[0].flags, vec!["up", "broadcast", "multicast"]);
        assert_eq!(
            nics[0].addrs,
            vec!["fe80::1411:2171:3621:7f5b/64", "10.0.0.148/24"]
        );
        // The loopback link has no hardware address.
        let lo = IfaddrsEntry {
            name: "lo0".to_owned(),
            flags: 0x8049,
            addr: Some(vec![
                20, 18, 1, 0, 24, 3, 0, 0, 108, 111, 48, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ]),
            netmask: None,
        };
        let nics = darwin_interfaces(&[lo]);
        assert_eq!(nics[0].hardware_addr, "");
        assert_eq!(nics[0].flags, vec!["up", "loopback", "multicast"]);
    }

    #[test]
    fn ip_net_strings_follow_go() {
        use std::net::IpAddr;
        let v4: IpAddr = "10.0.0.1".parse().unwrap();
        let v6: IpAddr = "fe80::1".parse().unwrap();
        assert_eq!(
            super::super::ip_net_string(v4, &[255, 255, 255, 0]),
            "10.0.0.1/24"
        );
        assert_eq!(
            super::super::ip_net_string(v4, &[255, 0, 255, 0]),
            "10.0.0.1/ff00ff00"
        );
        assert_eq!(
            super::super::ip_net_string(v6, &[255, 255, 255, 0]),
            "<nil>"
        );
        let mapped: IpAddr = "::ffff:1.2.3.4".parse().unwrap();
        let mut mask = [255u8; 16];
        mask[15] = 0;
        assert_eq!(super::super::ip_net_string(mapped, &mask), "1.2.3.4/24");
    }

    #[test]
    fn disk_usage_percent_ignores_reserved_blocks() {
        let usage = disk_usage(4096, 100, 40, 30);
        assert_eq!(usage.total, 409_600);
        assert_eq!(usage.free, 122_880);
        assert_eq!(usage.used, 245_760);
        assert_eq!(usage.used_percent, 60.0 / 90.0 * 100.0);
        assert_eq!(disk_usage(4096, 0, 0, 0).used_percent, 0.0);
    }
}
