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

//! gopsutil v3.24.5's Darwin cgo readers: `sysctl`, Mach `host_statistics`,
//! `getfsstat`, IOKit (the vendored `iostat_darwin.c` and `go-m1cpu`
//! v0.2.1), and `getifaddrs` for Go's `net.Interfaces`. Every FFI call is
//! confined to this module.

#![allow(unsafe_code)]

use std::ffi::{c_char, c_void, CStr, CString};
use std::sync::OnceLock;

use super::{
    parse, CpuInfo, CpuTimes, DiskIoCounters, LoadAvg, NetInterface, NetIoCounters, Partition,
    SwapMemory, VirtualMemory,
};

/// x/sys `unix.SysctlRaw`: the value's bytes.
fn sysctl_raw(name: &str) -> Result<Vec<u8>, String> {
    let cname = CString::new(name).map_err(|error| error.to_string())?;
    let mut len: libc::size_t = 0;
    // SAFETY: a NULL buffer asks only for the length, written to `len`.
    let rc = unsafe {
        libc::sysctlbyname(
            cname.as_ptr(),
            std::ptr::null_mut(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    let mut buf = vec![0u8; len];
    // SAFETY: `buf` holds `len` writable bytes, and `len` is updated to the
    // number written.
    let rc = unsafe {
        libc::sysctlbyname(
            cname.as_ptr(),
            buf.as_mut_ptr().cast::<c_void>(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    buf.truncate(len);
    Ok(buf)
}

/// x/sys `unix.SysctlUint32`/`SysctlUint64`: exactly `N` bytes or EIO.
fn sysctl_fixed<const N: usize>(name: &str) -> Result<[u8; N], String> {
    let cname = CString::new(name).map_err(|error| error.to_string())?;
    let mut buf = [0u8; N];
    let mut len: libc::size_t = N;
    // SAFETY: `buf` holds `N` writable bytes and `len` says so.
    let rc = unsafe {
        libc::sysctlbyname(
            cname.as_ptr(),
            buf.as_mut_ptr().cast::<c_void>(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    if len != N {
        return Err("input/output error".into());
    }
    Ok(buf)
}

fn sysctl_uint32(name: &str) -> Result<u32, String> {
    sysctl_fixed::<4>(name).map(u32::from_ne_bytes)
}

fn sysctl_uint64(name: &str) -> Result<u64, String> {
    sysctl_fixed::<8>(name).map(u64::from_ne_bytes)
}

pub(super) fn cpu_info() -> Result<Vec<CpuInfo>, String> {
    let cores = sysctl_uint32("machdep.cpu.core_count").unwrap_or(0);
    let cache_size = sysctl_uint32("machdep.cpu.cache.size").unwrap_or(0);
    let mhz = if cfg!(target_arch = "aarch64") {
        // go-m1cpu `PCoreHz` / 1_000_000 in integer arithmetic.
        (m1cpu_pcore_hz() / 1_000_000) as f64
    } else {
        // Use the rated frequency of the CPU. This is a static value and does not
        // account for low power or Turbo Boost modes.
        sysctl_uint64("hw.cpufrequency").map_or(0.0, |hz| hz as f64 / 1_000_000.0)
    };
    Ok(vec![CpuInfo {
        mhz,
        cache_size: cache_size as i32,
        cores: cores as i32,
    }])
}

pub(super) fn cpu_counts(logical: bool) -> Result<i64, String> {
    let name = if logical {
        "hw.logicalcpu"
    } else {
        "hw.physicalcpu"
    };
    sysctl_uint32(name).map(i64::from)
}

#[allow(deprecated)]
fn mach_host_self() -> libc::mach_port_t {
    // SAFETY: returns the host port send right; no arguments.
    unsafe { libc::mach_host_self() }
}

pub(super) fn cpu_times() -> Result<Vec<CpuTimes>, String> {
    // go-sysconf's Darwin `SC_CLK_TCK`.
    const CLOCKS_PER_SEC: f64 = 100.0;
    let mut cpuload = libc::host_cpu_load_info { cpu_ticks: [0; 4] };
    let mut count = libc::HOST_CPU_LOAD_INFO_COUNT;
    // SAFETY: `cpuload` is a host_cpu_load_info sized by `count` integers.
    let status = unsafe {
        libc::host_statistics(
            mach_host_self(),
            libc::HOST_CPU_LOAD_INFO,
            (&mut cpuload as *mut libc::host_cpu_load_info).cast::<libc::integer_t>(),
            &mut count,
        )
    };
    if status != libc::KERN_SUCCESS {
        return Err(format!("host_statistics error={status}"));
    }
    let ticks = |state: libc::c_int| f64::from(cpuload.cpu_ticks[state as usize]) / CLOCKS_PER_SEC;
    Ok(vec![CpuTimes {
        user: ticks(libc::CPU_STATE_USER),
        system: ticks(libc::CPU_STATE_SYSTEM),
        nice: ticks(libc::CPU_STATE_NICE),
        idle: ticks(libc::CPU_STATE_IDLE),
        ..CpuTimes::default()
    }])
}

extern "C" {
    /// The kernel page size `<mach/vm_page_size.h>` exports.
    static vm_kernel_page_size: libc::vm_size_t;
}

pub(super) fn virtual_memory() -> Result<VirtualMemory, String> {
    // SAFETY: an all-zero vm_statistics is a valid out-parameter.
    let mut vmstat: libc::vm_statistics = unsafe { std::mem::zeroed() };
    let mut count = (std::mem::size_of::<libc::vm_statistics>()
        / std::mem::size_of::<libc::integer_t>())
        as libc::mach_msg_type_number_t;
    // SAFETY: `vmstat` is HOST_VM_INFO's output struct sized by `count`.
    let status = unsafe {
        libc::host_statistics(
            mach_host_self(),
            libc::HOST_VM_INFO,
            (&mut vmstat as *mut libc::vm_statistics).cast::<libc::integer_t>(),
            &mut count,
        )
    };
    if status != libc::KERN_SUCCESS {
        return Err(format!("host_statistics error={status}"));
    }
    // SAFETY: a read of an immutable libSystem global.
    let page_size = unsafe { vm_kernel_page_size } as u64;
    let total = sysctl_uint64("hw.memsize")?;
    let available_count = vmstat.inactive_count.wrapping_add(vmstat.free_count);
    Ok(VirtualMemory {
        total,
        available: page_size.wrapping_mul(u64::from(available_count)),
    })
}

pub(super) fn swap_memory() -> Result<SwapMemory, String> {
    let value = sysctl_raw("vm.swapusage")?;
    if value.len() != 32 {
        return Err(format!(
            "unexpected output of sysctl vm.swapusage: {value:?} (len: {})",
            value.len()
        ));
    }
    let field = |i: usize| u64::from_ne_bytes(value[i * 8..i * 8 + 8].try_into().unwrap_or([0; 8]));
    Ok(SwapMemory {
        total: field(0),
        free: field(1),
        used: field(2),
    })
}

/// gopsutil `common.ByteToString`: skip leading NULs, stop at the next.
fn byte_to_string(orig: &[c_char]) -> String {
    let bytes: Vec<u8> = orig.iter().map(|c| *c as u8).collect();
    let Some(start) = bytes.iter().position(|b| *b != 0) else {
        return String::from_utf8_lossy(&bytes).into_owned();
    };
    let end = bytes[start..]
        .iter()
        .position(|b| *b == 0)
        .map_or(bytes.len(), |n| start + n);
    String::from_utf8_lossy(&bytes[start..end]).into_owned()
}

pub(super) fn disk_partitions() -> Result<Vec<Partition>, String> {
    // x/sys darwin MNT_* values.
    const FLAGS: &[(u32, &str)] = &[
        (0x2, "sync"),
        (0x4, "noexec"),
        (0x8, "nosuid"),
        (0x20, "union"),
        (0x40, "async"),
        (0x100000, "nobrowse"),
        (0x400000, "automounted"),
        (0x800000, "journaled"),
        (0x4000000, "multilabel"),
        (0x10000000, "noatime"),
        (0x10, "nodev"),
    ];
    const MNT_RDONLY: u32 = 0x1;
    const MNT_WAIT: libc::c_int = 1;
    // SAFETY: a NULL buffer asks only for the count.
    let count = unsafe { libc::getfsstat(std::ptr::null_mut(), 0, MNT_WAIT) };
    if count < 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    // SAFETY: an all-zero statfs is a valid value to overwrite.
    let mut fs: Vec<libc::statfs> = (0..count).map(|_| unsafe { std::mem::zeroed() }).collect();
    let bytes = (std::mem::size_of::<libc::statfs>() * fs.len()) as libc::c_int;
    // SAFETY: `fs` holds `bytes` writable bytes of statfs records.
    let count = unsafe { libc::getfsstat(fs.as_mut_ptr(), bytes, MNT_WAIT) };
    if count < 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    // On 10.14, and possibly other OS versions, the actual count may be
    // less than from the first call.
    fs.truncate(count as usize);
    Ok(fs
        .iter()
        .map(|stat| {
            let mut opts = vec![if stat.f_flags & MNT_RDONLY != 0 {
                "ro"
            } else {
                "rw"
            }
            .to_owned()];
            for (flag, name) in FLAGS {
                if stat.f_flags & flag != 0 {
                    opts.push((*name).to_owned());
                }
            }
            Partition {
                device: byte_to_string(&stat.f_mntfromname),
                mountpoint: byte_to_string(&stat.f_mntonname),
                fstype: byte_to_string(&stat.f_fstypename),
                opts,
            }
        })
        .collect())
}

type CFTypeRef = *const c_void;
type CFStringRef = *const c_void;
type CFDictionaryRef = *const c_void;
type CFMutableDictionaryRef = *mut c_void;
type CFAllocatorRef = *const c_void;
type CFIndex = isize;
type IoObject = libc::mach_port_t;

#[repr(C)]
struct CFRange {
    location: CFIndex,
    length: CFIndex,
}

#[link(name = "CoreFoundation", kind = "framework")]
extern "C" {
    static kCFAllocatorDefault: CFAllocatorRef;
    static kCFBooleanTrue: CFTypeRef;
    fn CFStringCreateWithCString(
        alloc: CFAllocatorRef,
        c_str: *const c_char,
        encoding: u32,
    ) -> CFStringRef;
    fn CFStringGetSystemEncoding() -> u32;
    fn CFStringGetCString(s: CFStringRef, buffer: *mut c_char, size: CFIndex, encoding: u32) -> u8;
    fn CFDictionaryGetValue(dict: CFDictionaryRef, key: *const c_void) -> *const c_void;
    fn CFDictionaryAddValue(dict: CFMutableDictionaryRef, key: *const c_void, value: *const c_void);
    fn CFNumberGetValue(number: CFTypeRef, the_type: CFIndex, value: *mut c_void) -> u8;
    fn CFDataGetLength(data: CFTypeRef) -> CFIndex;
    fn CFDataGetBytes(data: CFTypeRef, range: CFRange, buffer: *mut u8);
    fn CFRelease(cf: CFTypeRef);
}

#[link(name = "IOKit", kind = "framework")]
extern "C" {
    fn IOServiceMatching(name: *const c_char) -> CFMutableDictionaryRef;
    fn IOServiceGetMatchingServices(
        main_port: libc::mach_port_t,
        matching: CFDictionaryRef,
        existing: *mut IoObject,
    ) -> libc::kern_return_t;
    fn IOIteratorNext(iterator: IoObject) -> IoObject;
    fn IOObjectRelease(object: IoObject) -> libc::kern_return_t;
    fn IOObjectConformsTo(object: IoObject, class_name: *const c_char) -> u32;
    fn IORegistryEntryGetName(entry: IoObject, name: *mut c_char) -> libc::kern_return_t;
    fn IORegistryEntryGetParentEntry(
        entry: IoObject,
        plane: *const c_char,
        parent: *mut IoObject,
    ) -> libc::kern_return_t;
    fn IORegistryEntryCreateCFProperty(
        entry: IoObject,
        key: CFStringRef,
        allocator: CFAllocatorRef,
        options: u32,
    ) -> CFTypeRef;
    fn IORegistryEntryCreateCFProperties(
        entry: IoObject,
        properties: *mut CFMutableDictionaryRef,
        allocator: CFAllocatorRef,
        options: u32,
    ) -> libc::kern_return_t;
}

const KCF_NUMBER_SINT64_TYPE: CFIndex = 4;

/// `CFSTR(key)`, released on drop.
struct CfString(CFStringRef);

impl CfString {
    fn new(key: &str) -> Self {
        let ckey = CString::new(key).expect("IOKit keys have no NUL");
        // SAFETY: `ckey` is NUL-terminated; the system encoding reads ASCII.
        Self(unsafe {
            CFStringCreateWithCString(
                kCFAllocatorDefault,
                ckey.as_ptr(),
                CFStringGetSystemEncoding(),
            )
        })
    }
}

impl Drop for CfString {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: created above, released once.
            unsafe { CFRelease(self.0) };
        }
    }
}

/// go-m1cpu `getFrequency`: the little-endian u32 four to eight bytes
/// before the end of the voltage-states table.
fn get_frequency(type_ref: CFTypeRef) -> u64 {
    if type_ref.is_null() {
        return 0;
    }
    // SAFETY: the property is the CFData go-m1cpu reads.
    let size = unsafe { CFDataGetLength(type_ref) };
    if size < 8 {
        return 0;
    }
    let mut buf = vec![0u8; size as usize];
    // SAFETY: `buf` holds `size` bytes for the requested range.
    unsafe {
        CFDataGetBytes(
            type_ref,
            CFRange {
                location: 0,
                length: size,
            },
            buf.as_mut_ptr(),
        )
    };
    let n = buf.len();
    u64::from(u32::from_le_bytes([
        buf[n - 8],
        buf[n - 7],
        buf[n - 6],
        buf[n - 5],
    ]))
}

/// go-m1cpu's `initialize` (run once, as Go's package init) and `PCoreHz`.
///
/// `toHz` scales by 1000 when `fmt.Sscanf(ModelName(), "Apple M%", &gen)`
/// yields generation 4 or later, but that format ends in a bare `%`, which
/// Sscanf rejects, so `gen` stays 0 and the raw value is returned.
fn m1cpu_pcore_hz() -> u64 {
    static PCORE_CLOCK: OnceLock<u64> = OnceLock::new();
    *PCORE_CLOCK.get_or_init(|| {
        let mut clock = 0;
        // SAFETY: standard IOKit iteration; every object obtained is
        // released, and CF properties are released after reading.
        unsafe {
            let matching = IOServiceMatching(c"AppleARMIODevice".as_ptr());
            let mut iter: IoObject = 0;
            if IOServiceGetMatchingServices(0, matching, &mut iter) != libc::KERN_SUCCESS {
                return 0;
            }
            loop {
                let obj = IOIteratorNext(iter);
                if obj == 0 {
                    break;
                }
                let mut name = [0 as c_char; 512];
                IORegistryEntryGetName(obj, name.as_mut_ptr());
                if CStr::from_ptr(name.as_ptr()).to_bytes() == b"pmgr" {
                    let key = CfString::new("voltage-states5-sram");
                    let p_core_ref =
                        IORegistryEntryCreateCFProperty(obj, key.0, kCFAllocatorDefault, 0);
                    clock = get_frequency(p_core_ref);
                    if !p_core_ref.is_null() {
                        CFRelease(p_core_ref);
                    }
                    IOObjectRelease(obj);
                    break;
                }
                IOObjectRelease(obj);
            }
            IOObjectRelease(iter);
        }
        clock
    })
}

/// The `iostat_darwin.c` statistics keys and their `DriveStats` slots.
const STATS_KEYS: [&str; 4] = [
    "Bytes (Read)",
    "Bytes (Write)",
    "Operations (Read)",
    "Operations (Write)",
];

/// `iostat_darwin.c` `gopsutil_v3_readdrivestat`: whole media whose parent
/// is a block storage driver, at most `NDRIVE` (16) of them.
pub(super) fn disk_io_counters() -> Result<Vec<DiskIoCounters>, String> {
    const NDRIVE: usize = 16;
    const NAMELEN: usize = 31;
    let mut ret = Vec::new();
    // SAFETY: standard IOKit iteration over registry entries; CF objects
    // created or copied here are released before returning.
    unsafe {
        let matching = IOServiceMatching(c"IOMedia".as_ptr());
        let whole = CfString::new("Whole");
        CFDictionaryAddValue(matching, whole.0, kCFBooleanTrue);
        let mut drives: IoObject = 0;
        if IOServiceGetMatchingServices(0, matching, &mut drives) != libc::KERN_SUCCESS {
            return Err("IOServiceGetMatchingServices failed".into());
        }
        while ret.len() < NDRIVE {
            let d = IOIteratorNext(drives);
            if d == 0 {
                break;
            }
            // getdrivestat
            let mut parent: IoObject = 0;
            if IORegistryEntryGetParentEntry(d, c"IOService".as_ptr(), &mut parent)
                != libc::KERN_SUCCESS
            {
                return Err("IORegistryEntryGetParentEntry failed".into());
            }
            if IOObjectConformsTo(parent, c"IOBlockStorageDriver".as_ptr()) == 0 {
                IOObjectRelease(parent);
                IOObjectRelease(d);
                continue;
            }
            let mut props: CFMutableDictionaryRef = std::ptr::null_mut();
            if IORegistryEntryCreateCFProperties(d, &mut props, kCFAllocatorDefault, 0)
                != libc::KERN_SUCCESS
            {
                IOObjectRelease(parent);
                return Err("IORegistryEntryCreateCFProperties failed".into());
            }
            let bsd_name = CfString::new("BSD Name");
            let name_ref = CFDictionaryGetValue(props, bsd_name.0);
            let mut name = [0 as c_char; NAMELEN + 1];
            if !name_ref.is_null() {
                CFStringGetCString(
                    name_ref,
                    name.as_mut_ptr(),
                    (NAMELEN + 1) as CFIndex,
                    CFStringGetSystemEncoding(),
                );
            }
            CFRelease(props);
            // fillstat
            let mut parent_props: CFMutableDictionaryRef = std::ptr::null_mut();
            if IORegistryEntryCreateCFProperties(parent, &mut parent_props, kCFAllocatorDefault, 0)
                != libc::KERN_SUCCESS
            {
                IOObjectRelease(parent);
                return Err("IORegistryEntryCreateCFProperties failed".into());
            }
            let statistics_key = CfString::new("Statistics");
            let statistics = CFDictionaryGetValue(parent_props, statistics_key.0);
            if statistics.is_null() {
                CFRelease(parent_props);
                IOObjectRelease(parent);
                return Err("missing block storage statistics".into());
            }
            let mut values = [0i64; 4];
            for (slot, key) in values.iter_mut().zip(STATS_KEYS) {
                let key = CfString::new(key);
                let num = CFDictionaryGetValue(statistics, key.0);
                if !num.is_null() {
                    CFNumberGetValue(num, KCF_NUMBER_SINT64_TYPE, (slot as *mut i64).cast());
                }
            }
            CFRelease(parent_props);
            IOObjectRelease(parent);
            IOObjectRelease(d);
            ret.push(DiskIoCounters {
                name: CStr::from_ptr(name.as_ptr()).to_string_lossy().into_owned(),
                read_bytes: values[0] as u64,
                write_bytes: values[1] as u64,
                read_count: values[2] as u64,
                write_count: values[3] as u64,
                ..DiskIoCounters::default()
            });
        }
        IOObjectRelease(drives);
    }
    // gopsutil keys the result by name; a repeated name keeps the last.
    let mut unique: Vec<DiskIoCounters> = Vec::new();
    for d in ret {
        match unique.iter_mut().find(|existing| existing.name == d.name) {
            Some(existing) => *existing = d,
            None => unique.push(d),
        }
    }
    Ok(unique)
}

pub(super) fn net_interfaces() -> Result<Vec<NetInterface>, String> {
    let mut entries = Vec::new();
    let mut ifap: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: getifaddrs allocates the list freed below with freeifaddrs.
    if unsafe { libc::getifaddrs(&mut ifap) } != 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    let raw = |sa: *const libc::sockaddr| -> Option<Vec<u8>> {
        if sa.is_null() {
            return None;
        }
        // SAFETY: a kernel socket address spans `sa_len` bytes.
        unsafe {
            let len = usize::from((*sa).sa_len);
            Some(std::slice::from_raw_parts(sa.cast::<u8>(), len).to_vec())
        }
    };
    let mut cursor = ifap;
    while !cursor.is_null() {
        // SAFETY: `cursor` walks the list getifaddrs returned.
        let ifa = unsafe { &*cursor };
        entries.push(parse::IfaddrsEntry {
            // SAFETY: ifa_name is a NUL-terminated interface name.
            name: unsafe { CStr::from_ptr(ifa.ifa_name) }
                .to_string_lossy()
                .into_owned(),
            flags: ifa.ifa_flags,
            addr: raw(ifa.ifa_addr),
            netmask: raw(ifa.ifa_netmask),
        });
        cursor = ifa.ifa_next;
    }
    // SAFETY: frees the list getifaddrs allocated, once.
    unsafe { libc::freeifaddrs(ifap) };
    Ok(parse::darwin_interfaces(&entries))
}

/// gopsutil `invoke.CommandWithContext`: stdout and stderr together; a
/// failed start or non-zero exit is an error.
fn command(name: &str, args: &[&str]) -> Result<String, String> {
    let out = std::process::Command::new(name)
        .args(args)
        .output()
        .map_err(|error| error.to_string())?;
    if !out.status.success() {
        return Err(out.status.to_string());
    }
    let mut combined = out.stdout;
    combined.extend_from_slice(&out.stderr);
    Ok(String::from_utf8_lossy(&combined).into_owned())
}

pub(super) fn net_io_counters() -> Result<Vec<NetIoCounters>, String> {
    let out = command("netstat", &["-ibdnW"])?;
    parse::darwin_net_io_counters(&out, &command)
}

pub(super) fn load_avg() -> Result<LoadAvg, String> {
    // struct loadavg { fixpt_t ldavg[3]; long fscale; }
    let b = sysctl_raw("vm.loadavg")?;
    if b.len() < 24 {
        return Err("short vm.loadavg".into());
    }
    let load = |i: usize| {
        f64::from(u32::from_ne_bytes([
            b[i * 4],
            b[i * 4 + 1],
            b[i * 4 + 2],
            b[i * 4 + 3],
        ]))
    };
    let scale = i64::from_ne_bytes(b[16..24].try_into().unwrap_or([0; 8])) as f64;
    Ok(LoadAvg {
        load1: load(0) / scale,
        load5: load(1) / scale,
        load15: load(2) / scale,
    })
}

/// Go `runtime.NumCPU` on Darwin: `hw.ncpu`.
pub(super) fn num_cpu() -> usize {
    sysctl_uint32("hw.ncpu").map_or(1, |n| n as usize)
}
