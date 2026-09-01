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

//! Native allocator statistics used by the Rust transcreation of Go
//! `pkg/util/memory`.

#[cfg(feature = "jemalloc")]
use std::ffi::{c_char, c_void, CString};
#[cfg(feature = "jemalloc")]
use std::io;
#[cfg(feature = "jemalloc")]
use std::path::Path;

#[cfg(feature = "jemalloc")]
fn stat(name: &[u8]) -> Option<i64> {
    let mut value: usize = 0;
    let mut size = std::mem::size_of::<usize>();
    // SAFETY: `name` is a NUL-terminated literal, and `value`/`size` are
    // valid out-parameters for a `size_t` read; no pointer escapes.
    let ok = unsafe {
        tikv_jemalloc_sys::mallctl(
            name.as_ptr().cast(),
            (&mut value as *mut usize).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    } == 0;
    ok.then(|| i64::try_from(value).unwrap_or(i64::MAX))
}

/// Reads `(allocated, active, resident)` from the running jemalloc.
#[cfg(feature = "jemalloc")]
#[must_use]
pub fn sample() -> Option<(i64, i64, i64)> {
    let mut epoch: u64 = 0;
    let mut epoch_size = std::mem::size_of::<u64>();
    // SAFETY: passing the same `u64` as old and new value refreshes the
    // documented statistics epoch; no pointer escapes.
    let refreshed = unsafe {
        tikv_jemalloc_sys::mallctl(
            c"epoch".as_ptr(),
            (&mut epoch as *mut u64).cast(),
            &mut epoch_size,
            (&mut epoch as *mut u64).cast(),
            epoch_size,
        )
    } == 0;
    if !refreshed {
        return None;
    }
    Some((
        stat(b"stats.allocated\0")?,
        stat(b"stats.active\0")?,
        stat(b"stats.resident\0")?,
    ))
}

/// Writes jemalloc's sampled live-allocation profile to `path`.
#[cfg(feature = "jemalloc")]
pub fn dump(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    use std::os::unix::ffi::OsStrExt;

    #[cfg(unix)]
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "profile path contains NUL"))?;
    #[cfg(not(unix))]
    let path = CString::new(path.to_string_lossy().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "profile path contains NUL"))?;
    let mut path_ptr = path.as_ptr();
    // SAFETY: `prof.dump` consumes one `const char *`; the CString lives
    // through the call and no pointer escapes.
    let result = unsafe {
        tikv_jemalloc_sys::mallctl(
            c"prof.dump".as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            (&mut path_ptr as *mut *const c_char).cast::<c_void>(),
            std::mem::size_of::<*const c_char>(),
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(result))
    }
}

#[cfg(feature = "jemalloc")]
mod profile_config {
    use std::ffi::c_char;

    union Pointer {
        bytes: &'static u8,
        chars: &'static c_char,
    }

    #[allow(non_upper_case_globals)]
    #[export_name = "_rjem_malloc_conf"]
    pub static malloc_conf: Option<&'static c_char> = Some(unsafe {
        Pointer {
            bytes: &b"prof:true,lg_prof_sample:19\0"[0],
        }
        .chars
    });
}
