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

use std::ffi::c_void;
use std::fmt;
use std::ptr;

use backtrace::{BacktraceFmt, PrintFmt};

// pingcap/errors.callers captures at most 32 return addresses.
const DEPTH: usize = 32;

pub(super) struct Frames {
    addresses: [usize; DEPTH],
    len: usize,
}

impl Frames {
    #[inline(never)]
    pub(super) fn capture() -> Option<Self> {
        // Include an extra slot for this wrapper's own frame.
        let mut raw = [ptr::null_mut::<c_void>(); DEPTH + 1];
        // SAFETY: raw is a writable array of DEPTH + 1 pointers. backtrace
        // writes at most the supplied count and does not retain the pointer.
        let captured = unsafe { libc::backtrace(raw.as_mut_ptr(), raw.len() as libc::c_int) };
        let len = usize::try_from(captured).ok()?.saturating_sub(1).min(DEPTH);
        if len == 0 {
            return None;
        }

        let mut addresses = [0; DEPTH];
        for (address, &source) in addresses.iter_mut().zip(&raw[1..1 + len]) {
            *address = source as usize;
        }
        Some(Self { addresses, len })
    }

    pub(super) fn fmt(&self, formatter: &mut fmt::Formatter<'_>, format: PrintFmt) -> fmt::Result {
        let mut print_path = |f: &mut fmt::Formatter<'_>,
                              path: backtrace::BytesOrWideString<'_>| {
            write!(f, "{}", path.into_path_buf().display())
        };
        let mut output = BacktraceFmt::new(formatter, format, &mut print_path);
        output.add_context()?;
        for &address in &self.addresses[..self.len] {
            let ip = address as *mut c_void;
            let mut frame = output.frame();
            let mut result = Ok(());
            let mut resolved = false;
            backtrace::resolve(ip, |symbol| {
                if result.is_ok() {
                    result = frame.print_raw_with_column(
                        ip,
                        symbol.name(),
                        symbol.filename_raw(),
                        symbol.lineno(),
                        symbol.colno(),
                    );
                }
                resolved = true;
            });
            result?;
            if !resolved {
                frame.print_raw(ip, None, None, None)?;
            }
        }
        output.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[inline(never)]
    fn nested_capture(depth: usize) -> Frames {
        if depth == 0 {
            return Frames::capture().unwrap();
        }
        std::hint::black_box(nested_capture(depth - 1))
    }

    #[test]
    fn caps_capture_at_pingcap_error_stack_depth() {
        let stack = nested_capture(2 * DEPTH);
        assert_eq!(stack.len, DEPTH);
        assert!(stack.addresses.iter().all(|&address| address != 0));
    }
}
