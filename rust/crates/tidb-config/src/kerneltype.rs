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

//! Transcreation of Go `pkg/config/kerneltype`: the kernel type (Classic or
//! NextGen) chosen at compile time.
//!
//! Go selects via the `nextgen` build tag; the Rust crate uses the
//! equivalent cargo feature `nextgen`.

const CLASSIC_KERNEL_NAME: &str = "Classic";
const NEXTGEN_KERNEL_NAME: &str = "Next Generation";

/// Whether the current kernel type is NextGen (Go `IsNextGen`).
pub const fn is_next_gen() -> bool {
    cfg!(feature = "nextgen")
}

/// Whether the current kernel type is Classic (Go `IsClassic`).
pub const fn is_classic() -> bool {
    !is_next_gen()
}

/// The name of the current kernel type (Go `Name`).
pub fn name() -> &'static str {
    if is_next_gen() {
        NEXTGEN_KERNEL_NAME
    } else {
        CLASSIC_KERNEL_NAME
    }
}

/// Whether the given PD kernel type matches this instance (Go `IsMatch`).
/// Old PD versions report an empty kernel type; treat that as Classic.
pub fn is_match(pd_kernel_type: &str) -> bool {
    if pd_kernel_type.is_empty() {
        return CLASSIC_KERNEL_NAME == name();
    }
    pd_kernel_type == name()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_type() {
        assert_eq!(!is_classic(), is_next_gen());
        assert_eq!(is_classic(), !is_next_gen());
    }

    #[test]
    fn matches() {
        if is_classic() {
            assert!(is_match(""));
            assert!(is_match("Classic"));
        } else {
            assert!(is_match("Next Generation"));
        }
        assert!(!is_match("Unknown"));
    }
}
