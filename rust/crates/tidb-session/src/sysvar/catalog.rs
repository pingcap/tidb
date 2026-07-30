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

//! The flat, name-ordered system-variable registry, assembled from the
//! alphabetical catalog slices.
//!
//! Go keeps all 948 entries in one `sysVars` map in
//! `pkg/sessionctx/variable/sysvar.go`; the slices exist only to keep any one
//! Rust source file readable, and are concatenated back into a single sorted
//! slice here so a lookup stays a binary search.

use super::{SysVarDef, VarType};

mod a_h;
mod i;
mod j_p;
mod q_tidb_b;
mod tidb_c_e;
mod tidb_f_o;
mod tidb_p_z;

/// Total entry count across every catalog slice.
const TOTAL: usize = a_h::ENTRIES.len()
    + i::ENTRIES.len()
    + j_p::ENTRIES.len()
    + q_tidb_b::ENTRIES.len()
    + tidb_c_e::ENTRIES.len()
    + tidb_f_o::ENTRIES.len()
    + tidb_p_z::ENTRIES.len();

/// Fills [`concatenated`]'s buffer before the real entries are copied in;
/// every slot is overwritten before the array is returned.
const PLACEHOLDER: SysVarDef = SysVarDef {
    name: "",
    scope: 0,
    value: "",
    var_type: VarType::Str,
    read_only: false,
    allow_auto_value: false,
    min_value: 0,
    max_value: 0,
    possible_values: &[],
    auto_convert_negative_bool: false,
};

/// Copies every slice into one array, preserving slice order and therefore the
/// registry's name order.
const fn concatenated() -> [SysVarDef; TOTAL] {
    let mut out = [PLACEHOLDER; TOTAL];
    let mut written = 0;
    let mut index = 0;
    while index < a_h::ENTRIES.len() {
        out[written] = a_h::ENTRIES[index];
        index += 1;
        written += 1;
    }
    let mut index = 0;
    while index < i::ENTRIES.len() {
        out[written] = i::ENTRIES[index];
        index += 1;
        written += 1;
    }
    let mut index = 0;
    while index < j_p::ENTRIES.len() {
        out[written] = j_p::ENTRIES[index];
        index += 1;
        written += 1;
    }
    let mut index = 0;
    while index < q_tidb_b::ENTRIES.len() {
        out[written] = q_tidb_b::ENTRIES[index];
        index += 1;
        written += 1;
    }
    let mut index = 0;
    while index < tidb_c_e::ENTRIES.len() {
        out[written] = tidb_c_e::ENTRIES[index];
        index += 1;
        written += 1;
    }
    let mut index = 0;
    while index < tidb_f_o::ENTRIES.len() {
        out[written] = tidb_f_o::ENTRIES[index];
        index += 1;
        written += 1;
    }
    let mut index = 0;
    while index < tidb_p_z::ENTRIES.len() {
        out[written] = tidb_p_z::ENTRIES[index];
        index += 1;
        written += 1;
    }
    out
}

/// The registry, sorted by name so a binary search finds an entry.
pub static SYS_VARS: &[SysVarDef] = &concatenated();
