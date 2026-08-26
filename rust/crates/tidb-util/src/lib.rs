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

//! Complete transcreations of TiDB's dependency-leaf utility packages.
//!
//! Each public module corresponds to one complete Go package. Modules are
//! added only when that package's production files, tests, and support
//! obligations move together.

/// Serializes tests that touch the process-wide logger.
///
/// `logutil`'s tests point the global logger at a file and then read that file
/// back, so any test that logs through the global logger — the ported code
/// does, wherever Go called `log.Info` — must not run alongside them.
#[cfg(test)]
pub(crate) fn global_logger_test_guard() -> std::sync::MutexGuard<'static, ()> {
    static GLOBAL_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());
    GLOBAL_TEST_GUARD
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests_mathutil;
#[cfg(test)]
mod tests_dbterror;

pub mod arena;
pub mod backoff;
pub mod bitmap;
pub mod br_key_utils;
pub mod br_summary;
pub mod cgroup;
pub mod channel;
pub mod checksum;
pub mod column_mapping;
pub mod compress;
pub mod context;
pub mod dbterror;
pub mod disjointset;
pub mod disk;
pub mod disttask;
pub mod encrypt;
pub mod errno_summary;
pub mod fastrand;
pub mod fast_hash;
pub mod filter;
pub mod format;
pub mod generic;
pub mod globalconn;
pub mod intest;
pub mod intset;
pub mod israce;
pub mod keyspace;
pub mod kvcache;
pub mod layered_io;
pub mod lightning_verification;
pub mod logutil;
pub mod master_key;
pub mod mathutil;
pub mod membuf;
pub mod memory;
pub mod memoryusagealarm;
pub mod mvmap;
pub use tidb_naming as naming;
pub mod nocopy;
pub mod paging;
pub mod partialjson;
pub mod password_validation;
pub mod plancodec;
pub mod ppcpuusage;
pub mod prefetch;
pub mod printer;
pub mod promutil;
pub mod queue;
pub mod redact;
pub mod regexpr_router;
pub mod selection;
pub mod sem;
pub mod sem_v2;
pub mod serialization;
pub mod servermemorylimit;
pub mod set;
pub mod sieve;
pub mod size;
pub mod slice;
pub mod split;
pub mod sqlescape;
pub mod sqlkiller;
pub mod stringutil;
pub mod sys;
pub mod systimemon;
pub mod table_filter;
pub mod table_router;
pub mod table_rule_selector;
pub mod texttree;
pub mod tlsutil;
pub use tidb_tikvutil as tikvutil;
pub mod timeutil;
pub mod tls;
pub mod topsql_reporter;
pub mod topsql_state;
pub mod topsql_stmtstats;
pub mod traceevent;
pub mod tracing;
pub mod versioninfo;
pub mod vitess;
pub mod watcher;
pub mod zeropool;

#[cfg(test)]
mod tests_naming;
#[cfg(test)]
mod tests_redact;
