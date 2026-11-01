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

//! What mysql-tester and `run-tests.sh` set up around every recording: the
//! connection settings, the topic database and the server's process state.
//! Part of the recordings' provenance, not the engine's defaults.

/// What mysql-tester's driver issues on every connection it opens, before the
/// script's first statement: the handshake collation, the DSN's
/// `time_zone='Asia/Shanghai'` (go-sql-driver sends it as a SET), and the
/// tester binary's own `tidb_*` DSN list, read out of
/// `tests/integrationtest/mysql_tester` verbatim:
///
/// ```sh
/// strings tests/integrationtest/mysql_tester \
///   | grep -oE "tidb_[a-z_]+=('?[A-Za-z0-9_]+'?)"
/// ```
///
/// Recorded answers depend on them: `r/sessionctx/setvar.result` reads the
/// chunk sizes back, and `tidb_hash_join_concurrency=1` changes hash-join
/// costs (`getPlanCostVer24PhysicalHashJoin` divides by `p.Concurrency`) and
/// so the plans.
pub const MYSQL_TESTER_CONNECTION_SETUP: &[&str] = &[
    "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci",
    "SET time_zone = 'Asia/Shanghai'",
    "SET @@tidb_init_chunk_size=1",
    "SET @@tidb_max_chunk_size=32",
    "SET @@tidb_hash_join_concurrency=1",
    "SET @@tidb_enable_analyze_snapshot=1",
    "SET @@tidb_enable_pseudo_for_outdated_stats=false",
    "SET @@tidb_multi_statement_mode=1",
    "SET @@tidb_enable_clustered_index='int_only'",
];

/// The database mysql-tester runs `t/<path>.test` against: the topic path with
/// each separator replaced by `__`.
pub fn topic_database(topic: &str) -> String {
    topic.replace(['/', '\\'], "__")
}

/// The recording server's process state, installed once per process before
/// any session reads the local clock: run-tests.sh exports
/// `TZ=Asia/Shanghai`, which the `SYSTEM` time zone falls back to, and the
/// recording host had less than the 120 GiB that opens
/// `disableReuseChunkIfNeeded`'s relaxed path (`executor/chunk_reuse`), so
/// `memory.MemTotal` reads 64 GiB.
pub fn recording_server_time_zone() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        std::env::set_var("TZ", "Asia/Shanghai");
        tidb_memory::override_mem_total(Some(64 << 30));
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_path_becomes_the_database_the_scripts_name() {
        assert_eq!(topic_database("executor/admin"), "executor__admin");
        assert_eq!(topic_database("subquery"), "subquery");
    }
}
