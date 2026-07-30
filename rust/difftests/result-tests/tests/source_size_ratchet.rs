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

//! The source-file size ratchet: no file grows huge SILENTLY again.
//!
//! `driver.rs` was split at 7,793 lines and regrew past 7,400 before anyone
//! noticed; `tidb-session/src/lib.rs` absorbed six features and became the
//! site of three cross-worker seam breaks. Size itself is not the sin --
//! a transcreated data catalog is legitimately long -- the sin is growth
//! nobody DECIDED. So this test applies the corpus-ratchet mechanism to
//! file sizes:
//!
//! * A file NOT in [`GRANDFATHERED`] may not exceed [`SOFT_LIMIT`] lines.
//!   Splitting the work into a sibling module (see `driver/*.rs` and the
//!   `cluster_session_node/` split for the playbook) is the expected fix;
//!   raising the limit is not on the menu.
//! * A file IN the table may not exceed its recorded bound. Growing one is
//!   allowed only by RAISING ITS BOUND IN THE SAME COMMIT -- which turns
//!   silent drift into a reviewed decision. Adding a sysvar to
//!   `sysvar.rs` is a fine reason; "the driver needed one more helper" is
//!   what the split playbook is for.
//! * An entry whose file has shrunk to the soft limit or below must be
//!   REMOVED from the table, so the table only ever ratchets down.
//!
//! Test files are ratcheted too: a huge test file is where the next
//! merge conflict lives, even though moving tests renames them (so
//! splitting one is a deliberate, separately-reviewed act).

use std::fs;
use std::path::PathBuf;

/// The bound a file must stay under unless it carries a grandfathered entry.
const SOFT_LIMIT: usize = 2_200;

/// Every file over [`SOFT_LIMIT`] at the moment the ratchet was introduced,
/// with its size THEN as its bound. Growth past the bound fails; shrinking
/// to the soft limit or below retires the entry.
const GRANDFATHERED: &[(&str, usize)] = &[
    ("crates/tidb-parser/tests/parser_run_test_source.rs", 4_668),
    (
        "crates/tidb-distsql/tests/direct_unary_client_runtime_source.rs",
        3_135,
    ),
    ("crates/tidb-session/src/sysvar.rs", 12_028),
    ("crates/tidb-error/src/tidb/errname.rs", 11_678),
    ("crates/tidb-error/src/mysql/errname.rs", 9_559),
    ("crates/tidb-util/src/memory/arbitrator.rs", 7_331),
    ("crates/tidb-session/src/tests_core.rs", 4_512),
    ("crates/tidb-error/src/tidb/errcode.rs", 4_202),
    ("crates/tidb-expr/src/builtin_ext/json.rs", 3_810),
    // Raised 3,464 -> 3,486: the Apply-below-aggregation stage added its
    // driver tests (8b7ad5fe55).
    ("crates/tidb-executor/src/driver/tests.rs", 3_486),
    ("crates/tidb-lexer/src/keyword_catalog.rs", 3_450),
    ("crates/tidb-error/src/mysql/errcode.rs", 3_431),
    ("crates/tidb-session/src/privilege.rs", 3_216),
    ("crates/tidb-server/src/cluster_session_node/mod.rs", 3_111),
    ("crates/tidb-parser/src/select.rs", 2_917),
    ("crates/tidb-parser/src/tests/ddl.rs", 2_852),
    ("crates/tidb-expr/src/tests/mod.rs", 2_719),
    ("crates/tidb-executor/src/kv_table.rs", 2_705),
    ("crates/tidb-txnkv/src/transaction/coordinator.rs", 2_629),
    ("crates/tidb-pd-client/src/client.rs", 2_539),
    ("crates/tidb-txnkv/src/region/cache.rs", 2_518),
    ("crates/tidb-session/src/tests_grants.rs", 2_491),
    ("crates/tidb-planner/src/read_only_scan.rs", 2_476),
    ("crates/tidb-session/src/tests_window.rs", 2_417),
    ("crates/tidb-server/src/real_tikv_node.rs", 2_310),
    ("crates/tidb-executor/src/ddl.rs", 2_240),
];

fn rust_root() -> PathBuf {
    difftest::parser_oracle::repo_root().join("rust")
}

fn source_files(dir: &PathBuf, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            source_files(&path, out);
        } else if path.extension().is_some_and(|x| x == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn no_source_file_grows_huge_silently() {
    let root = rust_root();
    let crates = root.join("crates");
    let mut files = Vec::new();
    source_files(&crates, &mut files);

    let mut violations = Vec::new();
    let mut retired = Vec::new();

    for path in &files {
        let rel = path
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let lines = fs::read_to_string(path).unwrap().lines().count();
        let bound = GRANDFATHERED
            .iter()
            .find(|(name, _)| *name == rel)
            .map(|(_, bound)| *bound);
        match bound {
            Some(bound) if lines > bound => violations.push(format!(
                "\n  {rel}: {lines} lines, grandfathered bound {bound}. Growing it is a \
                 DECISION: raise the bound in this same commit with the reason, or split \
                 the file (driver/*.rs is the playbook)."
            )),
            Some(bound) if lines <= SOFT_LIMIT => retired.push(format!(
                "\n  {rel}: {lines} lines (bound was {bound}) -- now within the soft \
                 limit. Remove its GRANDFATHERED entry so the table ratchets down."
            )),
            Some(_) | None if bound.is_none() && lines > SOFT_LIMIT => violations.push(format!(
                "\n  {rel}: {lines} lines, over the {SOFT_LIMIT}-line limit for new \
                 files. Split it into sibling modules rather than adding an entry."
            )),
            _ => {}
        }
    }

    assert!(
        violations.is_empty(),
        "source files grew past their bounds:{}",
        violations.join("")
    );
    assert!(
        retired.is_empty(),
        "grandfathered entries to retire:{}",
        retired.join("")
    );
}
