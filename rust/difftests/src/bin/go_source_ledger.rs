// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Generates the source-first rewrite inventory for every non-test Go file.
//!
//! Tests prove behavior, but they do not by themselves prove that every
//! production source owner has been inspected. This ledger is the other half
//! of the rewrite queue: one exact Go source file, its target design crate,
//! and a conservative evidence status. Files outside the SQL node remain
//! visible as deferred external tools, test support, or tooling.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[path = "../evidence_fragments.rs"]
mod evidence_fragments;

use evidence_fragments::{sorted_tsv_files, validate_fragment_owner};

const LEDGER_RELATIVE_PATH: &str = "rust/difftests/corpus/coverage/go_source_inventory.tsv";
const EVIDENCE_DIRECTORY_RELATIVE_PATH: &str = "rust/difftests/corpus/coverage/evidence/source";
const PARSER_MANIFEST_RELATIVE_PATH: &str =
    "rust/difftests/corpus/coverage/parser_translation_manifest.tsv";

const TARGETS: [&str; 23] = [
    "tidb-proto",
    "tidb-datatype",
    "tidb-lexer",
    "tidb-parser",
    "tidb-ast",
    "tidb-expr",
    "tidb-chunk",
    "tidb-codec",
    "tidb-catalog",
    "tidb-txnkv",
    "tidb-distsql",
    "tidb-planner",
    "tidb-exec",
    "tidb-session",
    "tidb-protocol",
    "tidb-ddl",
    "tidb-stats",
    "tidb-server",
    "deferred-external",
    "test-support",
    "tooling",
    "eliminated-go-runtime",
    "unassigned",
];

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Source {
    path: String,
    lines: usize,
    target: &'static str,
    generated: bool,
}

#[derive(Clone, Debug)]
struct Evidence {
    status: String,
    owner: String,
    artifact: String,
    note: String,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("difftest must live at rust/difftests")
        .to_path_buf()
}

fn ignored_root(name: &str) -> bool {
    matches!(
        name,
        ".agents" | ".git" | "node_modules" | "rust" | "target" | "vendor"
    )
}

fn walk(root: &Path, current: &Path, files: &mut Vec<PathBuf>) -> io::Result<()> {
    for item in fs::read_dir(current)? {
        let item = item?;
        let path = item.path();
        let file_type = item.file_type()?;
        if file_type.is_dir() {
            if current == root && ignored_root(&item.file_name().to_string_lossy()) {
                continue;
            }
            if matches!(item.file_name().to_str(), Some(".git" | "target")) {
                continue;
            }
            walk(root, &path, files)?;
        } else if file_type.is_file() {
            files.push(path);
        }
    }
    Ok(())
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .expect("walk returns repository paths")
        .to_string_lossy()
        .replace('\\', "/")
}

fn has_prefix(path: &str, prefixes: &[&str]) -> bool {
    prefixes.iter().any(|prefix| path.starts_with(prefix))
}

/// Routes a Go source owner to the crate boundary named by the design.
/// Specific historical `pkg/util` homes are matched before broad subsystems.
fn target_for(path: &str) -> &'static str {
    if has_prefix(
        path,
        &[
            "br/",
            "dumpling/",
            "lightning/",
            "pkg/dumpformat/",
            "pkg/importsdk/",
            "pkg/lightning/",
            // These migration/import helpers belong to the separate BR,
            // Lightning, and Dumpling binaries, not the Rust SQL node.
            "pkg/util/column-mapping/",
            "pkg/util/extsort/",
            "pkg/util/httputil/",
            "pkg/util/regexpr-router/",
            "pkg/util/table-filter/",
            "pkg/util/table-router/",
            "pkg/util/table-rule-selector/",
        ],
    ) || path == "pkg/util/filter/filter.go"
        // `dbutil` is mostly a `database/sql` client support package consumed
        // by BR, Lightning, Dumpling, and standalone tools. Keep the one SQL
        // node table-mode invariant below with catalog ownership instead of
        // deferring the whole historical Go package by name.
        || matches!(
            path,
            "pkg/util/dbutil/common.go"
                | "pkg/util/dbutil/index.go"
                | "pkg/util/dbutil/interface.go"
                | "pkg/util/dbutil/query.go"
                | "pkg/util/dbutil/retry.go"
                | "pkg/util/dbutil/types.go"
                | "pkg/util/dbutil/variable.go"
        )
        // These object-store paths preserve BR/tool behavior rather than a SQL
        // execution contract: BR's dry-run batch and lock protocols, CLI flag
        // wiring, and the dynamically selectable no-op backend.
        || matches!(
            path,
            "pkg/objstore/batch.go"
                | "pkg/objstore/flags.go"
                | "pkg/objstore/locking.go"
                | "pkg/objstore/noop.go"
        )
    {
        "deferred-external"
    // These are production-named Go files whose package contract is solely
    // test scaffolding. Keep that explicit rather than routing mocks and DXF
    // test helpers into the runtime crates they exercise.
    } else if has_prefix(
        path,
        &[
            "pkg/testkit/",
            "pkg/dxf/framework/mock/",
            "pkg/dxf/framework/scheduler/mock/",
            "pkg/dxf/framework/testutil/",
            "pkg/dxf/importinto/mock/",
            "pkg/ingestor/ingestcli/mock/",
            "pkg/ingestor/testutils/",
            "pkg/objstore/mockobjstore/",
            "pkg/objstore/ossstore/mock/",
            "pkg/objstore/s3like/mock/",
            "pkg/objstore/s3store/mock/",
            "pkg/util/breakpoint/",
            "pkg/util/cpuprofile/testutil/",
            "pkg/util/dbutil/dbutiltest/",
            "pkg/util/deeptest/",
            "pkg/util/injectfailpoint/",
            "pkg/util/intest/",
            "pkg/util/mock/",
            "pkg/util/skip/",
            "pkg/util/sqlexec/mock/",
            "pkg/util/topsql/collector/mock/",
            "pkg/util/topsql/reporter/mock/",
        ],
    ) || matches!(
        path,
        "pkg/ingestor/globalsort/testutil.go"
            | "pkg/objstore/memstore.go"
            | "pkg/resourcemanager/util/mock_gpool.go"
            | "pkg/util/sem/compat/testhelper.go"
            | "pkg/util/sem/v2/testhelper.go"
    ) {
        "test-support"
    // DXF's example applications document/verify framework integration; they
    // are not part of the SQL node runtime.
    } else if has_prefix(
        path,
        &[
            "pkg/dxf/example/",
            "pkg/plugin/conn_ip_example/",
            "pkg/util/benchdaily/",
            "pkg/util/collate/ucadata/generator/",
            "pkg/util/collate/ucaimpl/",
            "pkg/util/ddl-checker/",
            "pkg/util/importer/",
            "pkg/util/linter/",
        ],
    ) {
        "tooling"
    // The rewrite design explicitly removes Go GC/runtime adaptation. These
    // exact packages have no Rust crate owner because their behavior must
    // disappear; adjacent memory semantics remain real porting work.
    } else if has_prefix(
        path,
        &[
            "pkg/util/gctuner/",
            "pkg/util/servermemorylimit/",
            "pkg/util/hack/",
            "pkg/util/israce/",
            "pkg/util/nocopy/",
        ],
    ) || matches!(
        path,
        "pkg/util/gogc.go"
            // Rust replaces the Go runtime's private random hook and heap
            // sampling with native language facilities. Their callers' real
            // random and memory contracts remain routed separately.
            | "pkg/util/fastrand/runtime.go"
            | "pkg/util/memory/memstats.go"
    ) {
        "eliminated-go-runtime"
    // Object-store source is split only where a real Rust owner exists instead
    // of recreating the historical Go package as a generic crate. LOAD DATA
    // owns this dependency-closed compressed stream leaf. `compress.go` stays
    // unassigned because it mixes that SQL path with Dumpling/Lightning store
    // wrappers in one source file.
    } else if path.starts_with("pkg/objstore/compressedio/") {
        "tidb-exec"
    // Access counters are merged by the process-wide DXF metering runtime and
    // the S3 metric is globally registered. Their package-owned responsibility
    // is server observability even though storage adapters call into them.
    } else if matches!(
        path,
        "pkg/objstore/recording/recording.go" | "pkg/objstore/s3like/metrics.go"
    ) {
        "tidb-server"
    // `pkg/util` is a historical directory, not a Rust crate. Route only
    // contracts with a clear design owner; generic containers and mixed
    // helpers deliberately remain visible in the unassigned queue.
    } else if path.starts_with("pkg/util/collate/") {
        "tidb-datatype"
    } else if path.starts_with("pkg/util/parser/") {
        "tidb-parser"
    // These generic-looking files have dependency-closed SQL owners. Route
    // them file-by-file: neighboring files in the same Go utility package
    // often have different consumers and deliberately stay unassigned.
    } else if matches!(
        path,
        "pkg/util/disjointset/int_set.go"
            | "pkg/util/mathutil/rand.go"
            | "pkg/util/mvmap/fnv.go"
            | "pkg/util/mvmap/mvmap.go"
            | "pkg/util/set/float64_set.go"
    ) || has_prefix(path, &["pkg/util/generatedexpr/", "pkg/util/vitess/"])
        || matches!(
            path,
            "pkg/util/encrypt/aes.go" | "pkg/util/encrypt/crypt.go"
        )
    {
        "tidb-expr"
    } else if path.starts_with("pkg/util/checksum/")
        || matches!(
            path,
            "pkg/util/disjointset/set.go" | "pkg/util/encrypt/aes_layer.go"
        )
    {
        // The generic disjoint set has one production consumer in chunk;
        // checksum and AES-layer files implement chunk's spill envelope. AES
        // scalar-function semantics above stay with expressions.
        "tidb-chunk"
    } else if has_prefix(
        path,
        &[
            "pkg/util/domainutil/",
            "pkg/util/partialjson/",
            "pkg/util/schemacmp/",
            "pkg/util/tableutil/",
        ],
    ) || matches!(
        path,
        "pkg/util/dbutil/table.go" | "pkg/util/filter/schema.go"
    ) {
        "tidb-catalog"
    } else if has_prefix(
        path,
        &[
            "pkg/util/deadlockhistory/",
            "pkg/util/gcutil/",
            "pkg/util/regionsplit/",
            "pkg/util/resourcegrouptag/",
            "pkg/util/tikvutil/",
            "pkg/util/trxevents/",
        ],
    ) || matches!(path, "pkg/util/prefix_helper.go" | "pkg/util/split.go")
    {
        "tidb-txnkv"
    } else if has_prefix(
        path,
        &[
            "pkg/util/engine/",
            "pkg/util/paging/",
            "pkg/util/tiflash/",
            "pkg/util/tiflashcompute/",
        ],
    ) {
        "tidb-distsql"
    } else if has_prefix(
        path,
        &[
            "pkg/util/config/",
            "pkg/util/hint/",
            "pkg/util/intset/",
            "pkg/util/plancodec/",
            "pkg/util/replayer/",
            "pkg/util/texttree/",
        ],
    ) || matches!(
        path,
        "pkg/util/id_generator.go" | "pkg/util/set/set.go" | "pkg/util/tracing/opt_trace.go"
    ) {
        "tidb-planner"
    } else if has_prefix(
        path,
        &[
            "pkg/util/admin/",
            "pkg/util/bitmap/",
            "pkg/util/cdcutil/",
            "pkg/util/channel/",
            "pkg/util/cteutil/",
            "pkg/util/disk/",
            "pkg/util/execdetails/",
            "pkg/util/keydecoder/",
            "pkg/util/queue/",
            "pkg/util/selection/",
            "pkg/util/serialization/",
        ],
    ) || matches!(
        path,
        "pkg/util/format/format.go" | "pkg/util/set/set_with_memory_usage.go"
    ) {
        "tidb-exec"
    } else if has_prefix(
        path,
        &[
            "pkg/util/context/",
            "pkg/util/ppcpuusage/",
            "pkg/util/sem/",
            "pkg/util/sqlescape/",
            "pkg/util/sqlexec/",
            "pkg/util/sqlkiller/",
        ],
    ) || path == "pkg/util/session_pool.go"
    {
        "tidb-session"
    } else if has_prefix(
        path,
        &[
            "pkg/util/arena/",
            "pkg/util/dbterror/",
            "pkg/util/errmsg/",
            "pkg/util/password-validation/",
            "pkg/util/tls/",
        ],
    ) || path == "pkg/util/security.go"
    {
        "tidb-protocol"
    } else if has_prefix(
        path,
        &[
            "pkg/util/cgmon/",
            "pkg/util/cgroup/",
            "pkg/util/cpu/",
            "pkg/util/cpuprofile/",
            "pkg/util/disttask/",
            "pkg/util/etcd/",
            "pkg/util/expensivequery/",
            "pkg/util/globalconn/",
            "pkg/util/logutil/",
            "pkg/util/memoryusagealarm/",
            "pkg/util/naming/",
            "pkg/util/printer/",
            "pkg/util/profile/",
            "pkg/util/promutil/",
            "pkg/util/signal/",
            "pkg/util/sli/",
            "pkg/util/stmtsummary/",
            "pkg/util/sys/",
            "pkg/util/systimemon/",
            "pkg/util/topsql/",
            "pkg/util/traceevent/",
            "pkg/util/versioninfo/",
            "pkg/util/workloadrepo/",
        ],
    ) || matches!(
        path,
        "pkg/util/cpu_posix.go"
            | "pkg/util/cpu_windows.go"
            | "pkg/util/etcd.go"
            | "pkg/util/printer.go"
            | "pkg/util/rlimit_other.go"
            | "pkg/util/rlimit_windows.go"
            | "pkg/util/tracing/util.go"
            | "pkg/util/urls.go"
            | "pkg/util/wait_group_wrapper.go"
            | "pkg/util/mathutil/exponential_average.go"
            | "pkg/util/tokenlimiter.go"
    ) {
        "tidb-server"
    } else if path == "pkg/util/generic/bounded_min_heap.go" {
        // Statistics is the sole production consumer; the neighboring generic
        // SyncMap is shared by DDL and server resource-group state and stays
        // unassigned.
        "tidb-stats"
    } else if path.starts_with("pkg/util/rowDecoder/") {
        // The decoder's special default/generated-column flow is consumed by
        // online-DDL backfill; executor sampling is the secondary caller.
        "tidb-ddl"
    } else if path.starts_with("tests/") {
        "test-support"
    } else if path.starts_with("cmd/tidb-server/") {
        "tidb-server"
    // Exact source owners from the checked parser translation manifest. Do
    // not absorb generic parser helpers into the lexer by prefix. Charset is
    // datatype authority: string values, ENUM/SET, codec, expression, and
    // execution all consume the same registered charset/collation relation.
    } else if matches!(
        path,
        "pkg/parser/keywords.go"
            | "pkg/parser/lexer.go"
            | "pkg/parser/lexer_bridge.go"
            | "pkg/parser/lexer_helpers.go"
            | "pkg/parser/misc.go"
            | "pkg/parser/reserved_words.go"
            | "pkg/parser/tokens.go"
    ) {
        "tidb-lexer"
    } else if path.starts_with("pkg/parser/ast/") {
        "tidb-ast"
    } else if has_prefix(
        path,
        &["pkg/parser/charset/", "pkg/parser/types/", "pkg/types/"],
    ) {
        "tidb-datatype"
    } else if path.starts_with("pkg/parser/") {
        "tidb-parser"
    } else if path.starts_with("pkg/expression/") {
        "tidb-expr"
    // Import Into task implementations and their reusable operators execute
    // statement work. The cluster-wide DXF scheduler below is process wiring,
    // not an executor operator.
    } else if has_prefix(path, &["pkg/dxf/importinto/", "pkg/dxf/operator/"]) {
        "tidb-exec"
    } else if path.starts_with("pkg/util/chunk/") {
        "tidb-chunk"
    } else if has_prefix(
        path,
        &["pkg/util/codec/", "pkg/util/rowcodec/", "pkg/tablecodec/"],
    ) {
        "tidb-codec"
    } else if has_prefix(
        path,
        &[
            "pkg/domain/",
            "pkg/infoschema/",
            "pkg/meta/",
            "pkg/table/",
            "pkg/structure/",
            "pkg/autoid_service/",
        ],
    ) {
        "tidb-catalog"
    // Keyspace/meta-service discovery and SST ingestion are storage-client
    // responsibilities. Ingestor's package doc explicitly defines it as the
    // interface to ingest into the underlying storage layer; DDL and Import
    // Into are consumers, not alternate owners.
    } else if has_prefix(
        path,
        &[
            "pkg/kv/",
            "pkg/store/",
            "pkg/keyspace/",
            "pkg/metaservice/",
            "pkg/ingestor/",
        ],
    ) {
        "tidb-txnkv"
    } else if path.starts_with("pkg/distsql/") {
        "tidb-distsql"
    } else if has_prefix(
        path,
        &[
            "pkg/bindinfo/",
            "pkg/planner/",
            "pkg/util/ranger/",
            "pkg/workloadlearning/",
        ],
    ) {
        "tidb-planner"
    } else if path.starts_with("pkg/executor/") {
        "tidb-exec"
    } else if has_prefix(
        path,
        &[
            "pkg/privilege/",
            "pkg/session/",
            "pkg/sessionctx/",
            "pkg/sessiontxn/",
            "pkg/errctx/",
            "pkg/lock/",
        ],
    ) {
        "tidb-session"
    // These packages own MySQL wire values and errors, including prepared
    // statement binary parameters and text-protocol row serialization.
    } else if has_prefix(
        path,
        &["pkg/server/", "pkg/format/", "pkg/param/", "pkg/errno/"],
    ) {
        "tidb-protocol"
    } else if path.starts_with("pkg/ddl/") {
        "tidb-ddl"
    } else if path.starts_with("pkg/statistics/") {
        "tidb-stats"
    } else if path.starts_with("pkg/proto/") {
        "tidb-proto"
    // The design assigns config, Domain/background-task wiring, telemetry,
    // and extension lifecycle to the server binary. Package docs confirm DXF
    // framework, timers, TTL, and owner election are cluster/process-wide
    // runtimes rather than scalar/query operators.
    } else if has_prefix(
        path,
        &[
            "pkg/config/",
            "pkg/dxf/framework/",
            "pkg/extension/",
            "pkg/plugin/",
            "pkg/resourcemanager/",
            "pkg/resourcegroup/",
            "pkg/timer/",
            "pkg/ttl/",
            "pkg/owner/",
            "pkg/metrics/",
            "pkg/telemetry/",
            "pkg/extworkload/",
            "pkg/standby/",
            "pkg/tidbmanager/",
        ],
    ) {
        "tidb-server"
    } else if has_prefix(path, &["build/", "cmd/", "docs/", "scripts/", "tools/"]) {
        "tooling"
    } else {
        "unassigned"
    }
}

fn is_generated(path: &str, source: &str) -> bool {
    path.ends_with(".pb.go")
        || path.ends_with(".pb.gw.go")
        || path.ends_with("_generated.go")
        || path.ends_with(".gen.go")
        || source
            .lines()
            .take(12)
            .any(|line| line.contains("Code generated") && line.contains("DO NOT EDIT"))
}

fn collect(root: &Path) -> io::Result<BTreeSet<Source>> {
    let mut files = Vec::new();
    walk(root, root, &mut files)?;
    let mut sources = BTreeSet::new();
    for path in files {
        let rel = relative(root, &path);
        if !rel.ends_with(".go") || rel.ends_with("_test.go") {
            continue;
        }
        let text = fs::read_to_string(&path)?;
        sources.insert(Source {
            path: rel.clone(),
            lines: text.lines().count(),
            target: target_for(&rel),
            generated: is_generated(&rel, &text),
        });
    }
    Ok(sources)
}

fn read_evidence(
    root: &Path,
    sources: &BTreeSet<Source>,
) -> Result<BTreeMap<String, Evidence>, String> {
    let mut evidence = BTreeMap::new();
    let mut origins = BTreeMap::new();
    for path in sorted_tsv_files(root, EVIDENCE_DIRECTORY_RELATIVE_PATH)? {
        let text = fs::read_to_string(&path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        for (index, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let fields: Vec<_> = line.split('\t').collect();
            if fields.len() != 5 {
                return Err(format!(
                    "{}:{}: expected 5 tab-separated fields",
                    path.display(),
                    index + 1
                ));
            }
            if !sources.iter().any(|source| source.path == fields[0]) {
                return Err(format!(
                    "{}:{}: stale source path {}",
                    path.display(),
                    index + 1,
                    fields[0]
                ));
            }
            if !matches!(fields[1], "PARTIAL" | "COVERED" | "BLOCKED") {
                return Err(format!(
                    "{}:{}: status must be PARTIAL, COVERED, or BLOCKED",
                    path.display(),
                    index + 1
                ));
            }
            if fields[2..].iter().any(|field| field.is_empty()) {
                return Err(format!(
                    "{}:{}: owner, artifact, and note must be nonempty",
                    path.display(),
                    index + 1
                ));
            }
            validate_fragment_owner(&path, fields[2])
                .map_err(|error| format!("{}:{}: {error}", path.display(), index + 1))?;
            let artifact = root.join(fields[3]);
            if !artifact.is_file() {
                return Err(format!(
                    "{}:{}: evidence artifact {} does not exist",
                    path.display(),
                    index + 1,
                    artifact.display()
                ));
            }
            let row = Evidence {
                status: fields[1].to_owned(),
                owner: fields[2].to_owned(),
                artifact: fields[3].to_owned(),
                note: fields[4].to_owned(),
            };
            if let Some(first_path) = origins.get(fields[0]) {
                return Err(format!(
                    "{}:{}: duplicate source path {} (first declared in {})",
                    path.display(),
                    index + 1,
                    fields[0],
                    first_path
                ));
            }
            origins.insert(fields[0].to_owned(), path.display().to_string());
            evidence.insert(fields[0].to_owned(), row);
        }
    }
    read_parser_manifest_evidence(root, sources, &mut evidence)?;
    Ok(evidence)
}

fn parser_evidence(status: &str, artifact: &str) -> Result<Option<Evidence>, String> {
    let (status, note) = match status {
        "ported" => (
            "COVERED",
            "Imported from the checked parser translation manifest; this Go source is fully owned by the named Rust module",
        ),
        "partial" => (
            "PARTIAL",
            "Imported from the checked parser translation manifest; the named Rust module owns a bounded subset and remaining source behavior stays open",
        ),
        "unassigned" => return Ok(None),
        other => {
            return Err(format!(
                "unknown parser status {other:?}; expected ported, partial, or unassigned"
            ));
        }
    };
    if artifact == "-" || artifact.is_empty() {
        return Err(format!(
            "parser status {status} needs a nonempty Rust evidence artifact"
        ));
    }
    Ok(Some(Evidence {
        status: status.to_owned(),
        owner: "parser-workstream".to_owned(),
        artifact: artifact.to_owned(),
        note: note.to_owned(),
    }))
}

fn read_parser_manifest_evidence(
    root: &Path,
    sources: &BTreeSet<Source>,
    evidence: &mut BTreeMap<String, Evidence>,
) -> Result<(), String> {
    let path = root.join(PARSER_MANIFEST_RELATIVE_PATH);
    let text = fs::read_to_string(&path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let mut lines = text.lines();
    if lines.next() != Some("go_source\trust_module\tstatus") {
        return Err(format!(
            "{}: expected go_source, rust_module, status header",
            path.display()
        ));
    }
    for (index, line) in lines.enumerate() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() != 3 {
            return Err(format!(
                "{}:{}: expected 3 tab-separated fields",
                path.display(),
                index + 2
            ));
        }
        if !sources.iter().any(|source| source.path == fields[0]) {
            return Err(format!(
                "{}:{}: stale parser source path {}",
                path.display(),
                index + 2,
                fields[0]
            ));
        }
        let Some(row) = parser_evidence(fields[2], fields[1])
            .map_err(|error| format!("{}:{}: {error}", path.display(), index + 2))?
        else {
            if fields[1] != "-" {
                return Err(format!(
                    "{}:{}: unassigned parser source must use '-' for rust_module",
                    path.display(),
                    index + 2
                ));
            }
            continue;
        };
        let artifact = root.join(&row.artifact);
        if !artifact.is_file() {
            return Err(format!(
                "{}:{}: parser evidence artifact {} does not exist",
                path.display(),
                index + 2,
                artifact.display()
            ));
        }
        if evidence.insert(fields[0].to_owned(), row).is_some() {
            return Err(format!(
                "{}:{}: source path {} has duplicate generic and parser evidence",
                path.display(),
                index + 2,
                fields[0]
            ));
        }
    }
    Ok(())
}

fn rendered(sources: &BTreeSet<Source>, evidence: &BTreeMap<String, Evidence>) -> String {
    let mut output = String::from(
        "# Generated by cargo run -j 12 -p difftest --bin go_source_ledger -- --write.\n",
    );
    output.push_str(
        "# source_path\tlines\ttarget_crate\tgenerated\tporting_status\towner\tevidence_artifact\tnote\n",
    );
    for source in sources {
        let (status, owner, artifact, note) =
            evidence
                .get(&source.path)
                .map_or(("UNTRIAGED", "-", "-", "-"), |row| {
                    (
                        row.status.as_str(),
                        row.owner.as_str(),
                        row.artifact.as_str(),
                        row.note.as_str(),
                    )
                });
        output.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            source.path,
            source.lines,
            source.target,
            source.generated,
            status,
            owner,
            artifact,
            note
        ));
    }
    output
}

fn print_summary(sources: &BTreeSet<Source>, evidence: &BTreeMap<String, Evidence>) {
    println!("source_files: {}", sources.len());
    println!(
        "source_lines: {}",
        sources.iter().map(|source| source.lines).sum::<usize>()
    );
    println!(
        "generated_files: {}",
        sources.iter().filter(|source| source.generated).count()
    );
    for target in TARGETS {
        let files = sources
            .iter()
            .filter(|source| source.target == target)
            .count();
        let lines = sources
            .iter()
            .filter(|source| source.target == target)
            .map(|source| source.lines)
            .sum::<usize>();
        println!("{target}: files={files} lines={lines}");
    }
    for status in ["UNTRIAGED", "PARTIAL", "COVERED", "BLOCKED"] {
        let count = if status == "UNTRIAGED" {
            sources.len() - evidence.len()
        } else {
            evidence.values().filter(|row| row.status == status).count()
        };
        println!("{status}: {count}");
    }
}

fn print_queue(sources: &BTreeSet<Source>, evidence: &BTreeMap<String, Evidence>, target: &str) {
    for source in sources.iter().filter(|source| source.target == target) {
        let status = evidence
            .get(&source.path)
            .map_or("UNTRIAGED", |row| row.status.as_str());
        println!(
            "{}\t{}\t{}\t{}\t{}",
            source.target, source.path, status, source.lines, source.generated
        );
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "--check".to_owned());
    let queue_target = if mode == "--queue" {
        Some(args.next().ok_or("--queue needs a target crate")?)
    } else {
        None
    };
    if !matches!(
        mode.as_str(),
        "--write" | "--check" | "--summary" | "--queue"
    ) {
        return Err(format!(
            "unknown mode {mode:?}; use --write, --check, --summary, or --queue <target>"
        )
        .into());
    }
    if let Some(target) = &queue_target {
        if !TARGETS.contains(&target.as_str()) {
            return Err(format!(
                "unknown target {target:?}; use one of {}",
                TARGETS.join(", ")
            )
            .into());
        }
    }

    let root = repo_root();
    let sources = collect(&root)?;
    let evidence = read_evidence(&root, &sources)
        .map_err(|error| format!("source evidence invalid: {error}"))?;
    if mode == "--summary" {
        print_summary(&sources, &evidence);
        return Ok(());
    }
    if let Some(target) = queue_target {
        print_queue(&sources, &evidence, &target);
        return Ok(());
    }

    let ledger = root.join(LEDGER_RELATIVE_PATH);
    let wanted = rendered(&sources, &evidence);
    if mode == "--write" {
        fs::create_dir_all(ledger.parent().expect("ledger path has parent"))?;
        fs::write(ledger, wanted)?;
        print_summary(&sources, &evidence);
        return Ok(());
    }
    let current = fs::read_to_string(&ledger).map_err(|error| {
        format!(
            "cannot read {} ({error}); generate it with cargo run -p difftest --bin go_source_ledger -- --write",
            ledger.display()
        )
    })?;
    if current != wanted {
        return Err(format!(
            "{} is stale: the upstream Go source surface changed",
            ledger.display()
        )
        .into());
    }
    print_summary(&sources, &evidence);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{is_generated, parser_evidence, target_for};

    #[test]
    fn routes_specific_util_and_sql_node_domains_before_generic_paths() {
        for (path, target) in [
            ("pkg/parser/ast/ddl.go", "tidb-ast"),
            ("pkg/parser/charset/charset.go", "tidb-datatype"),
            ("pkg/parser/types/field_type.go", "tidb-datatype"),
            ("pkg/util/chunk/chunk.go", "tidb-chunk"),
            ("pkg/util/codec/number.go", "tidb-codec"),
            ("pkg/util/rowcodec/decoder.go", "tidb-codec"),
            ("pkg/tablecodec/tablecodec.go", "tidb-codec"),
            ("pkg/util/ranger/ranger.go", "tidb-planner"),
            ("pkg/infoschema/infoschema.go", "tidb-catalog"),
            ("pkg/store/driver/txn/txn_driver.go", "tidb-txnkv"),
            ("pkg/server/conn.go", "tidb-protocol"),
            ("pkg/format/textrow/textrow.go", "tidb-protocol"),
            ("pkg/param/binary_params.go", "tidb-protocol"),
            ("pkg/errno/errcode.go", "tidb-protocol"),
            ("cmd/tidb-server/main.go", "tidb-server"),
            ("pkg/dxf/importinto/task_executor.go", "tidb-exec"),
            ("pkg/dxf/operator/operator.go", "tidb-exec"),
            ("pkg/ingestor/ingestctrl/engine.go", "tidb-txnkv"),
            ("pkg/keyspace/keyspace.go", "tidb-txnkv"),
            ("pkg/metaservice/etcd.go", "tidb-txnkv"),
            ("pkg/structure/hash.go", "tidb-catalog"),
            ("pkg/autoid_service/autoid.go", "tidb-catalog"),
            ("pkg/workloadlearning/handle.go", "tidb-planner"),
            ("pkg/sessiontxn/isolation/base.go", "tidb-session"),
            ("pkg/errctx/context.go", "tidb-session"),
            ("pkg/lock/lock.go", "tidb-session"),
        ] {
            assert_eq!(target_for(path), target, "{path}");
        }
    }

    #[test]
    fn routes_only_manifest_owned_parser_sources_to_the_lexer() {
        for path in [
            "pkg/parser/keywords.go",
            "pkg/parser/lexer.go",
            "pkg/parser/lexer_bridge.go",
            "pkg/parser/lexer_helpers.go",
            "pkg/parser/misc.go",
            "pkg/parser/reserved_words.go",
            "pkg/parser/tokens.go",
        ] {
            assert_eq!(target_for(path), "tidb-lexer", "{path}");
        }
        assert_eq!(target_for("pkg/parser/charset/charset.go"), "tidb-datatype");
        assert_eq!(target_for("pkg/parser/parser.go"), "tidb-parser");
    }

    #[test]
    fn routes_object_store_contracts_by_real_owner() {
        for path in [
            "pkg/objstore/recording/recording.go",
            "pkg/objstore/s3like/metrics.go",
        ] {
            assert_eq!(target_for(path), "tidb-server", "{path}");
        }
        for path in [
            "pkg/objstore/compressedio/buffer.go",
            "pkg/objstore/compressedio/def.go",
            "pkg/objstore/compressedio/reader.go",
            "pkg/objstore/compressedio/writer.go",
        ] {
            assert_eq!(target_for(path), "tidb-exec", "{path}");
        }
        for path in [
            "pkg/objstore/batch.go",
            "pkg/objstore/flags.go",
            "pkg/objstore/locking.go",
            "pkg/objstore/noop.go",
        ] {
            assert_eq!(target_for(path), "deferred-external", "{path}");
        }
        for path in [
            "pkg/objstore/memstore.go",
            "pkg/objstore/mockobjstore/objstore_mock.go",
            "pkg/objstore/ossstore/mock/api_mock.go",
            "pkg/objstore/ossstore/mock/provider_mock.go",
            "pkg/objstore/s3like/mock/client_mock.go",
            "pkg/objstore/s3store/mock/s3api_mock.go",
        ] {
            assert_eq!(target_for(path), "test-support", "{path}");
        }
        for path in [
            "pkg/objstore/azblob.go",
            "pkg/objstore/compress.go",
            "pkg/objstore/gcs.go",
            "pkg/objstore/helper.go",
            "pkg/objstore/objectio/writer.go",
            "pkg/objstore/ossstore/store.go",
            "pkg/objstore/parse.go",
            "pkg/objstore/s3like/store.go",
            "pkg/objstore/s3store/store.go",
            "pkg/objstore/storage.go",
            "pkg/objstore/storeapi/storage.go",
        ] {
            assert_eq!(target_for(path), "unassigned", "{path}");
        }
    }

    #[test]
    fn routes_process_wide_runtime_owners_to_the_server_boundary() {
        for path in [
            "pkg/config/config.go",
            "pkg/dxf/framework/handle/handle.go",
            "pkg/extension/registry.go",
            "pkg/plugin/plugin.go",
            "pkg/resourcemanager/rm.go",
            "pkg/resourcegroup/runaway/manager.go",
            "pkg/timer/runtime/runtime.go",
            "pkg/ttl/ttlworker/job_manager.go",
            "pkg/owner/manager.go",
            "pkg/metrics/server.go",
            "pkg/telemetry/data.go",
            "pkg/extworkload/manager.go",
            "pkg/standby/standby.go",
            "pkg/tidbmanager/tidbmanager.go",
        ] {
            assert_eq!(target_for(path), "tidb-server", "{path}");
        }
    }

    #[test]
    fn routes_historical_util_packages_to_design_owners() {
        for (path, target) in [
            ("pkg/util/collate/collate.go", "tidb-datatype"),
            ("pkg/util/parser/parser.go", "tidb-parser"),
            ("pkg/util/encrypt/aes.go", "tidb-expr"),
            ("pkg/util/encrypt/crypt.go", "tidb-expr"),
            ("pkg/util/generatedexpr/generated_expr.go", "tidb-expr"),
            ("pkg/util/vitess/vitess_hash.go", "tidb-expr"),
            ("pkg/util/checksum/checksum.go", "tidb-chunk"),
            ("pkg/util/encrypt/aes_layer.go", "tidb-chunk"),
            ("pkg/util/domainutil/repair_vars.go", "tidb-catalog"),
            ("pkg/util/filter/schema.go", "tidb-catalog"),
            ("pkg/util/partialjson/extract.go", "tidb-catalog"),
            ("pkg/util/schemacmp/table.go", "tidb-catalog"),
            ("pkg/util/tableutil/tableutil.go", "tidb-catalog"),
            ("pkg/util/deadlockhistory/deadlock_history.go", "tidb-txnkv"),
            ("pkg/util/gcutil/gcutil.go", "tidb-txnkv"),
            ("pkg/util/regionsplit/split_handle.go", "tidb-txnkv"),
            (
                "pkg/util/resourcegrouptag/resource_group_tag.go",
                "tidb-txnkv",
            ),
            ("pkg/util/tikvutil/tikvutil.go", "tidb-txnkv"),
            ("pkg/util/trxevents/trx_events.go", "tidb-txnkv"),
            ("pkg/util/prefix_helper.go", "tidb-txnkv"),
            ("pkg/util/split.go", "tidb-txnkv"),
            ("pkg/util/engine/engine.go", "tidb-distsql"),
            ("pkg/util/paging/paging.go", "tidb-distsql"),
            ("pkg/util/tiflash/tiflash_replica_read.go", "tidb-distsql"),
            ("pkg/util/tiflashcompute/dispatch_policy.go", "tidb-distsql"),
            ("pkg/util/config/config.go", "tidb-planner"),
            ("pkg/util/hint/hint.go", "tidb-planner"),
            ("pkg/util/intset/fast_int_set.go", "tidb-planner"),
            ("pkg/util/plancodec/codec.go", "tidb-planner"),
            ("pkg/util/replayer/replayer.go", "tidb-planner"),
            ("pkg/util/texttree/texttree.go", "tidb-planner"),
            ("pkg/util/tracing/opt_trace.go", "tidb-planner"),
            ("pkg/util/admin/admin.go", "tidb-exec"),
            ("pkg/util/bitmap/concurrent.go", "tidb-exec"),
            ("pkg/util/cdcutil/cdc.go", "tidb-exec"),
            ("pkg/util/channel/channel.go", "tidb-exec"),
            ("pkg/util/cteutil/storage.go", "tidb-exec"),
            ("pkg/util/disk/tracker.go", "tidb-exec"),
            ("pkg/util/execdetails/execdetails.go", "tidb-exec"),
            ("pkg/util/keydecoder/keydecoder.go", "tidb-exec"),
            ("pkg/util/queue/queue.go", "tidb-exec"),
            ("pkg/util/selection/selection.go", "tidb-exec"),
            ("pkg/util/serialization/serialization_util.go", "tidb-exec"),
            ("pkg/util/context/context.go", "tidb-session"),
            ("pkg/util/ppcpuusage/cpuusages.go", "tidb-session"),
            ("pkg/util/sem/sem.go", "tidb-session"),
            ("pkg/util/sqlescape/utils.go", "tidb-session"),
            (
                "pkg/util/sqlexec/restricted_sql_executor.go",
                "tidb-session",
            ),
            ("pkg/util/sqlkiller/sqlkiller.go", "tidb-session"),
            ("pkg/util/session_pool.go", "tidb-session"),
            ("pkg/util/arena/arena.go", "tidb-protocol"),
            ("pkg/util/dbterror/terror.go", "tidb-protocol"),
            ("pkg/util/errmsg/errmsg.go", "tidb-protocol"),
            (
                "pkg/util/password-validation/password_validation.go",
                "tidb-protocol",
            ),
            ("pkg/util/tls/tls.go", "tidb-protocol"),
            ("pkg/util/security.go", "tidb-protocol"),
            ("pkg/util/cgroup/cgroup.go", "tidb-server"),
            ("pkg/util/cpuprofile/cpuprofile.go", "tidb-server"),
            ("pkg/util/disttask/idservice.go", "tidb-server"),
            ("pkg/util/etcd/etcd.go", "tidb-server"),
            ("pkg/util/globalconn/globalconn.go", "tidb-server"),
            ("pkg/util/logutil/log.go", "tidb-server"),
            ("pkg/util/naming/naming.go", "tidb-server"),
            ("pkg/util/printer/printer.go", "tidb-server"),
            ("pkg/util/stmtsummary/statement_summary.go", "tidb-server"),
            ("pkg/util/topsql/topsql.go", "tidb-server"),
            ("pkg/util/traceevent/traceevent.go", "tidb-server"),
            ("pkg/util/tracing/util.go", "tidb-server"),
            ("pkg/util/workloadrepo/worker.go", "tidb-server"),
            ("pkg/util/wait_group_wrapper.go", "tidb-server"),
            ("pkg/util/rowDecoder/decoder.go", "tidb-ddl"),
        ] {
            assert_eq!(target_for(path), target, "{path}");
        }
    }

    #[test]
    fn routes_only_dependency_closed_util_files_out_of_mixed_packages() {
        for (path, target) in [
            ("pkg/util/dbutil/table.go", "tidb-catalog"),
            ("pkg/util/disjointset/int_set.go", "tidb-expr"),
            ("pkg/util/disjointset/set.go", "tidb-chunk"),
            ("pkg/util/format/format.go", "tidb-exec"),
            ("pkg/util/generic/bounded_min_heap.go", "tidb-stats"),
            ("pkg/util/id_generator.go", "tidb-planner"),
            ("pkg/util/mathutil/exponential_average.go", "tidb-server"),
            ("pkg/util/mathutil/rand.go", "tidb-expr"),
            ("pkg/util/mvmap/fnv.go", "tidb-expr"),
            ("pkg/util/mvmap/mvmap.go", "tidb-expr"),
            ("pkg/util/set/float64_set.go", "tidb-expr"),
            ("pkg/util/set/set.go", "tidb-planner"),
            ("pkg/util/set/set_with_memory_usage.go", "tidb-exec"),
            ("pkg/util/tokenlimiter.go", "tidb-server"),
        ] {
            assert_eq!(target_for(path), target, "{path}");
        }

        // Adjacent files do not inherit the routed sibling's owner. Each of
        // these still has production consumers across incompatible crates.
        for path in [
            "pkg/util/fastrand/random.go",
            "pkg/util/generic/sync_map.go",
            "pkg/util/mathutil/math.go",
            "pkg/util/set/int_set.go",
            "pkg/util/set/string_set.go",
        ] {
            assert_eq!(target_for(path), "unassigned", "{path}");
        }
    }

    #[test]
    fn splits_util_runtime_test_tool_and_external_owners() {
        for path in [
            "pkg/util/cpuprofile/testutil/util.go",
            "pkg/util/dbutil/dbutiltest/utils.go",
            "pkg/util/sem/v2/testhelper.go",
            "pkg/util/sqlexec/mock/mock.go",
            "pkg/util/topsql/collector/mock/mock.go",
        ] {
            assert_eq!(target_for(path), "test-support", "{path}");
        }
        for path in [
            "pkg/util/benchdaily/bench_daily.go",
            "pkg/util/collate/ucadata/generator/main.go",
            "pkg/util/collate/ucaimpl/main.go",
            "pkg/util/ddl-checker/executable_checker.go",
            "pkg/util/importer/importer.go",
        ] {
            assert_eq!(target_for(path), "tooling", "{path}");
        }
        for path in [
            "pkg/util/column-mapping/column.go",
            "pkg/util/dbutil/common.go",
            "pkg/util/dbutil/index.go",
            "pkg/util/dbutil/interface.go",
            "pkg/util/dbutil/query.go",
            "pkg/util/dbutil/retry.go",
            "pkg/util/dbutil/types.go",
            "pkg/util/dbutil/variable.go",
            "pkg/util/extsort/external_sorter.go",
            "pkg/util/filter/filter.go",
            "pkg/util/httputil/http.go",
            "pkg/util/table-filter/table_filter.go",
            "pkg/util/table-router/router.go",
        ] {
            assert_eq!(target_for(path), "deferred-external", "{path}");
        }
        for path in [
            "pkg/util/fastrand/runtime.go",
            "pkg/util/gogc.go",
            "pkg/util/israce/israce.go",
            "pkg/util/memory/memstats.go",
            "pkg/util/nocopy/nocopy.go",
        ] {
            assert_eq!(target_for(path), "eliminated-go-runtime", "{path}");
        }
        for path in [
            "pkg/util/memory/tracker.go",
            "pkg/util/set/int_set.go",
            "pkg/util/size/size.go",
            "pkg/util/stringutil/string_util.go",
            "pkg/util/watcher/watcher.go",
        ] {
            assert_eq!(target_for(path), "unassigned", "{path}");
        }
    }

    #[test]
    fn keeps_non_sql_node_sources_visible_in_explicit_queues() {
        assert_eq!(
            target_for("lightning/pkg/importer/import.go"),
            "deferred-external"
        );
        assert_eq!(target_for("tests/llmtest/main.go"), "test-support");
        assert_eq!(target_for("pkg/testkit/testkit.go"), "test-support");
        assert_eq!(
            target_for("pkg/dxf/framework/testutil/context.go"),
            "test-support"
        );
        assert_eq!(
            target_for("pkg/dxf/framework/mock/plan_mock.go"),
            "test-support"
        );
        assert_eq!(
            target_for("pkg/dxf/importinto/mock/import_mock.go"),
            "test-support"
        );
        assert_eq!(target_for("pkg/ingestor/testutils/util.go"), "test-support");
        assert_eq!(
            target_for("pkg/ingestor/ingestcli/mock/client_mock.go"),
            "test-support"
        );
        assert_eq!(
            target_for("pkg/ingestor/globalsort/testutil.go"),
            "test-support"
        );
        assert_eq!(
            target_for("pkg/objstore/mockobjstore/objstore_mock.go"),
            "test-support"
        );
        assert_eq!(
            target_for("pkg/resourcemanager/util/mock_gpool.go"),
            "test-support"
        );
        assert_eq!(target_for("tools/check/main.go"), "tooling");
        assert_eq!(target_for("pkg/dxf/example/simple_task.go"), "tooling");
        assert_eq!(
            target_for("pkg/plugin/conn_ip_example/conn_ip_example.go"),
            "tooling"
        );
        assert_eq!(target_for("pkg/util/memory/tracker.go"), "unassigned");
        for path in [
            "pkg/util/gctuner/tuner.go",
            "pkg/util/servermemorylimit/server_memory_limit.go",
            "pkg/util/hack/hack.go",
        ] {
            assert_eq!(target_for(path), "eliminated-go-runtime", "{path}");
        }
        assert_eq!(target_for("pkg/objstore/gcs.go"), "unassigned");
    }

    #[test]
    fn detects_generated_headers_and_suffixes() {
        assert!(is_generated("pkg/proto/a.pb.go", "package proto\n"));
        assert!(is_generated(
            "pkg/foo/table.go",
            "// Code generated by tool. DO NOT EDIT.\npackage foo\n"
        ));
        assert!(!is_generated("pkg/foo/table.go", "package foo\n"));
    }

    #[test]
    fn maps_checked_parser_manifest_statuses_without_claiming_unassigned_work() {
        let covered = parser_evidence("ported", "rust/crates/tidb-parser/src/admin.rs")
            .expect("ported status is valid")
            .expect("ported status yields evidence");
        assert_eq!(covered.status, "COVERED");
        let partial = parser_evidence("partial", "rust/crates/tidb-parser/src/ddl.rs")
            .expect("partial status is valid")
            .expect("partial status yields evidence");
        assert_eq!(partial.status, "PARTIAL");
        assert!(parser_evidence("unassigned", "-")
            .expect("unassigned status is valid")
            .is_none());
        assert!(parser_evidence("complete", "rust/crates/tidb-parser/src/lib.rs").is_err());
        assert!(parser_evidence("ported", "-").is_err());
    }
}
