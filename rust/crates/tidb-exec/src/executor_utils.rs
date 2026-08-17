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

//! Rust port of `pkg/executor/utils.go`.
//!
//! `pkg/executor` is a very large package (hundreds of files: `insert.go`,
//! `delete.go`, `update.go`, the `information_schema` retrievers, DDL
//! executors, and more). This module ports exactly one file from it,
//! `utils.go` (274 production lines): the package-wide grab-bag of small
//! helpers — string-set bookkeeping, password encoding, a batch-retrieval
//! cursor, a DML chunk-capacity estimator, and a small worker pool. It is
//! seed evidence for that one file, not a claim about `pkg/executor` as a
//! whole.
//!
//! One function from a sibling file is also ported here because its test,
//! `TestEqualDatumsAsBinary`, lives in `utils_test.go`:
//! `InsertValues.equalDatumsAsBinary` from `pkg/executor/insert_common.go`
//! (see [`equal_datums_as_binary`] below for the exact narrowing).
//!
//! ## Narrowed dependencies (each named at its definition below)
//!
//! - `// boundary:` Go `pkg/parser/mysql` auth-plugin name and password-hash
//!   length constants. `tidb-mysql` is not a direct dependency of
//!   `tidb-exec` (only `tidb-ast`/`tidb-parser`/`tidb-executor` depend on
//!   it, and none re-export these constants), so the handful this file
//!   needs are copied locally as `AUTH_*`/`*_PWD_HASH_LEN` rather than
//!   adding a new dependency edge.
//! - `// boundary:` Go `pkg/parser/ast.AuthOption` and the `AuthOpt` field
//!   of `pkg/parser/ast.UserSpec`. `tidb-ast`'s own [`tidb_ast::UserSpec`]
//!   (a different, already-ported type for `CREATE`/`ALTER USER` restore)
//!   has no `AuthOpt`-shaped field, so [`AuthOption`] is a local seed
//!   struct mirroring the Go field set exactly. `encode_password_with_plugin`
//!   and `encoded_password` only ever read `u.AuthOpt`, so the surrounding
//!   `ast.UserSpec` (its `User` identity, `DualPasswordOption`, `IsRole`)
//!   is narrowed away entirely; both functions take `Option<&AuthOption>`
//!   directly instead of a fabricated `UserSpec`-shaped wrapper.
//! - `// boundary:` Go `pkg/extension.AuthPlugin` is a plugin-registration
//!   struct with several fields; `encodePasswordWithPlugin` calls exactly
//!   two of its function-typed fields. Narrowed to the 2-method
//!   [`AuthPlugin`] trait.
//! - `// boundary:` Go `pkg/util/chunk.EstimateTypeWidth` /
//!   `chunk.ZeroCapacity` (package `pkg/util/chunk`, a different Go file
//!   than `utils.go`). `tidb-chunk` is not a direct dependency of
//!   `tidb-exec` (`tidb-executor` uses it privately in `access_cost.rs`
//!   without re-exporting it), so [`estimate_dml_child_chunk_init_cap`]
//!   takes an already-summed `row_width` instead of `&[FieldType]` plus a
//!   width estimator, and `newDMLChildChunk` (which additionally needs
//!   `executor/internal/exec.Executor` and `chunk.Chunk`) is not ported —
//!   see that function's doc for the full reasoning.
//! - `// boundary:` Go `InsertValues.equalDatumsAsBinary`'s receiver `e`
//!   (an embedded `exec.BaseExecutor`) is used only to reach
//!   `e.Ctx().GetSessionVars().StmtCtx.TypeCtx()`, which is then threaded
//!   into `Datum.Compare`. The already-ported `tidb_datatype::Datum::compare`
//!   takes only a `Collation`, no type context, so [`equal_datums_as_binary`]
//!   is a free function taking the two datum slices directly.
//! - `runtime` (Go's stack-dump-on-panic support): Go's `growWorkerStack16K`
//!   pads a goroutine's stack before spawning `run` so a subsequent panic's
//!   stack trace prints in full; it is host-runtime/stack-growth mechanics
//!   with no observable effect on task scheduling or results, and Rust's
//!   `std::thread` stacks/panics work differently, so it is dropped rather
//!   than narrowed.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tidb_datatype::{Collation, Datum, DatumValueError};

// ---------------------------------------------------------------------
// Group 1: string-set operations and small pure helpers
// (Go `pkg/executor/utils.go` lines 37-70)
// ---------------------------------------------------------------------

/// Rust port of `SetFromString` (`utils.go:40`).
///
/// Constructs a vector of strings from a comma-separated string. Assumes no
/// duplicated entry; use [`add_to_set`] to maintain that property.
#[must_use]
pub fn set_from_string(value: &str) -> Vec<String> {
    if value.is_empty() {
        return Vec::new();
    }
    value.split(',').map(str::to_owned).collect()
}

/// Rust port of `setToString` (`utils.go:47`).
#[must_use]
pub fn set_to_string(set: &[String]) -> String {
    set.join(",")
}

/// Rust port of `addToSet` (`utils.go:53`).
///
/// Adds a value to the set, e.g. `addToSet(["Select", "Insert", "Update"],
/// "Update")` returns `["Select", "Insert", "Update"]` unchanged.
#[must_use]
pub fn add_to_set(mut set: Vec<String>, value: &str) -> Vec<String> {
    if set.iter().any(|v| v == value) {
        return set;
    }
    set.push(value.to_owned());
    set
}

/// Rust port of `deleteFromSet` (`utils.go:62`).
///
/// Deletes the value from the set, e.g. `deleteFromSet(["Select", "Insert",
/// "Update"], "Update")` returns `["Select", "Insert"]`.
#[must_use]
pub fn delete_from_set(mut set: Vec<String>, value: &str) -> Vec<String> {
    if let Some(pos) = set.iter().position(|v| v == value) {
        set.remove(pos);
    }
    set
}

// ---------------------------------------------------------------------
// Group 2: equal_datums_as_binary and password encoding
// (Go `pkg/executor/insert_common.go:1444` and `utils.go:130-206`)
// ---------------------------------------------------------------------

/// Rust port of `InsertValues.equalDatumsAsBinary` (`insert_common.go:1444`).
///
/// Compares whether `a` and `b` contain the same datum values under binary
/// collation. See the module-level `// boundary:` note on why this is a
/// free function rather than an `InsertValues` method.
pub fn equal_datums_as_binary(a: &[Datum], b: &[Datum]) -> Result<bool, DatumValueError> {
    if a.len() != b.len() {
        return Ok(false);
    }
    for (ai, bi) in a.iter().zip(b.iter()) {
        if ai.compare(bi, Collation::Binary)? != Ordering::Equal {
            return Ok(false);
        }
    }
    Ok(true)
}

// boundary: Go `pkg/parser/mysql` auth-plugin names and password-hash
// lengths (`pkg/parser/mysql/const.go`). `tidb-mysql` is not a direct
// dependency of `tidb-exec`; see the module-level doc for why these are
// copied locally instead of adding a dependency edge.
const AUTH_NATIVE_PASSWORD: &str = "mysql_native_password";
const AUTH_CACHING_SHA2_PASSWORD: &str = "caching_sha2_password";
const AUTH_TIDB_SM3_PASSWORD: &str = "tidb_sm3_password";
const AUTH_SOCKET: &str = "auth_socket";
const AUTH_LDAP_SIMPLE: &str = "authentication_ldap_simple";
const AUTH_LDAP_SASL: &str = "authentication_ldap_sasl";
/// Go `mysql.PWDHashLen`: hash length excluding the leading `*`.
const PWD_HASH_LEN: usize = 40;
/// Go `mysql.SHAPWDHashLen`.
const SHA_PWD_HASH_LEN: usize = 70;
/// Go `mysql.SM3PWDHashLen`.
const SM3_PWD_HASH_LEN: usize = 70;

/// boundary: Go `pkg/parser/ast.AuthOption` (`pkg/parser/ast/misc.go:98`).
/// Local seed struct with the exact same fields; see the module-level doc.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthOption {
    /// If true, authorization is done by `auth_string`; otherwise by
    /// `hash_string` (or, per `by_hash_string`, an explicit hash form).
    pub by_auth_string: bool,
    /// Plaintext password, used when `by_auth_string` is set.
    pub auth_string: String,
    /// If true, `hash_string` is an explicit `AS 'hash'` form.
    pub by_hash_string: bool,
    /// Precomputed hash (or LDAP dn), used when `by_auth_string` is unset.
    pub hash_string: String,
    /// `IDENTIFIED WITH <plugin>` plugin name, or empty to use the default.
    pub auth_plugin: String,
}

/// boundary: Go `pkg/extension.AuthPlugin` narrowed to the two callback
/// fields `encodePasswordWithPlugin` invokes: `GenerateAuthString` and
/// `ValidateAuthString`. See the module-level doc.
pub trait AuthPlugin {
    /// Go `AuthPlugin.GenerateAuthString(authString string) (string, bool)`.
    fn generate_auth_string(&self, auth_string: &str) -> (String, bool);
    /// Go `AuthPlugin.ValidateAuthString(hash string) bool`.
    fn validate_auth_string(&self, hash_string: &str) -> bool;
}

/// Rust port of `encodePasswordWithPlugin` (`utils.go:131`).
///
/// Encodes the password for the user, invoking the auth plugin if one is
/// available. `auth_opt` narrows Go's `u ast.UserSpec` parameter down to
/// its only field this function reads (`u.AuthOpt`); see the module-level
/// doc.
pub fn encode_password_with_plugin(
    auth_opt: Option<&AuthOption>,
    auth_plugin: Option<&dyn AuthPlugin>,
    default_plugin: &str,
) -> (String, bool) {
    let Some(opt) = auth_opt else {
        return (String::new(), true);
    };
    if let Some(plugin) = auth_plugin {
        if opt.by_auth_string {
            return plugin.generate_auth_string(&opt.auth_string);
        }
        if plugin.validate_auth_string(&opt.hash_string) {
            return (opt.hash_string.clone(), true);
        }
        return (String::new(), false);
    }
    encoded_password(auth_opt, default_plugin)
}

/// Rust port of `encodedPassword` (`utils.go:151`).
///
/// Returns the encoded password (the real data stored in `mysql.user`). The
/// boolean return indicates whether the input password format is legal.
/// `n` narrows Go's `n *ast.UserSpec` parameter the same way as
/// [`encode_password_with_plugin`].
pub fn encoded_password(n: Option<&AuthOption>, default_plugin: &str) -> (String, bool) {
    let Some(opt) = n else {
        return (String::new(), true);
    };

    let auth_plugin = if opt.auth_plugin.is_empty() {
        default_plugin
    } else {
        opt.auth_plugin.as_str()
    };

    if opt.by_auth_string {
        return match auth_plugin {
            AUTH_CACHING_SHA2_PASSWORD | AUTH_TIDB_SM3_PASSWORD => (
                tidb_parser::auth::new_hash_password(&opt.auth_string, auth_plugin)
                    .unwrap_or_default(),
                true,
            ),
            AUTH_SOCKET => (String::new(), true),
            _ => (tidb_parser::auth::encode_password(&opt.auth_string), true),
        };
    }

    // store the LDAP dn directly in the password field
    if auth_plugin == AUTH_LDAP_SIMPLE || auth_plugin == AUTH_LDAP_SASL {
        // TODO: validate the HashString to be a `dn` for LDAP
        // It seems fine to not validate here, and the LDAP server will give an
        // error when the client tries to log this user in. The percona server
        // implementation doesn't have a validation for this HashString.
        // However, returning an error for an obviously wrong format is more
        // friendly.
        return (opt.hash_string.clone(), true);
    }

    // In case we have 'IDENTIFIED WITH <plugin>' but no 'BY <password>' to set
    // an empty password.
    if opt.hash_string.is_empty() {
        return (opt.hash_string.clone(), true);
    }

    // Not a legal password string.
    match auth_plugin {
        AUTH_CACHING_SHA2_PASSWORD => {
            if opt.hash_string.len() != SHA_PWD_HASH_LEN {
                return (String::new(), false);
            }
        }
        AUTH_TIDB_SM3_PASSWORD => {
            if opt.hash_string.len() != SM3_PWD_HASH_LEN {
                return (String::new(), false);
            }
        }
        "" | AUTH_NATIVE_PASSWORD => {
            if opt.hash_string.len() != (PWD_HASH_LEN + 1) || !opt.hash_string.starts_with('*') {
                return (String::new(), false);
            }
        }
        AUTH_SOCKET => {}
        _ => return (String::new(), false),
    }
    (opt.hash_string.clone(), true)
}

// ---------------------------------------------------------------------
// Group 3: DML chunk-capacity estimator (Go `utils.go:72-93`)
// ---------------------------------------------------------------------

/// Go `chunk.ZeroCapacity` (`pkg/util/chunk`), copied locally; see the
/// module-level `// boundary:` note on `tidb-chunk` not being a dependency
/// of `tidb-exec`.
pub const ZERO_CAPACITY: i64 = 0;

/// Rust port of the `dmlChildChunkTargetBytes` constant (`utils.go:72`).
pub const DML_CHILD_CHUNK_TARGET_BYTES: i64 = 256 * 1024;

/// Rust port of `estimateDMLChildChunkInitCap` (`utils.go:81`).
///
/// Uses the child `InitCap` as the upper bound, but reduces it for wide rows
/// to avoid a large upfront allocation.
///
/// `row_width` narrows Go's `fields []*types.FieldType` parameter: Go sums
/// `chunk.EstimateTypeWidth(field)` over the fields inside this function,
/// but `chunk.EstimateTypeWidth` lives in `pkg/util/chunk`
/// (`tidb_chunk::codec::estimate_type_width` in Rust), which is not
/// reachable from `tidb-exec` without a new dependency edge (see the
/// module-level doc). Callers that have a `tidb-chunk` dependency available
/// sum the per-field width themselves and pass the total here; the
/// threshold/clamping arithmetic below — the part `utils.go` actually
/// contributes and the part with no upstream test coverage — is otherwise
/// byte-exact with Go.
///
/// `newDMLChildChunk` (`utils.go:75`), the thin wrapper that additionally
/// calls `dmlExec.MaxChunkSize()` and `dmlExec.NewChunkWithCapacity(...)`,
/// is not ported: it needs `executor/internal/exec.Executor` and
/// `chunk.Chunk`, neither of which this crate depends on.
#[must_use]
pub fn estimate_dml_child_chunk_init_cap(
    row_width: i64,
    max_chunk_size: i64,
    max_init_cap: i64,
) -> i64 {
    if max_chunk_size <= 0 || max_init_cap <= 0 {
        return ZERO_CAPACITY;
    }
    if row_width <= 0 {
        return max_chunk_size.min(max_init_cap);
    }
    1.max(
        max_chunk_size
            .min(max_init_cap)
            .min(DML_CHILD_CHUNK_TARGET_BYTES / row_width),
    )
}

// ---------------------------------------------------------------------
// Group 4: batchRetrieverHelper (Go `utils.go:95-128`)
// ---------------------------------------------------------------------

/// Rust port of `batchRetrieverHelper` (`utils.go:97`).
///
/// A helper for batch-returning data with a known total row count. Helps
/// implement memtable retrievers of some `information_schema` tables.
/// Initialize `batch_size` and `total_rows` to use it.
#[derive(Debug, Clone, Copy, Default)]
pub struct BatchRetrieverHelper {
    /// When true, retrieving is finished.
    pub retrieved: bool,
    /// The index that the retrieving process has been done up to (exclusive).
    pub retrieved_idx: i64,
    /// Number of rows to retrieve per batch.
    pub batch_size: i64,
    /// Total number of rows to retrieve across all batches.
    pub total_rows: i64,
}

impl BatchRetrieverHelper {
    /// Rust port of `batchRetrieverHelper.nextBatch` (`utils.go:108`).
    ///
    /// Calculates the index range of the next batch. If there is such a
    /// non-empty range, `retrieve_range` is invoked with `[start, end)`.
    /// Returns the error `retrieve_range` returns, if any.
    pub fn next_batch<E>(
        &mut self,
        mut retrieve_range: impl FnMut(i64, i64) -> Result<(), E>,
    ) -> Result<(), E> {
        if self.retrieved_idx >= self.total_rows {
            self.retrieved = true;
        }
        if self.retrieved {
            return Ok(());
        }
        let start = self.retrieved_idx;
        let end = (self.retrieved_idx + self.batch_size).min(self.total_rows);

        if let Err(err) = retrieve_range(start, end) {
            self.retrieved = true;
            return Err(err);
        }
        self.retrieved_idx = end;
        if self.retrieved_idx == self.total_rows {
            self.retrieved = true;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------
// Group 5: worker pool (Go `utils.go:208-274`)
// ---------------------------------------------------------------------

/// A queued unit of work. Go's `workerTask` also carries a `next` pointer
/// (the FIFO queue is Go's own singly linked list); [`VecDeque`] plays that
/// role here instead.
type WorkerTask = Box<dyn FnOnce() + Send + 'static>;

struct WorkerPoolInner {
    queue: VecDeque<WorkerTask>,
    tasks: u32,
    workers: u32,
}

/// Rust port of `workerPool` (`utils.go:217`).
///
/// A pool of OS threads that lazily spawns workers on `submit` according to
/// `need_spawn`, and lets each worker thread exit once the queue drains.
/// This targets the OBSERVABLE contract Go's goroutine-based pool provides
/// — every submitted task eventually runs exactly once, results are
/// collected from all of them, and no more than the concurrency bound
/// implied by `need_spawn` runs at a time — using `std::thread` rather than
/// reproducing goroutine scheduling.
///
/// boundary: Go's package-level `globalTaskPool sync.Pool` is a
/// `*workerTask` allocation-reuse cache shared by every `workerPool`
/// instance in the process. It is a pure allocation optimization with no
/// effect on task ordering, concurrency, or results, so it is dropped here;
/// Rust's allocator (each task is a freshly boxed closure) plays that role.
///
/// Submission and running take `self: &Arc<Self>` (rather than `&self`)
/// because a running task must be able to call `submit` again on the same
/// pool from a spawned worker thread — mirroring Go's tests, which submit
/// nested tasks from within a running task via the shared `*workerPool`
/// pointer.
pub struct WorkerPool {
    inner: Mutex<WorkerPoolInner>,
    /// Rust port of the `needSpawn func(workers, tasks uint32) bool` field.
    /// `None` matches Go's nil `needSpawn`: always spawn.
    pub need_spawn: Option<Box<dyn Fn(u32, u32) -> bool + Send + Sync>>,
}

impl WorkerPool {
    /// Constructs an empty pool with no tasks and no workers running yet.
    #[must_use]
    pub fn new(need_spawn: Option<Box<dyn Fn(u32, u32) -> bool + Send + Sync>>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(WorkerPoolInner {
                queue: VecDeque::new(),
                tasks: 0,
                workers: 0,
            }),
            need_spawn,
        })
    }

    /// Rust port of `workerPool.submit` (`utils.go:227`).
    pub fn submit(self: &Arc<Self>, f: impl FnOnce() + Send + 'static) {
        let task: WorkerTask = Box::new(f);

        let spawn = {
            let mut inner = self.inner.lock().expect("worker pool mutex poisoned");
            inner.queue.push_back(task);
            inner.tasks += 1;
            let should_spawn = inner.workers == 0
                || self
                    .need_spawn
                    .as_ref()
                    .is_none_or(|need_spawn| need_spawn(inner.workers, inner.tasks));
            if should_spawn {
                inner.workers += 1;
            }
            should_spawn
        };

        if spawn {
            let pool = Arc::clone(self);
            std::thread::spawn(move || pool.run());
        }
    }

    /// Rust port of `workerPool.run` (`utils.go:251`).
    fn run(self: Arc<Self>) {
        loop {
            let task = {
                let mut inner = self.inner.lock().expect("worker pool mutex poisoned");
                match inner.queue.pop_front() {
                    None => {
                        inner.workers -= 1;
                        return;
                    }
                    Some(task) => {
                        inner.tasks -= 1;
                        task
                    }
                }
            };
            task();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Condvar;
    use std::time::Duration;

    use super::*;

    // -------------------------------------------------------------
    // Source: `TestBatchRetrieverHelper` (`utils_test.go:34`).
    // -------------------------------------------------------------

    #[test]
    fn batch_retriever_helper() {
        let mut range_starts: Vec<i64> = Vec::new();
        let mut range_ends: Vec<i64> = Vec::new();
        let collect = |range_starts: &mut Vec<i64>, range_ends: &mut Vec<i64>, start, end| {
            range_starts.push(start);
            range_ends.push(end);
            Ok::<(), ()>(())
        };

        let mut r = BatchRetrieverHelper::default();
        r.next_batch(|s, e| collect(&mut range_starts, &mut range_ends, s, e))
            .unwrap();
        assert_eq!(range_starts, Vec::<i64>::new());
        assert_eq!(range_ends, Vec::<i64>::new());

        let mut r = BatchRetrieverHelper {
            retrieved: true,
            batch_size: 3,
            total_rows: 10,
            ..Default::default()
        };
        r.next_batch(|s, e| collect(&mut range_starts, &mut range_ends, s, e))
            .unwrap();
        assert_eq!(range_starts, Vec::<i64>::new());
        assert_eq!(range_ends, Vec::<i64>::new());

        let mut r = BatchRetrieverHelper {
            batch_size: 3,
            total_rows: 10,
            ..Default::default()
        };
        let result = r.next_batch(|_, _| Err::<(), _>("some error"));
        assert!(result.is_err());
        assert!(r.retrieved);

        let mut r = BatchRetrieverHelper {
            batch_size: 3,
            total_rows: 10,
            ..Default::default()
        };
        while !r.retrieved {
            r.next_batch(|s, e| collect(&mut range_starts, &mut range_ends, s, e))
                .unwrap();
        }
        assert_eq!(range_starts, vec![0, 3, 6, 9]);
        assert_eq!(range_ends, vec![3, 6, 9, 10]);
        range_starts.clear();
        range_ends.clear();

        let mut r = BatchRetrieverHelper {
            batch_size: 3,
            total_rows: 9,
            ..Default::default()
        };
        while !r.retrieved {
            r.next_batch(|s, e| collect(&mut range_starts, &mut range_ends, s, e))
                .unwrap();
        }
        assert_eq!(range_starts, vec![0, 3, 6]);
        assert_eq!(range_ends, vec![3, 6, 9]);
        range_starts.clear();
        range_ends.clear();

        let mut r = BatchRetrieverHelper {
            batch_size: 100,
            total_rows: 10,
            ..Default::default()
        };
        while !r.retrieved {
            r.next_batch(|s, e| collect(&mut range_starts, &mut range_ends, s, e))
                .unwrap();
        }
        assert_eq!(range_starts, vec![0]);
        assert_eq!(range_ends, vec![10]);
    }

    // -------------------------------------------------------------
    // Source: `TestEqualDatumsAsBinary` (`utils_test.go:107`).
    //
    // Go builds each case with `types.MakeDatums(...)`; ints become
    // `Datum::Int`, strings become `Datum::Bytes` (binary collation makes
    // string-vs-bytes-kind indistinguishable here), and `nil` becomes
    // `Datum::Null`.
    // -------------------------------------------------------------

    fn di(v: i64) -> Datum {
        Datum::Int(v)
    }

    fn ds(v: &str) -> Datum {
        Datum::Bytes(v.as_bytes().to_vec())
    }

    #[test]
    fn equal_datums_as_binary_test() {
        let cases: Vec<(Vec<Datum>, Vec<Datum>, bool)> = vec![
            // Positive cases
            (vec![di(1)], vec![di(1)], true),
            (vec![di(1), ds("aa")], vec![di(1), ds("aa")], true),
            (
                vec![di(1), ds("aa"), di(1)],
                vec![di(1), ds("aa"), di(1)],
                true,
            ),
            // Negative cases
            (vec![di(1)], vec![di(2)], false),
            (vec![di(1), ds("a")], vec![di(1), ds("aaaaaa")], false),
            (
                vec![di(1), ds("aa"), di(3)],
                vec![di(1), ds("aa"), di(2)],
                false,
            ),
            // Corner cases
            (vec![], vec![], true),
            (vec![Datum::Null], vec![Datum::Null], true),
            (vec![], vec![di(1)], false),
            (vec![di(1)], vec![di(1), di(1)], false),
            (vec![Datum::Null], vec![di(1)], false),
        ];

        for (a, b, same) in cases {
            let res = equal_datums_as_binary(&a, &b).unwrap();
            assert_eq!(res, same, "a={a:?} b={b:?}");
        }
    }

    // -------------------------------------------------------------
    // Source: `TestEncodePasswordWithPlugin` (`utils_test.go:143`).
    // -------------------------------------------------------------

    struct MockAuthPlugin<G, V> {
        generate: G,
        validate: V,
    }

    impl<G, V> AuthPlugin for MockAuthPlugin<G, V>
    where
        G: Fn(&str) -> (String, bool),
        V: Fn(&str) -> bool,
    {
        fn generate_auth_string(&self, auth_string: &str) -> (String, bool) {
            (self.generate)(auth_string)
        }

        fn validate_auth_string(&self, hash_string: &str) -> bool {
            (self.validate)(hash_string)
        }
    }

    #[test]
    fn encode_password_with_plugin_test() {
        let hash_string = "*3D56A309CD04FA2EEF181462E59011F075C89548";
        let plugin = MockAuthPlugin {
            validate: |_s: &str| false,
            generate: |s: &str| {
                if s == "xxx" {
                    ("xxxxxxx".to_owned(), true)
                } else {
                    (String::new(), false)
                }
            },
        };

        let mut opt = AuthOption {
            by_auth_string: false,
            auth_string: "xxx".to_owned(),
            by_hash_string: false,
            hash_string: hash_string.to_owned(),
            auth_plugin: String::new(),
        };

        let (_, ok) = encode_password_with_plugin(Some(&opt), Some(&plugin), "");
        assert!(!ok);

        opt.auth_string = "xxx".to_owned();
        opt.by_auth_string = true;
        let (pwd, ok) = encode_password_with_plugin(Some(&opt), Some(&plugin), "");
        assert!(ok);
        assert_eq!(pwd, "xxxxxxx");

        let (pwd, ok) = encode_password_with_plugin(None, Some(&plugin), "");
        assert!(ok);
        assert_eq!(pwd, "");
    }

    // -------------------------------------------------------------
    // Source: `TestWorkerPool` (`utils_test.go:184`).
    //
    // A minimal `sync.WaitGroup` stand-in: Go's stdlib type has no direct
    // Rust equivalent in scope here, and it is pure test scaffolding (not
    // part of `utils.go`), so it is reproduced locally rather than ported.
    // -------------------------------------------------------------

    #[derive(Default)]
    struct WaitGroup {
        state: Mutex<i64>,
        condvar: Condvar,
    }

    impl WaitGroup {
        fn add(&self, delta: i64) {
            let mut count = self.state.lock().unwrap();
            *count += delta;
            self.condvar.notify_all();
        }

        fn done(&self) {
            self.add(-1);
        }

        fn wait(&self) {
            let mut count = self.state.lock().unwrap();
            while *count > 0 {
                count = self.condvar.wait(count).unwrap();
            }
        }
    }

    fn sleep_ms(ms: u64) {
        std::thread::sleep(Duration::from_millis(ms));
    }

    /// Runs one `TestWorkerPool` subtest: submits a task that pushes `1`,
    /// submits a nested task that (after a 10ms sleep) pushes `3` then `4`,
    /// sleeps 1ms, then pushes `2`; waits for both to finish and returns the
    /// collected order.
    fn run_worker_pool_subtest(
        need_spawn: impl Fn(u32, u32) -> bool + Send + Sync + 'static,
    ) -> Vec<i64> {
        let list = Arc::new(Mutex::new(Vec::<i64>::new()));
        let push = {
            let list = Arc::clone(&list);
            move |v: i64| list.lock().unwrap().push(v)
        };

        let pool = WorkerPool::new(Some(Box::new(need_spawn)));
        let wg = Arc::new(WaitGroup::default());
        wg.add(1);

        let pool2 = Arc::clone(&pool);
        let wg2 = Arc::clone(&wg);
        let push2 = push.clone();
        pool.submit(move || {
            push2(1);
            wg2.add(1);
            let wg3 = Arc::clone(&wg2);
            let push3 = push2.clone();
            pool2.submit(move || {
                push3(3);
                sleep_ms(10);
                push3(4);
                wg3.done();
            });
            sleep_ms(1);
            push2(2);
            wg2.done();
        });
        wg.wait();

        let result = list.lock().unwrap().clone();
        result
    }

    #[test]
    fn worker_pool_single_worker() {
        let result = run_worker_pool_subtest(|workers, tasks| workers < 1 && tasks > 0);
        assert_eq!(result, vec![1, 2, 3, 4]);
    }

    #[test]
    fn worker_pool_two_workers() {
        let result = run_worker_pool_subtest(|workers, tasks| workers < 2 && tasks > 0);
        assert_eq!(result, vec![1, 3, 2, 4]);
    }

    #[test]
    fn worker_pool_tolerate_one_pending_task() {
        let result = run_worker_pool_subtest(|workers, tasks| workers < 2 && tasks > 1);
        assert_eq!(result, vec![1, 2, 3, 4]);
    }

    // -------------------------------------------------------------
    // Source: `TestEncodedPassword` (`utils_test.go:282`).
    // -------------------------------------------------------------

    #[test]
    fn encoded_password_test() {
        let hash_string = "*3D56A309CD04FA2EEF181462E59011F075C89548";
        let hash_caching_string =
            "0123456789012345678901234567890123456789012345678901234567890123456789";
        let mut opt = AuthOption {
            by_auth_string: false,
            auth_string: "xxx".to_owned(),
            by_hash_string: false,
            hash_string: hash_string.to_owned(),
            auth_plugin: String::new(),
        };

        let (pwd, ok) = encoded_password(Some(&opt), "");
        assert!(ok);
        assert_eq!(pwd, opt.hash_string);

        opt.hash_string = "not-good-password-format".to_owned();
        let (_, ok) = encoded_password(Some(&opt), "");
        assert!(!ok);

        opt.by_auth_string = true;
        // mysql_native_password
        let (pwd, ok) = encoded_password(Some(&opt), "");
        assert!(ok);
        assert_eq!(pwd, hash_string);

        // caching_sha2_password
        opt.hash_string = hash_caching_string.to_owned();
        let (pwd, ok) = encoded_password(Some(&opt), AUTH_CACHING_SHA2_PASSWORD);
        assert!(ok);
        assert_eq!(pwd.len(), SHA_PWD_HASH_LEN);

        opt.auth_string = String::new();
        let (pwd, ok) = encoded_password(Some(&opt), "");
        assert!(ok);
        assert_eq!(pwd, "");
    }
}
