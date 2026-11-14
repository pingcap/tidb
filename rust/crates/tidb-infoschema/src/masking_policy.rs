// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `pkg/infoschema/infoschema.go`: snapshot-aware lazy masking policies.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};
use tidb_ast::{CiString, MaskingPolicyRestrictOps};
use tidb_datatype::Datum;
use tidb_model::{MaskingPolicyInfo, MaskingPolicyStatus, MaskingPolicyType, SchemaState};
use tidb_terror::MysqlError;

/// Go `factory`'s session resource. Dropping it closes the acquired resource.
pub trait MaskingPolicySession: Send {
    /// Go `ExecRestrictedSQL` with `ExecOptionUseCurSession` and snapshot TS.
    fn exec_restricted_sql(
        &self,
        snapshot_ts: u64,
        sql: &str,
        table_ids: &[i64],
    ) -> Result<Vec<Vec<Datum>>, MysqlError>;
}

/// Go `infoSchema.factory` (the domain's system-session pool factory).
pub type MaskingPolicySessionFactory =
    Arc<dyn Fn() -> Result<Box<dyn MaskingPolicySession>, MysqlError> + Send + Sync>;

#[derive(Clone, Debug, Default)]
struct State {
    loaded: bool,
    loading: bool,
    policies: BTreeMap<i64, BTreeMap<i64, MaskingPolicyInfo>>,
}

/// Go `infoSchema`'s masking-policy mutex, map and loading channel.
#[derive(Default)]
pub struct MaskingPolicyCache {
    state: Mutex<State>,
    loaded: Condvar,
    factory: Option<MaskingPolicySessionFactory>,
    snapshot_ts: u64,
}

impl std::fmt::Debug for MaskingPolicyCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaskingPolicyCache")
            .field("snapshot_ts", &self.snapshot_ts)
            .finish_non_exhaustive()
    }
}

impl MaskingPolicyCache {
    /// Go `Builder.InitWithOldInfoSchema`: copy maps and loaded state, never
    /// the in-flight loading channel. Session snapshots share this cache;
    /// only a builder creates an independent copy.
    pub fn clone_for_builder(&self) -> Self {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if state.loading {
            state.loaded = false;
        }
        state.loading = false;
        Self {
            state: Mutex::new(state),
            loaded: Condvar::new(),
            factory: self.factory.clone(),
            snapshot_ts: self.snapshot_ts,
        }
    }

    /// Go `Builder.Build(schemaTS)` and `NewBuilder(..., factory, ...)`.
    pub fn with_factory_and_ts(
        mut self,
        factory: Option<MaskingPolicySessionFactory>,
        ts: u64,
    ) -> Self {
        self.factory = factory;
        self.snapshot_ts = ts;
        self
    }

    /// Go `Builder.Build`: snapshot used by a later lazy load.
    pub fn set_snapshot_ts(&mut self, ts: u64) {
        self.snapshot_ts = ts;
    }

    /// Go `infoSchema.resetMaskingPolicyCache` on the builder's new image.
    pub fn reset_masking_policy_cache(&mut self) {
        *self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = State::default();
    }

    /// Go `infoSchema.loadMaskingPoliciesIfNeeded`: one reader loads, others
    /// wait. A generic failure is retried on the next access, a missing table
    /// is an empty loaded cache until the next schema reset.
    fn load_masking_policies_if_needed(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if state.loaded {
                return;
            }
            let Some(factory) = &self.factory else {
                state.loaded = true;
                return;
            };
            if state.loading {
                state = self
                    .loaded
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                continue;
            }
            state.loading = true;
            drop(state);
            let result = load_masking_policies(factory, self.snapshot_ts);
            state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match result {
                Ok(policies) if !state.loaded => {
                    let mut map = BTreeMap::<_, BTreeMap<_, _>>::new();
                    for policy in policies {
                        map.entry(policy.table_id)
                            .or_default()
                            .insert(policy.column_id, policy);
                    }
                    state.policies = map;
                    state.loaded = true;
                }
                Err(error)
                    if error.identity
                        == Some(crate::infoschema_errors::ERR_TABLE_NOT_EXISTS.identity()) =>
                {
                    state.loaded = true
                }
                // Do not log arbitrary storage error text (it may contain credentials).
                Err(_) | Ok(_) => {}
            }
            state.loading = false;
            self.loaded.notify_all();
            return;
        }
    }

    /// Go `infoSchema.CloneMaskingPoliciesByTableColumn`.
    pub fn clone_masking_policies_by_table_column(
        &self,
    ) -> BTreeMap<i64, BTreeMap<i64, MaskingPolicyInfo>> {
        self.load_masking_policies_if_needed();
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .policies
            .clone()
    }

    /// Go `infoSchema.MaskingPolicyByTableColumn`.
    pub fn masking_policy_by_table_column(
        &self,
        table_id: i64,
        column_id: i64,
    ) -> Option<MaskingPolicyInfo> {
        self.load_masking_policies_if_needed();
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .policies
            .get(&table_id)?
            .get(&column_id)
            .cloned()
    }

    /// Go `infoSchema.MaskingPolicyByID`.
    pub fn masking_policy_by_id(&self, id: i64) -> Option<MaskingPolicyInfo> {
        self.load_masking_policies_if_needed();
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .policies
            .values()
            .flat_map(|columns| columns.values())
            .find(|policy| policy.id == id)
            .cloned()
    }

    /// Go `infoSchema.MaskingPolicyByName`: ambiguous names return no policy.
    pub fn masking_policy_by_name(&self, name: &CiString) -> Option<MaskingPolicyInfo> {
        self.load_masking_policies_if_needed();
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut found = state
            .policies
            .values()
            .flat_map(|columns| columns.values())
            .filter(|p| p.name.lowercase() == name.lowercase());
        let policy = found.next()?.clone();
        if found.next().is_some() {
            None
        } else {
            Some(policy)
        }
    }

    /// Go `infoSchema.AllMaskingPolicies`: stable lowercase-name / ID order.
    pub fn all_masking_policies(&self) -> Vec<MaskingPolicyInfo> {
        self.load_masking_policies_if_needed();
        let mut policies: Vec<_> = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .policies
            .values()
            .flat_map(|columns| columns.values().cloned())
            .collect();
        policies.sort_by(|a, b| {
            a.name
                .lowercase()
                .cmp(b.name.lowercase())
                .then(a.id.cmp(&b.id))
        });
        policies
    }
}

/// Go `needRefreshMaskingPoliciesForTableDiff`.
pub fn need_refresh_masking_policies_for_table_diff(tp: tidb_model::ActionType) -> bool {
    use tidb_model::ActionType as A;
    matches!(
        tp,
        A::ACTION_CREATE_MASKING_POLICY
            | A::ACTION_ALTER_MASKING_POLICY
            | A::ACTION_DROP_MASKING_POLICY
            | A::ACTION_DROP_TABLE
            | A::ACTION_DROP_COLUMN
            | A::ACTION_MODIFY_COLUMN
            | A::ACTION_RENAME_TABLE
            | A::ACTION_RENAME_TABLES
            | A::ACTION_TRUNCATE_TABLE
            | A::ACTION_DROP_SCHEMA
    )
}

/// Go `LoadMaskingPolicies`.
pub fn load_masking_policies(
    factory: &MaskingPolicySessionFactory,
    snapshot_ts: u64,
) -> Result<Vec<MaskingPolicyInfo>, MysqlError> {
    load_masking_policies_with_table_ids(factory, &[], snapshot_ts)
}

/// Go `normalizeMaskingPolicyTableIDs`.
pub fn normalize_masking_policy_table_ids(table_ids: &[i64]) -> (Vec<i64>, bool) {
    let mut ids: Vec<_> = table_ids.iter().copied().filter(|id| *id > 0).collect();
    ids.sort_unstable();
    ids.dedup();
    (ids, !table_ids.is_empty())
}

/// Go `buildLoadMaskingPoliciesQuery`.
pub fn build_load_masking_policies_query(table_ids: &[i64]) -> String {
    let mut query = "SELECT policy_id, policy_name, db_name, table_name, table_id, column_name, column_id, expression, status, masking_type, restrict_on, created_at, updated_at, created_by\nFROM mysql.tidb_masking_policy".to_owned();
    if !table_ids.is_empty() {
        query.push_str(" WHERE table_id IN (");
        query.push_str(&vec!["%?"; table_ids.len()].join(", "));
        query.push(')');
    }
    query.push_str(" ORDER BY table_id, column_id, policy_id");
    query
}

/// Go `loadMaskingPoliciesWithTableIDs`: acquire/close one resource, query
/// in batches of 1024 at the schema's snapshot, then order by IDs.
pub fn load_masking_policies_with_table_ids(
    factory: &MaskingPolicySessionFactory,
    table_ids: &[i64],
    snapshot_ts: u64,
) -> Result<Vec<MaskingPolicyInfo>, MysqlError> {
    let session = factory()?;
    let (ids, filtered) = normalize_masking_policy_table_ids(table_ids);
    let mut policies = Vec::new();
    let mut load = |ids: &[i64]| -> Result<(), MysqlError> {
        for row in session.exec_restricted_sql(
            snapshot_ts,
            &build_load_masking_policies_query(ids),
            ids,
        )? {
            policies.push(masking_policy_info_from_chunk_row(&row).map_err(MysqlError::unknown)?);
        }
        Ok(())
    };
    if filtered {
        for batch in ids.chunks(1024) {
            load(batch)?;
        }
    } else {
        load(&[])?;
    }
    policies.sort_by_key(|p| (p.table_id, p.column_id, p.id));
    Ok(policies)
}

/// Go `maskingPolicyInfoFromChunkRow`.
pub fn masking_policy_info_from_chunk_row(row: &[Datum]) -> Result<MaskingPolicyInfo, String> {
    let text = |index: usize| match &row[index] {
        Datum::String(s) => String::from_utf8_lossy(s.bytes()).into_owned(),
        Datum::Bytes(s) => String::from_utf8_lossy(s).into_owned(),
        _ => String::new(),
    };
    let int = |index: usize| match row[index] {
        Datum::Int(n) => n,
        Datum::UInt(n) => n as i64,
        _ => 0,
    };
    let mut policy = MaskingPolicyInfo::default();
    policy.id = int(0);
    policy.name = CiString::new(text(1));
    policy.db_name = CiString::new(text(2));
    policy.table_name = CiString::new(text(3));
    policy.table_id = int(4);
    policy.column_name = CiString::new(text(5));
    policy.column_id = int(6);
    policy.expression = text(7);
    policy.status = match text(8).trim().to_uppercase().as_str() {
        "ENABLE" | "ENABLED" => MaskingPolicyStatus::ENABLE,
        "DISABLE" | "DISABLED" => MaskingPolicyStatus::DISABLE,
        _ => return Err(format!("unknown masking policy status: {}", text(8))),
    };
    let mut ops = MaskingPolicyRestrictOps::default();
    for token in text(10).trim().to_uppercase().split(',') {
        let trimmed = token.trim();
        if trimmed != "NONE" && !trimmed.is_empty() && !ops.insert_name(trimmed) {
            return Err(format!("unknown masking policy restrict option: {token}"));
        }
    }
    policy.restrict_ops = ops;
    for (index, target) in [(11, &mut policy.created_at), (12, &mut policy.updated_at)] {
        if let Datum::Time(time) = &row[index] {
            *target = time
                .clone()
                .go_time(&chrono::Local)
                .map_err(|err| err.to_string())?
                .fixed_offset();
        }
    }
    let tp = MaskingPolicyType::new(&text(9).trim().to_uppercase());
    if ![
        MaskingPolicyType::FULL,
        MaskingPolicyType::PARTIAL,
        MaskingPolicyType::NULL,
        MaskingPolicyType::DATE,
        MaskingPolicyType::CUSTOM,
    ]
    .contains(&tp)
    {
        return Err(format!("unknown masking policy type: {}", text(9)));
    }
    policy.masking_type = tp;
    policy.created_by = text(13);
    policy.state = SchemaState::PUBLIC;
    Ok(policy)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn policy(id: i64, table: i64, col: i64, name: &str) -> MaskingPolicyInfo {
        MaskingPolicyInfo {
            id,
            table_id: table,
            column_id: col,
            name: CiString::new(name),
            ..Default::default()
        }
    }

    /// Go `TestLoadMaskingPoliciesRetryOnGenericError`.
    #[test]
    fn test_load_masking_policies_retry_on_generic_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let cache = MaskingPolicyCache::default().with_factory_and_ts(
            Some(Arc::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
                Err(MysqlError::unknown("temporary load failure"))
            })),
            0,
        );
        for n in 1..=2 {
            cache.load_masking_policies_if_needed();
            assert!(!cache.state.lock().unwrap().loaded);
            assert_eq!(calls.load(Ordering::SeqCst), n);
        }
    }

    /// Go `TestLoadMaskingPoliciesNoRetryWhenTableNotReady`.
    #[test]
    fn test_load_masking_policies_no_retry_when_table_not_ready() {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let cache = MaskingPolicyCache::default().with_factory_and_ts(
            Some(Arc::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
                Err(MysqlError::from_terror(
                    &crate::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .gen_with_stack_by_args(&["mysql".into(), "tidb_masking_policy".into()]),
                ))
            })),
            0,
        );
        cache.load_masking_policies_if_needed();
        assert!(cache.state.lock().unwrap().loaded);
        cache.load_masking_policies_if_needed();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// Go `TestInitWithOldInfoSchemaCopiesMaskingLoadedState`.
    #[test]
    fn test_init_with_old_info_schema_copies_masking_loaded_state() {
        let old = MaskingPolicyCache::default();
        {
            let mut state = old.state.lock().unwrap();
            state.loaded = true;
            state
                .policies
                .entry(42)
                .or_default()
                .insert(7, policy(1, 42, 7, "p"));
        }
        let mut new = old.clone_for_builder();
        assert!(new.state.lock().unwrap().loaded);
        assert_eq!(new.masking_policy_by_table_column(42, 7).unwrap().id, 1);
        new.reset_masking_policy_cache();
        assert!(new.all_masking_policies().is_empty());
        assert_eq!(old.all_masking_policies().len(), 1);
    }

    /// Go `TestInitWithOldInfoSchemaDoesNotInheritMaskingLoadChannel`.
    #[test]
    fn test_init_with_old_info_schema_does_not_inherit_masking_load_channel() {
        let old = MaskingPolicyCache::default();
        {
            let mut state = old.state.lock().unwrap();
            state.loading = true;
            state
                .policies
                .entry(42)
                .or_default()
                .insert(7, policy(1, 42, 7, "p"));
        }
        let new = old.clone_for_builder();
        let state = new.state.lock().unwrap();
        assert!(!state.loading);
        assert!(!state.loaded);
        assert_eq!(state.policies[&42][&7].id, 1);
    }

    /// Go `TestMaskingPolicyByNameAmbiguous`.
    #[test]
    fn test_masking_policy_by_name_ambiguous() {
        let cache = MaskingPolicyCache::default();
        {
            let mut state = cache.state.lock().unwrap();
            state.loaded = true;
            state
                .policies
                .entry(101)
                .or_default()
                .insert(11, policy(1, 101, 11, "dup"));
            state
                .policies
                .entry(202)
                .or_default()
                .insert(22, policy(2, 202, 22, "dup"));
        }
        assert!(cache
            .masking_policy_by_name(&CiString::new("dup"))
            .is_none());
    }

    /// Go `TestNormalizeMaskingPolicyTableIDs`.
    #[test]
    fn test_normalize_masking_policy_table_ids() {
        assert_eq!(normalize_masking_policy_table_ids(&[]), (vec![], false));
        assert_eq!(
            normalize_masking_policy_table_ids(&[0, -1, 42, 42, 7]),
            (vec![7, 42], true)
        );
        assert_eq!(normalize_masking_policy_table_ids(&[0, -1]), (vec![], true));
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn row(id: i64, name: &str, status: &str) -> Vec<Datum> {
        let text = |s: &str| Datum::Bytes(s.as_bytes().to_vec());
        vec![
            Datum::Int(id),
            text(name),
            text("test"),
            text("t"),
            Datum::Int(id),
            text("c"),
            Datum::Int(1),
            text("`c`"),
            text(status),
            text("CUSTOM"),
            text("NONE"),
            Datum::Null,
            Datum::Null,
            Datum::Null,
        ]
    }

    struct SnapshotSession {
        closed: Arc<AtomicUsize>,
    }
    impl Drop for SnapshotSession {
        fn drop(&mut self) {
            self.closed.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl MaskingPolicySession for SnapshotSession {
        fn exec_restricted_sql(
            &self,
            ts: u64,
            sql: &str,
            ids: &[i64],
        ) -> Result<Vec<Vec<Datum>>, MysqlError> {
            assert_eq!(sql, build_load_masking_policies_query(ids));
            Ok(vec![
                row(1, "Z", if ts == 10 { "ENABLED" } else { "DISABLED" }),
                row(2, "a", "ENABLED"),
            ])
        }
    }

    /// Go `TestMaskingPolicySnapshotAndLookup` (zzprobe/d01): LoadMaskingPolicies
    /// reads ENABLED at the prior snapshot, DISABLED at current; All sorts a
    /// before Z, both lookup forms identify Z case-insensitively.
    /// Go `TestLoadMaskingPoliciesWithTableIDs` (zzprobe/d01).
    #[test]
    fn test_load_masking_policies_with_table_ids() {
        struct RecordingSession {
            batches: Arc<Mutex<Vec<Vec<i64>>>>,
            closed: Arc<AtomicUsize>,
        }
        impl Drop for RecordingSession {
            fn drop(&mut self) {
                self.closed.fetch_add(1, Ordering::SeqCst);
            }
        }
        impl MaskingPolicySession for RecordingSession {
            fn exec_restricted_sql(
                &self,
                ts: u64,
                sql: &str,
                ids: &[i64],
            ) -> Result<Vec<Vec<Datum>>, MysqlError> {
                assert_eq!(ts, 0);
                assert_eq!(sql, build_load_masking_policies_query(ids));
                self.batches.lock().unwrap().push(ids.to_vec());
                Ok(if ids.contains(&1) {
                    vec![row(1, "p", "ENABLED")]
                } else {
                    Vec::new()
                })
            }
        }
        let batches = Arc::new(Mutex::new(Vec::new()));
        let closed = Arc::new(AtomicUsize::new(0));
        let factory: MaskingPolicySessionFactory = Arc::new({
            let batches = Arc::clone(&batches);
            let closed = Arc::clone(&closed);
            move || {
                Ok(Box::new(RecordingSession {
                    batches: Arc::clone(&batches),
                    closed: Arc::clone(&closed),
                }))
            }
        });
        let ids: Vec<_> = [0, -1, 1, 1].into_iter().chain(1..=1025).collect();
        let policies = load_masking_policies_with_table_ids(&factory, &ids, 0).unwrap();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].name.original(), "p");
        assert_eq!(
            *batches.lock().unwrap(),
            [(1..=1024).collect::<Vec<_>>(), vec![1025]]
        );
        assert_eq!(closed.load(Ordering::SeqCst), 1);
        batches.lock().unwrap().clear();
        assert!(load_masking_policies_with_table_ids(&factory, &[0, -1], 0)
            .unwrap()
            .is_empty());
        assert!(batches.lock().unwrap().is_empty());
        assert_eq!(closed.load(Ordering::SeqCst), 2);
    }

    /// Go `TestMaskingPolicyConcurrentReaders` (zzprobe/d01).
    #[test]
    fn test_masking_policy_concurrent_readers() {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let factory: MaskingPolicySessionFactory = Arc::new(move || {
            count.fetch_add(1, Ordering::SeqCst);
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Ok(Box::new(SnapshotSession {
                closed: Arc::new(AtomicUsize::new(0)),
            }))
        });
        let cache = Arc::new(MaskingPolicyCache::default().with_factory_and_ts(Some(factory), 10));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let cache = Arc::clone(&cache);
                std::thread::spawn(move || assert_eq!(cache.all_masking_policies().len(), 2))
            })
            .collect();
        entered_rx.recv().unwrap();
        release_tx.send(()).unwrap();
        for reader in readers {
            reader.join().unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_masking_policy_snapshot_and_lookup() {
        let closed = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&closed);
        let factory: MaskingPolicySessionFactory = Arc::new(move || {
            Ok(Box::new(SnapshotSession {
                closed: Arc::clone(&count),
            }))
        });
        assert_eq!(
            load_masking_policies(&factory, 10).unwrap()[0].status,
            MaskingPolicyStatus::ENABLE
        );
        assert_eq!(
            load_masking_policies(&factory, 0).unwrap()[0].status,
            MaskingPolicyStatus::DISABLE
        );
        assert_eq!(closed.load(Ordering::SeqCst), 2);
        let cache = MaskingPolicyCache::default().with_factory_and_ts(Some(factory), 10);
        assert_eq!(
            cache
                .all_masking_policies()
                .iter()
                .map(|p| p.name.original())
                .collect::<Vec<_>>(),
            ["a", "Z"]
        );
        assert_eq!(
            cache
                .masking_policy_by_table_column(1, 1)
                .unwrap()
                .name
                .original(),
            "Z"
        );
        assert_eq!(
            cache
                .masking_policy_by_name(&CiString::new("z"))
                .unwrap()
                .name
                .original(),
            "Z"
        );
        assert_eq!(cache.masking_policy_by_id(1).unwrap().name.original(), "Z");
        assert!(cache.masking_policy_by_id(-1).is_none());
        assert_eq!(
            closed.load(Ordering::SeqCst),
            3,
            "all lookups share one load and close its resource"
        );
        let mut invalid = row(1, "Z", "DISABLED");
        invalid[9] = Datum::Bytes(b"bad".to_vec());
        assert_eq!(
            masking_policy_info_from_chunk_row(&invalid).unwrap_err(),
            "unknown masking policy type: bad"
        );
    }
}
