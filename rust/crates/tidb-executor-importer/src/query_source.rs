//! Go `pkg/executor/importer/query_source.go`: the SELECT of IMPORT INTO ...
//! FROM SELECT as the source of an import that a DXF worker runs.
//!
//! What needs a session (`CaptureQuerySource`, `RestoreStates`,
//! `BuildMinimalInfoSchema`, `ParseImportQuery`) lives with the session
//! (`tidb-session`'s `import_query`), over the types and rules here.

use serde::{Deserialize, Deserializer, Serialize};

/// Go `QuerySource`: the SELECT of IMPORT INTO ... FROM SELECT as the source
/// of an import: what a worker needs to run the same query as the
/// submitter, captured after tenant privilege checks.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct QuerySource {
    #[serde(rename = "ReadTS", default)]
    pub read_ts: u64,
    #[serde(rename = "SchemaVersion", default)]
    pub schema_version: i64,
    /// Go `EncodeTimestamp`: the Unix time in seconds when the query is
    /// captured, which the encoder takes as the session timestamp of the
    /// query chunks.
    #[serde(rename = "EncodeTimestamp", default)]
    pub encode_timestamp: i64,
    /// Go `Tables`: the included source tables.
    #[serde(rename = "Tables", default, deserialize_with = "null_default", serialize_with = "nil_when_empty")]
    pub tables: Vec<SourceTable>,
    /// Go `SessionStates`: the states of the submitting session.
    #[serde(rename = "SessionStates", default)]
    pub session_states: Option<tidb_sessionstates::SessionStates>,
    /// Go `InMViewMaintenance`: whether the submitter runs in MV
    /// maintenance, which the session states do not carry.
    #[serde(rename = "InMViewMaintenance", default, skip_serializing_if = "std::ops::Not::not")]
    pub in_mview_maintenance: bool,
}

/// Go `SourceTable`: a source table definition of an import query.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceTable {
    #[serde(rename = "DBID", default)]
    pub db_id: i64,
    #[serde(rename = "ID", default)]
    pub id: i64,
    #[serde(rename = "Revision", default)]
    pub revision: u64,
}

fn null_default<'de, D: Deserializer<'de>, T: Default + Deserialize<'de>>(d: D) -> Result<T, D::Error> {
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

/// Go marshals a nil slice as `null`.
fn nil_when_empty<S: serde::Serializer>(tables: &[SourceTable], s: S) -> Result<S::Ok, S::Error> {
    if tables.is_empty() {
        s.serialize_none()
    } else {
        s.collect_seq(tables)
    }
}

/// Go `importQueryWorkerVars`: decided by the worker instead of the
/// submitter: the snapshot and transaction it reads in, and its memory
/// budget.
pub const IMPORT_QUERY_WORKER_VARS: &[&str] = &[
    tidb_vardef::tidb_vars::TIDB_SNAPSHOT,
    tidb_vardef::tidb_vars::TIDB_READ_STALENESS,
    tidb_vardef::tidb_vars::TIDB_TXN_READ_TS,
    tidb_vardef::tidb_vars::TIDB_ENABLE_EXTERNAL_TS_READ,
    // Go `vardef.AutoCommit`.
    "autocommit",
    tidb_vardef::tidb_vars::TIDB_TXN_MODE,
    // Go `vardef.TxnIsolation`, `TransactionIsolation`, `TxReadOnly`,
    // `TransactionReadOnly`.
    "tx_isolation",
    "transaction_isolation",
    "tx_read_only",
    "transaction_read_only",
    tidb_vardef::tidb_vars::TIDB_MEM_QUOTA_QUERY,
];

/// The session `RestoreStates` restores into: Go `sctx.GetSessionVars()
/// .Status()` and `sctx.DecodeStates`.
pub trait QueryStatesSession {
    /// Go `SessionVars.Status()`.
    fn status(&self) -> u32;
    /// Go `sctx.DecodeStates(ctx, states)`.
    fn decode_states(&mut self, states: &tidb_sessionstates::SessionStates) -> Result<(), String>;
}

impl QuerySource {
    /// Go `QuerySource.RestoreStates`: restores the captured session states
    /// into the worker session, except the variables in
    /// [`IMPORT_QUERY_WORKER_VARS`].
    pub fn restore_states(&self, sctx: &mut dyn QueryStatesSession) -> Result<(), String> {
        let captured = self.session_states.as_ref().ok_or("import query has no session states")?;
        let mut states = captured.clone();
        states.system_vars = captured
            .system_vars
            .iter()
            .filter(|(name, _)| !IMPORT_QUERY_WORKER_VARS.contains(&name.as_str()))
            // Internal session variables cannot be set in a session.
            .filter(|(name, _)| {
                !tidb_variable::sysvar::get_sys_var(name).is_some_and(|sv| sv.is_internal_session_variable())
            })
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        // The status tells whether the submitter is in a transaction, which
        // the worker's session is not, and the warnings belong to the
        // statements of the submitter before.
        states.status = sctx.status();
        states.warnings = Vec::new();
        sctx.decode_states(&states)
    }
}

impl QuerySource {
    /// Go `QuerySource.BuildMinimalInfoSchema`: an info schema that holds
    /// only the source tables at `q.ReadTS`, read from `snapshot` (Go
    /// `meta.NewReader(store.GetSnapshot(q.ReadTS))`), failing if a table
    /// changed after the submitter resolved the query. Its tables are the
    /// caller's to build (the domain's catalog builder).
    pub fn build_minimal_info_schema<S: tidb_infoschema::cluster_catalog::MetaSnapshot + ?Sized>(
        &self,
        snapshot: &mut S,
        parent: &tidb_infoschema::SessionExtendedInfoSchema,
    ) -> Result<tidb_infoschema::SessionExtendedInfoSchema, String> {
        let mut db_by_id: std::collections::HashMap<i64, tidb_model::GoShared<tidb_model::db::DBInfo>> =
            std::collections::HashMap::new();
        let mut dbs: Vec<tidb_model::GoShared<tidb_model::db::DBInfo>> = Vec::with_capacity(1);
        let mut tables: std::collections::HashMap<i64, Vec<Option<tidb_model::GoShared<tidb_model::TableInfo>>>> =
            std::collections::HashMap::new();
        for src in &self.tables {
            if !db_by_id.contains_key(&src.db_id) {
                let db = tidb_infoschema::cluster_catalog::with_meta_reader(snapshot, |m| m.database(src.db_id))
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| format!("source database {} not found at the import query snapshot", src.db_id))?;
                let db = tidb_model::GoShared::new(db);
                db_by_id.insert(src.db_id, db.clone());
                dbs.push(db);
            }
            let tbl = tidb_infoschema::cluster_catalog::with_meta_reader(snapshot, |m| m.table(src.db_id, src.id))
                .map_err(|e| e.to_string())?;
            // The submitter resolved the query with this revision.
            let Some(mut tbl) = tbl.filter(|tbl| tbl.revision == src.revision) else {
                return Err(format!("source table {} changed before the import query snapshot", src.id));
            };
            tidb_infoschema::cluster_catalog::normalize_full_loaded_table_info(&mut tbl);
            let db_id = db_by_id[&src.db_id].read().id;
            tbl.db_id = db_id;
            tbl.table_cache_status_type = tidb_model::TableCacheStatusType::DISABLE;
            tables.entry(src.db_id).or_default().push(Some(tidb_model::GoShared::new(tbl)));
        }
        for (db_id, db) in &db_by_id {
            db.write().deprecated_tables =
                tidb_model::GoSharedPointerSlice::from_handles(tables.remove(db_id).unwrap_or_default());
        }
        let mut is = tidb_infoschema::SessionExtendedInfoSchema::init_with_db_infos_for_cross_ks(
            dbs,
            parent.policies().cloned().collect(),
            Vec::new(),
            self.schema_version,
        );
        is.set_schema_ts(self.read_ts);
        Ok(is)
    }
}

/// Go `hasSequenceFunc`: whether `node` calls a sequence function.
pub fn has_sequence_func<N: tidb_ast::Visitable + Clone>(node: &N) -> bool {
    struct SequenceFuncFinder {
        found: bool,
    }
    impl tidb_ast::Visitor for SequenceFuncFinder {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(tidb_ast::Expr::Func { name, .. }) = node.downcast_ref::<tidb_ast::Expr>() {
                if matches!(tidb_hack::go_to_lower(name).as_str(), "nextval" | "lastval" | "setval") {
                    self.found = true;
                }
            }
            self.found
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut node = node.clone();
    let mut v = SequenceFuncFinder { found: false };
    node.accept(&mut v);
    v.found
}

/// What [`check_query_source_table`] reads of a source table: Go
/// `tbl.Meta()` and `tbl.Type().IsVirtualTable()`.
pub struct QuerySourceTableKind {
    pub temporary: bool,
    pub virtual_table: bool,
    pub sequence: bool,
}

/// Go `checkQuerySourceTable`: rejects a source table that a worker cannot
/// read the same way as the submitter, including temporary tables,
/// sequences, and virtual tables.
pub fn check_query_source_table(tbl: &QuerySourceTableKind) -> Result<(), tidb_terror::TerrorError> {
    let kind = if tbl.temporary {
        "temporary source tables"
    } else if tbl.virtual_table {
        "virtual source tables"
    } else if tbl.sequence {
        "sequences"
    } else {
        return Ok(());
    };
    Err(tidb_plannererrors::ERR_NOT_SUPPORTED_YET
        .gen_with_stack_by_args(&[format!("{kind} in IMPORT INTO FROM SELECT").into()]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_source_json_matches_go() {
        let q = QuerySource::default();
        assert_eq!(
            serde_json::to_string(&q).unwrap(),
            r#"{"ReadTS":0,"SchemaVersion":0,"EncodeTimestamp":0,"Tables":null,"SessionStates":null}"#
        );
        let q = QuerySource {
            read_ts: 7,
            tables: vec![SourceTable { db_id: 1, id: 2, revision: 3 }],
            in_mview_maintenance: true,
            ..QuerySource::default()
        };
        let text = serde_json::to_string(&q).unwrap();
        assert_eq!(
            text,
            r#"{"ReadTS":7,"SchemaVersion":0,"EncodeTimestamp":0,"Tables":[{"DBID":1,"ID":2,"Revision":3}],"SessionStates":null,"InMViewMaintenance":true}"#
        );
        let back = serde_json::from_str::<QuerySource>(&text).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), text);
        let empty = serde_json::from_str::<QuerySource>(r#"{"Tables":null}"#).unwrap();
        assert!(empty.tables.is_empty() && empty.session_states.is_none() && !empty.in_mview_maintenance);
    }
}
