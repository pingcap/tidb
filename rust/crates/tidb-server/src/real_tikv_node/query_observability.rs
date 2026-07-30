//! What a running query on this node reports: how many are in flight, the
//! transport evidence one publication carries, and the guard that keeps a
//! query's leases alive exactly as long as its result set.
//!
//! [`QueryActivity`] is this node's half of Go's `SHOW PROCESSLIST`
//! accounting; [`QueryCompletion`] is what makes a dropped result set release
//! its cancellation registration and its activity slot, so a client that
//! disconnects mid-query leaves neither behind. [`ObservedResultSet`] wraps
//! the record set to emit that evidence exactly once, whether the query was
//! read to the end or closed early.

use super::*;

#[derive(Default)]
pub(crate) struct QueryActivity {
    active: AtomicUsize,
    max_active: AtomicUsize,
}

impl QueryActivity {
    pub(crate) fn begin(self: &Arc<Self>, connection_id: u64, query_id: u64) -> QueryActivityLease {
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_active.fetch_max(active, Ordering::AcqRel);
        eprintln!(
            "{{\"event\":\"query_activity\",\"phase\":\"begin\",\"connection_id\":{connection_id},\"query_id\":{query_id},\"active\":{active},\"max_active\":{}}}",
            self.max_active.load(Ordering::Acquire)
        );
        QueryActivityLease {
            activity: Arc::clone(self),
            connection_id,
            query_id,
        }
    }
}

pub(crate) struct QueryActivityLease {
    activity: Arc<QueryActivity>,
    connection_id: u64,
    query_id: u64,
}

pub(crate) fn install_remote_publication_observer<E>(
    snapshot_ts: Option<u64>,
    install: impl FnOnce() -> Result<(), E>,
) -> Result<(), E> {
    if snapshot_ts.is_some() {
        install()?;
    }
    Ok(())
}

impl Drop for QueryActivityLease {
    fn drop(&mut self) {
        let previous = self.activity.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "query activity count underflow");
        eprintln!(
            "{{\"event\":\"query_activity\",\"phase\":\"end\",\"connection_id\":{},\"query_id\":{},\"active\":{},\"max_active\":{}}}",
            self.connection_id,
            self.query_id,
            previous - 1,
            self.activity.max_active.load(Ordering::Acquire)
        );
    }
}

fn emit_query_transport_publication(
    connection_id: u64,
    query_id: u64,
    authority_id: u64,
    session_id: u64,
    published: &PublishedDispatchEvidence,
) {
    let publication = &published.publication;
    let forwarded_host = publication
        .forwarded_host()
        .map_or_else(|| "null".to_owned(), |host| format!("{host:?}"));
    eprintln!(
        "{{\"event\":\"query_transport_published\",\"connection_id\":{connection_id},\"query_id\":{query_id},\"authority_id\":{authority_id},\"session_id\":{session_id},\"region_id\":{},\"physical_address\":{:?},\"physical_channel_version\":{},\"stream_generation\":{},\"forwarded_host\":{forwarded_host}}}",
        published.region_id,
        publication.physical_address(),
        publication.physical_channel_version(),
        publication.batch_stream_generation(),
    );
}

pub(crate) fn observe_real_tikv_query<'a>(
    context: &SessionContext,
    query: RealTiKvQuery,
    query_id: u64,
    cancellation_lease: QueryCancellationLease,
    query_activity: QueryActivityLease,
    cluster_id: u64,
    evidence: DirectUnaryTransportEvidenceHandle,
) -> Result<QueryResult<'a>, SqlQueryError> {
    let snapshot_ts = query.snapshot_ts();
    let snapshot_ts_json =
        snapshot_ts.map_or_else(|| "null".to_owned(), |timestamp| timestamp.to_string());
    let table_id = query.table_id();
    let identity = query.session_identity();
    let executor_kinds = query
        .plan_evidence()
        .executor_kinds()
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>();
    let predicate_count = query.plan_evidence().predicate_count();
    let output_offsets = query.plan_evidence().output_offsets().to_vec();
    let handle_range_count = query.plan_evidence().handle_range_count();
    let handle_ranges = query
        .plan_evidence()
        .handle_ranges()
        .iter()
        .map(|range| {
            format!(
                "{{\"low\":{},\"high\":{},\"low_exclude\":{},\"high_exclude\":{}}}",
                range.low(),
                range.high(),
                range.low_exclude(),
                range.high_exclude(),
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let connection_id = context.connection_id;
    let authority_id = identity.authority_id();
    let session_id = identity.session_id();
    install_remote_publication_observer(snapshot_ts, || {
        evidence.set_publication_observer(move |published| {
            emit_query_transport_publication(
                connection_id,
                query_id,
                authority_id,
                session_id,
                published,
            );
        })
    })
    .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    eprintln!(
        "{{\"event\":\"query_snapshot\",\"connection_id\":{},\"query_id\":{query_id},\"authority_id\":{},\"session_id\":{},\"cluster_id\":{cluster_id},\"snapshot_ts\":{snapshot_ts_json},\"table_id\":{table_id},\"executor_kinds\":{executor_kinds:?},\"predicate_count\":{predicate_count},\"output_offsets\":{output_offsets:?},\"handle_range_count\":{handle_range_count},\"handle_ranges\":[{handle_ranges}],\"user\":{:?},\"host\":{:?}}}",
        connection_id,
        authority_id,
        session_id,
        context.identity.username(),
        context.identity.host(),
    );
    Ok(QueryResult::new(Box::new(ObservedResultSet {
        inner: query.into_record_set(),
        evidence,
        connection_id,
        query_id,
        authority_id,
        session_id,
        emitted: false,
        _completion: QueryCompletion::new(cancellation_lease, query_activity),
    })))
}

struct ObservedResultSet {
    inner: DistSqlRecordSet,
    evidence: DirectUnaryTransportEvidenceHandle,
    connection_id: u64,
    query_id: u64,
    authority_id: u64,
    session_id: u64,
    emitted: bool,
    _completion: QueryCompletion,
}

/// Keeps cancellation registration and activity accounting alive until one
/// query result is finished or dropped. Multi-relation sessions reuse this
/// guard instead of creating a second lifecycle authority.
pub(crate) struct QueryCompletion {
    _cancellation_lease: QueryCancellationLease,
    _query_activity: QueryActivityLease,
}

impl QueryCompletion {
    pub(crate) const fn new(
        cancellation_lease: QueryCancellationLease,
        query_activity: QueryActivityLease,
    ) -> Self {
        Self {
            _cancellation_lease: cancellation_lease,
            _query_activity: query_activity,
        }
    }
}

impl ObservedResultSet {
    fn emit_evidence(&mut self) {
        if self.emitted {
            return;
        }
        self.emitted = true;
        let evidence = self.evidence.snapshot();
        let located_regions = evidence
            .located_region_ids
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let dispatched_regions = evidence
            .dispatched_region_ids
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        eprintln!(
            "{{\"event\":\"query_transport\",\"connection_id\":{},\"query_id\":{},\"authority_id\":{},\"session_id\":{},\"located_region_ids\":[{located_regions}],\"dispatched_region_ids\":[{dispatched_regions}],\"batch_attempts\":{},\"unary_attempts\":{}}}",
            self.connection_id,
            self.query_id,
            self.authority_id,
            self.session_id,
            evidence.batch_attempts,
            evidence.unary_attempts
        );
    }
}

impl ResultSetSource for ObservedResultSet {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
        self.inner
            .next_batch(max_rows)
            .map_err(|error| error.to_string())
    }

    fn columns(&mut self) -> Result<Vec<tidb_protocol::ColumnInfo>, String> {
        Ok(self.inner.columns().to_vec())
    }

    fn finish(&mut self) -> Result<(), String> {
        let result = self.inner.finish().map_err(|error| error.to_string());
        self.emit_evidence();
        result
    }

    fn close(&mut self) -> Result<(), String> {
        let result = self.inner.close().map_err(|error| error.to_string());
        self.emit_evidence();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_query_completion_owns_activity_and_cancellation_leases() {
        let activity = Arc::new(QueryActivity::default());
        let cancellation = crate::sql_node::ConnectionCancellation::default();
        let cancellation_lease = cancellation.install(Arc::new(CancelHandle::default()));
        let activity_lease = activity.begin(7, 11);
        assert_eq!(activity.active.load(Ordering::Acquire), 1);

        let completion = QueryCompletion::new(cancellation_lease, activity_lease);
        assert_eq!(activity.active.load(Ordering::Acquire), 1);
        drop(completion);
        assert_eq!(activity.active.load(Ordering::Acquire), 0);
    }
}
