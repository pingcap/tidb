//! Keeps a query's cancellation lease alive exactly as long as its result set.

use super::*;

pub(crate) fn complete_real_tikv_query<'a>(
    query: RealTiKvQuery,
    cancellation_lease: QueryCancellationLease,
) -> QueryResult<'a> {
    QueryResult::new(Box::new(QueryLifecycleResultSet {
        inner: query.into_record_set(),
        _cancellation_lease: cancellation_lease,
    }))
}

struct QueryLifecycleResultSet {
    inner: DistSqlRecordSet,
    _cancellation_lease: QueryCancellationLease,
}

impl ResultSetSource for QueryLifecycleResultSet {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
        self.inner
            .next_batch(max_rows)
            .map_err(|error| error.to_string())
    }

    fn supports_text_batch(&self) -> bool {
        self.inner.supports_text_batch()
    }

    fn next_text_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Option<Box<dyn tidb_exec::distsql_recordset::TextResultBatch>>, String> {
        self.inner
            .next_text_batch(max_rows)
            .map_err(|error| error.to_string())
    }

    fn columns(&mut self) -> Result<Vec<tidb_protocol::ColumnInfo>, String> {
        Ok(self.inner.columns().to_vec())
    }

    fn finish(&mut self) -> Result<(), String> {
        self.inner.finish().map_err(|error| error.to_string())
    }

    fn close(&mut self) -> Result<(), String> {
        self.inner.close().map_err(|error| error.to_string())
    }
}
