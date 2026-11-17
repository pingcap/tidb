// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `conflictedkv/collector.go`: the count, checksum and text of the
//! conflicted rows.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

use tidb_datatype::Datum;
use tidb_dxf_execute::Collector as ProgressCollector;
use tidb_executor::importer::kv_encode::TableKVEncoder;
use tidb_gocontext::Context;
use tidb_lightning_backend_kv::Pairs;
use tidb_lightning_verification::KvChecksum;
use tidb_log::{Field, Value};
use tidb_objstore::objectio::Writer;
use tidb_objstore::storeapi::{Storage as ObjStore, WriterOption};
use tidb_table::tables::TableCommon;
use tidb_txnkv::kv_storage::Storage;

use crate::handler::{BaseHandler, ConflictKVHandler, EncodedRowHandler, PairReceiver, TrafficRecorder};
use crate::row_handle::{BoundedKeySet, KeyFilter};
use crate::{other, Result};

/// Go `MaxConflictRowFileSize`: the size of one conflict row file. A
/// variable for tests.
pub static MAX_CONFLICT_ROW_FILE_SIZE: AtomicI64 = AtomicI64::new(8 << 30);

/// Go `maxTotalConflictRowFileSize`: the hard limit of all conflict row
/// files of one collect-conflicts subtask, hardcoded on purpose until users
/// ask for a knob. A variable for tests.
static MAX_TOTAL_CONFLICT_ROW_FILE_SIZE: AtomicI64 = AtomicI64::new(1 << 30);

/// Go `CollectResult`.
#[derive(Clone)]
pub struct CollectResult {
    /// Go `RowCount`: the conflicted rows.
    pub row_count: i64,
    /// Go `TotalFileSize`.
    pub total_file_size: i64,
    /// Go `RowRecordingCapped`: recording stopped at the total size limit.
    pub row_recording_capped: bool,
    /// Go `Checksum`: of the encoded KVs of the conflicted rows.
    pub checksum: KvChecksum,
    /// Go `Filenames`: the files the conflicted rows are written to.
    pub filenames: Vec<String>,
}

impl CollectResult {
    /// Go `NewCollectResult`.
    #[must_use]
    pub fn new(keyspace: &[u8]) -> Self {
        Self {
            row_count: 0,
            total_file_size: 0,
            row_recording_capped: false,
            checksum: KvChecksum::with_keyspace(keyspace),
            filenames: Vec::with_capacity(1),
        }
    }

    /// Go `Merge`.
    pub fn merge(&mut self, other: &CollectResult) {
        self.row_count += other.row_count;
        self.total_file_size += other.total_file_size;
        self.row_recording_capped |= other.row_recording_capped;
        self.checksum.add(&other.checksum);
        self.filenames.extend(other.filenames.iter().cloned());
    }
}

/// Go `Collector`'s `EncodedRowHandler` part.
struct CollectSink {
    store: Arc<dyn ObjStore>,
    filename_prefix: String,
    result: CollectResult,
    /// Shared by the collectors of one subtask, for one hard limit.
    shared_total_file_size: Arc<AtomicI64>,
    file_seq: usize,
    curr_file_size: i64,
    writer: Option<Box<dyn Writer>>,
    /// Once set, no more rows are written to files.
    stop_recording: bool,
}

/// Go `Collector`.
pub struct Collector {
    handler: ConflictKVHandler<CollectSink>,
}

impl Collector {
    /// Go `NewCollector`.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        target_tbl: TableCommon,
        obj_store: Arc<dyn ObjStore>,
        store: &Arc<dyn Storage>,
        keyspace: &[u8],
        filename_prefix: String,
        kv_group: &str,
        encoder: TableKVEncoder,
        global_set: Arc<RwLock<BoundedKeySet>>,
        local_set: BoundedKeySet,
        shared_total_file_size: Arc<AtomicI64>,
        progress_collector: Option<Arc<dyn ProgressCollector>>,
        traffic_rec: Option<Arc<dyn TrafficRecorder>>,
    ) -> Self {
        let sink = CollectSink {
            store: obj_store,
            filename_prefix,
            result: CollectResult::new(keyspace),
            shared_total_file_size,
            file_seq: 0,
            curr_file_size: 0,
            writer: None,
            stop_recording: false,
        };
        let base = BaseHandler::new(target_tbl, kv_group, Some(encoder), progress_collector);
        let filter = KeyFilter::new(global_set, local_set);
        Self { handler: ConflictKVHandler::for_kv_group(base, store, traffic_rec, Some(filter), sink) }
    }

    /// Go `Run`.
    ///
    /// # Errors
    /// A KV could not be handled.
    pub fn run(&mut self, ctx: &Context, rx: &PairReceiver) -> Result<()> {
        self.handler.pre_run()?;
        self.handler.run(ctx, rx)
    }

    /// Go `Close`: the handler, then the open file.
    ///
    /// # Errors
    /// The buffered rows could not be handled, or the file not closed.
    pub fn close(&mut self, ctx: &Context) -> Result<()> {
        let result = self.handler.close(ctx);
        let close = match self.handler.row_handler.writer.take() {
            Some(mut w) => w.close(ctx).map_err(|e| other(e.to_string())),
            None => Ok(()),
        };
        result.and(close)
    }

    /// Go `GetCollectResult`.
    #[must_use]
    pub fn collect_result(&self) -> &CollectResult {
        &self.handler.row_handler.result
    }

    /// The local key set, to merge once the KV group is done.
    pub fn take_local_set(&mut self) -> Option<BoundedKeySet> {
        self.handler.take_key_filter().map(KeyFilter::into_local_set)
    }
}

impl EncodedRowHandler for CollectSink {
    /// Go `HandleEncodedRow`.
    fn handle_encoded_row(&mut self, ctx: &Context, _: &[u8], row: &[Datum], pairs: &Pairs) -> Result<()> {
        self.record_row_to_file(ctx, row)?;
        self.result.row_count += 1;
        self.result.checksum.update(&pairs.pairs);
        Ok(())
    }
}

impl CollectSink {
    /// Go `recordRowToFile`.
    fn record_row_to_file(&mut self, ctx: &Context, row: &[Datum]) -> Result<()> {
        if self.stop_recording {
            return Ok(());
        }
        let s = tidb_datatype::datums_to_string(row, true, false).map_err(|e| other(e.to_string()))?;
        let content_size = s.len() as i64 + 1;
        // The limit is checked after the add to keep the accounting across
        // collectors simple; a small overflow is accepted.
        let mut limit = MAX_TOTAL_CONFLICT_ROW_FILE_SIZE.load(Ordering::Relaxed);
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/dxf/importinto/conflictedkv/mockTotalConflictRowFileSizeLimit",
            &mut limit,
        );
        let global_total_size = self.shared_total_file_size.fetch_add(content_size, Ordering::SeqCst) + content_size;
        if global_total_size > limit {
            return self.on_total_size_limit_exceeded(ctx, global_total_size, content_size, limit);
        }
        if self.writer.is_none() || self.curr_file_size >= MAX_CONFLICT_ROW_FILE_SIZE.load(Ordering::Relaxed) {
            self.switch_file(ctx)?;
            self.curr_file_size = 0;
        }
        let mut content = s.into_bytes();
        content.push(b'\n');
        self.writer
            .as_mut()
            .expect("switched above")
            .write(ctx, &content)
            .map_err(|e| other(e.to_string()))?;
        self.result.total_file_size += content_size;
        self.curr_file_size += content_size;
        Ok(())
    }

    /// Go `switchFile`.
    fn switch_file(&mut self, ctx: &Context) -> Result<()> {
        if let Some(mut w) = self.writer.take() {
            if let Err(e) = w.close(ctx) {
                tidb_logutil::bg_logger()
                    .warn("failed to close conflict row writer", &[Field::new("error", Value::Str(e.to_string()))]);
                return Err(other(e.to_string()));
            }
        }
        self.file_seq += 1;
        let filename = get_row_file_name(&self.filename_prefix, self.file_seq);
        tidb_logutil::bg_logger().info(
            "switch conflict row file",
            &[
                Field::new("filename", Value::Str(filename.clone())),
                Field::new("lastFileSize", Value::Str(tidb_config_configtypes::bytes_size(self.curr_file_size as f64))),
            ],
        );
        let writer = self
            .store
            .create(
                ctx,
                &filename,
                Some(&WriterOption { concurrency: 20, part_size: tidb_ingestor_simplesst::MIN_UPLOAD_PART_SIZE }),
            )
            .map_err(|e| other(e.to_string()))?;
        self.result.filenames.push(filename);
        self.writer = Some(writer);
        Ok(())
    }

    /// Go `onTotalSizeLimitExceeded`.
    fn on_total_size_limit_exceeded(&mut self, ctx: &Context, total_size: i64, row_size: i64, limit: i64) -> Result<()> {
        if self.stop_recording {
            return Ok(());
        }
        self.stop_recording = true;
        self.result.row_recording_capped = true;
        tidb_logutil::bg_logger().info(
            "conflicted row files reached the hardcoded total size limit, stop recording more rows",
            &[
                Field::new("sizeLimit", Value::Str(tidb_config_configtypes::bytes_size(limit as f64))),
                Field::new("currentTotalFileSize", Value::Str(tidb_config_configtypes::bytes_size(total_size as f64))),
                Field::new("nextRowSize", Value::Str(tidb_config_configtypes::bytes_size(row_size as f64))),
            ],
        );
        if let Some(mut w) = self.writer.take() {
            self.curr_file_size = 0;
            if let Err(e) = w.close(ctx) {
                tidb_logutil::bg_logger()
                    .warn("failed to close conflict row writer", &[Field::new("error", Value::Str(e.to_string()))]);
                return Err(other(e.to_string()));
            }
        }
        Ok(())
    }
}

/// Go `getRowFileName`: the file the user reads the conflicted rows from.
pub(crate) fn get_row_file_name(prefix: &str, seq: usize) -> String {
    format!("{}/data-{seq:04}.txt", prefix.trim_end_matches('/'))
}

#[cfg(test)]
mod tests {
    //! Go `collector_test.go`'s `TestCollectorHandleEncodedRow` and
    //! `TestCollectorHandleEncodedRowMaxTotalFileSize`, with Go's numbers.

    use super::*;
    use tidb_lightning_verification::KvPair;

    fn sink(store: Arc<dyn ObjStore>, shared_total: Arc<AtomicI64>) -> CollectSink {
        CollectSink {
            store,
            filename_prefix: "test".to_owned(),
            result: CollectResult::new(&[]),
            shared_total_file_size: shared_total,
            file_seq: 0,
            curr_file_size: 0,
            writer: None,
            stop_recording: false,
        }
    }

    fn handle_rows(s: &mut CollectSink, ctx: &Context, rows: usize, sum: &mut KvChecksum) {
        let row = [Datum::new_string_datum("id"), Datum::new_string_datum("value")];
        for i in 0..rows {
            let pairs = Pairs { pairs: vec![KvPair { key: (123 * (i + 1)).to_string().into_bytes(), ..KvPair::default() }] };
            sum.update(&pairs.pairs);
            s.handle_encoded_row(ctx, &[i as u8], &row, &pairs).unwrap();
        }
        if let Some(mut w) = s.writer.take() {
            w.close(ctx).unwrap();
        }
    }

    fn local() -> (tempfile::TempDir, Arc<dyn ObjStore>) {
        let dir = tempfile::tempdir().unwrap();
        let store = tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap();
        (dir, Arc::new(store))
    }

    #[test]
    fn handle_encoded_row_rotates_files() {
        let ctx = Context::background();
        for (max_size, out_file_cnt) in [(90, 8), (300, 3), (800, 1)] {
            MAX_CONFLICT_ROW_FILE_SIZE.store(max_size, Ordering::SeqCst);
            let (dir, store) = local();
            let mut s = sink(store, Arc::new(AtomicI64::new(0)));
            let mut sum = KvChecksum::with_keyspace(&[]);
            handle_rows(&mut s, &ctx, 48, &mut sum);
            // Go's values: `("id", "value")` is 15 bytes and a newline.
            assert_eq!(s.result.row_count, 48);
            assert_eq!(s.result.total_file_size, 768);
            assert_eq!((s.result.checksum.sum_size(), s.result.checksum.sum_kvs()), (184, 48));
            assert_eq!(s.result.checksum.sum(), 14_672_641_476_652_606_594);
            let want: Vec<String> = (1..=out_file_cnt).map(|i| format!("test/data-{i:04}.txt")).collect();
            assert_eq!(s.result.filenames, want, "max size {max_size}");
            let mut lines = 0;
            for f in &want {
                let text = std::fs::read_to_string(dir.path().join(f)).unwrap();
                lines += text.trim().split('\n').count();
                assert!(text.lines().all(|l| l == "(\"id\", \"value\")"), "{text}");
            }
            assert_eq!(lines, 48);
        }
    }

    #[test]
    fn handle_encoded_row_stops_at_total_size() {
        let ctx = Context::background();
        MAX_CONFLICT_ROW_FILE_SIZE.store(1 << 20, Ordering::SeqCst);
        MAX_TOTAL_CONFLICT_ROW_FILE_SIZE.store(32, Ordering::SeqCst);
        let (dir, store) = local();
        let shared = Arc::new(AtomicI64::new(0));
        let mut s = sink(store, Arc::clone(&shared));
        let mut sum = KvChecksum::with_keyspace(&[]);
        handle_rows(&mut s, &ctx, 5, &mut sum);
        assert_eq!(shared.load(Ordering::SeqCst), 48);
        assert_eq!(s.result.row_count, 5);
        assert_eq!(s.result.total_file_size, 32);
        assert!(s.result.row_recording_capped && s.stop_recording && s.writer.is_none());
        assert!(s.result.checksum == sum);
        assert_eq!(s.result.filenames, ["test/data-0001.txt"]);
        let text = std::fs::read_to_string(dir.path().join("test/data-0001.txt")).unwrap();
        assert_eq!(text.trim().split('\n').count(), 2);
    }
}
