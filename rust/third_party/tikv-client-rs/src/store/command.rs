//! Command identity and classification shared by TiKV RPC transport paths.
//!
//! This is the representation and batch-request-conversion part of client-go's
//! `tikvrpc.go` command wrapper. Request context cloning, batch-response
//! correlation, and streaming response handling remain on the unfinished
//! transport slice.

use crate::proto::{coprocessor, kvrpcpb, tikvpb};
use crate::stats::{
    increment_batch_stream_request_counter, observe_batch_stream_tail, BatchStreamRequestCounter,
    BatchStreamTailKind,
};
use crate::{Error, Result};
use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

use super::batch::BatchRequestTelemetry;

/// The concrete TiKV RPC request or response kind.
///
/// Numeric values deliberately match client-go's `CmdType` declarations,
/// including its continued Go `iota` offsets between command groups.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[repr(u16)]
pub enum CommandType {
    Get = 1,
    Scan = 2,
    Prewrite = 3,
    Commit = 4,
    Cleanup = 5,
    BatchGet = 6,
    BatchRollback = 7,
    ScanLock = 8,
    ResolveLock = 9,
    Gc = 10,
    DeleteRange = 11,
    PessimisticLock = 12,
    PessimisticRollback = 13,
    TxnHeartBeat = 14,
    CheckTxnStatus = 15,
    CheckSecondaryLocks = 16,
    FlashbackToVersion = 17,
    PrepareFlashbackToVersion = 18,
    Flush = 19,
    BufferBatchGet = 20,

    RawGet = 276,
    RawBatchGet = 277,
    RawPut = 278,
    RawBatchPut = 279,
    RawDelete = 280,
    RawBatchDelete = 281,
    RawDeleteRange = 282,
    RawScan = 283,
    RawGetKeyTtl = 284,
    RawCompareAndSwap = 285,
    RawChecksum = 286,

    UnsafeDestroyRange = 287,
    RegisterLockObserver = 288,
    CheckLockObserver = 289,
    RemoveLockObserver = 290,
    PhysicalScanLock = 291,
    StoreSafeTs = 292,
    LockWaitInfo = 293,
    GetHealthFeedback = 294,
    BroadcastTxnStatus = 295,

    Coprocessor = 552,
    CoprocessorStream = 553,
    BatchCoprocessor = 554,
    DispatchMppTask = 555,
    EstablishMppConnection = 556,
    CancelMppTask = 557,
    IsMppAlive = 558,

    MvccGetByKey = 1071,
    MvccGetByStartTs = 1072,
    SplitRegion = 1073,

    DebugGetRegionProperties = 2098,
    Compact = 2099,
    GetTiFlashSystemTable = 2100,

    Empty = 3125,
}

#[allow(dead_code)]
impl CommandType {
    /// client-go's `CmdType.String()` result.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Get => "Get",
            Self::Scan => "Scan",
            Self::Prewrite => "Prewrite",
            Self::Commit => "Commit",
            Self::Cleanup => "Cleanup",
            Self::BatchGet => "BatchGet",
            Self::BatchRollback => "BatchRollback",
            Self::ScanLock => "ScanLock",
            Self::ResolveLock => "ResolveLock",
            Self::Gc => "GC",
            Self::DeleteRange => "DeleteRange",
            Self::PessimisticLock => "PessimisticLock",
            Self::PessimisticRollback => "PessimisticRollback",
            Self::TxnHeartBeat => "TxnHeartBeat",
            Self::CheckTxnStatus => "CheckTxnStatus",
            Self::CheckSecondaryLocks => "CheckSecondaryLocks",
            Self::FlashbackToVersion => "FlashbackToVersion",
            Self::PrepareFlashbackToVersion => "PrepareFlashbackToVersion",
            Self::Flush => "Flush",
            Self::BufferBatchGet => "BufferBatchGet",
            Self::RawGet => "RawGet",
            Self::RawBatchGet => "RawBatchGet",
            Self::RawPut => "RawPut",
            Self::RawBatchPut => "RawBatchPut",
            Self::RawDelete => "RawDelete",
            Self::RawBatchDelete => "RawBatchDelete",
            Self::RawDeleteRange => "RawDeleteRange",
            Self::RawScan => "RawScan",
            Self::RawGetKeyTtl => "RawGetKeyTTL",
            Self::RawCompareAndSwap => "RawCompareAndSwap",
            Self::RawChecksum => "RawChecksum",
            Self::UnsafeDestroyRange => "UnsafeDestroyRange",
            Self::RegisterLockObserver => "RegisterLockObserver",
            Self::CheckLockObserver => "CheckLockObserver",
            Self::RemoveLockObserver => "RemoveLockObserver",
            Self::PhysicalScanLock => "PhysicalScanLock",
            Self::StoreSafeTs => "StoreSafeTS",
            Self::LockWaitInfo => "LockWaitInfo",
            Self::GetHealthFeedback => "GetHealthFeedback",
            Self::BroadcastTxnStatus => "BroadcastTxnStatus",
            Self::Coprocessor => "Cop",
            Self::CoprocessorStream => "CopStream",
            Self::BatchCoprocessor => "BatchCop",
            Self::DispatchMppTask => "DispatchMPPTask",
            Self::EstablishMppConnection => "EstablishMPPConnection",
            Self::CancelMppTask => "CancelMPPTask",
            Self::IsMppAlive => "MPPAlive",
            Self::MvccGetByKey => "MvccGetByKey",
            Self::MvccGetByStartTs => "MvccGetByStartTS",
            Self::SplitRegion => "SplitRegion",
            Self::DebugGetRegionProperties => "DebugGetRegionProperties",
            Self::Compact => "Compact",
            Self::GetTiFlashSystemTable => "GetTiFlashSystemTable",
            Self::Empty => "Unknown",
        }
    }

    /// Whether client-go treats this as a debug service call.
    pub const fn is_debug(self) -> bool {
        matches!(self, Self::DebugGetRegionProperties)
    }

    /// Whether client-go permits query-kill interruption for this command.
    pub const fn is_interruptible(self) -> bool {
        !matches!(
            self,
            Self::PessimisticRollback | Self::BatchRollback | Self::Commit
        )
    }

    /// Whether this is a Green-GC protocol RPC.
    pub const fn is_green_gc(self) -> bool {
        matches!(
            self,
            Self::RegisterLockObserver
                | Self::CheckLockObserver
                | Self::RemoveLockObserver
                | Self::PhysicalScanLock
        )
    }

    /// Whether client-go classifies this command as a transactional write.
    pub const fn is_txn_write(self) -> bool {
        matches!(
            self,
            Self::PessimisticLock
                | Self::Prewrite
                | Self::Commit
                | Self::BatchRollback
                | Self::PessimisticRollback
                | Self::CheckTxnStatus
                | Self::CheckSecondaryLocks
                | Self::Cleanup
                | Self::TxnHeartBeat
                | Self::ResolveLock
                | Self::FlashbackToVersion
                | Self::PrepareFlashbackToVersion
                | Self::Flush
        )
    }

    /// Whether client-go classifies this command as a raw write.
    pub const fn is_raw_write(self) -> bool {
        matches!(self, Self::RawPut | Self::RawBatchPut | Self::RawDelete)
    }
}

/// An owned request that client-go can encode as a `BatchCommands` entry.
///
/// This exactly covers the `ToBatchCommandsRequest` switch in
/// `tikvrpc/tikvrpc.go`. Commands absent from that switch (for example
/// `RawChecksum`, diagnostics, and streaming calls) intentionally have no
/// variant here.
#[allow(dead_code)]
pub enum BatchCommandRequest {
    Get(kvrpcpb::GetRequest),
    Scan(kvrpcpb::ScanRequest),
    Prewrite(kvrpcpb::PrewriteRequest),
    Commit(kvrpcpb::CommitRequest),
    Cleanup(kvrpcpb::CleanupRequest),
    BatchGet(kvrpcpb::BatchGetRequest),
    BatchRollback(kvrpcpb::BatchRollbackRequest),
    ScanLock(kvrpcpb::ScanLockRequest),
    ResolveLock(kvrpcpb::ResolveLockRequest),
    Gc(kvrpcpb::GcRequest),
    DeleteRange(kvrpcpb::DeleteRangeRequest),
    RawGet(kvrpcpb::RawGetRequest),
    RawBatchGet(kvrpcpb::RawBatchGetRequest),
    RawPut(kvrpcpb::RawPutRequest),
    RawBatchPut(kvrpcpb::RawBatchPutRequest),
    RawDelete(kvrpcpb::RawDeleteRequest),
    RawBatchDelete(kvrpcpb::RawBatchDeleteRequest),
    RawDeleteRange(kvrpcpb::RawDeleteRangeRequest),
    RawScan(kvrpcpb::RawScanRequest),
    Coprocessor(coprocessor::Request),
    PessimisticLock(kvrpcpb::PessimisticLockRequest),
    PessimisticRollback(kvrpcpb::PessimisticRollbackRequest),
    Empty(tikvpb::BatchCommandsEmptyRequest),
    CheckTxnStatus(kvrpcpb::CheckTxnStatusRequest),
    CheckSecondaryLocks(kvrpcpb::CheckSecondaryLocksRequest),
    TxnHeartBeat(kvrpcpb::TxnHeartBeatRequest),
    FlashbackToVersion(kvrpcpb::FlashbackToVersionRequest),
    PrepareFlashbackToVersion(kvrpcpb::PrepareFlashbackToVersionRequest),
    Flush(kvrpcpb::FlushRequest),
    BufferBatchGet(kvrpcpb::BufferBatchGetRequest),
    GetHealthFeedback(kvrpcpb::GetHealthFeedbackRequest),
    BroadcastTxnStatus(kvrpcpb::BroadcastTxnStatusRequest),
}

/// Monotonic `BatchCommands` request-ID allocator.
///
/// Client-go's batch builder increments from zero before it publishes an
/// entry, so zero is never a valid in-flight request ID.
#[derive(Debug, Default)]
pub(crate) struct BatchRequestIdAllocator(AtomicU64);

impl BatchRequestIdAllocator {
    pub(crate) fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// The outcome of matching one source `BatchCommands` response ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BatchResponseDisposition {
    /// The response was routed to an interested caller.
    Delivered,
    /// The request was tracked but its caller cancelled after send. The entry
    /// is retired, just as client-go's receive loop does.
    Cancelled,
    /// No request was tracked for this ID (for example after an ambiguous
    /// send failure or a duplicate response).
    Outdated,
}

struct BatchPendingResponse {
    forwarded_host: String,
    sender: oneshot::Sender<Result<BatchCommandResponse>>,
    telemetry: Arc<BatchRequestTelemetry>,
    stream_metrics: Option<BatchStreamMetricLabels>,
}

/// The stream identity that owns one published response slot. Keeping it with
/// the slot lets every retirement path—including pool close—emit the same
/// source counter labels without guessing from a later stream failure.
#[derive(Clone)]
pub(crate) struct BatchStreamMetricLabels {
    pub(crate) target: String,
    pub(crate) connection_index: usize,
    pub(crate) forwarded: bool,
    pub(crate) progress: Arc<BatchStreamProgress>,
}

impl BatchStreamMetricLabels {
    pub(crate) fn new(target: String, connection_index: usize, forwarded: bool) -> Self {
        Self {
            target,
            connection_index,
            forwarded,
            progress: Arc::new(BatchStreamProgress::default()),
        }
    }

    fn increment(&self, counter: BatchStreamRequestCounter) {
        increment_batch_stream_request_counter(
            &self.target,
            self.connection_index,
            self.forwarded,
            counter,
            1,
        );
    }

    fn observe_cancelled_entry_tail(&self, telemetry: &BatchRequestTelemetry) {
        if let Some(duration) = telemetry.cancelled_response_tail() {
            observe_batch_stream_tail(
                &self.target,
                self.connection_index,
                self.forwarded,
                BatchStreamTailKind::CancelledEntry,
                duration,
            );
        }
    }
}

/// Per-stream response watermark used by the source slow-request diagnostic.
/// A later response ID confirms that TiKV has received every earlier request
/// on the same ordered BatchCommands stream.
#[derive(Default)]
pub(crate) struct BatchStreamProgress {
    max_response_id: AtomicU64,
}

impl BatchStreamProgress {
    pub(crate) fn observe_response_ids(&self, ids: &[u64]) {
        let Some(max_id) = ids.iter().copied().max() else {
            return;
        };
        let mut observed = self.max_response_id.load(Ordering::Acquire);
        while observed < max_id {
            match self.max_response_id.compare_exchange_weak(
                observed,
                max_id,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(current) => observed = current,
            }
        }
    }

    pub(crate) fn max_response_id(&self) -> u64 {
        self.max_response_id.load(Ordering::Acquire)
    }

    pub(crate) fn reset(&self) {
        self.max_response_id.store(0, Ordering::Release);
    }
}

/// Pending batch responses keyed by source `BatchCommands` request ID.
#[allow(dead_code)]
pub(crate) struct BatchPendingResponses {
    entries: Mutex<HashMap<u64, BatchPendingResponse>>,
}

impl BatchPendingResponses {
    pub(crate) fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Number of published entries not yet retired by a response or an
    /// explicit send/stream failure. This is client-go's batch-client `sent`
    /// counter expressed without its transient negative-value race.
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    /// Mirrors `inspectPendingBatchRequests`: count old pending entries and
    /// identify whether the stream has confirmed each one reached TiKV.
    pub(crate) fn inspect(&self, now: std::time::Instant) -> PendingBatchRequestStats {
        const SLOW_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(30);
        const HANG_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(90);

        let mut stats = PendingBatchRequestStats::default();
        for (id, entry) in self.entries.lock().unwrap().iter() {
            let wait = entry.telemetry.wait_duration(now);
            if wait < SLOW_THRESHOLD {
                continue;
            }
            stats.slow_count += 1;
            let unconfirmed = entry
                .stream_metrics
                .as_ref()
                .map_or(true, |metrics| metrics.progress.max_response_id() < *id);
            if unconfirmed {
                stats.slow_unconfirmed_count += 1;
            }
            if wait >= HANG_THRESHOLD {
                stats.hanging_count += 1;
                if unconfirmed {
                    stats.hanging_unconfirmed_count += 1;
                }
            }
            if stats.oldest_wait.is_none_or(|oldest| wait > oldest) {
                stats.oldest_id = Some(*id);
                stats.oldest_wait = Some(wait);
            }
        }
        stats
    }

    pub(crate) fn register(
        &self,
        id: u64,
        forwarded_host: impl Into<String>,
    ) -> oneshot::Receiver<Result<BatchCommandResponse>> {
        assert_ne!(id, 0, "batch request ID zero is reserved");
        let (sender, receiver) = oneshot::channel();
        self.register_sender_with_telemetry(
            id,
            forwarded_host,
            sender,
            Arc::new(BatchRequestTelemetry::new(0, std::time::Instant::now())),
            None,
        );
        receiver
    }

    /// Publication with the caller-owned batch timing state.
    pub(crate) fn register_sender_with_telemetry(
        &self,
        id: u64,
        forwarded_host: impl Into<String>,
        sender: oneshot::Sender<Result<BatchCommandResponse>>,
        telemetry: Arc<BatchRequestTelemetry>,
        stream_metrics: Option<BatchStreamMetricLabels>,
    ) {
        assert_ne!(id, 0, "batch request ID zero is reserved");
        let previous = self.entries.lock().unwrap().insert(
            id,
            BatchPendingResponse {
                forwarded_host: forwarded_host.into(),
                sender,
                telemetry,
                stream_metrics,
            },
        );
        assert!(
            previous.is_none(),
            "batch request ID must be unique while pending"
        );
    }

    /// Marks every group entry sent after the outbound stream accepts its
    /// protobuf envelope.
    pub(crate) fn mark_sent(&self, ids: &[u64], now: std::time::Instant) {
        let entries = self.entries.lock().unwrap();
        for id in ids {
            if let Some(entry) = entries.get(id) {
                entry.telemetry.mark_sent(now);
            }
        }
    }

    /// Completes an entry exactly once. A dropped receiver is a cancelled
    /// request, not an outdated response.
    pub(crate) fn complete(
        &self,
        id: u64,
        response: BatchCommandResponse,
    ) -> BatchResponseDisposition {
        self.complete_result(id, Ok(response))
    }

    /// The response conversion can fail for one item while the batch stream
    /// remains usable, so each ID is retired with its own result.
    pub(crate) fn complete_result(
        &self,
        id: u64,
        response: Result<BatchCommandResponse>,
    ) -> BatchResponseDisposition {
        match self.entries.lock().unwrap().remove(&id) {
            Some(entry) => {
                entry.telemetry.mark_received(std::time::Instant::now());
                if let Some(metrics) = entry.stream_metrics.as_ref() {
                    metrics.increment(BatchStreamRequestCounter::Completed);
                    metrics.increment(BatchStreamRequestCounter::Retired);
                }
                match response {
                    Ok(response) => {
                        if entry.sender.send(Ok(response)).is_ok() {
                            entry.telemetry.complete(None);
                            BatchResponseDisposition::Delivered
                        } else {
                            if let Some(metrics) = entry.stream_metrics.as_ref() {
                                metrics.observe_cancelled_entry_tail(&entry.telemetry);
                            }
                            entry.telemetry.complete(Some(&Error::StringError(
                                "batch request cancelled".to_owned(),
                            )));
                            BatchResponseDisposition::Cancelled
                        }
                    }
                    Err(error) => {
                        entry.telemetry.complete(Some(&error));
                        if entry.sender.send(Err(error)).is_ok() {
                            BatchResponseDisposition::Delivered
                        } else {
                            if let Some(metrics) = entry.stream_metrics.as_ref() {
                                metrics.observe_cancelled_entry_tail(&entry.telemetry);
                            }
                            BatchResponseDisposition::Cancelled
                        }
                    }
                }
            }
            None => BatchResponseDisposition::Outdated,
        }
    }

    /// Retires only IDs from the failed send. This cannot become a generic
    /// fail-all: a send may be ambiguous and a later TiKV response is then
    /// correctly treated as outdated.
    pub(crate) fn fail_ids<F>(&self, ids: &[u64], mut error: F) -> usize
    where
        F: FnMut() -> Error,
    {
        let retired = {
            let mut entries = self.entries.lock().unwrap();
            ids.iter()
                .filter_map(|id| entries.remove(id))
                .collect::<Vec<_>>()
        };
        let count = retired.len();
        for entry in retired {
            if let Some(metrics) = entry.stream_metrics.as_ref() {
                metrics.increment(BatchStreamRequestCounter::Retired);
            }
            let error = error();
            entry.telemetry.complete(Some(&error));
            let _ = entry.sender.send(Err(error));
        }
        count
    }

    /// Retires only the requests assigned to a failed direct or forwarding
    /// stream. Other forwarding hosts can still return responses.
    pub(crate) fn fail_for_host<F>(&self, forwarded_host: &str, mut error: F) -> usize
    where
        F: FnMut() -> Error,
    {
        let retired = {
            let mut entries = self.entries.lock().unwrap();
            let ids = entries
                .iter()
                .filter_map(|(id, entry)| (entry.forwarded_host == forwarded_host).then_some(*id))
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| entries.remove(&id))
                .collect::<Vec<_>>()
        };
        let count = retired.len();
        for entry in retired {
            if let Some(metrics) = entry.stream_metrics.as_ref() {
                metrics.increment(BatchStreamRequestCounter::Retired);
            }
            let error = error();
            entry.telemetry.complete(Some(&error));
            let _ = entry.sender.send(Err(error));
        }
        count
    }

    /// Retires every published entry during explicit client shutdown. Stream
    /// failures stay host-scoped; only a source `Close` tears down the whole
    /// connection pool.
    pub(crate) fn fail_all<F>(&self, mut error: F) -> usize
    where
        F: FnMut() -> Error,
    {
        let retired = self
            .entries
            .lock()
            .unwrap()
            .drain()
            .map(|(_, entry)| entry)
            .collect::<Vec<_>>();
        let count = retired.len();
        for entry in retired {
            if let Some(metrics) = entry.stream_metrics.as_ref() {
                metrics.increment(BatchStreamRequestCounter::Retired);
            }
            let error = error();
            entry.telemetry.complete(Some(&error));
            let _ = entry.sender.send(Err(error));
        }
        count
    }
}

/// Source diagnostic summary for BatchCommands entries still awaiting a
/// response. It intentionally does not alter request completion or retries.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PendingBatchRequestStats {
    pub(crate) oldest_id: Option<u64>,
    pub(crate) oldest_wait: Option<std::time::Duration>,
    pub(crate) slow_count: usize,
    pub(crate) slow_unconfirmed_count: usize,
    pub(crate) hanging_count: usize,
    pub(crate) hanging_unconfirmed_count: usize,
}

impl BatchCommandRequest {
    /// Converts an already-contextualized physical request into precisely the
    /// subset client-go admits through `ToBatchCommandsRequest`.
    pub(crate) fn from_store_request(request: &dyn super::Request) -> Option<Self> {
        macro_rules! request_variant {
            ($($variant:ident($type:ty)),+ $(,)?) => {
                $(if let Some(request) = request.as_any().downcast_ref::<$type>() {
                    return Some(Self::$variant(request.clone()));
                })+
            };
        }
        request_variant!(
            Get(kvrpcpb::GetRequest),
            Scan(kvrpcpb::ScanRequest),
            Prewrite(kvrpcpb::PrewriteRequest),
            Commit(kvrpcpb::CommitRequest),
            Cleanup(kvrpcpb::CleanupRequest),
            BatchGet(kvrpcpb::BatchGetRequest),
            BatchRollback(kvrpcpb::BatchRollbackRequest),
            ScanLock(kvrpcpb::ScanLockRequest),
            ResolveLock(kvrpcpb::ResolveLockRequest),
            Gc(kvrpcpb::GcRequest),
            DeleteRange(kvrpcpb::DeleteRangeRequest),
            RawGet(kvrpcpb::RawGetRequest),
            RawBatchGet(kvrpcpb::RawBatchGetRequest),
            RawPut(kvrpcpb::RawPutRequest),
            RawBatchPut(kvrpcpb::RawBatchPutRequest),
            RawDelete(kvrpcpb::RawDeleteRequest),
            RawBatchDelete(kvrpcpb::RawBatchDeleteRequest),
            RawDeleteRange(kvrpcpb::RawDeleteRangeRequest),
            RawScan(kvrpcpb::RawScanRequest),
            Coprocessor(coprocessor::Request),
            PessimisticLock(kvrpcpb::PessimisticLockRequest),
            PessimisticRollback(kvrpcpb::PessimisticRollbackRequest),
            CheckTxnStatus(kvrpcpb::CheckTxnStatusRequest),
            CheckSecondaryLocks(kvrpcpb::CheckSecondaryLocksRequest),
            TxnHeartBeat(kvrpcpb::TxnHeartBeatRequest),
            FlashbackToVersion(kvrpcpb::FlashbackToVersionRequest),
            PrepareFlashbackToVersion(kvrpcpb::PrepareFlashbackToVersionRequest),
            Flush(kvrpcpb::FlushRequest),
            BufferBatchGet(kvrpcpb::BufferBatchGetRequest),
            GetHealthFeedback(kvrpcpb::GetHealthFeedbackRequest),
            BroadcastTxnStatus(kvrpcpb::BroadcastTxnStatusRequest)
        );
        None
    }

    /// The source `CmdType` paired with this concrete request payload.
    pub const fn command_type(&self) -> CommandType {
        match self {
            Self::Get(_) => CommandType::Get,
            Self::Scan(_) => CommandType::Scan,
            Self::Prewrite(_) => CommandType::Prewrite,
            Self::Commit(_) => CommandType::Commit,
            Self::Cleanup(_) => CommandType::Cleanup,
            Self::BatchGet(_) => CommandType::BatchGet,
            Self::BatchRollback(_) => CommandType::BatchRollback,
            Self::ScanLock(_) => CommandType::ScanLock,
            Self::ResolveLock(_) => CommandType::ResolveLock,
            Self::Gc(_) => CommandType::Gc,
            Self::DeleteRange(_) => CommandType::DeleteRange,
            Self::RawGet(_) => CommandType::RawGet,
            Self::RawBatchGet(_) => CommandType::RawBatchGet,
            Self::RawPut(_) => CommandType::RawPut,
            Self::RawBatchPut(_) => CommandType::RawBatchPut,
            Self::RawDelete(_) => CommandType::RawDelete,
            Self::RawBatchDelete(_) => CommandType::RawBatchDelete,
            Self::RawDeleteRange(_) => CommandType::RawDeleteRange,
            Self::RawScan(_) => CommandType::RawScan,
            Self::Coprocessor(_) => CommandType::Coprocessor,
            Self::PessimisticLock(_) => CommandType::PessimisticLock,
            Self::PessimisticRollback(_) => CommandType::PessimisticRollback,
            Self::Empty(_) => CommandType::Empty,
            Self::CheckTxnStatus(_) => CommandType::CheckTxnStatus,
            Self::CheckSecondaryLocks(_) => CommandType::CheckSecondaryLocks,
            Self::TxnHeartBeat(_) => CommandType::TxnHeartBeat,
            Self::FlashbackToVersion(_) => CommandType::FlashbackToVersion,
            Self::PrepareFlashbackToVersion(_) => CommandType::PrepareFlashbackToVersion,
            Self::Flush(_) => CommandType::Flush,
            Self::BufferBatchGet(_) => CommandType::BufferBatchGet,
            Self::GetHealthFeedback(_) => CommandType::GetHealthFeedback,
            Self::BroadcastTxnStatus(_) => CommandType::BroadcastTxnStatus,
        }
    }

    /// Source batch request-stage metrics are labeled by the store peer in
    /// the request context, not by the pooled transport target.
    pub(crate) fn store_id(&self) -> u64 {
        macro_rules! contextual_store_id {
            ($($variant:ident),+ $(,)?) => {
                match self {
                    $(Self::$variant(request) => request
                        .context
                        .as_ref()
                        .and_then(|context| context.peer.as_ref())
                        .map_or(0, |peer| peer.store_id),)+
                    Self::Empty(_) => 0,
                }
            };
        }
        contextual_store_id!(
            Get,
            Scan,
            Prewrite,
            Commit,
            Cleanup,
            BatchGet,
            BatchRollback,
            ScanLock,
            ResolveLock,
            Gc,
            DeleteRange,
            RawGet,
            RawBatchGet,
            RawPut,
            RawBatchPut,
            RawDelete,
            RawBatchDelete,
            RawDeleteRange,
            RawScan,
            Coprocessor,
            PessimisticLock,
            PessimisticRollback,
            CheckTxnStatus,
            CheckSecondaryLocks,
            TxnHeartBeat,
            FlashbackToVersion,
            PrepareFlashbackToVersion,
            Flush,
            BufferBatchGet,
            GetHealthFeedback,
            BroadcastTxnStatus,
        )
    }

    /// Encode this request as the generated `BatchCommandsRequest` oneof.
    pub fn into_proto(self) -> tikvpb::batch_commands_request::Request {
        use tikvpb::batch_commands_request::request::Cmd;

        let cmd = match self {
            Self::Get(request) => Cmd::Get(request),
            Self::Scan(request) => Cmd::Scan(request),
            Self::Prewrite(request) => Cmd::Prewrite(request),
            Self::Commit(request) => Cmd::Commit(request),
            Self::Cleanup(request) => Cmd::Cleanup(request),
            Self::BatchGet(request) => Cmd::BatchGet(request),
            Self::BatchRollback(request) => Cmd::BatchRollback(request),
            Self::ScanLock(request) => Cmd::ScanLock(request),
            Self::ResolveLock(request) => Cmd::ResolveLock(request),
            Self::Gc(request) => Cmd::Gc(request),
            Self::DeleteRange(request) => Cmd::DeleteRange(request),
            Self::RawGet(request) => Cmd::RawGet(request),
            Self::RawBatchGet(request) => Cmd::RawBatchGet(request),
            Self::RawPut(request) => Cmd::RawPut(request),
            Self::RawBatchPut(request) => Cmd::RawBatchPut(request),
            Self::RawDelete(request) => Cmd::RawDelete(request),
            Self::RawBatchDelete(request) => Cmd::RawBatchDelete(request),
            Self::RawDeleteRange(request) => Cmd::RawDeleteRange(request),
            Self::RawScan(request) => Cmd::RawScan(request),
            Self::Coprocessor(request) => Cmd::Coprocessor(request),
            Self::PessimisticLock(request) => Cmd::PessimisticLock(request),
            Self::PessimisticRollback(request) => Cmd::PessimisticRollback(request),
            Self::Empty(request) => Cmd::Empty(request),
            Self::CheckTxnStatus(request) => Cmd::CheckTxnStatus(request),
            Self::CheckSecondaryLocks(request) => Cmd::CheckSecondaryLocks(request),
            Self::TxnHeartBeat(request) => Cmd::TxnHeartBeat(request),
            Self::FlashbackToVersion(request) => Cmd::FlashbackToVersion(request),
            Self::PrepareFlashbackToVersion(request) => Cmd::PrepareFlashbackToVersion(request),
            Self::Flush(request) => Cmd::Flush(request),
            Self::BufferBatchGet(request) => Cmd::BufferBatchGet(request),
            Self::GetHealthFeedback(request) => Cmd::GetHealthFeedback(request),
            Self::BroadcastTxnStatus(request) => Cmd::BroadcastTxnStatus(request),
        };
        tikvpb::batch_commands_request::Request { cmd: Some(cmd) }
    }
}

/// An owned response decoded from a `BatchCommands` entry.
///
/// This is the native equivalent of client-go's dynamic `Response` returned
/// by `FromBatchCommandsResponse`. It covers exactly the response alternatives
/// that the source accepts from batch transport.
#[allow(dead_code)]
pub enum BatchCommandResponse {
    Get(kvrpcpb::GetResponse),
    Scan(kvrpcpb::ScanResponse),
    Prewrite(kvrpcpb::PrewriteResponse),
    Commit(kvrpcpb::CommitResponse),
    Cleanup(kvrpcpb::CleanupResponse),
    BatchGet(kvrpcpb::BatchGetResponse),
    BatchRollback(kvrpcpb::BatchRollbackResponse),
    ScanLock(kvrpcpb::ScanLockResponse),
    ResolveLock(kvrpcpb::ResolveLockResponse),
    Gc(kvrpcpb::GcResponse),
    DeleteRange(kvrpcpb::DeleteRangeResponse),
    RawGet(kvrpcpb::RawGetResponse),
    RawBatchGet(kvrpcpb::RawBatchGetResponse),
    RawPut(kvrpcpb::RawPutResponse),
    RawBatchPut(kvrpcpb::RawBatchPutResponse),
    RawDelete(kvrpcpb::RawDeleteResponse),
    RawBatchDelete(kvrpcpb::RawBatchDeleteResponse),
    RawDeleteRange(kvrpcpb::RawDeleteRangeResponse),
    RawScan(kvrpcpb::RawScanResponse),
    Coprocessor(coprocessor::Response),
    PessimisticLock(kvrpcpb::PessimisticLockResponse),
    PessimisticRollback(kvrpcpb::PessimisticRollbackResponse),
    Empty(tikvpb::BatchCommandsEmptyResponse),
    CheckTxnStatus(kvrpcpb::CheckTxnStatusResponse),
    CheckSecondaryLocks(kvrpcpb::CheckSecondaryLocksResponse),
    TxnHeartBeat(kvrpcpb::TxnHeartBeatResponse),
    FlashbackToVersion(kvrpcpb::FlashbackToVersionResponse),
    PrepareFlashbackToVersion(kvrpcpb::PrepareFlashbackToVersionResponse),
    Flush(kvrpcpb::FlushResponse),
    BufferBatchGet(kvrpcpb::BufferBatchGetResponse),
    GetHealthFeedback(kvrpcpb::GetHealthFeedbackResponse),
    BroadcastTxnStatus(kvrpcpb::BroadcastTxnStatusResponse),
}

impl BatchCommandResponse {
    /// Erases a successfully correlated batch response into the same native
    /// response carrier used by the existing unary physical dispatch path.
    pub(crate) fn into_any(self) -> Box<dyn Any> {
        macro_rules! response_variant {
            ($($variant:ident),+ $(,)?) => {
                match self { $(Self::$variant(response) => Box::new(response),)+ }
            };
        }
        response_variant!(
            Get,
            Scan,
            Prewrite,
            Commit,
            Cleanup,
            BatchGet,
            BatchRollback,
            ScanLock,
            ResolveLock,
            Gc,
            DeleteRange,
            RawGet,
            RawBatchGet,
            RawPut,
            RawBatchPut,
            RawDelete,
            RawBatchDelete,
            RawDeleteRange,
            RawScan,
            Coprocessor,
            PessimisticLock,
            PessimisticRollback,
            Empty,
            CheckTxnStatus,
            CheckSecondaryLocks,
            TxnHeartBeat,
            FlashbackToVersion,
            PrepareFlashbackToVersion,
            Flush,
            BufferBatchGet,
            GetHealthFeedback,
            BroadcastTxnStatus
        )
    }

    /// Decode one generated response entry, rejecting an absent command as
    /// client-go's `FromBatchCommandsResponse` does.
    pub fn from_proto(response: tikvpb::batch_commands_response::Response) -> Result<Self> {
        use tikvpb::batch_commands_response::response::Cmd;

        match response.cmd {
            Some(Cmd::Get(response)) => Ok(Self::Get(response)),
            Some(Cmd::Scan(response)) => Ok(Self::Scan(response)),
            Some(Cmd::Prewrite(response)) => Ok(Self::Prewrite(response)),
            Some(Cmd::Commit(response)) => Ok(Self::Commit(response)),
            Some(Cmd::Cleanup(response)) => Ok(Self::Cleanup(response)),
            Some(Cmd::BatchGet(response)) => Ok(Self::BatchGet(response)),
            Some(Cmd::BatchRollback(response)) => Ok(Self::BatchRollback(response)),
            Some(Cmd::ScanLock(response)) => Ok(Self::ScanLock(response)),
            Some(Cmd::ResolveLock(response)) => Ok(Self::ResolveLock(response)),
            Some(Cmd::Gc(response)) => Ok(Self::Gc(response)),
            Some(Cmd::DeleteRange(response)) => Ok(Self::DeleteRange(response)),
            Some(Cmd::RawGet(response)) => Ok(Self::RawGet(response)),
            Some(Cmd::RawBatchGet(response)) => Ok(Self::RawBatchGet(response)),
            Some(Cmd::RawPut(response)) => Ok(Self::RawPut(response)),
            Some(Cmd::RawBatchPut(response)) => Ok(Self::RawBatchPut(response)),
            Some(Cmd::RawDelete(response)) => Ok(Self::RawDelete(response)),
            Some(Cmd::RawBatchDelete(response)) => Ok(Self::RawBatchDelete(response)),
            Some(Cmd::RawDeleteRange(response)) => Ok(Self::RawDeleteRange(response)),
            Some(Cmd::RawScan(response)) => Ok(Self::RawScan(response)),
            Some(Cmd::Coprocessor(response)) => Ok(Self::Coprocessor(response)),
            Some(Cmd::PessimisticLock(response)) => Ok(Self::PessimisticLock(response)),
            Some(Cmd::PessimisticRollback(response)) => Ok(Self::PessimisticRollback(response)),
            Some(Cmd::Empty(response)) => Ok(Self::Empty(response)),
            Some(Cmd::CheckTxnStatus(response)) => Ok(Self::CheckTxnStatus(response)),
            Some(Cmd::CheckSecondaryLocks(response)) => Ok(Self::CheckSecondaryLocks(response)),
            Some(Cmd::TxnHeartBeat(response)) => Ok(Self::TxnHeartBeat(response)),
            Some(Cmd::FlashbackToVersion(response)) => Ok(Self::FlashbackToVersion(response)),
            Some(Cmd::PrepareFlashbackToVersion(response)) => {
                Ok(Self::PrepareFlashbackToVersion(response))
            }
            Some(Cmd::Flush(response)) => Ok(Self::Flush(response)),
            Some(Cmd::BufferBatchGet(response)) => Ok(Self::BufferBatchGet(response)),
            Some(Cmd::GetHealthFeedback(response)) => Ok(Self::GetHealthFeedback(response)),
            Some(Cmd::BroadcastTxnStatus(response)) => Ok(Self::BroadcastTxnStatus(response)),
            None => Err(Error::StringError("Unknown command response".to_owned())),
            // These generated alternatives are absent from client-go's
            // `ToBatchCommandsRequest` switch. Its response conversion
            // reaches `panic("unreachable")` if a peer sends one for a
            // request it could not have encoded.
            Some(Cmd::Import(_) | Cmd::RawBatchScan(_) | Cmd::RawCoprocessor(_)) => {
                unreachable!("unexpected batch response command")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_command_values_and_names_are_stable() {
        assert_eq!(CommandType::Get as u16, 1);
        assert_eq!(CommandType::BufferBatchGet as u16, 20);
        assert_eq!(CommandType::RawGet as u16, 276);
        assert_eq!(CommandType::RawChecksum as u16, 286);
        assert_eq!(CommandType::Coprocessor as u16, 552);
        assert_eq!(CommandType::MvccGetByKey as u16, 1071);
        assert_eq!(CommandType::DebugGetRegionProperties as u16, 2098);
        assert_eq!(CommandType::Empty as u16, 3125);
        assert_eq!(CommandType::Gc.name(), "GC");
        assert_eq!(CommandType::RawGetKeyTtl.name(), "RawGetKeyTTL");
        assert_eq!(CommandType::MvccGetByStartTs.name(), "MvccGetByStartTS");
        assert_eq!(CommandType::Empty.name(), "Unknown");
    }

    #[test]
    fn source_command_classifications_match() {
        assert!(CommandType::DebugGetRegionProperties.is_debug());
        assert!(!CommandType::Get.is_debug());
        assert!(!CommandType::Commit.is_interruptible());
        assert!(!CommandType::PessimisticRollback.is_interruptible());
        assert!(CommandType::Get.is_interruptible());
        assert!(CommandType::PhysicalScanLock.is_green_gc());
        assert!(!CommandType::Get.is_green_gc());
        assert!(CommandType::Flush.is_txn_write());
        assert!(!CommandType::ScanLock.is_txn_write());
        assert!(CommandType::RawPut.is_raw_write());
        assert!(!CommandType::RawBatchDelete.is_raw_write());
    }

    #[test]
    fn batch_command_encoding_retains_source_oneof_and_identity() {
        let get = BatchCommandRequest::Get(kvrpcpb::GetRequest {
            key: b"key".to_vec(),
            ..Default::default()
        });
        assert_eq!(get.command_type(), CommandType::Get);
        assert!(matches!(
            get.into_proto().cmd,
            Some(tikvpb::batch_commands_request::request::Cmd::Get(request))
                if request.key == b"key"
        ));

        let raw_put = BatchCommandRequest::RawPut(kvrpcpb::RawPutRequest {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            ..Default::default()
        });
        assert_eq!(raw_put.command_type(), CommandType::RawPut);
        assert!(matches!(
            raw_put.into_proto().cmd,
            Some(tikvpb::batch_commands_request::request::Cmd::RawPut(request))
                if request.key == b"key" && request.value == b"value"
        ));

        let empty = BatchCommandRequest::Empty(tikvpb::BatchCommandsEmptyRequest {
            test_id: 7,
            delay_time: 11,
        });
        assert_eq!(empty.command_type(), CommandType::Empty);
        assert!(matches!(
            empty.into_proto().cmd,
            Some(tikvpb::batch_commands_request::request::Cmd::Empty(request))
                if request.test_id == 7 && request.delay_time == 11
        ));
    }

    #[test]
    fn batch_command_bridge_accepts_only_source_batchable_requests() {
        let get = kvrpcpb::GetRequest {
            key: b"key".to_vec(),
            ..Default::default()
        };
        assert!(matches!(
            BatchCommandRequest::from_store_request(&get),
            Some(BatchCommandRequest::Get(request)) if request.key == b"key"
        ));

        // client-go's ToBatchCommandsRequest switch deliberately excludes
        // RawChecksum even though it is an ordinary physical RPC.
        let checksum = kvrpcpb::RawChecksumRequest::default();
        assert!(BatchCommandRequest::from_store_request(&checksum).is_none());

        let response = BatchCommandResponse::Get(kvrpcpb::GetResponse {
            value: b"value".to_vec(),
            ..Default::default()
        })
        .into_any();
        assert_eq!(
            response
                .downcast::<kvrpcpb::GetResponse>()
                .expect("typed response carrier")
                .value,
            b"value"
        );
    }

    #[test]
    fn batch_command_response_decoding_preserves_oneof_and_unknown_error() {
        let response = tikvpb::batch_commands_response::Response {
            cmd: Some(tikvpb::batch_commands_response::response::Cmd::RawGet(
                kvrpcpb::RawGetResponse {
                    value: b"value".to_vec(),
                    ..Default::default()
                },
            )),
        };
        assert!(matches!(
            BatchCommandResponse::from_proto(response),
            Ok(BatchCommandResponse::RawGet(response)) if response.value == b"value"
        ));

        let empty = tikvpb::batch_commands_response::Response {
            cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                tikvpb::BatchCommandsEmptyResponse { test_id: 7 },
            )),
        };
        assert!(matches!(
            BatchCommandResponse::from_proto(empty),
            Ok(BatchCommandResponse::Empty(response)) if response.test_id == 7
        ));
        assert!(matches!(
            BatchCommandResponse::from_proto(tikvpb::batch_commands_response::Response { cmd: None }),
            Err(Error::StringError(message)) if message == "Unknown command response"
        ));
    }

    #[test]
    fn batch_request_ids_start_at_one_and_increase_monotonically() {
        let allocator = BatchRequestIdAllocator::default();
        assert_eq!(allocator.next(), 1);
        assert_eq!(allocator.next(), 2);
        assert_eq!(allocator.next(), 3);
    }

    #[tokio::test]
    async fn batch_pending_responses_match_ids_once_and_ignore_outdated_responses() {
        let pending = BatchPendingResponses::new();
        let receiver = pending.register(7, "");
        assert_eq!(
            pending.complete(
                7,
                BatchCommandResponse::Empty(tikvpb::BatchCommandsEmptyResponse { test_id: 7 })
            ),
            BatchResponseDisposition::Delivered
        );
        assert!(
            matches!(receiver.await, Ok(Ok(BatchCommandResponse::Empty(response))) if response.test_id == 7)
        );
        assert_eq!(
            pending.complete(
                7,
                BatchCommandResponse::Empty(tikvpb::BatchCommandsEmptyResponse { test_id: 7 })
            ),
            BatchResponseDisposition::Outdated
        );
    }

    #[tokio::test]
    async fn batch_pending_responses_retire_cancelled_and_failed_stream_entries() {
        let pending = BatchPendingResponses::new();
        let cancelled = pending.register(1, "");
        let direct = pending.register(2, "");
        let mut forwarded = pending.register(3, "store-2");
        drop(cancelled);

        assert_eq!(
            pending.complete(
                1,
                BatchCommandResponse::Empty(tikvpb::BatchCommandsEmptyResponse { test_id: 1 })
            ),
            BatchResponseDisposition::Cancelled
        );
        pending.fail_for_host("", || Error::StringError("direct stream failed".to_owned()));
        assert!(
            matches!(direct.await, Ok(Err(Error::StringError(message))) if message == "direct stream failed")
        );
        assert!(matches!(
            forwarded.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        pending.fail_ids(&[3], || Error::StringError("send failed".to_owned()));
        assert!(
            matches!(forwarded.await, Ok(Err(Error::StringError(message))) if message == "send failed")
        );
    }
}
