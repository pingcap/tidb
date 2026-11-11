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

//! Go `selectResult.fetchRespWithIntermediateResults` over the coprocessor's
//! response channel: one owned response at a time decoded into rows and
//! chunks, response warnings appended, the source error returned, and the
//! response closed after consumption.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tidb_chunk::chunk::Chunk as DecodedChunk;
use tidb_chunk::codec::Codec as ChunkCodec;
use tidb_chunk::codec::Decoder as ChunkDecoder;
use tidb_datatype::{Datum, FieldType, SessionTimeZone};
use tidb_proto::{Chunk as ResponseChunk, EncodeType, ExecutorExecutionSummary, SelectResponse};

use crate::chunk_decode::{decode_chunk, decode_select_response, ChunkDecodeError};
use crate::distsql_runtime::{SelectResultMetadata, SelectResultRuntimeStats};
use tidb_txnkv::LimiterWaitStats;
use crate::query_runtime::QueryResponse;
use tidb_txnkv::QueryResponseError;
use tidb_txnkv::ResponseRuntimeStats;
use crate::select_iter::SelectResultRow;
use crate::Warning;
use crate::WarningCollector;

use tidb_copr::response_channel::*;

/// The statement-wide consumer of every coprocessor response a select result
/// reads: Go's `selectResult.fetchResp` merges each response's
/// `CopRuntimeStats` into `StmtCtx.ExecDetails` (`MergeCopExecDetails`) and
/// the statement's `RuntimeStatsColl`. The statement owns the implementation;
/// the select iterator only reports what it consumed.
pub trait CopResponseSink: Send + Sync + fmt::Debug {
    /// One consumed response. `fetch_duration` is Go's `resp.Next` wait for
    /// it; `execution_summaries` are the response's tipb summaries.
    fn record_cop_response(
        &self,
        stream: u64,
        metadata: &SelectResultMetadata,
        sample: &ResponseRuntimeStats,
        fetch_duration: Duration,
        execution_summaries: &[ExecutorExecutionSummary],
    );

    /// Go `selectResult.closeImpl`: the select result `stream` (the id its
    /// responses were recorded under) closed.
    fn close_select_result(
        &self,
        _stream: u64,
        _metadata: &SelectResultMetadata,
        _close: &SelectResultClose,
    ) {
    }
}

/// What a select result knows only when it closes (Go `selectResult.close`).
#[derive(Clone, Copy, Debug, Default)]
pub struct SelectResultClose {
    /// Go `selectResult.fetchDuration`.
    pub fetch_duration: Duration,
    /// Go `HasLimiterWaitStats.GetLimiterWaitStats`.
    pub limiter_wait: LimiterWaitStats,
    /// Go `copr.CopInfo`.
    pub cop_info: Option<tidb_txnkv::CopInfo>,
}

static SELECT_RESULT_STREAMS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);


/// A response's error as its select result's.
fn response_channel_error(error: QueryResponseError) -> ResponseChannelError {
    match error {
        QueryResponseError::Pending => ResponseChannelError::Pending,
        QueryResponseError::Source(message) => ResponseChannelError::Source(message),
        QueryResponseError::Sql { code, message } => ResponseChannelError::Sql { code, message },
        QueryResponseError::Cancelled => ResponseChannelError::Cancelled,
    }
}

enum SelectResponseSource {
    Channel(ResponseChannel<prost::bytes::Bytes>),
    Query(Box<dyn QueryResponse + Send>),
}

impl SelectResponseSource {
    fn cop_info(&self) -> Option<tidb_txnkv::CopInfo> {
        match self {
            Self::Channel(_) => None,
            Self::Query(source) => source.cop_info(),
        }
    }

    fn limiter_wait_stats(&self) -> LimiterWaitStats {
        match self {
            Self::Channel(_) => LimiterWaitStats::default(),
            Self::Query(source) => source.limiter_wait_stats(),
        }
    }

    fn next_event(
        &mut self,
        required_rows: usize,
    ) -> Result<Option<ResponseChannelEvent<prost::bytes::Bytes>>, ResponseChannelError> {
        match self {
            Self::Channel(source) => Ok(source.next_event()),
            Self::Query(source) => match source.next_with_required_rows(required_rows) {
                Ok(Some(subset)) => Ok(Some(match subset.runtime {
                    Some(runtime_stats) => ResponseChannelEvent::ResultWithRuntime {
                        result: subset.data,
                        runtime_stats,
                    },
                    None => ResponseChannelEvent::Result(subset.data),
                })),
                Ok(None) => {
                    // The drained response is closed here, so Go
                    // `Response.Close`'s error is this fetch's.
                    source.close().map_err(response_channel_error)?;
                    Ok(Some(ResponseChannelEvent::Closed))
                }
                Err(error) => Err(response_channel_error(error)),
            },
        }
    }

    /// Go `selectResult.closeImpl`'s `r.resp.Close()`.
    fn close(&mut self) -> Result<(), ResponseChannelError> {
        match self {
            Self::Channel(source) => {
                source.close();
                Ok(())
            }
            Self::Query(source) => source.close().map_err(response_channel_error),
        }
    }

    fn push_result(&mut self, bytes: prost::bytes::Bytes) -> Result<(), ResponseChannelError> {
        match self {
            Self::Channel(source) => source.push_result(bytes),
            Self::Query(_) => Err(ResponseChannelError::Unsupported(
                ResponseChannelUnsupported::TransportOwnedResponseMutation,
            )),
        }
    }

    fn push_result_with_runtime(
        &mut self,
        bytes: prost::bytes::Bytes,
        runtime_stats: ResponseRuntimeStats,
    ) -> Result<(), ResponseChannelError> {
        match self {
            Self::Channel(source) => source.push_result_with_runtime(bytes, runtime_stats),
            Self::Query(_) => Err(ResponseChannelError::Unsupported(
                ResponseChannelUnsupported::TransportOwnedResponseMutation,
            )),
        }
    }

    fn finish(&mut self) -> Result<(), ResponseChannelError> {
        match self {
            Self::Channel(source) => source.finish(),
            Self::Query(_) => Err(ResponseChannelError::Unsupported(
                ResponseChannelUnsupported::TransportOwnedResponseMutation,
            )),
        }
    }
}

/// Go `selectResult` over a coprocessor response channel: the decoded row
/// iterator a raw channel converts into.
pub trait SelectResult {
    /// Converts this raw response source into the sole decoded row iterator.
    ///
    /// Consuming `self` makes the Go `selectResult` invalidation rule a Rust
    /// ownership invariant: raw reads, a second conversion, and direct close
    /// cannot race the returned iterator. The iterator itself owns final close.
    fn into_select_iter(
        self,
        final_field_types: Vec<FieldType>,
        intermediate_output_types: Vec<Vec<FieldType>>,
        warnings: WarningCollector,
    ) -> SelectResponseIter;

    /// Converts this source with the statement location used to recover
    /// `TIMESTAMP` values from TiKV's UTC packed representation.
    fn into_select_iter_in_timezone(
        self,
        final_field_types: Vec<FieldType>,
        intermediate_output_types: Vec<Vec<FieldType>>,
        time_zone: SessionTimeZone,
        warnings: WarningCollector,
    ) -> SelectResponseIter;
}

impl SelectResult for ResponseChannel<prost::bytes::Bytes> {
    /// Converts this raw response source into the sole decoded row iterator.
    ///
    /// Consuming `self` makes the Go `selectResult` invalidation rule a Rust
    /// ownership invariant: raw reads, a second conversion, and direct close
    /// cannot race the returned iterator. The iterator itself owns final close.
    fn into_select_iter(
        self,
        final_field_types: Vec<FieldType>,
        intermediate_output_types: Vec<Vec<FieldType>>,
        warnings: WarningCollector,
    ) -> SelectResponseIter {
        self.into_select_iter_in_timezone(
            final_field_types,
            intermediate_output_types,
            SessionTimeZone::utc(),
            warnings,
        )
    }

    /// Converts this source with the statement location used to recover
    /// `TIMESTAMP` values from TiKV's UTC packed representation.
    fn into_select_iter_in_timezone(
        self,
        final_field_types: Vec<FieldType>,
        intermediate_output_types: Vec<Vec<FieldType>>,
        time_zone: SessionTimeZone,
        warnings: WarningCollector,
    ) -> SelectResponseIter {
        SelectResponseIter {
            source: SelectResponseSource::Channel(self),
            final_field_types: Arc::from(final_field_types),
            intermediate_output_types: intermediate_output_types
                .into_iter()
                .map(Arc::from)
                .collect(),
            time_zone,
            warnings,
            channels: Vec::new(),
            reusable_chunks: Vec::new(),
            runtime_stats: SelectResultRuntimeStats::default(),
            runtime_stats_binding: None,
            result_metadata: None,
            cop_response_sink: None,
            last_fetch: Duration::ZERO,
            total_fetch: Duration::ZERO,
            stream_id: SELECT_RESULT_STREAMS.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            close_reported: false,
            memory_tracker: None,
            closed: false,
            close_err: Ok(()),
        }
    }
}

/// Connected decoder and reverse-priority iterator for raw select responses.
///
/// Each channel owns its chunk decoding and row consumption.
/// The final channel is consumed first, followed by
/// intermediate channels from highest to lowest index, exactly like Go's
/// `selectResultIter.Next`.
pub struct SelectResponseIter {
    source: SelectResponseSource,
    final_field_types: Arc<[FieldType]>,
    intermediate_output_types: Vec<Arc<[FieldType]>>,
    time_zone: SessionTimeZone,
    warnings: WarningCollector,
    channels: Vec<SelectResponseChannel>,
    /// Go `selectResult.respChunkDecoder` survives `fetchResp` calls. Keep
    /// each channel's decoder intermediate chunk after that channel is
    /// consumed, so a later response decodes into the same columns.
    reusable_chunks: Vec<Option<DecodedChunk>>,
    runtime_stats: SelectResultRuntimeStats,
    runtime_stats_binding: Option<RuntimeStatsBinding>,
    result_metadata: Option<SelectResultMetadata>,
    /// The statement's consumer of per-response execution details.
    cop_response_sink: Option<Arc<dyn CopResponseSink>>,
    /// How long the last `source.next_event` took, which Go adds to
    /// `ExecDetails.CopTime` for the response it produced.
    last_fetch: Duration,
    /// Go `selectResult.fetchDuration`: the sum of every fetch.
    total_fetch: Duration,
    /// The id this select result's responses are recorded under.
    stream_id: u64,
    /// Whether the sink has been told this select result ended.
    close_reported: bool,
    /// Go `selectResult.memTracker` and the bytes of the response it holds
    /// (`selectRespSize`).
    memory_tracker: Option<(Arc<tidb_memory::Tracker>, i64)>,
    closed: bool,
    /// Go `selectResult.closeErr`.
    close_err: Result<(), ResponseChannelError>,
}

#[derive(Clone, Debug)]
struct RuntimeStatsBinding {
    metadata: SelectResultMetadata,
    collector_enabled: bool,
}

#[derive(Debug)]
struct SelectResponseChannel {
    channel_index: usize,
    raw_encode_type: Option<i32>,
    chunks: Vec<ResponseChunk>,
    field_types: Arc<[FieldType]>,
    /// Go `selectResult.respChunkDecoder`, whose intermediate chunk outlives
    /// the response so a later TypeChunk reuses its column allocations.
    decoder: ChunkDecoder,
    time_zone: SessionTimeZone,
    next_chunk_index: usize,
    decoded: Option<DecodedChannel>,
}

#[derive(Debug)]
enum DecodedChannel {
    Rows(VecDeque<Vec<Datum>>),
    /// A TypeChunk image the channel's decoder holds. The row API takes the
    /// decoder's remaining rows once (Go `ReuseIntermChk`) and walks them.
    TypeChunk {
        rows: Option<(DecodedChunk, usize)>,
    },
}

impl DecodedChannel {
    fn next_row(
        &mut self,
        decoder: &mut ChunkDecoder,
        channel_index: usize,
        field_types: &[FieldType],
    ) -> Result<Option<SelectResultRow<Vec<Datum>>>, ResponseChannelError> {
        match self {
            Self::Rows(rows) => Ok(rows
                .pop_front()
                .map(|row| SelectResultRow::new(channel_index, row))),
            Self::TypeChunk { rows } => {
                let (chunk, next_row_index) = rows.get_or_insert_with(|| {
                    let mut chunk = DecodedChunk::new_empty_chunk(field_types);
                    if !decoder.is_finished() {
                        decoder.reuse_interm_chk(&mut chunk);
                    }
                    (chunk, 0)
                });
                if *next_row_index >= chunk.num_rows() {
                    return Ok(None);
                }
                let row = chunk
                    .get_row(*next_row_index)
                    .try_get_datum_row(field_types)
                    .map_err(|error| ResponseChannelError::RowDecode(error.to_string()))?;
                *next_row_index += 1;
                Ok(Some(SelectResultRow::new(channel_index, row)))
            }
        }
    }

    fn close(&mut self) {
        if let Self::Rows(rows) = self {
            rows.clear();
        }
    }

    /// Go `selectResult.readFromChunk`: append small response chunks until the
    /// caller's required row count is reached, but transfer a large typed
    /// chunk directly when the output is still empty.
    ///
    /// `true` means `output` is ready to return. `false` means this decoded
    /// channel is exhausted and the caller may continue with the next chunk.
    fn fill_chunk(
        &mut self,
        decoder: &mut ChunkDecoder,
        output: &mut DecodedChunk,
        required_rows: usize,
    ) -> Result<bool, ResponseChannelError> {
        match self {
            Self::Rows(rows) => {
                while output.num_rows() < required_rows {
                    let Some(row) = rows.pop_front() else {
                        return Ok(false);
                    };
                    for (column, value) in row.iter().enumerate() {
                        output.append_datum(column, value);
                    }
                }
                Ok(true)
            }
            Self::TypeChunk { rows } => {
                debug_assert!(rows.is_none(), "a channel is read by rows or by chunks, not both");
                if decoder.is_finished() {
                    return Ok(false);
                }
                // Go: a response chunk holding more than 0.8 x RequiredRows
                // is swapped in whole (`ReuseIntermChk`) when the output is
                // still empty; otherwise its rows are appended (`Decode`).
                let reuse_threshold = required_rows.saturating_mul(4) / 5;
                if decoder.remained_rows() > reuse_threshold {
                    if output.num_rows() != 0 {
                        return Ok(true);
                    }
                    decoder.reuse_interm_chk(output);
                    return Ok(true);
                }
                decoder.decode_with_required_rows(output, required_rows);
                Ok(output.num_rows() >= required_rows)
            }
        }
    }
}

impl SelectResponseChannel {
    fn recycle_decoded(&mut self) {
        self.decoded = None;
    }

    fn decode_next_chunk(&mut self) -> Result<(), ResponseChannelError> {
        let Some(chunk) = self.chunks.get(self.next_chunk_index) else {
            return Ok(());
        };
        let decoded = decode_channel(
            self.raw_encode_type,
            chunk,
            &self.field_types,
            &self.time_zone,
            &mut self.decoder,
        )?;
        self.next_chunk_index += 1;
        self.decoded = Some(decoded);
        Ok(())
    }

    fn next_row(&mut self) -> Result<Option<SelectResultRow<Vec<Datum>>>, ResponseChannelError> {
        loop {
            if self.decoded.is_some() {
                if let Some(row) = self
                    .decoded
                    .as_mut()
                    .expect("decoded channel is present")
                    .next_row(&mut self.decoder, self.channel_index, &self.field_types)?
                {
                    return Ok(Some(row));
                }
                self.recycle_decoded();
            }
            if self.next_chunk_index == self.chunks.len() {
                return Ok(None);
            }
            self.decode_next_chunk()?;
        }
    }

    fn close(&mut self) {
        if let Some(decoded) = &mut self.decoded {
            decoded.close();
        }
        self.chunks.clear();
    }

    fn fill_chunk(
        &mut self,
        output: &mut DecodedChunk,
        required_rows: usize,
    ) -> Result<bool, ResponseChannelError> {
        loop {
            if self.decoded.is_some() {
                if self
                    .decoded
                    .as_mut()
                    .expect("decoded channel is present")
                    .fill_chunk(&mut self.decoder, output, required_rows)?
                {
                    return Ok(true);
                }
                self.recycle_decoded();
            }
            if self.next_chunk_index == self.chunks.len() {
                return Ok(false);
            }
            self.decode_next_chunk()?;
        }
    }
}

impl SelectResponseIter {
    fn recycle_channel(&mut self, channel: SelectResponseChannel) {
        if let Some(reusable_chunk) = self.reusable_chunks.get_mut(channel.channel_index) {
            *reusable_chunk = Some(channel.decoder.into_intermediate_chunk());
        }
    }

    fn pop_last_channel(&mut self) {
        if let Some(channel) = self.channels.pop() {
            self.recycle_channel(channel);
        }
    }

    fn clear_channels(&mut self) {
        while let Some(channel) = self.channels.pop() {
            self.recycle_channel(channel);
        }
    }

    pub fn from_query_response(
        response: Box<dyn QueryResponse + Send>,
        final_field_types: Vec<FieldType>,
        intermediate_output_types: Vec<Vec<FieldType>>,
        time_zone: SessionTimeZone,
        warnings: WarningCollector,
        result_metadata: SelectResultMetadata,
        runtime_stats_collector_enabled: Option<bool>,
    ) -> Self {
        let runtime_stats_binding =
            runtime_stats_collector_enabled.map(|collector_enabled| RuntimeStatsBinding {
                metadata: result_metadata.clone(),
                collector_enabled,
            });
        Self {
            source: SelectResponseSource::Query(response),
            final_field_types: Arc::from(final_field_types),
            intermediate_output_types: intermediate_output_types
                .into_iter()
                .map(Arc::from)
                .collect(),
            time_zone,
            warnings,
            channels: Vec::new(),
            reusable_chunks: Vec::new(),
            runtime_stats: SelectResultRuntimeStats::default(),
            runtime_stats_binding,
            result_metadata: Some(result_metadata),
            cop_response_sink: None,
            last_fetch: Duration::ZERO,
            total_fetch: Duration::ZERO,
            stream_id: SELECT_RESULT_STREAMS.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            close_reported: false,
            memory_tracker: None,
            closed: false,
            close_err: Ok(()),
        }
    }

    /// Returns the next decoded row or `None` after the response source closes.
    pub fn next_row(
        &mut self,
    ) -> Result<Option<SelectResultRow<Vec<Datum>>>, ResponseChannelError> {
        self.next_row_with_required_rows(1)
    }

    /// Borrows the field types that decoded a response channel. Chunk-backed
    /// consumers use this to preserve the source column-aware datum contract
    /// while formatting directly from a retained chunk row.
    #[must_use]
    pub fn field_types_for_channel(&self, channel_index: usize) -> Option<&[FieldType]> {
        self.channels
            .iter()
            .find(|channel| channel.channel_index == channel_index)
            .map(|channel| channel.field_types.as_ref())
            .or_else(|| {
                if channel_index == self.intermediate_output_types.len() {
                    Some(self.final_field_types.as_ref())
                } else {
                    self.intermediate_output_types
                        .get(channel_index)
                        .map(Arc::as_ref)
                }
            })
    }

    /// Returns the immutable field metadata owner for a decoded response
    /// channel. Go's `selectResult` retains this metadata across result
    /// batches; callers that keep a chunk-backed batch can therefore clone
    /// the `Arc` instead of materializing another `Vec<FieldType>`.
    #[must_use]
    pub fn field_types_shared_for_channel(&self, channel_index: usize) -> Option<Arc<[FieldType]>> {
        self.channels
            .iter()
            .find(|channel| channel.channel_index == channel_index)
            .map(|channel| Arc::clone(&channel.field_types))
            .or_else(|| {
                if channel_index == self.intermediate_output_types.len() {
                    Some(Arc::clone(&self.final_field_types))
                } else {
                    self.intermediate_output_types
                        .get(channel_index)
                        .map(Arc::clone)
                }
            })
    }

    /// Returns the next decoded row while forwarding the caller's remaining
    /// row budget to the raw response owner when another response must be
    /// pulled. A zero budget consumes nothing.
    pub fn next_row_with_required_rows(
        &mut self,
        required_rows: usize,
    ) -> Result<Option<SelectResultRow<Vec<Datum>>>, ResponseChannelError> {
        if required_rows == 0 {
            return Ok(None);
        }
        if self.closed {
            return Ok(None);
        }
        loop {
            while let Some(channel) = self.channels.last_mut() {
                match channel.next_row()? {
                    Some(row) => return Ok(Some(SelectResultRow::new(row.channel_index, row.row))),
                    None => {
                        self.pop_last_channel();
                    }
                }
            }

            let fetch_started = std::time::Instant::now();
            let fetched = self.source.next_event(required_rows);
            self.last_fetch = fetch_started.elapsed();
            self.total_fetch += self.last_fetch;
            let event = match fetched {
                Ok(event) => event,
                Err(ResponseChannelError::Pending) => return Err(ResponseChannelError::Pending),
                Err(error) => {
                    let _ = self.close();
                    return Err(error);
                }
            };
            match event {
                Some(ResponseChannelEvent::Result(bytes)) => {
                    if let Err(error) = self.install_encoded_response(bytes, None) {
                        let _ = self.close();
                        return Err(error);
                    }
                }
                Some(ResponseChannelEvent::ResultWithRuntime {
                    result,
                    runtime_stats,
                }) => {
                    if let Err(error) = self.install_encoded_response(result, Some(&runtime_stats))
                    {
                        let _ = self.close();
                        return Err(error);
                    }
                }
                Some(ResponseChannelEvent::Warning(warning)) => {
                    append_warning(&self.warnings, warning);
                }
                Some(ResponseChannelEvent::Error(message)) => {
                    let error = ResponseChannelError::Source(message);
                    let _ = self.close();
                    return Err(error);
                }
                Some(ResponseChannelEvent::Closed) => {
                    // Go `fetchResp`: the ended stream releases its response;
                    // `Close` registers the select result.
                    self.release_response_memory();
                    self.closed = true;
                    return Ok(None);
                }
                None => return Err(ResponseChannelError::Pending),
            }
        }
    }

    /// Returns the next decoded response batch while preserving the same
    /// reverse channel priority and response lifecycle as [`Self::next_row`].
    /// TypeChunk responses transfer their decoded chunk directly; default-row
    /// responses are collected into one chunk for compatibility.
    pub fn next_chunk_with_required_rows(
        &mut self,
        required_rows: usize,
    ) -> Result<Option<SelectResultRow<DecodedChunk>>, ResponseChannelError> {
        self.next_chunk_reusing(required_rows, &mut None)
    }

    /// Go `selectResult.Next(ctx, chk)` / `readFromChunk`: decodes the next
    /// rows INTO the caller's `chk`, which keeps its own columns -- and so
    /// its capacity -- unless a response chunk holds more than 0.8 x
    /// `required_rows` rows, which `ReuseIntermChk` swaps in instead.
    /// Answers whether the response had rows left to decode.
    pub fn fill_chunk_with_required_rows(
        &mut self,
        required_rows: usize,
        chk: &mut DecodedChunk,
    ) -> Result<bool, ResponseChannelError> {
        chk.reset();
        let mut reuse = Some(std::mem::take(chk));
        let next = self.next_chunk_reusing(required_rows, &mut reuse);
        match next {
            Ok(Some(row)) => {
                *chk = row.row;
                Ok(true)
            }
            other => {
                *chk = reuse.take().expect("an unused caller chunk is handed back");
                other.map(|_| false)
            }
        }
    }

    /// [`Self::next_chunk_with_required_rows`], whose output is `reuse`'s
    /// chunk when there is one; that chunk goes back to `reuse` if it is left
    /// empty.
    fn next_chunk_reusing(
        &mut self,
        required_rows: usize,
        reuse: &mut Option<DecodedChunk>,
    ) -> Result<Option<SelectResultRow<DecodedChunk>>, ResponseChannelError> {
        if required_rows == 0 {
            return Ok(None);
        }
        if self.closed {
            return Ok(None);
        }
        let mut output: Option<SelectResultRow<DecodedChunk>> = None;
        let mut output_reused = false;
        loop {
            while let Some(channel_index) =
                self.channels.last().map(|channel| channel.channel_index)
            {
                if output
                    .as_ref()
                    .is_some_and(|chunk| chunk.channel_index != channel_index)
                {
                    return Ok(output);
                }
                let output_chunk = output.get_or_insert_with(|| {
                    let field_types = &self
                        .channels
                        .last()
                        .expect("the channel index was just read")
                        .field_types;
                    // No preallocated column data: the usual outcome below is
                    // Go's `ReuseIntermChk` swap, after which the decoder
                    // points these columns at the response payload and drops
                    // any buffer they came with (a `required_rows`-sized
                    // jemalloc extent per column, faulted in and freed per
                    // chunk); the append path grows its columns on demand.
                    let reused = reuse.take();
                    output_reused = reused.is_some();
                    SelectResultRow::new(
                        channel_index,
                        reused.unwrap_or_else(|| DecodedChunk::new(field_types, 0, required_rows)),
                    )
                });
                let ready = self
                    .channels
                    .last_mut()
                    .expect("the channel index was just read")
                    .fill_chunk(&mut output_chunk.row, required_rows)?;
                if ready {
                    return Ok(output.take());
                }
                self.pop_last_channel();
                let output_is_empty = output_chunk.row.num_rows() == 0;
                if output_is_empty {
                    let output = output.take();
                    if std::mem::take(&mut output_reused) {
                        *reuse = output.map(|output| output.row);
                    }
                }
            }

            let fetch_started = std::time::Instant::now();
            let fetched = self.source.next_event(required_rows);
            self.last_fetch = fetch_started.elapsed();
            self.total_fetch += self.last_fetch;
            let event = match fetched {
                Ok(event) => event,
                Err(ResponseChannelError::Pending) => {
                    return output
                        .map_or(Err(ResponseChannelError::Pending), |chunk| Ok(Some(chunk)))
                }
                Err(error) => {
                    let _ = self.close();
                    return Err(error);
                }
            };
            match event {
                Some(ResponseChannelEvent::Result(bytes)) => {
                    if let Err(error) = self.install_encoded_response(bytes, None) {
                        let _ = self.close();
                        return Err(error);
                    }
                }
                Some(ResponseChannelEvent::ResultWithRuntime {
                    result,
                    runtime_stats,
                }) => {
                    if let Err(error) = self.install_encoded_response(result, Some(&runtime_stats))
                    {
                        let _ = self.close();
                        return Err(error);
                    }
                }
                Some(ResponseChannelEvent::Warning(warning)) => {
                    append_warning(&self.warnings, warning);
                }
                Some(ResponseChannelEvent::Error(message)) => {
                    let error = ResponseChannelError::Source(message);
                    let _ = self.close();
                    return Err(error);
                }
                Some(ResponseChannelEvent::Closed) => {
                    // Go `fetchResp`: the ended stream releases its response;
                    // `Close` registers the select result.
                    self.release_response_memory();
                    self.closed = true;
                    return Ok(output);
                }
                None => {
                    return output
                        .map_or(Err(ResponseChannelError::Pending), |chunk| Ok(Some(chunk)))
                }
            }
        }
    }

    /// Go `selectResult.close`: closes the underlying response source and
    /// drops pending decoded rows, once; every call answers that close's
    /// error (`closeErr`).
    ///
    /// # Errors
    /// The response's close error.
    pub fn close(&mut self) -> Result<(), ResponseChannelError> {
        if self.closed {
            // A drained stream is still registered by its `Close`.
            self.report_select_result_close();
            return self.close_err.clone();
        }
        for channel in &mut self.channels {
            channel.close();
        }
        self.channels.clear();
        self.close_err = self.source.close();
        self.report_select_result_close();
        // Go `closeImpl`: `memConsume(-respSize)`.
        self.release_response_memory();
        self.closed = true;
        self.close_err.clone()
    }

    /// Go `memConsume(-respSize)`: the response this select result held is
    /// released, by `closeImpl` or by the `fetchResp` that finds the stream
    /// ended.
    fn release_response_memory(&mut self) {
        if let Some((tracker, charged)) = self.memory_tracker.as_mut() {
            tracker.consume(-*charged);
            *charged = 0;
        }
    }

    /// Go `selectResult.closeImpl`'s registration, once per select result,
    /// from `close`.
    fn report_select_result_close(&mut self) {
        if self.close_reported {
            return;
        }
        self.close_reported = true;
        let cop_info = self.source.cop_info();
        let mut limiter_wait = self.runtime_stats.limiter_wait;
        limiter_wait.merge(self.source.limiter_wait_stats());
        if let (Some(binding), Some(sink)) = (&self.runtime_stats_binding, &self.cop_response_sink)
        {
            if binding.collector_enabled {
                sink.close_select_result(
                    self.stream_id,
                    &binding.metadata,
                    &SelectResultClose {
                        fetch_duration: self.total_fetch,
                        limiter_wait,
                        cop_info,
                    },
                );
            }
        }
    }

    /// Returns whether close or natural source exhaustion has occurred.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.closed
    }

    /// Merges one response subset's runtime sample into this consumer.
    ///
    /// The caller supplies the non-protobuf coprocessor detail carried beside
    /// the raw response; the checked execution summaries remain authoritative
    /// tipb values. This delegates to the single runtime-stat accumulator.
    pub fn update_runtime_stats(
        &mut self,
        metadata: &SelectResultMetadata,
        collector_enabled: bool,
        callee_address: &str,
        request_rpc_stats_present: bool,
        backoff_sleep_ns: impl IntoIterator<Item = (String, u64)>,
        execution_summaries: &[ExecutorExecutionSummary],
    ) {
        self.runtime_stats.update(
            metadata,
            collector_enabled,
            callee_address,
            request_rpc_stats_present,
            backoff_sleep_ns,
            execution_summaries,
        );
    }

    /// Merges request-limiter wait evidence transferred from the response
    /// owner. Go records this once when `selectResult` closes, rather than
    /// attaching it to an individual protobuf response sample.
    pub fn record_limiter_wait(&mut self, stats: LimiterWaitStats) {
        self.runtime_stats.limiter_wait.merge(stats);
    }

    /// Borrows the runtime statistics accumulated by this consumer.
    #[must_use]
    pub const fn runtime_stats(&self) -> &SelectResultRuntimeStats {
        &self.runtime_stats
    }

    /// Borrows source result metadata retained with this live iterator.
    #[must_use]
    pub const fn result_metadata(&self) -> Option<&SelectResultMetadata> {
        self.result_metadata.as_ref()
    }

    /// Attaches the metadata created by the source `selectResult`
    /// constructor to the iterator that owns the returned response lifecycle.
    #[must_use]
    pub fn with_result_metadata(mut self, metadata: SelectResultMetadata) -> Self {
        self.result_metadata = Some(metadata);
        self
    }

    /// Binds plan metadata used to merge each consumed response's execution
    /// summaries automatically.
    #[must_use]
    pub fn with_runtime_stats(
        mut self,
        metadata: SelectResultMetadata,
        collector_enabled: bool,
    ) -> Self {
        self.runtime_stats_binding = Some(RuntimeStatsBinding {
            metadata,
            collector_enabled,
        });
        self
    }

    /// Binds the statement's consumer of per-response execution details
    /// (Go's `StmtCtx.ExecDetails` and `RuntimeStatsColl`).
    #[must_use]
    pub fn with_cop_response_sink(mut self, sink: Option<Arc<dyn CopResponseSink>>) -> Self {
        self.cop_response_sink = sink;
        self
    }

    /// Appends a response to the still-open owned source.
    ///
    /// This is the synchronous producer seam until the concrete blocking TiKV
    /// transport owns this iterator. A pending read never closes this seam.
    pub fn push_response(
        &mut self,
        bytes: impl Into<prost::bytes::Bytes>,
    ) -> Result<(), ResponseChannelError> {
        self.source.push_result(bytes.into())
    }

    /// Appends a response with CopRuntimeStats from the same producer call.
    pub fn push_response_with_runtime(
        &mut self,
        bytes: impl Into<prost::bytes::Bytes>,
        runtime_stats: ResponseRuntimeStats,
    ) -> Result<(), ResponseChannelError> {
        self.source
            .push_result_with_runtime(bytes.into(), runtime_stats)
    }

    /// Marks the owned producer seam complete.
    pub fn finish_source(&mut self) -> Result<(), ResponseChannelError> {
        self.source.finish()
    }

    fn install_encoded_response(
        &mut self,
        bytes: prost::bytes::Bytes,
        runtime_stats: Option<&ResponseRuntimeStats>,
    ) -> Result<(), ResponseChannelError> {
        // Go `fetchResp`: release the previous response, charge this one.
        if let Some((tracker, charged)) = self.memory_tracker.as_mut() {
            let size = i64::try_from(bytes.len()).unwrap_or(i64::MAX);
            tracker.consume(size - *charged);
            *charged = size;
        }
        let response = decode_select_response(bytes)
            .map_err(|error| ResponseChannelError::Decode(error.to_string()))?;
        self.install_response(response, runtime_stats)
    }

    /// Go `kvReq.MemTracker` of the reader: each response this select result
    /// holds is charged to `tracker` until the next replaces it or Close.
    #[must_use]
    pub fn with_memory_tracker(mut self, tracker: Option<Arc<tidb_memory::Tracker>>) -> Self {
        self.memory_tracker = tracker.map(|tracker| (tracker, 0));
        self
    }

    fn install_response(
        &mut self,
        response: SelectResponse,
        response_runtime_stats: Option<&ResponseRuntimeStats>,
    ) -> Result<(), ResponseChannelError> {
        if let Some(error) = response.error.as_ref() {
            return Err(ResponseChannelError::SelectResponse {
                code: error.code.unwrap_or_default(),
                message: error.msg.clone().unwrap_or_default(),
            });
        }
        if response.intermediate_outputs.len() != self.intermediate_output_types.len() {
            return Err(ResponseChannelError::IntermediateOutputCount {
                expected: self.intermediate_output_types.len(),
                actual: response.intermediate_outputs.len(),
            });
        }
        for warning in &response.warnings {
            self.warnings.append_tikv_warning(
                warning.code.unwrap_or_default(),
                warning.msg.clone().unwrap_or_default(),
            );
        }
        if let (Some(binding), Some(sample), Some(sink)) = (
            &self.runtime_stats_binding,
            response_runtime_stats,
            &self.cop_response_sink,
        ) {
            if binding.collector_enabled {
                sink.record_cop_response(
                    self.stream_id,
                    &binding.metadata,
                    sample,
                    self.last_fetch,
                    &response.execution_summaries,
                );
            }
        }
        if let (Some(binding), Some(sample)) = (&self.runtime_stats_binding, response_runtime_stats)
        {
            self.runtime_stats.update(
                &binding.metadata,
                binding.collector_enabled,
                &sample.callee_address,
                sample.request_rpc_stats_present,
                sample.backoff_sleep_ns.clone(),
                &response.execution_summaries,
            );
        }

        self.clear_channels();
        let channel_count = self.intermediate_output_types.len() + 1;
        if self.reusable_chunks.len() != channel_count {
            self.reusable_chunks.resize_with(channel_count, || None);
        }
        for (channel_index, output) in response.intermediate_outputs.into_iter().enumerate() {
            let field_types = Arc::clone(&self.intermediate_output_types[channel_index]);
            let reusable_chunk = self.reusable_chunks[channel_index].take();
            self.channels.push(SelectResponseChannel {
                channel_index,
                raw_encode_type: output.encode_type,
                chunks: output.chunks,
                field_types: Arc::clone(&field_types),
                decoder: response_decoder(reusable_chunk, field_types),
                time_zone: self.time_zone.clone(),
                next_chunk_index: 0,
                decoded: None,
            });
        }
        let final_field_types = Arc::clone(&self.final_field_types);
        let reusable_chunk = self
            .reusable_chunks
            .get_mut(self.intermediate_output_types.len())
            .and_then(Option::take);
        self.channels.push(SelectResponseChannel {
            channel_index: self.intermediate_output_types.len(),
            raw_encode_type: response.encode_type,
            chunks: response.chunks,
            field_types: Arc::clone(&final_field_types),
            decoder: response_decoder(reusable_chunk, final_field_types),
            time_zone: self.time_zone.clone(),
            next_chunk_index: 0,
            decoded: None,
        });
        Ok(())
    }
}

/// Go `chunk.NewDecoder(chunk.NewChunkWithCapacity(fieldTypes, 0), fieldTypes)`,
/// or the decoder over the intermediate chunk a previous response left.
fn response_decoder(reusable_chunk: Option<DecodedChunk>, field_types: Arc<[FieldType]>) -> ChunkDecoder {
    let intermediate = reusable_chunk.unwrap_or_else(|| DecodedChunk::new_empty_chunk(&field_types));
    ChunkDecoder::with_codec(intermediate, ChunkCodec::new_shared(field_types))
}

fn decode_channel(
    raw_encode_type: Option<i32>,
    chunk: &ResponseChunk,
    field_types: &[FieldType],
    time_zone: &SessionTimeZone,
    decoder: &mut ChunkDecoder,
) -> Result<DecodedChannel, ResponseChannelError> {
    let raw_encode_type = raw_encode_type.unwrap_or(EncodeType::TypeDefault as i32);
    let encode_type = EncodeType::try_from(raw_encode_type).map_err(|_| {
        ResponseChannelError::RowDecode(format!("invalid tipb encode type {raw_encode_type}"))
    })?;
    if chunk.rows_data.as_deref().unwrap_or_default().is_empty() {
        return Ok(DecodedChannel::Rows(VecDeque::new()));
    }
    let raw = decode_chunk(chunk, encode_type).map_err(map_chunk_error)?;
    match encode_type {
        EncodeType::TypeDefault => {
            let rows = raw
                .decode_default_datums_in_timezone(field_types, time_zone)
                .map_err(map_chunk_error)?;
            Ok(DecodedChannel::Rows(rows.into()))
        }
        EncodeType::TypeChunk => {
            // Go `respChunkDecoder.Reset`: the columns share the response
            // payload (`decodeColumn`).
            let empty = prost::bytes::Bytes::new();
            let rows_data = chunk.rows_data.as_ref().unwrap_or(&empty);
            decoder
                .try_reset_bytes(rows_data)
                .map_err(|error| ResponseChannelError::RowDecode(error.to_string()))?;
            Ok(DecodedChannel::TypeChunk { rows: None })
        }
        EncodeType::TypeChBlock => Err(ResponseChannelError::RowDecode(
            "TypeCHBlock row materialization is not implemented".to_owned(),
        )),
    }
}

fn append_warning(collector: &WarningCollector, warning: Warning) {
    collector.append_owned_warning(warning);
}

fn map_chunk_error(error: ChunkDecodeError) -> ResponseChannelError {
    ResponseChannelError::RowDecode(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::FieldTypeCode;

    fn decoder_over(fields: &[FieldType], source: &DecodedChunk) -> ChunkDecoder {
        let image = prost::bytes::Bytes::from(ChunkCodec::new(fields.to_vec()).encode(source));
        let mut decoder = response_decoder(None, Arc::from(fields.to_vec()));
        decoder.try_reset_bytes(&image).expect("decode response image");
        decoder
    }

    /// Go `pkg/util/chunk/codec.go::ReuseIntermChk` swaps an empty caller
    /// chunk with the decoder's sufficiently large intermediate chunk, so the
    /// decoded rows transfer without copying and the caller's columns become
    /// the intermediate chunk the next response decodes into.
    #[test]
    fn typed_chunk_large_result_reuses_the_empty_output_chunk() {
        let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
        let mut source = DecodedChunk::new_chunk_with_capacity(&fields, 2);
        source.append_int64(0, 11);
        source.append_int64(0, 22);
        let mut decoder = decoder_over(&fields, &source);
        let mut decoded = DecodedChannel::TypeChunk { rows: None };
        let mut output = DecodedChunk::new_chunk_with_capacity(&fields, 1);

        assert!(decoded.fill_chunk(&mut decoder, &mut output, 1).expect("fill chunk"));
        assert_eq!(output.num_rows(), 2);
        assert_eq!(
            output.get_row(0).get_datum(0, &fields[0]),
            Datum::new_int(11)
        );
        assert_eq!(
            output.get_row(1).get_datum(0, &fields[0]),
            Datum::new_int(22)
        );
        assert!(decoder.is_finished());
        assert_eq!(output.capacity(), 1);
        assert_eq!(decoder.into_intermediate_chunk().num_rows(), 0);
    }

    /// Go `selectResult.readFromChunk` appends a small response through
    /// `Decoder.Decode`, which rounds the rows it takes up to a multiple of
    /// eight: a chunk holding 8 of its 10 required rows takes all 5 rows of
    /// the next response, ending at 13.
    #[test]
    fn typed_chunk_appends_rounded_up_rows_like_go_decode() {
        let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
        let response = |rows: i64| {
            let mut source = DecodedChunk::new_chunk_with_capacity(&fields, rows as usize);
            for value in 0..rows {
                source.append_int64(0, value);
            }
            source
        };
        let mut output = DecodedChunk::new(&fields, 0, 10);
        let mut decoder = decoder_over(&fields, &response(8));
        let mut decoded = DecodedChannel::TypeChunk { rows: None };
        assert!(!decoded.fill_chunk(&mut decoder, &mut output, 10).expect("fill chunk"));
        assert_eq!(output.num_rows(), 8);
        assert!(!decoded.fill_chunk(&mut decoder, &mut output, 10).expect("fill chunk"));

        let image = prost::bytes::Bytes::from(ChunkCodec::new(fields.clone()).encode(&response(5)));
        decoder.try_reset_bytes(&image).expect("decode response image");
        assert!(decoded.fill_chunk(&mut decoder, &mut output, 10).expect("fill chunk"));
        assert_eq!(output.num_rows(), 13);
        assert!(decoder.is_finished());
    }

    /// Go `Decoder.Decode` appends a response's offsets and null bitmap with
    /// one `append` each: four VARCHAR rows decoded into Go's capacity-0
    /// HashAgg input chunk (`chunk.New(fields, 0, maxChunkSize)`) grow it
    /// from 240 to 336 bytes (offsets 5/6, data 7/8, bitmap 1/8), as the Go
    /// probe of `b, count(a) ... GROUP BY b` measured.
    #[test]
    fn typed_chunk_decode_grows_columns_like_go_append() {
        let fields = vec![
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::Varchar),
        ];
        let mut source = DecodedChunk::new_chunk_with_capacity(&fields, 4);
        for (count, value) in [(1, "x"), (1, "yy"), (1, "zzz"), (1, "w")] {
            source.append_int64(0, count);
            source.append_string(1, value);
        }
        let mut decoder = decoder_over(&fields, &source);
        let mut decoded = DecodedChannel::TypeChunk { rows: None };
        let mut output = DecodedChunk::new(&fields, 0, 1024);
        assert_eq!(output.memory_usage(), 240);

        assert!(!decoded.fill_chunk(&mut decoder, &mut output, 1024).expect("fill chunk"));
        assert_eq!(output.num_rows(), 4);
        assert_eq!(output.memory_usage(), 336);
    }
}
