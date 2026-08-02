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

use std::time::{Duration, Instant};

use tidb_proto::pdpb::{self, pd_client::PdClient as TonicPdClient};
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::{PdClientError, PdOperation};

pub(crate) const MAX_TSO_RETRIES: usize = 20;
pub(crate) const TSO_RETRY_INTERVAL: Duration = Duration::from_millis(100);

const PHYSICAL_SHIFT_BITS: u32 = 18;
const MAX_LOGICAL: i64 = (1_i64 << PHYSICAL_SHIFT_BITS) - 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TimestampParts {
    physical: i64,
    logical: i64,
}

/// One PD Tso reply covering `count` consecutive timestamps.
///
/// Go boundary: `pd/client`'s `tso_dispatcher.go` -> `processRequests`, which
/// treats the reply's `(physical, logical)` as the *last* timestamp of the
/// batch and recovers the first with
/// `firstLogical = AddLogical(logical, -(count-1), suffixBits)`. Waiter `i`
/// then receives `(physical, AddLogical(firstLogical, i, suffixBits))`
/// (`tso_batch_controller.go` -> `finishCollectedRequests`), so every waiter
/// gets a distinct, increasing timestamp out of a single round trip.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TsoBatch {
    physical: i64,
    first_logical: i64,
    suffix_bits: u32,
    count: u32,
}

impl TsoBatch {
    /// Returns the timestamp handed to the `index`-th waiter of this batch.
    pub(crate) fn split(&self, index: u32) -> TimestampParts {
        debug_assert!(index < self.count);
        TimestampParts {
            physical: self.physical,
            logical: add_logical(self.first_logical, i64::from(index), self.suffix_bits),
        }
    }

    /// The batch's last timestamp, the one PD actually reported.
    pub(crate) fn last(&self) -> TimestampParts {
        self.split(self.count - 1)
    }
}

/// Go boundary: `pd/client`'s `tsoutil.AddLogical` — the count is shifted by
/// the suffix bits PD reserves for its local-TSO allocator suffix.
fn add_logical(logical: i64, count: i64, suffix_bits: u32) -> i64 {
    logical + (count << suffix_bits)
}

impl TimestampParts {
    fn from_response(
        endpoint: &str,
        expected_cluster_id: u64,
        response: pdpb::TsoResponse,
        expected_count: u32,
    ) -> Result<TsoBatch, PdClientError> {
        let header = response
            .header
            .as_ref()
            .ok_or(PdClientError::MissingHeader(PdOperation::Tso))?;
        if let Some(error) = &header.error {
            return Err(PdClientError::HeaderError {
                operation: PdOperation::Tso,
                error_type: error.r#type,
                message: error.message.clone(),
            });
        }
        if header.cluster_id != expected_cluster_id {
            return Err(PdClientError::ClusterMismatch {
                operation: PdOperation::Tso,
                expected: expected_cluster_id,
                actual: header.cluster_id,
            });
        }
        if response.count != expected_count {
            return Err(invalid_tso(
                "tso_count_mismatch",
                format!(
                    "PD Tso from {endpoint} returned count {}, expected {expected_count}",
                    response.count
                ),
            ));
        }
        let timestamp = response.timestamp.ok_or_else(|| {
            invalid_tso(
                "missing_tso_timestamp",
                format!("PD Tso from {endpoint} omitted its timestamp"),
            )
        })?;
        if timestamp.physical < 0 {
            return Err(invalid_tso(
                "negative_tso_physical",
                format!(
                    "PD Tso from {endpoint} returned negative physical time {}",
                    timestamp.physical
                ),
            ));
        }
        if !(0..=MAX_LOGICAL).contains(&timestamp.logical) {
            return Err(invalid_tso(
                "invalid_tso_logical",
                format!(
                    "PD Tso from {endpoint} returned logical time {} outside 0..={MAX_LOGICAL}",
                    timestamp.logical
                ),
            ));
        }
        // PD reports the batch's LAST timestamp; walk back over the remaining
        // `count - 1` slots to find the first one this batch owns.
        let first_logical = add_logical(
            timestamp.logical,
            -i64::from(expected_count) + 1,
            timestamp.suffix_bits,
        );
        if first_logical < 0 {
            return Err(invalid_tso(
                "invalid_tso_batch_range",
                format!(
                    "PD Tso from {endpoint} returned logical {} too small for a batch of {expected_count}",
                    timestamp.logical
                ),
            ));
        }
        Ok(TsoBatch {
            physical: timestamp.physical,
            first_logical,
            suffix_bits: timestamp.suffix_bits,
            count: expected_count,
        })
    }

    pub(crate) fn compose(self) -> Result<u64, PdClientError> {
        let timestamp = u64::try_from(self.physical)
            .ok()
            .and_then(|physical| physical.checked_shl(PHYSICAL_SHIFT_BITS))
            .and_then(|physical| physical.checked_add(self.logical as u64))
            .ok_or_else(|| {
                invalid_tso(
                    "tso_overflow",
                    format!(
                        "PD Tso ({}, {}) does not fit the TiKV timestamp layout",
                        self.physical, self.logical
                    ),
                )
            })?;
        if timestamp == 0 {
            return Err(invalid_tso(
                "zero_tso",
                "PD Tso composed to zero".to_owned(),
            ));
        }
        Ok(timestamp)
    }

    pub(crate) fn ensure_after(self, previous: Option<Self>) -> Result<(), PdClientError> {
        if let Some(previous) = previous {
            if self.physical < previous.physical
                || (self.physical == previous.physical && self.logical <= previous.logical)
            {
                return Err(invalid_tso(
                    "tso_fallback",
                    format!(
                        "PD Tso ({}, {}) is not after ({}, {})",
                        self.physical, self.logical, previous.physical, previous.logical
                    ),
                ));
            }
        }
        Ok(())
    }
}

pub(crate) struct RetainedTsoStream {
    endpoint: String,
    requests: tokio::sync::mpsc::Sender<pdpb::TsoRequest>,
    responses: tonic::Streaming<pdpb::TsoResponse>,
}

impl RetainedTsoStream {
    pub(crate) fn open_and_request(
        runtime: &tokio::runtime::Runtime,
        client: &mut TonicPdClient<Channel>,
        endpoint: &str,
        cluster_id: u64,
        deadline: Instant,
        shutdown: &watch::Receiver<bool>,
        count: u32,
    ) -> Result<(Self, TsoBatch), PdClientError> {
        let timeout = remaining(deadline, endpoint)?;
        let (requests, receiver) = tokio::sync::mpsc::channel(1);
        let request = tso_request(cluster_id, count);
        if *shutdown.borrow() {
            return Err(PdClientError::Closed);
        }
        let mut cancellation = shutdown.clone();
        let response = runtime.block_on(async {
            tokio::select! {
                biased;
                () = shutdown_requested(&mut cancellation) => None,
                response = tokio::time::timeout(timeout, async {
                // PD may wait for its first inbound TSO request before it
                // publishes response headers. Queue the request before
                // polling the tonic open future, matching grpc-go's
                // send-capable stream creation without an open-before-send
                // cycle.
                requests
                    .send(request)
                    .await
                    .map_err(|_| tonic::Status::unavailable("PD Tso request stream is closed"))?;
                let mut responses = client
                    .tso(ReceiverStream::new(receiver))
                    .await?
                    .into_inner();
                let response = responses.message().await?.ok_or_else(|| {
                    tonic::Status::unavailable("PD Tso response stream is closed")
                })?;
                Ok::<_, tonic::Status>((responses, response))
                }) => Some(response),
            }
        });
        let (responses, response) = match response {
            Some(Ok(Ok(response))) => response,
            Some(Ok(Err(status))) => return Err(map_status(endpoint, status)),
            Some(Err(_)) => return Err(timeout_error(endpoint, timeout)),
            None => return Err(PdClientError::Closed),
        };
        let timestamp = TimestampParts::from_response(endpoint, cluster_id, response, count)?;
        Ok((
            Self {
                endpoint: endpoint.to_owned(),
                requests,
                responses,
            },
            timestamp,
        ))
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(crate) fn request(
        &mut self,
        runtime: &tokio::runtime::Runtime,
        cluster_id: u64,
        deadline: Instant,
        shutdown: &watch::Receiver<bool>,
        count: u32,
    ) -> Result<TsoBatch, PdClientError> {
        let timeout = remaining(deadline, &self.endpoint)?;
        let request = tso_request(cluster_id, count);
        if *shutdown.borrow() {
            return Err(PdClientError::Closed);
        }
        let mut cancellation = shutdown.clone();
        let response = runtime.block_on(async {
            tokio::select! {
                biased;
                () = shutdown_requested(&mut cancellation) => None,
                response = tokio::time::timeout(timeout, async {
                    self.requests.send(request).await.map_err(|_| {
                        tonic::Status::unavailable("PD Tso request stream is closed")
                    })?;
                    self.responses.message().await?.ok_or_else(|| {
                        tonic::Status::unavailable("PD Tso response stream is closed")
                    })
                })
                => Some(response),
            }
        });
        let response = match response {
            Some(Ok(Ok(response))) => response,
            Some(Ok(Err(status))) => return Err(map_status(&self.endpoint, status)),
            Some(Err(_)) => return Err(timeout_error(&self.endpoint, timeout)),
            None => return Err(PdClientError::Closed),
        };
        TimestampParts::from_response(&self.endpoint, cluster_id, response, count)
    }
}

async fn shutdown_requested(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.changed().await;
}

fn tso_request(cluster_id: u64, count: u32) -> pdpb::TsoRequest {
    pdpb::TsoRequest {
        header: Some(pdpb::RequestHeader {
            cluster_id,
            sender_id: 0,
            caller_id: String::new(),
            caller_component: String::new(),
        }),
        count,
        dc_location: String::new(),
    }
}

pub(crate) fn is_retryable_tso_error(error: &PdClientError) -> bool {
    let (code, message) = match error {
        PdClientError::Transport { code, message, .. } => (Some(code.as_str()), message.as_str()),
        PdClientError::HeaderError { message, .. } => (None, message.as_str()),
        _ => return false,
    };
    let message = message.to_ascii_lowercase();
    code.is_some_and(|code| matches!(code, "Unavailable" | "Cancelled"))
        || [
            "no leader",
            "not leader",
            "not served",
            "not primary",
            "mismatch callee id",
        ]
        .iter()
        .any(|marker| message.contains(marker))
}

pub(crate) fn retry_delay(retry_index: usize) -> Duration {
    if retry_index == 0 {
        Duration::ZERO
    } else {
        TSO_RETRY_INTERVAL
    }
}

pub(crate) fn remaining(deadline: Instant, endpoint: &str) -> Result<Duration, PdClientError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| timeout_error(endpoint, Duration::ZERO))
}

fn map_status(endpoint: &str, status: tonic::Status) -> PdClientError {
    PdClientError::Transport {
        operation: PdOperation::Tso,
        endpoint: endpoint.to_owned(),
        code: format!("{:?}", status.code()),
        message: status.message().to_owned(),
    }
}

fn timeout_error(endpoint: &str, timeout: Duration) -> PdClientError {
    PdClientError::Timeout {
        operation: PdOperation::Tso,
        endpoint: endpoint.to_owned(),
        timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
    }
}

fn invalid_tso(kind: &'static str, message: String) -> PdClientError {
    PdClientError::InvalidTopology { kind, message }
}

/// The suffix-bit arithmetic, pinned against an independently written model of
/// the Go formula.
///
/// A live playground PD always reports `suffix_bits = 0`, so the shifting path
/// below is unreachable from any local cluster; these tests are the only guard
/// that a multi-DC PD (`enable-local-tso = true`, which makes PD's
/// `CalSuffixBits` return a non-zero width) would still be split correctly.
#[cfg(test)]
mod tests {
    use super::{add_logical, TimestampParts, MAX_LOGICAL, PHYSICAL_SHIFT_BITS};
    use tidb_proto::pdpb;

    const CLUSTER_ID: u64 = 7;
    const SUFFIX_WIDTHS: [u32; 5] = [0, 1, 2, 4, 8];

    fn reply(physical: i64, last_logical: i64, suffix_bits: u32, count: u32) -> pdpb::TsoResponse {
        pdpb::TsoResponse {
            header: Some(pdpb::ResponseHeader {
                cluster_id: CLUSTER_ID,
                error: None,
            }),
            count,
            timestamp: Some(pdpb::Timestamp {
                physical,
                logical: last_logical,
                suffix_bits,
            }),
        }
    }

    /// Independent model of Go's composition, written from the *last* logical
    /// PD reports rather than from a recovered first one, so it shares no
    /// arithmetic with `TsoBatch`.
    ///
    /// `pd/client`'s `tsoutil.AddLogical` is
    /// `logical + count<<suffixBits`; `tso_dispatcher.go` recovers
    /// `firstLogical := AddLogical(result.logical, -int64(result.count)+1, result.suffixBits)`
    /// and `tso_batch_controller.go` hands waiter `i`
    /// `AddLogical(firstLogical, int64(i), suffixBits)`. Substituting one into
    /// the other, waiter `i` of a `count`-wide batch owns
    /// `last_logical - ((count - 1 - i) << suffixBits)`.
    fn go_model_logical(last_logical: i64, suffix_bits: u32, count: u32, index: u32) -> i64 {
        let steps_back = i64::from(count - 1 - index);
        last_logical - (steps_back << suffix_bits)
    }

    /// `client-go`'s `oracle.ComposeTS`: `uint64((physical << 18) + logical)`.
    fn go_model_compose(physical: i64, logical: i64) -> u64 {
        #[expect(clippy::cast_sign_loss, reason = "model mirrors Go's int64->uint64")]
        {
            ((physical << PHYSICAL_SHIFT_BITS) + logical) as u64
        }
    }

    fn batch(physical: i64, last_logical: i64, suffix_bits: u32, count: u32) -> super::TsoBatch {
        TimestampParts::from_response(
            "model",
            CLUSTER_ID,
            reply(physical, last_logical, suffix_bits, count),
            count,
        )
        .expect("batch fits the logical range")
    }

    #[test]
    fn add_logical_is_gos_shift_before_add() {
        // tsoutil.AddLogical: `return logical + count<<suffixBits`.
        for suffix_bits in SUFFIX_WIDTHS {
            for logical in 0..64_i64 {
                for count in -8..=8_i64 {
                    assert_eq!(
                        add_logical(logical, count, suffix_bits),
                        logical + (count << suffix_bits),
                        "add_logical({logical}, {count}, {suffix_bits})"
                    );
                }
            }
        }
    }

    /// Exhaustive over the whole small range: every reachable `(last_logical,
    /// suffix_bits, count)` triple is split and compared timestamp by
    /// timestamp against the model.
    #[test]
    fn every_batch_split_matches_the_go_model() {
        const PHYSICAL: i64 = 1_700_000_000_000;
        let mut checked = 0_u32;
        for suffix_bits in SUFFIX_WIDTHS {
            for last_logical in 0..256_i64 {
                for count in 1..=8_u32 {
                    let first = go_model_logical(last_logical, suffix_bits, count, 0);
                    let parsed = TimestampParts::from_response(
                        "model",
                        CLUSTER_ID,
                        reply(PHYSICAL, last_logical, suffix_bits, count),
                        count,
                    );
                    if first < 0 {
                        // The batch reaches below logical zero; PD cannot have
                        // produced it and we must refuse rather than wrap.
                        assert_eq!(
                            parsed.expect_err("underflowing batch is refused").kind(),
                            "invalid_tso_batch_range"
                        );
                        continue;
                    }
                    let batch = parsed.expect("in-range batch parses");
                    let mut previous: Option<u64> = None;
                    for index in 0..count {
                        let parts = batch.split(index);
                        let expected = go_model_logical(last_logical, suffix_bits, count, index);
                        assert_eq!(
                            parts,
                            TimestampParts {
                                physical: PHYSICAL,
                                logical: expected,
                            },
                            "split({index}) of last={last_logical} suffix={suffix_bits} count={count}"
                        );
                        let composed = parts.compose().expect("composes");
                        assert_eq!(composed, go_model_compose(PHYSICAL, expected));
                        // Distinct and strictly monotonic within the batch.
                        if let Some(previous) = previous {
                            assert!(
                                previous < composed,
                                "batch went backwards at {index} (suffix={suffix_bits})"
                            );
                        }
                        previous = Some(composed);
                        checked += 1;
                    }
                    // The batch's last entry is exactly what PD reported.
                    assert_eq!(
                        batch.last(),
                        TimestampParts {
                            physical: PHYSICAL,
                            logical: last_logical,
                        }
                    );
                }
            }
        }
        assert!(checked > 5_000, "the sweep must actually cover the space");
    }

    /// Regression guard: with `suffix_bits = 0` — the only value any local PD
    /// has ever returned — the batch is the contiguous run this client has
    /// always handed out. If this ever changes, every recorded expectation
    /// built against a playground PD moved with it.
    #[test]
    fn suffix_bits_zero_keeps_the_contiguous_range() {
        for last_logical in 0..256_i64 {
            for count in 1..=8_u32 {
                if last_logical < i64::from(count) - 1 {
                    continue;
                }
                let batch = batch(5, last_logical, 0, count);
                let logicals: Vec<i64> = (0..count).map(|i| batch.split(i).logical).collect();
                let expected: Vec<i64> =
                    (last_logical - i64::from(count) + 1..=last_logical).collect();
                assert_eq!(logicals, expected);
            }
        }
    }

    /// Across suffix widths the batches share exactly one timestamp — the last
    /// one, which is the value PD itself reported. Anything else coinciding
    /// would mean two allocators could hand out the same timestamp.
    #[test]
    fn only_the_reported_timestamp_coincides_across_suffix_widths() {
        // Wide enough that the widest suffix (8 bits, 256 per step) still
        // leaves the batch inside the logical field.
        const LAST: i64 = 2_000;
        const COUNT: u32 = 5;
        for (a_index, a) in SUFFIX_WIDTHS.iter().enumerate() {
            for b in &SUFFIX_WIDTHS[a_index + 1..] {
                let left = batch(9, LAST, *a, COUNT);
                let right = batch(9, LAST, *b, COUNT);
                for i in 0..COUNT {
                    for j in 0..COUNT {
                        let collide = left.split(i) == right.split(j);
                        let expected = go_model_logical(LAST, *a, COUNT, i)
                            == go_model_logical(LAST, *b, COUNT, j);
                        assert_eq!(
                            collide, expected,
                            "suffix {a} slot {i} vs suffix {b} slot {j}"
                        );
                    }
                }
                // Only the final slot is shared.
                assert_eq!(left.split(COUNT - 1), right.split(COUNT - 1));
                for i in 0..COUNT - 1 {
                    assert_ne!(left.split(i), right.split(i));
                }
            }
        }
    }

    /// A logical part outside PD's 18-bit field is refused before any
    /// arithmetic runs, so the shift can never carry into the physical part.
    #[test]
    fn out_of_range_logical_is_refused_for_every_suffix_width() {
        for suffix_bits in SUFFIX_WIDTHS {
            for logical in [-1, MAX_LOGICAL + 1] {
                let error = TimestampParts::from_response(
                    "model",
                    CLUSTER_ID,
                    reply(5, logical, suffix_bits, 1),
                    1,
                )
                .expect_err("out-of-range logical is refused");
                assert_eq!(error.kind(), "invalid_tso_logical");
            }
        }
    }
}
