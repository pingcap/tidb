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

use std::time::Duration;

use prost::Message;
use tidb_proto::{
    KvrpcCheckTxnStatusRequest, KvrpcCheckTxnStatusResponse, KvrpcContext, KvrpcResolveLockRequest,
    KvrpcResolveLockResponse,
};

use crate::region::StoreLiveness;
use crate::{DirectUnaryClient, DirectUnaryRequest, DirectUnaryResponse};

use super::batch::{BatchCommandEntry, BatchCoprocessorPending, BatchPublicationReceipt};
use super::liveness::DEFAULT_STORE_LIVENESS_TIMEOUT;
use super::unary::{RawTransportClient, RawUnaryRequest, UnaryCallContext};
use super::{
    AsyncRequestDispatcher, DirectUnaryClientError, PendingRequest, TransportShutdownCancellation,
};

pub(super) use super::unary::RawProtobufCodec;

const MAX_PROTOBUF_FIELD_NUMBER: u64 = (1 << 29) - 1;
const COPROCESSOR_PATH: &str = "/tikvpb.Tikv/Coprocessor";
const CHECK_TXN_STATUS_PATH: &str = "/tikvpb.Tikv/KvCheckTxnStatus";
const RESOLVE_LOCK_PATH: &str = "/tikvpb.Tikv/KvResolveLock";

/// Synchronous client-go-shaped TiKV transport capability backed by tonic.
///
/// A dedicated worker thread owns the Tokio runtime and every tonic channel.
/// Consequently the synchronous trait is safe to call from either ordinary or
/// already-async-hosted threads: it never nests `Runtime::block_on`. Channels
/// are created lazily, reused by address, and versioned on recreation.
pub struct TonicCoprocessorClient {
    transport: RawTransportClient,
}

impl Clone for TonicCoprocessorClient {
    /// Creates a non-owning command capability for the same transport worker.
    ///
    /// Closing or dropping the clone invalidates only that clone. The original
    /// client returned by [`Self::new`] retains the unique worker lifecycle.
    fn clone(&self) -> Self {
        let transport = self.transport.clone();
        Self { transport }
    }
}

impl TonicCoprocessorClient {
    /// Constructs a live client without opening a socket.
    pub fn new() -> Result<Self, DirectUnaryClientError> {
        Ok(Self {
            transport: RawTransportClient::new()?,
        })
    }

    /// Whether this value retains the unique worker shutdown and join authority.
    #[must_use]
    pub const fn is_transport_owner(&self) -> bool {
        self.transport.is_owner()
    }

    /// Admits opaque commands to the retained scheduler and tonic duplex stream.
    ///
    /// Each entry already carries the sole completion returned to its caller.
    /// The returned receipts prove atomic in-flight publication; terminal send,
    /// receive, reconnect, cancellation, and close outcomes arrive only through
    /// those original completions.
    pub fn submit_batch_commands(
        &mut self,
        address: &str,
        entries: Vec<BatchCommandEntry>,
    ) -> Result<Vec<BatchPublicationReceipt>, DirectUnaryClientError> {
        self.transport.submit_batch(address, entries)
    }

    fn submit_batch_commands_with_call(
        &mut self,
        address: &str,
        entries: Vec<BatchCommandEntry>,
        call: &UnaryCallContext,
    ) -> Result<Vec<BatchPublicationReceipt>, DirectUnaryClientError> {
        self.transport
            .submit_batch_with_call(address, entries, call)
    }

    /// Returns the active BatchCommands generation for one physical/logical route.
    ///
    /// This is a worker barrier as well as a focused lifecycle diagnostic: all
    /// receive events accepted before it have already retired their old route.
    #[must_use]
    pub fn batch_stream_generation(
        &self,
        address: &str,
        forwarded_host: Option<&str>,
    ) -> Option<u64> {
        self.transport.inspect_batch(address, forwarded_host).0
    }

    /// Returns the greatest request ID allocated by the sole batch scheduler.
    ///
    /// This worker barrier distinguishes caller submissions from empty stream
    /// recreation after a transport failure. A stable value proves that the
    /// transport did not schedule another request behind the caller's back.
    #[must_use]
    pub fn batch_request_id_watermark(&self) -> u64 {
        self.transport.inspect_batch("", None).1
    }

    /// Returns the value-local cancellation fired before orderly close is queued.
    ///
    /// Embedders coordinating a concurrent blocking call may fire this handle
    /// before the owner thread invokes [`DirectUnaryClient::close`]. A cloned
    /// request capability receives detached cancellation state and therefore
    /// cannot cancel the process-owned worker.
    #[must_use]
    pub fn shutdown_cancellation(&self) -> TransportShutdownCancellation {
        self.transport.shutdown_cancellation()
    }

    /// Closes the current generation only when it is not newer than `version`.
    fn close_address_generation(
        &mut self,
        address: &str,
        version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        self.transport.close_address_version(address, version)
    }

    /// Runs one foreground health check with client-go's one-second default.
    pub fn liveness_default(&self, address: &str) -> Result<StoreLiveness, DirectUnaryClientError> {
        self.liveness(address, DEFAULT_STORE_LIVENESS_TIMEOUT)
    }

    /// Returns the active generation for focused lifecycle diagnostics.
    #[must_use]
    pub fn connection_version(&self, address: &str) -> Option<u64> {
        self.inspect(address).0
    }

    /// Returns the number of active address-keyed channels.
    #[must_use]
    pub fn active_address_count(&self) -> usize {
        self.inspect("").1
    }

    fn inspect(&self, address: &str) -> (Option<u64>, usize) {
        self.transport.inspect(address)
    }

    fn shutdown(&mut self) -> Result<(), DirectUnaryClientError> {
        self.transport.shutdown()
    }

    /// Sends the exact pinned CheckTxnStatus command through the shared core.
    pub fn check_txn_status(
        &mut self,
        address: &str,
        request: &KvrpcCheckTxnStatusRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        let mut request = request.clone();
        request.context = Some(context.clone());
        let response = self.transport.send(
            address,
            RawUnaryRequest {
                path: CHECK_TXN_STATUS_PATH,
                encoded_request: request.encode_to_vec(),
                forwarded_host: None,
            },
            call,
        )?;
        KvrpcCheckTxnStatusResponse::decode(response.encoded_response.as_slice()).map_err(|error| {
            DirectUnaryClientError::InvalidRequest(format!(
                "invalid CheckTxnStatus response: {error}"
            ))
        })
    }

    /// Sends the exact pinned keyed ResolveLock command through the shared core.
    pub fn resolve_lock(
        &mut self,
        address: &str,
        request: &KvrpcResolveLockRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcResolveLockResponse, DirectUnaryClientError> {
        let mut request = request.clone();
        request.context = Some(context.clone());
        let response = self.transport.send(
            address,
            RawUnaryRequest {
                path: RESOLVE_LOCK_PATH,
                encoded_request: request.encode_to_vec(),
                forwarded_host: None,
            },
            call,
        )?;
        KvrpcResolveLockResponse::decode(response.encoded_response.as_slice()).map_err(|error| {
            DirectUnaryClientError::InvalidRequest(format!("invalid ResolveLock response: {error}"))
        })
    }
}

impl DirectUnaryClient for TonicCoprocessorClient {
    fn send_request(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.send_request_with_context(address, request, &UnaryCallContext::with_timeout(timeout))
    }

    fn send_request_with_context(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.send_request_with_route(address, None, request, call)
    }

    fn send_request_with_route(
        &mut self,
        address: &str,
        forwarded_host: Option<&str>,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        let body = replace_top_level_context(&request.encoded_request, &request.context)?;
        let response = self.transport.send(
            address,
            RawUnaryRequest {
                path: COPROCESSOR_PATH,
                encoded_request: body,
                forwarded_host: forwarded_host.map(str::to_owned),
            },
            call,
        )?;
        Ok(DirectUnaryResponse::from_physical_channel(
            response.encoded_response,
            response.physical_channel,
        ))
    }

    fn close_address(&mut self, address: &str) -> Result<(), DirectUnaryClientError> {
        self.transport.close_address(address)
    }

    fn close_address_version(
        &mut self,
        address: &str,
        version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        self.close_address_generation(address, version)
    }

    fn liveness(
        &self,
        address: &str,
        timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        self.transport.liveness(address, timeout)
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        self.shutdown()
    }
}

impl AsyncRequestDispatcher for TonicCoprocessorClient {
    type Pending = BatchCoprocessorPending;

    fn begin(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<Self::Pending, DirectUnaryClientError> {
        if call.cancellation().is_cancelled() {
            return Err(DirectUnaryClientError::CallerCancelled);
        }
        let body = replace_top_level_context(&request.encoded_request, &request.context)?;
        let (entry, mut pending) = BatchCoprocessorPending::entry(body, forwarded_host);
        let receipts =
            match self.submit_batch_commands_with_call(physical_address, vec![entry], call) {
                Ok(receipts) => receipts,
                Err(error) => {
                    pending.cancel();
                    return Err(error);
                }
            };
        if !receipts.is_empty() {
            if let Err(error) = pending.bind_publication(&receipts) {
                pending.cancel();
                return Err(error);
            }
        }
        if call.cancellation().is_cancelled() {
            pending.cancel();
            return Err(DirectUnaryClientError::CallerCancelled);
        }
        Ok(pending)
    }
}

/// Replaces every top-level context field while preserving every other wire
/// byte, including fields outside the dependency-closed local proto.
fn replace_top_level_context(
    encoded_request: &[u8],
    context: &KvrpcContext,
) -> Result<Vec<u8>, DirectUnaryClientError> {
    let mut position = 0;
    let mut body_without_context = Vec::with_capacity(encoded_request.len());
    while position < encoded_request.len() {
        let field_start = position;
        let tag = read_varint(encoded_request, &mut position)?;
        let field_number = tag >> 3;
        let wire_type = (tag & 0x07) as u8;
        validate_field_number(field_number)?;
        if field_number == 1 && wire_type != 2 {
            return Err(invalid_wire(
                "coprocessor context has a non-message wire type",
            ));
        }
        skip_field_value(encoded_request, &mut position, field_number, wire_type)?;
        if field_number != 1 {
            body_without_context.extend_from_slice(&encoded_request[field_start..position]);
        }
    }

    let encoded_context = context.encode_to_vec();
    let mut result = Vec::with_capacity(
        1 + varint_len(encoded_context.len() as u64)
            + encoded_context.len()
            + body_without_context.len(),
    );
    write_varint(10, &mut result);
    write_varint(encoded_context.len() as u64, &mut result);
    result.extend_from_slice(&encoded_context);
    result.extend_from_slice(&body_without_context);
    Ok(result)
}

fn skip_field_value(
    bytes: &[u8],
    position: &mut usize,
    field_number: u64,
    wire_type: u8,
) -> Result<(), DirectUnaryClientError> {
    match wire_type {
        0 => {
            read_varint(bytes, position)?;
        }
        1 => advance(bytes, position, 8)?,
        2 => {
            let length = usize::try_from(read_varint(bytes, position)?)
                .map_err(|_| invalid_wire("length-delimited field does not fit usize"))?;
            advance(bytes, position, length)?;
        }
        3 => loop {
            let nested_tag = read_varint(bytes, position)?;
            let nested_field = nested_tag >> 3;
            let nested_wire = (nested_tag & 0x07) as u8;
            validate_field_number(nested_field)?;
            if nested_wire == 4 {
                if nested_field != field_number {
                    return Err(invalid_wire("protobuf group ended with the wrong field"));
                }
                break;
            }
            skip_field_value(bytes, position, nested_field, nested_wire)?;
        },
        4 => return Err(invalid_wire("unexpected protobuf end-group tag")),
        5 => advance(bytes, position, 4)?,
        _ => return Err(invalid_wire("unknown protobuf wire type")),
    }
    Ok(())
}

fn validate_field_number(field_number: u64) -> Result<(), DirectUnaryClientError> {
    if field_number == 0 {
        return Err(invalid_wire("protobuf field number is zero"));
    }
    if field_number > MAX_PROTOBUF_FIELD_NUMBER {
        return Err(invalid_wire("protobuf field number exceeds the maximum"));
    }
    Ok(())
}

fn read_varint(bytes: &[u8], position: &mut usize) -> Result<u64, DirectUnaryClientError> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let byte = *bytes
            .get(*position)
            .ok_or_else(|| invalid_wire("truncated protobuf varint"))?;
        *position += 1;
        if shift == 63 && byte > 1 {
            return Err(invalid_wire("protobuf varint overflow"));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(invalid_wire("protobuf varint overflow"))
}

fn advance(
    bytes: &[u8],
    position: &mut usize,
    length: usize,
) -> Result<(), DirectUnaryClientError> {
    let end = position
        .checked_add(length)
        .ok_or_else(|| invalid_wire("protobuf field length overflow"))?;
    if end > bytes.len() {
        return Err(invalid_wire("truncated protobuf field"));
    }
    *position = end;
    Ok(())
}

fn write_varint(mut value: u64, destination: &mut Vec<u8>) {
    while value >= 0x80 {
        destination.push((value as u8) | 0x80);
        value >>= 7;
    }
    destination.push(value as u8);
}

fn varint_len(mut value: u64) -> usize {
    let mut length = 1;
    while value >= 0x80 {
        length += 1;
        value >>= 7;
    }
    length
}

fn invalid_wire(message: &str) -> DirectUnaryClientError {
    DirectUnaryClientError::InvalidRequest(message.to_owned())
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use tidb_proto::{CoprocessorRequest, KvrpcContext};

    use super::{replace_top_level_context, write_varint, MAX_PROTOBUF_FIELD_NUMBER};

    #[test]
    fn wire_context_replacement_preserves_unprojected_fields() {
        let stale = KvrpcContext {
            region_id: 7,
            ..KvrpcContext::default()
        };
        let authoritative = KvrpcContext {
            region_id: 11,
            cluster_id: 13,
            ..KvrpcContext::default()
        };
        let mut encoded = CoprocessorRequest {
            context: Some(stale.clone()),
            data: b"dag".to_vec(),
            ..CoprocessorRequest::default()
        }
        .encode_to_vec();

        // Add a duplicate stale field 1 and an unprojected length-delimited
        // field 100. The replacement must remove both old authorities while
        // retaining the unknown field byte-for-byte.
        let stale = stale.encode_to_vec();
        write_varint(10, &mut encoded);
        write_varint(stale.len() as u64, &mut encoded);
        encoded.extend_from_slice(&stale);
        let mut unknown = Vec::new();
        write_varint((100_u64 << 3) | 2, &mut unknown);
        write_varint(3, &mut unknown);
        unknown.extend_from_slice(&[0xaa, 0xbb, 0xcc]);
        encoded.extend_from_slice(&unknown);

        let replaced = replace_top_level_context(&encoded, &authoritative).unwrap();
        assert!(replaced.ends_with(&unknown));
        let decoded = CoprocessorRequest::decode(replaced.as_slice()).unwrap();
        assert_eq!(decoded.context.as_ref(), Some(&authoritative));
        assert_eq!(decoded.data, b"dag");
    }

    #[test]
    fn malformed_wire_request_fails_closed() {
        let error =
            replace_top_level_context(&[0x0a, 0x02, 0x01], &KvrpcContext::default()).unwrap_err();
        assert_eq!(error.kind(), "invalid_request");

        let mut illegal_field = Vec::new();
        write_varint((MAX_PROTOBUF_FIELD_NUMBER + 1) << 3, &mut illegal_field);
        illegal_field.push(0);
        let error =
            replace_top_level_context(&illegal_field, &KvrpcContext::default()).unwrap_err();
        assert_eq!(error.kind(), "invalid_request");
    }
}
