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

use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::{Buf, BufMut};
use prost::Message;
use tidb_proto::KvrpcContext;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

use crate::{DirectUnaryClient, DirectUnaryRequest, DirectUnaryResponse};

use super::channel_pool::ChannelPool;
use super::{DirectUnaryClientError, DirectUnaryConnectionError};

// client-go internal/client sets MaxRecvMsgSize to math.MaxInt64-1. Tonic's
// default is only 4 MiB, which is too small for valid Coprocessor responses.
const MAX_RECV_MESSAGE_SIZE: usize = (i64::MAX as usize).saturating_sub(1);
const MAX_PROTOBUF_FIELD_NUMBER: u64 = (1 << 29) - 1;
const COPROCESSOR_PATH: &str = "/tikvpb.Tikv/Coprocessor";

#[derive(Clone, Copy, Debug, Default)]
struct RawProtobufCodec;

#[derive(Clone, Copy, Debug, Default)]
struct RawProtobufEncoder;

#[derive(Clone, Copy, Debug, Default)]
struct RawProtobufDecoder;

impl Codec for RawProtobufCodec {
    type Encode = Vec<u8>;
    type Decode = Vec<u8>;
    type Encoder = RawProtobufEncoder;
    type Decoder = RawProtobufDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawProtobufEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawProtobufDecoder
    }
}

impl Encoder for RawProtobufEncoder {
    type Item = Vec<u8>;
    type Error = tonic::Status;

    fn encode(
        &mut self,
        item: Self::Item,
        destination: &mut EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        destination.put_slice(&item);
        Ok(())
    }
}

impl Decoder for RawProtobufDecoder {
    type Item = Vec<u8>;
    type Error = tonic::Status;

    fn decode(&mut self, source: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        Ok(Some(copy_remaining(source)))
    }
}

enum WorkerCommand {
    Send {
        address: String,
        request: Box<DirectUnaryRequest>,
        timeout: Duration,
        reply: mpsc::Sender<Result<DirectUnaryResponse, DirectUnaryClientError>>,
    },
    CloseAddress {
        address: String,
        reply: mpsc::Sender<()>,
    },
    CloseAddressVersion {
        address: String,
        version: u64,
        reply: mpsc::Sender<()>,
    },
    Inspect {
        address: String,
        reply: mpsc::Sender<(Option<u64>, usize)>,
    },
    Close {
        reply: mpsc::Sender<()>,
    },
}

/// Synchronous client-go-shaped unary capability backed by tonic.
///
/// A dedicated worker thread owns the Tokio runtime and every tonic channel.
/// Consequently the synchronous trait is safe to call from either ordinary or
/// already-async-hosted threads: it never nests `Runtime::block_on`. Channels
/// are created lazily, reused by address, and versioned on recreation.
pub struct TonicCoprocessorClient {
    commands: Option<mpsc::Sender<WorkerCommand>>,
    worker: Option<JoinHandle<()>>,
}

impl TonicCoprocessorClient {
    /// Constructs a live client without opening a socket.
    pub fn new() -> Result<Self, DirectUnaryClientError> {
        let (commands, receiver) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = ready_tx.send(Err(DirectUnaryClientError::Runtime(error.to_string())));
                    return;
                }
            };
            if ready_tx.send(Ok(())).is_err() {
                return;
            }
            run_worker(runtime, receiver);
        });
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                commands: Some(commands),
                worker: Some(worker),
            }),
            Ok(Err(error)) => {
                let _ = worker.join();
                Err(error)
            }
            Err(error) => {
                let _ = worker.join();
                Err(DirectUnaryClientError::Runtime(error.to_string()))
            }
        }
    }

    /// Closes the current generation only when it is not newer than `version`.
    pub fn close_address_version(&mut self, address: &str, version: u64) {
        let Some(commands) = &self.commands else {
            return;
        };
        let (reply, response) = mpsc::channel();
        if commands
            .send(WorkerCommand::CloseAddressVersion {
                address: address.to_owned(),
                version,
                reply,
            })
            .is_ok()
        {
            let _ = response.recv();
        }
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
        let Some(commands) = &self.commands else {
            return (None, 0);
        };
        let (reply, response) = mpsc::channel();
        if commands
            .send(WorkerCommand::Inspect {
                address: address.to_owned(),
                reply,
            })
            .is_err()
        {
            return (None, 0);
        }
        response.recv().unwrap_or((None, 0))
    }

    fn shutdown(&mut self) {
        if let Some(commands) = self.commands.take() {
            let (reply, response) = mpsc::channel();
            if commands.send(WorkerCommand::Close { reply }).is_ok() {
                let _ = response.recv();
            }
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl DirectUnaryClient for TonicCoprocessorClient {
    fn send_request(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        let Some(commands) = &self.commands else {
            return Err(DirectUnaryClientError::Closed);
        };
        let (reply, response) = mpsc::channel();
        commands
            .send(WorkerCommand::Send {
                address: address.to_owned(),
                request: Box::new(request.clone()),
                timeout,
                reply,
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        response
            .recv()
            .unwrap_or(Err(DirectUnaryClientError::Closed))
    }

    fn close_address(&mut self, address: &str) -> Result<(), DirectUnaryClientError> {
        let Some(commands) = &self.commands else {
            return Ok(());
        };
        let (reply, response) = mpsc::channel();
        commands
            .send(WorkerCommand::CloseAddress {
                address: address.to_owned(),
                reply,
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        response.recv().map_err(|_| DirectUnaryClientError::Closed)
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        self.shutdown();
        Ok(())
    }
}

impl Drop for TonicCoprocessorClient {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run_worker(runtime: tokio::runtime::Runtime, receiver: mpsc::Receiver<WorkerCommand>) {
    let mut channels = ChannelPool::new();
    while let Ok(command) = receiver.recv() {
        match command {
            WorkerCommand::Send {
                address,
                request,
                timeout,
                reply,
            } => {
                let result = send_coprocessor(&runtime, &mut channels, &address, &request, timeout);
                let _ = reply.send(result);
            }
            WorkerCommand::CloseAddress { address, reply } => {
                channels.close_address(&address);
                let _ = reply.send(());
            }
            WorkerCommand::CloseAddressVersion {
                address,
                version,
                reply,
            } => {
                channels.close_address_version(&address, version);
                let _ = reply.send(());
            }
            WorkerCommand::Inspect { address, reply } => {
                let _ = reply.send((channels.version(&address), channels.len()));
            }
            WorkerCommand::Close { reply } => {
                channels.close();
                let _ = reply.send(());
                break;
            }
        }
    }
}

fn send_coprocessor(
    runtime: &tokio::runtime::Runtime,
    channels: &mut ChannelPool,
    address: &str,
    request: &DirectUnaryRequest,
    timeout: Duration,
) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
    let body = replace_top_level_context(&request.encoded_request, &request.context)?;

    let selected = channels.get_or_create(address, runtime)?;
    let mut client =
        tonic::client::Grpc::new(selected.channel).max_decoding_message_size(MAX_RECV_MESSAGE_SIZE);
    // The caller-owned local timer is the sole timeout authority. Setting the
    // gRPC timeout header to the same duration races tonic's internal timer,
    // which can surface transport-specific Cancelled instead of our stable
    // typed Timeout result.
    let rpc_request = tonic::Request::new(body);
    let path = tonic::codegen::http::uri::PathAndQuery::from_static(COPROCESSOR_PATH);
    let result = runtime.block_on(async {
        tokio::time::timeout(timeout, async {
            client.ready().await.map_err(|error| {
                tonic::Status::unknown(format!("TiKV gRPC service is not ready: {error}"))
            })?;
            client.unary(rpc_request, path, RawProtobufCodec).await
        })
        .await
    });
    let response = match result {
        Ok(Ok(response)) => response.into_inner(),
        Ok(Err(error)) if error.code() == tonic::Code::DeadlineExceeded => {
            return Err(timeout_error(address, selected.version, timeout, error));
        }
        Ok(Err(error)) => {
            return Err(connection_error(address, selected.version, error));
        }
        Err(error) => {
            return Err(timeout_error(address, selected.version, timeout, error));
        }
    };
    Ok(DirectUnaryResponse {
        encoded_response: response,
    })
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

fn copy_remaining(source: &mut impl Buf) -> Vec<u8> {
    source.copy_to_bytes(source.remaining()).to_vec()
}

fn invalid_wire(message: &str) -> DirectUnaryClientError {
    DirectUnaryClientError::InvalidRequest(message.to_owned())
}

fn connection_error(
    address: &str,
    version: u64,
    error: impl std::fmt::Display,
) -> DirectUnaryClientError {
    DirectUnaryClientError::Connection(connection_identity(address, version, error))
}

fn timeout_error(
    address: &str,
    version: u64,
    timeout: Duration,
    error: impl std::fmt::Display,
) -> DirectUnaryClientError {
    DirectUnaryClientError::Timeout {
        connection: connection_identity(address, version, error),
        timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
    }
}

fn connection_identity(
    address: &str,
    version: u64,
    error: impl std::fmt::Display,
) -> DirectUnaryConnectionError {
    DirectUnaryConnectionError {
        address: address.to_owned(),
        version,
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use prost::Message;
    use tidb_proto::{CoprocessorRequest, KvrpcContext};

    use super::{
        copy_remaining, replace_top_level_context, write_varint, MAX_PROTOBUF_FIELD_NUMBER,
    };

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
    fn raw_decoder_copy_preserves_every_response_byte() {
        let expected = vec![0x0a, 0x01, 0x2a, 0xb2, 0x09, 0x02, 0xde, 0xad];
        let mut source = Bytes::from(expected.clone());
        assert_eq!(copy_remaining(&mut source), expected);
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
