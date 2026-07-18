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

//! Transport-neutral BatchCommands wire envelopes.
//!
//! Command bodies remain immutable encoded protobuf messages. Their exact
//! pinned oneof tags live here, while command interpretation stays with the
//! transaction or coprocessor owner above this transport layer.

use std::collections::HashSet;
use std::fmt;

use prost::Message;
use tidb_proto::tikvpb::{
    batch_commands_request, batch_commands_response, BatchCommandsRequest, BatchCommandsResponse,
};

macro_rules! define_command_tags {
    ($($variant:ident = $number:literal),+ $(,)?) => {
        /// Pinned `tikvpb.BatchCommands{Request,Response}` command field.
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[repr(u32)]
        pub enum BatchCommandTag {
            $(
                #[doc = concat!("Pinned BatchCommands command field `", stringify!($number), "`.")]
                $variant = $number
            ),+
        }

        impl BatchCommandTag {
            /// Every command field admitted by the pinned BatchCommands wire.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// Returns the exact protobuf oneof field number.
            #[must_use]
            pub const fn field_number(self) -> u32 {
                self as u32
            }
        }

        fn into_request_cmd(command: OpaqueBatchCommand) -> batch_commands_request::request::Cmd {
            let OpaqueBatchCommand { tag, body } = command;
            match tag {
                $(BatchCommandTag::$variant => batch_commands_request::request::Cmd::$variant(body)),+
            }
        }

        fn from_request_cmd(cmd: batch_commands_request::request::Cmd) -> OpaqueBatchCommand {
            match cmd {
                $(batch_commands_request::request::Cmd::$variant(body) => {
                    OpaqueBatchCommand::new(BatchCommandTag::$variant, body)
                }),+
            }
        }

        fn into_response_cmd(command: OpaqueBatchCommand) -> batch_commands_response::response::Cmd {
            let OpaqueBatchCommand { tag, body } = command;
            match tag {
                $(BatchCommandTag::$variant => batch_commands_response::response::Cmd::$variant(body)),+
            }
        }

        fn from_response_cmd(cmd: batch_commands_response::response::Cmd) -> OpaqueBatchCommand {
            match cmd {
                $(batch_commands_response::response::Cmd::$variant(body) => {
                    OpaqueBatchCommand::new(BatchCommandTag::$variant, body)
                }),+
            }
        }
    };
}

define_command_tags! {
    Get = 1,
    Scan = 2,
    Prewrite = 3,
    Commit = 4,
    Import = 5,
    Cleanup = 6,
    BatchGet = 7,
    BatchRollback = 8,
    ScanLock = 9,
    ResolveLock = 10,
    Gc = 11,
    DeleteRange = 12,
    RawGet = 13,
    RawBatchGet = 14,
    RawPut = 15,
    RawBatchPut = 16,
    RawDelete = 17,
    RawBatchDelete = 18,
    RawScan = 19,
    RawDeleteRange = 20,
    RawBatchScan = 21,
    Coprocessor = 22,
    PessimisticLock = 23,
    PessimisticRollback = 24,
    CheckTxnStatus = 25,
    TxnHeartBeat = 26,
    CheckSecondaryLocks = 33,
    RawCoprocessor = 34,
    FlashbackToVersion = 35,
    PrepareFlashbackToVersion = 36,
    Flush = 37,
    BufferBatchGet = 38,
    GetHealthFeedback = 39,
    BroadcastTxnStatus = 40,
    Empty = 255,
}

/// Immutable pre-encoded body paired with its exact BatchCommands field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpaqueBatchCommand {
    tag: BatchCommandTag,
    body: Vec<u8>,
}

impl OpaqueBatchCommand {
    /// Creates an owned command body that remains valid across repeated sends.
    #[must_use]
    pub fn new(tag: BatchCommandTag, body: impl Into<Vec<u8>>) -> Self {
        Self {
            tag,
            body: body.into(),
        }
    }

    /// Returns the pinned command field.
    #[must_use]
    pub const fn tag(&self) -> BatchCommandTag {
        self.tag
    }

    /// Returns the exact encoded inner protobuf message.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

/// Whether a malformed envelope was a request or response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchEnvelopeKind {
    /// Client-to-TiKV request envelope.
    Request,
    /// TiKV-to-client response envelope.
    Response,
}

impl fmt::Display for BatchEnvelopeKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request => formatter.write_str("request"),
            Self::Response => formatter.write_str("response"),
        }
    }
}

/// Invalid or undecodable BatchCommands wire input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BatchWireError {
    /// Protobuf framing could not be decoded.
    Decode(String),
    /// Commands and request IDs did not have one-to-one cardinality.
    Cardinality {
        /// Envelope direction.
        kind: BatchEnvelopeKind,
        /// Number of command bodies.
        commands: usize,
        /// Number of request IDs.
        request_ids: usize,
    },
    /// A command entry carried no oneof body.
    MissingCommand {
        /// Envelope direction.
        kind: BatchEnvelopeKind,
        /// Zero-based command index.
        index: usize,
    },
    /// Request ID zero is reserved as the scheduler's unassigned value.
    ZeroRequestId {
        /// Envelope direction.
        kind: BatchEnvelopeKind,
        /// Zero-based request-ID index.
        index: usize,
    },
    /// A request ID appeared more than once in one envelope.
    DuplicateRequestId {
        /// Envelope direction.
        kind: BatchEnvelopeKind,
        /// Repeated request ID.
        request_id: u64,
    },
}

impl fmt::Display for BatchWireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(error) => write!(formatter, "decode BatchCommands envelope: {error}"),
            Self::Cardinality {
                kind,
                commands,
                request_ids,
            } => write!(
                formatter,
                "BatchCommands {kind} cardinality mismatch: {commands} commands for {request_ids} request IDs"
            ),
            Self::MissingCommand { kind, index } => {
                write!(formatter, "BatchCommands {kind} command {index} is empty")
            }
            Self::ZeroRequestId { kind, index } => write!(
                formatter,
                "BatchCommands {kind} request ID {index} is zero"
            ),
            Self::DuplicateRequestId { kind, request_id } => write!(
                formatter,
                "BatchCommands {kind} repeats request ID {request_id}"
            ),
        }
    }
}

fn validate_request_ids(
    kind: BatchEnvelopeKind,
    request_ids: &[u64],
) -> Result<(), BatchWireError> {
    let mut unique = HashSet::with_capacity(request_ids.len());
    for (index, request_id) in request_ids.iter().copied().enumerate() {
        if request_id == 0 {
            return Err(BatchWireError::ZeroRequestId { kind, index });
        }
        if !unique.insert(request_id) {
            return Err(BatchWireError::DuplicateRequestId { kind, request_id });
        }
    }
    Ok(())
}

impl std::error::Error for BatchWireError {}

fn validate_cardinality(
    kind: BatchEnvelopeKind,
    commands: usize,
    request_ids: usize,
) -> Result<(), BatchWireError> {
    if commands == request_ids {
        Ok(())
    } else {
        Err(BatchWireError::Cardinality {
            kind,
            commands,
            request_ids,
        })
    }
}

/// Validated client-to-TiKV BatchCommands packet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchWireRequest {
    commands: Vec<OpaqueBatchCommand>,
    request_ids: Vec<u64>,
    client_send_time_ns: u64,
}

impl BatchWireRequest {
    /// Creates a request with exactly one ID for each opaque command.
    pub fn new(
        commands: Vec<OpaqueBatchCommand>,
        request_ids: Vec<u64>,
        client_send_time_ns: u64,
    ) -> Result<Self, BatchWireError> {
        validate_cardinality(
            BatchEnvelopeKind::Request,
            commands.len(),
            request_ids.len(),
        )?;
        validate_request_ids(BatchEnvelopeKind::Request, &request_ids)?;
        Ok(Self {
            commands,
            request_ids,
            client_send_time_ns,
        })
    }

    /// Decodes and validates a pinned BatchCommands request envelope.
    pub fn decode(bytes: &[u8]) -> Result<Self, BatchWireError> {
        let request = BatchCommandsRequest::decode(bytes)
            .map_err(|error| BatchWireError::Decode(error.to_string()))?;
        validate_cardinality(
            BatchEnvelopeKind::Request,
            request.requests.len(),
            request.request_ids.len(),
        )?;
        let commands = request
            .requests
            .into_iter()
            .enumerate()
            .map(|(index, request)| {
                request
                    .cmd
                    .map(from_request_cmd)
                    .ok_or(BatchWireError::MissingCommand {
                        kind: BatchEnvelopeKind::Request,
                        index,
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(commands, request.request_ids, request.client_send_time_ns)
    }

    /// Encodes without consuming or releasing any inner command body.
    #[must_use]
    pub fn encode_to_vec(&self) -> Vec<u8> {
        self.clone().into_proto().encode_to_vec()
    }

    /// Consumes this validated envelope into the generated tonic request.
    ///
    /// Keeping oneof construction here prevents the concrete stream owner from
    /// becoming a second wire authority.
    #[must_use]
    pub fn into_proto(self) -> BatchCommandsRequest {
        BatchCommandsRequest {
            requests: self
                .commands
                .into_iter()
                .map(|command| batch_commands_request::Request {
                    cmd: Some(into_request_cmd(command)),
                })
                .collect(),
            request_ids: self.request_ids,
            client_send_time_ns: self.client_send_time_ns,
        }
    }

    /// Commands in wire order.
    #[must_use]
    pub fn commands(&self) -> &[OpaqueBatchCommand] {
        &self.commands
    }

    /// Request IDs in the same order as [`Self::commands`].
    #[must_use]
    pub fn request_ids(&self) -> &[u64] {
        &self.request_ids
    }

    /// Client send timestamp carried by the envelope.
    #[must_use]
    pub const fn client_send_time_ns(&self) -> u64 {
        self.client_send_time_ns
    }
}

/// Validated TiKV-to-client BatchCommands packet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchWireResponse {
    commands: Vec<OpaqueBatchCommand>,
    request_ids: Vec<u64>,
    transport_layer_load: u64,
    health_feedback: Option<Vec<u8>>,
    tikv_send_time_ns: u64,
}

impl BatchWireResponse {
    /// Creates a response with exactly one ID for each opaque command.
    pub fn new(
        commands: Vec<OpaqueBatchCommand>,
        request_ids: Vec<u64>,
        transport_layer_load: u64,
        health_feedback: Option<Vec<u8>>,
        tikv_send_time_ns: u64,
    ) -> Result<Self, BatchWireError> {
        validate_cardinality(
            BatchEnvelopeKind::Response,
            commands.len(),
            request_ids.len(),
        )?;
        validate_request_ids(BatchEnvelopeKind::Response, &request_ids)?;
        Ok(Self {
            commands,
            request_ids,
            transport_layer_load,
            health_feedback,
            tikv_send_time_ns,
        })
    }

    /// Decodes and validates a pinned BatchCommands response envelope.
    pub fn decode(bytes: &[u8]) -> Result<Self, BatchWireError> {
        let response = BatchCommandsResponse::decode(bytes)
            .map_err(|error| BatchWireError::Decode(error.to_string()))?;
        Self::try_from(response)
    }

    /// Consumes this validated envelope into the generated tonic response.
    #[must_use]
    pub fn into_proto(self) -> BatchCommandsResponse {
        BatchCommandsResponse {
            responses: self
                .commands
                .into_iter()
                .map(|command| batch_commands_response::Response {
                    cmd: Some(into_response_cmd(command)),
                })
                .collect(),
            request_ids: self.request_ids,
            transport_layer_load: self.transport_layer_load,
            health_feedback: self.health_feedback,
            tikv_send_time_ns: self.tikv_send_time_ns,
        }
    }

    /// Encodes without consuming or releasing any inner response body.
    #[must_use]
    pub fn encode_to_vec(&self) -> Vec<u8> {
        self.clone().into_proto().encode_to_vec()
    }

    /// Responses in wire order.
    #[must_use]
    pub fn commands(&self) -> &[OpaqueBatchCommand] {
        &self.commands
    }

    /// Request IDs in the same order as [`Self::commands`].
    #[must_use]
    pub fn request_ids(&self) -> &[u64] {
        &self.request_ids
    }

    /// TiKV transport-layer load, where `280` means 280 percent.
    #[must_use]
    pub const fn transport_layer_load(&self) -> u64 {
        self.transport_layer_load
    }

    /// Exact encoded `kvrpcpb.HealthFeedback`, when the field was present.
    #[must_use]
    pub fn health_feedback(&self) -> Option<&[u8]> {
        self.health_feedback.as_deref()
    }

    /// TiKV send timestamp carried by the envelope.
    #[must_use]
    pub const fn tikv_send_time_ns(&self) -> u64 {
        self.tikv_send_time_ns
    }
}

impl TryFrom<BatchCommandsResponse> for BatchWireResponse {
    type Error = BatchWireError;

    fn try_from(response: BatchCommandsResponse) -> Result<Self, Self::Error> {
        validate_cardinality(
            BatchEnvelopeKind::Response,
            response.responses.len(),
            response.request_ids.len(),
        )?;
        let commands = response
            .responses
            .into_iter()
            .enumerate()
            .map(|(index, response)| {
                response
                    .cmd
                    .map(from_response_cmd)
                    .ok_or(BatchWireError::MissingCommand {
                        kind: BatchEnvelopeKind::Response,
                        index,
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(
            commands,
            response.request_ids,
            response.transport_layer_load,
            response.health_feedback,
            response.tikv_send_time_ns,
        )
    }
}
