// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Shared error definitions and classification from PD's complete `errs` package.
//! Codes and causes remain separate from diagnostic text. The imported Go errors
//! library is adapted with native typed constructors and `std::error::Error`.

use std::backtrace::Backtrace;
use std::borrow::Cow;
use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

pub type SharedError = Arc<dyn StdError + Send + Sync>;

/// An upstream error prototype. `error()` retains its singleton identity;
/// generating or wrapping an error produces a distinct value with the same code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Definition {
    name: &'static str,
    code: Option<&'static str>,
    message: &'static str,
}

impl Definition {
    pub fn name(self) -> &'static str {
        self.name
    }
    pub fn code(self) -> Option<&'static str> {
        self.code
    }
    pub fn message_template(self) -> &'static str {
        self.message
    }

    pub fn error(self) -> PdError {
        PdError {
            definition: self,
            message: Cow::Borrowed(self.message),
            formatted: None,
            cause: None,
            representation: Representation::Prototype,
        }
    }

    /// Native `FastGenByArgs` after type-checked source-template formatting.
    fn with_formatted_args(self, message: impl Into<String>) -> PdError {
        let mut error = self.error();
        error.formatted = Some(message.into());
        error.representation = Representation::Generated;
        error
    }

    pub fn wrap(self, cause: Option<SharedError>) -> Option<PdError> {
        self.error().wrap(cause)
    }
}

#[derive(Clone, Debug)]
enum Representation {
    Prototype,
    Normalized,
    Generated,
    Stacked(Arc<Backtrace>),
}

/// A PD error instance, retaining a downcastable cause and source prototype.
#[derive(Clone, Debug)]
pub struct PdError {
    definition: Definition,
    message: Cow<'static, str>,
    formatted: Option<String>,
    cause: Option<SharedError>,
    representation: Representation,
}

impl PdError {
    pub fn definition(&self) -> Definition {
        self.definition
    }
    pub fn code(&self) -> Option<&'static str> {
        self.definition.code
    }
    pub fn message(&self) -> &str {
        self.formatted.as_deref().unwrap_or(&self.message)
    }
    pub fn is_prototype(&self, definition: Definition) -> bool {
        self.definition == definition && matches!(self.representation, Representation::Prototype)
    }
    /// Wrap follows Go's nil-in/nil-out contract and preserves the original cause.
    pub fn wrap(&self, cause: Option<SharedError>) -> Option<Self> {
        cause.map(|cause| {
            let mut copy = self.clone();
            copy.cause = Some(cause);
            copy.representation = Representation::Normalized;
            copy
        })
    }
    pub fn with_stack(mut self) -> Self {
        self.representation = Representation::Stacked(Arc::new(Backtrace::capture()));
        self
    }
    /// Go's GenWithStackByCause replaces the template with the cause text while
    /// retaining that cause. The repeated text in Display is intentional.
    pub fn gen_with_stack_by_cause(mut self) -> Self {
        if let Some(cause) = &self.cause {
            self.message = Cow::Owned(cause.to_string());
        }
        self.formatted = None;
        self.with_stack()
    }
    pub fn backtrace(&self) -> Option<&Backtrace> {
        match &self.representation {
            Representation::Stacked(trace) => Some(trace),
            _ => None,
        }
    }
    fn fast_gen_without_args(&self) -> Self {
        let mut copy = self.clone();
        copy.formatted = None;
        copy.representation = Representation::Generated;
        copy
    }
    fn is_direct_normalized(&self) -> bool {
        self.code().is_some()
            && matches!(
                self.representation,
                Representation::Prototype | Representation::Normalized
            )
    }
}

impl fmt::Display for PdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(code) = self.code() {
            write!(f, "[{code}]")?;
        }
        f.write_str(self.message())?;
        if let Some(cause) = &self.cause {
            write!(f, ": {cause}")?;
        }
        Ok(())
    }
}
impl StdError for PdError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.cause.as_deref().map(|error| error as _)
    }
}

// The crate error enum is only a native transport envelope, not a Go wrapper.
fn direct_pd_error<'a>(error: &'a (dyn StdError + 'static)) -> Option<&'a PdError> {
    error
        .downcast_ref::<PdError>()
        .or_else(|| match error.downcast_ref::<crate::Error>() {
            Some(crate::Error::Pd(error)) => Some(error),
            _ => None,
        })
}

/// Go IsLeaderChange deliberately checks direct singleton identity before text.
/// A stacked/generated stream-closed value alone does not satisfy that check.
pub fn is_leader_change(error: Option<&(dyn StdError + 'static)>) -> bool {
    let Some(error) = error else {
        return false;
    };
    if direct_pd_error(error).is_some_and(|e| e.is_prototype(ERR_CLIENT_TSO_STREAM_CLOSED)) {
        return true;
    }
    let message = error.to_string();
    [
        NO_LEADER_ERR,
        NOT_LEADER_ERR,
        NOT_SERVED_ERR,
        NOT_PRIMARY_ERR,
    ]
    .iter()
    .any(|text| message.contains(text))
}

pub fn is_callee_mismatch(error: Option<&(dyn StdError + 'static)>) -> bool {
    error.is_some_and(|error| error.to_string().contains(MISMATCH_CALLEE_ID_ERR))
}

pub fn is_network_error(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
    )
}

/// Structured logging field equivalent to zap's ErrorType, or None for Skip.
/// Borrowing foreign errors keeps their concrete type available to the logger.
pub struct ErrorField<'a> {
    value: FieldValue<'a>,
}
enum FieldValue<'a> {
    Borrowed(&'a (dyn StdError + 'static)),
    Owned(PdError),
    TypedNil,
}
impl ErrorField<'_> {
    pub fn key(&self) -> &'static str {
        "error"
    }
    pub fn error(&self) -> Option<&(dyn StdError + 'static)> {
        match &self.value {
            FieldValue::Borrowed(error) => Some(*error),
            FieldValue::Owned(error) => Some(error),
            FieldValue::TypedNil => None,
        }
    }
}
impl fmt::Display for ErrorField<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.error() {
            Some(error) => write!(f, "{error}"),
            None => f.write_str("<nil>"),
        }
    }
}

/// Go ZapError uses only the first supplied cause, and only for a direct
/// normalized error. `&[]` and `&[None]` intentionally mean different things.
pub fn zap_error<'a>(
    error: Option<&'a (dyn StdError + 'static)>,
    causes: &[Option<SharedError>],
) -> Option<ErrorField<'a>> {
    let error = error?;
    let value = match direct_pd_error(error).filter(|error| error.is_direct_normalized()) {
        Some(error) => match causes.first() {
            Some(cause) => match error.wrap(cause.clone()) {
                Some(error) => FieldValue::Owned(error),
                None => FieldValue::TypedNil,
            },
            None => FieldValue::Owned(error.fast_gen_without_args()),
        },
        None => FieldValue::Borrowed(error),
    };
    Some(ErrorField { value })
}

/// Source ErrClientGetResourceGroup. An explicit diagnostic cause takes
/// precedence, but never replaces or discards the underlying error.
#[derive(Clone, Debug, Default)]
pub struct ClientGetResourceGroupError {
    pub resource_group_name: String,
    pub cause: String,
    pub error: Option<SharedError>,
}
impl fmt::Display for ClientGetResourceGroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cause = if !self.cause.is_empty() {
            Cow::Borrowed(self.cause.as_str())
        } else if let Some(error) = &self.error {
            Cow::Owned(error.to_string())
        } else {
            Cow::Borrowed("")
        };
        write!(
            f,
            "get resource group {} failed, {}",
            self.resource_group_name,
            if cause.is_empty() {
                "unknown error"
            } else {
                &cause
            }
        )
    }
}
impl StdError for ClientGetResourceGroupError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.error.as_deref().map(|error| error as _)
    }
}

// Definitions follow errno.go, including source spellings that differ from names.
pub const NO_LEADER_ERR: &str = "no leader";
pub const NOT_LEADER_ERR: &str = "not leader";
pub const NOT_SERVED_ERR: &str = "is not served";
pub const RETRY_TIMEOUT_ERR: &str = "retry timeout";
pub const NOT_PRIMARY_ERR: &str = "not primary";
pub const MISMATCH_CALLEE_ID_ERR: &str = "mismatch callee id";

pub const ERR_UNMATCHED_CLUSTER_ID: Definition = Definition {
    name: "ErrUnmatchedClusterID",
    code: None,
    message: "[pd] unmatched cluster id",
};
pub const ERR_FAIL_INIT_CLUSTER_ID: Definition = Definition {
    name: "ErrFailInitClusterID",
    code: None,
    message: "[pd] failed to get cluster id",
};
pub const ERR_CLOSING: Definition = Definition {
    name: "ErrClosing",
    code: None,
    message: "[pd] closing",
};
pub const ERR_TSO_LENGTH: Definition = Definition {
    name: "ErrTSOLength",
    code: None,
    message: "[pd] tso length in rpc response is incorrect",
};
pub const ERR_NO_SERVICE_MODE_RETURNED: Definition = Definition {
    name: "ErrNoServiceModeReturned",
    code: None,
    message: "[pd] no service mode returned",
};
pub const ERR_CLIENT_GET_PROTO_CLIENT: Definition = Definition {
    name: "ErrClientGetProtoClient",
    code: Some("PD:client:ErrClientGetProtoClient"),
    message: "failed to get proto client",
};
pub const ERR_CLIENT_GET_META_STORAGE_CLIENT: Definition = Definition {
    name: "ErrClientGetMetaStorageClient",
    code: Some("PD:client:ErrClientGetMetaStorageClient"),
    message: "failed to get meta storage client",
};
pub const ERR_CLIENT_CREATE_TSO_STREAM: Definition = Definition {
    name: "ErrClientCreateTSOStream",
    code: Some("PD:client:ErrClientCreateTSOStream"),
    message: "create TSO stream failed, %s",
};
pub const ERR_CLIENT_TSO_STREAM_CLOSED: Definition = Definition {
    name: "ErrClientTSOStreamClosed",
    code: Some("PD:client:ErrClientTSOStreamClosed"),
    message: "encountered TSO stream being closed unexpectedly",
};
pub const ERR_CLIENT_GET_TSO: Definition = Definition {
    name: "ErrClientGetTSO",
    code: Some("PD:client:ErrClientGetTSO"),
    message: "get TSO failed, %v",
};
pub const ERR_CLIENT_GET_MIN_TSO: Definition = Definition {
    name: "ErrClientGetMinTSO",
    code: Some("PD:client:ErrClientGetMinTSO"),
    message: "get min TSO failed, %v",
};
pub const ERR_CLIENT_GET_LEADER: Definition = Definition {
    name: "ErrClientGetLeader",
    code: Some("PD:client:ErrClientGetLeader"),
    message: "get leader failed, %v",
};
pub const ERR_CLIENT_GET_MEMBER: Definition = Definition {
    name: "ErrClientGetMember",
    code: Some("PD:client:ErrClientGetMember"),
    message: "get member failed",
};
pub const ERR_CLIENT_GET_CLUSTER_INFO: Definition = Definition {
    name: "ErrClientGetClusterInfo",
    code: Some("PD:client:ErrClientGetClusterInfo"),
    message: "get cluster info failed",
};
pub const ERR_CLIENT_UPDATE_MEMBER: Definition = Definition {
    name: "ErrClientUpdateMember",
    code: Some("PD:client:ErrUpdateMember"),
    message: "update member failed, %v",
};
pub const ERR_CLIENT_NO_AVAILABLE_MEMBER: Definition = Definition {
    name: "ErrClientNoAvailableMember",
    code: Some("PD:client:ErrClientNoAvailableMember"),
    message: "no available member",
};
pub const ERR_CLIENT_NO_TARGET_MEMBER: Definition = Definition {
    name: "ErrClientNoTargetMember",
    code: Some("PD:client:ErrClientNoTargetMember"),
    message: "no target member",
};
pub const ERR_CLIENT_PROTO_UNMARSHAL: Definition = Definition {
    name: "ErrClientProtoUnmarshal",
    code: Some("PD:proto:ErrClientProtoUnmarshal"),
    message: "failed to unmarshal proto",
};
pub const ERR_CLIENT_GET_MULTI_RESPONSE: Definition = Definition {
    name: "ErrClientGetMultiResponse",
    code: Some("PD:client:ErrClientGetMultiResponse"),
    message: "get invalid value response %v, must only one",
};
pub const ERR_CLIENT_GET_SERVING_ENDPOINT: Definition = Definition {
    name: "ErrClientGetServingEndpoint",
    code: Some("PD:client:ErrClientGetServingEndpoint"),
    message: "get serving endpoint failed",
};
pub const ERR_CLIENT_FIND_GROUP_BY_KEYSPACE_ID: Definition = Definition {
    name: "ErrClientFindGroupByKeyspaceID",
    code: Some("PD:client:ErrClientFindGroupByKeyspaceID"),
    message: "can't find keyspace group by keyspace id",
};
pub const ERR_CLIENT_WATCH_GC_SAFE_POINT_V2_STREAM: Definition = Definition {
    name: "ErrClientWatchGCSafePointV2Stream",
    code: Some("PD:client:ErrClientWatchGCSafePointV2Stream"),
    message: "watch gc safe point v2 stream failed",
};
pub const ERR_CIRCUIT_BREAKER_OPEN: Definition = Definition {
    name: "ErrCircuitBreakerOpen",
    code: Some("PD:client:ErrCircuitBreakerOpen"),
    message: "circuit breaker is open",
};
pub const ERR_CLIENT_ROUTER_CONNECTION_TIMEOUT: Definition = Definition {
    name: "ErrClientRouterConnectionTimeout",
    code: Some("PD:client:ErrClientRouterConnectionTimeout"),
    message: "router connection is not ready until timeout",
};
pub const ERR_SECURITY_CONFIG: Definition = Definition {
    name: "ErrSecurityConfig",
    code: Some("PD:grpcutil:ErrSecurityConfig"),
    message: "security config error: %s",
};
pub const ERR_TLS_CONFIG: Definition = Definition {
    name: "ErrTLSConfig",
    code: Some("PD:grpcutil:ErrTLSConfig"),
    message: "TLS config error",
};
pub const ERR_URL_PARSE: Definition = Definition {
    name: "ErrURLParse",
    code: Some("PD:url:ErrURLParse"),
    message: "parse url error",
};
pub const ERR_GRPC_DIAL: Definition = Definition {
    name: "ErrGRPCDial",
    code: Some("PD:grpc:ErrGRPCDial"),
    message: "dial error",
};
pub const ERR_CLOSE_GRPC_CONN: Definition = Definition {
    name: "ErrCloseGRPCConn",
    code: Some("PD:grpc:ErrCloseGRPCConn"),
    message: "close gRPC connection failed",
};
pub const ERR_CRYPTO_X509_KEY_PAIR: Definition = Definition {
    name: "ErrCryptoX509KeyPair",
    code: Some("PD:crypto:ErrCryptoX509KeyPair"),
    message: "x509 keypair error",
};
pub const ERR_CRYPTO_APPEND_CERTS_FROM_PEM: Definition = Definition {
    name: "ErrCryptoAppendCertsFromPEM",
    code: Some("PD:crypto:ErrCryptoAppendCertsFromPEM"),
    message: "cert pool append certs error",
};
pub const ERR_CLIENT_LIST_RESOURCE_GROUP: Definition = Definition {
    name: "ErrClientListResourceGroup",
    code: Some("PD:client:ErrClientListResourceGroup"),
    message: "get all resource group failed, %v",
};
pub const ERR_CLIENT_RESOURCE_GROUP_CONFIG_UNAVAILABLE: Definition = Definition {
    name: "ErrClientResourceGroupConfigUnavailable",
    code: Some("PD:client:ErrClientResourceGroupConfigUnavailable"),
    message: "resource group config is unavailable, %v",
};
pub const ERR_CLIENT_RESOURCE_GROUP_THROTTLED: Definition = Definition {
    name: "ErrClientResourceGroupThrottled",
    code: Some("PD:client:ErrClientResourceGroupThrottled"),
    message:
        "exceeded resource group quota limitation, estimated wait time %s, ltb state is %.2f:%.2f",
};
pub const ERR_CLIENT_PUT_RESOURCE_GROUP_MISMATCH_KEYSPACE_ID: Definition = Definition {
    name: "ErrClientPutResourceGroupMismatchKeyspaceID",
    code: Some("PD:client:ErrClientPutResourceGroupMismatchKeyspaceID"),
    message: "resource group keyspace ID %d does not match inner client keyspace ID %d",
};
pub const ERR_SCHEDULER_CONFIG_UNAVAILABLE: Definition = Definition {
    name: "ErrSchedulerConfigUnavailable",
    code: Some("PD:client:ErrSchedulerConfigUnavailable"),
    message: "scheduler config is unavailable, %v",
};

// Every source definition, in declaration order, for the independent oracle.
#[cfg(test)]
const DEFINITIONS: &[Definition] = &[
    ERR_UNMATCHED_CLUSTER_ID,
    ERR_FAIL_INIT_CLUSTER_ID,
    ERR_CLOSING,
    ERR_TSO_LENGTH,
    ERR_NO_SERVICE_MODE_RETURNED,
    ERR_CLIENT_GET_PROTO_CLIENT,
    ERR_CLIENT_GET_META_STORAGE_CLIENT,
    ERR_CLIENT_CREATE_TSO_STREAM,
    ERR_CLIENT_TSO_STREAM_CLOSED,
    ERR_CLIENT_GET_TSO,
    ERR_CLIENT_GET_MIN_TSO,
    ERR_CLIENT_GET_LEADER,
    ERR_CLIENT_GET_MEMBER,
    ERR_CLIENT_GET_CLUSTER_INFO,
    ERR_CLIENT_UPDATE_MEMBER,
    ERR_CLIENT_NO_AVAILABLE_MEMBER,
    ERR_CLIENT_NO_TARGET_MEMBER,
    ERR_CLIENT_PROTO_UNMARSHAL,
    ERR_CLIENT_GET_MULTI_RESPONSE,
    ERR_CLIENT_GET_SERVING_ENDPOINT,
    ERR_CLIENT_FIND_GROUP_BY_KEYSPACE_ID,
    ERR_CLIENT_WATCH_GC_SAFE_POINT_V2_STREAM,
    ERR_CIRCUIT_BREAKER_OPEN,
    ERR_CLIENT_ROUTER_CONNECTION_TIMEOUT,
    ERR_SECURITY_CONFIG,
    ERR_TLS_CONFIG,
    ERR_URL_PARSE,
    ERR_GRPC_DIAL,
    ERR_CLOSE_GRPC_CONN,
    ERR_CRYPTO_X509_KEY_PAIR,
    ERR_CRYPTO_APPEND_CERTS_FROM_PEM,
    ERR_CLIENT_LIST_RESOURCE_GROUP,
    ERR_CLIENT_RESOURCE_GROUP_CONFIG_UNAVAILABLE,
    ERR_CLIENT_RESOURCE_GROUP_THROTTLED,
    ERR_CLIENT_PUT_RESOURCE_GROUP_MISMATCH_KEYSPACE_ID,
    ERR_SCHEDULER_CONFIG_UNAVAILABLE,
];

// Typed adapters for every source template with arguments. Arbitrary Rust error
// values use Display where Go uses %v; no text is used to choose an error code.
macro_rules! one_argument {
    ($($function:ident, $definition:ident, $format:literal);* $(;)?) => {$ (
        pub fn $function(value: impl fmt::Display) -> PdError {
            $definition.with_formatted_args(format!($format, value))
        }
    )*};
}
one_argument! {
    client_create_tso_stream, ERR_CLIENT_CREATE_TSO_STREAM, "create TSO stream failed, {}";
    client_get_tso, ERR_CLIENT_GET_TSO, "get TSO failed, {}";
    client_get_min_tso, ERR_CLIENT_GET_MIN_TSO, "get min TSO failed, {}";
    client_get_leader, ERR_CLIENT_GET_LEADER, "get leader failed, {}";
    client_update_member, ERR_CLIENT_UPDATE_MEMBER, "update member failed, {}";
    client_get_multi_response, ERR_CLIENT_GET_MULTI_RESPONSE, "get invalid value response {}, must only one";
    security_config, ERR_SECURITY_CONFIG, "security config error: {}";
    client_list_resource_group, ERR_CLIENT_LIST_RESOURCE_GROUP, "get all resource group failed, {}";
    client_resource_group_config_unavailable, ERR_CLIENT_RESOURCE_GROUP_CONFIG_UNAVAILABLE, "resource group config is unavailable, {}";
    scheduler_config_unavailable, ERR_SCHEDULER_CONFIG_UNAVAILABLE, "scheduler config is unavailable, {}";
}

pub fn client_resource_group_throttled(
    wait: impl fmt::Display,
    first: f64,
    second: f64,
) -> PdError {
    fn decimal(value: f64) -> String {
        if value == f64::INFINITY {
            "+Inf".to_owned()
        } else if value == f64::NEG_INFINITY {
            "-Inf".to_owned()
        } else {
            format!("{value:.2}")
        }
    }
    ERR_CLIENT_RESOURCE_GROUP_THROTTLED.with_formatted_args(format!(
        "exceeded resource group quota limitation, estimated wait time {wait}, ltb state is {}:{}",
        decimal(first),
        decimal(second)
    ))
}

pub fn client_put_resource_group_mismatch_keyspace_id(outer: u32, inner: u32) -> PdError {
    ERR_CLIENT_PUT_RESOURCE_GROUP_MISMATCH_KEYSPACE_ID.with_formatted_args(format!(
        "resource group keyspace ID {outer} does not match inner client keyspace ID {inner}"
    ))
}

#[cfg(test)]
#[path = "errs_tests.rs"]
mod tests;
