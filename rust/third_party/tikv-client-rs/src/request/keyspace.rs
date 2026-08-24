// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::{Bound, Range};

use serde_derive::{Deserialize, Serialize};

use crate::proto::keyspacepb;
use crate::transaction::Mutation;
use crate::{proto::kvrpcpb, Key};
use crate::{BoundRange, KvPair};

pub const RAW_KEY_PREFIX: u8 = b'r';
pub const TXN_KEY_PREFIX: u8 = b'x';
pub const KEYSPACE_PREFIX_LEN: usize = 4;
/// The numeric API V2 keyspace namespace occupies the final three prefix bytes.
pub const MAX_KEYSPACE_ID: u32 = 0x00ff_ffff;
/// Numeric identifier of client-go's default API V2 keyspace.
pub const DEFAULT_KEYSPACE_ID: u32 = 0;
/// API V1's keyspace-agnostic sentinel from the pinned PD client constants.
pub const NULL_KEYSPACE_ID: u32 = u32::MAX;
/// Client-go's canonical name for the default API V2 keyspace.
pub const DEFAULT_KEYSPACE_NAME: &str = "DEFAULT";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Keyspace {
    Disable,
    /// API V1 TTL construction mode, which is valid only for RawKV requests.
    ///
    /// Client-go deliberately uses the V1 raw codec for this mode. Its final
    /// codec step stamps request contexts as V1, as required by V1TTL servers.
    V1Ttl,
    Enable {
        keyspace_id: u32,
    },
    /// Use API V2 without adding or removing the API V2 keyspace/key-mode prefix.
    ///
    /// This mode is intended for **server-side embedding** use cases (e.g. embedding this client in
    /// `tikv-server`) where keys are already in API V2 "logical key bytes" form and must be passed
    /// through unchanged.
    ApiV2NoPrefix,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum KeyMode {
    Raw,
    Txn,
}

impl Keyspace {
    /// Creates a numeric API V2 keyspace after enforcing client-go's uint24 limit.
    ///
    /// API V2 reserves the first prefix byte for the raw/transaction mode, leaving
    /// exactly three bytes for the numeric keyspace identifier.
    pub fn try_enable(keyspace_id: u32) -> crate::Result<Self> {
        if keyspace_id > MAX_KEYSPACE_ID {
            return Err(crate::Error::StringError(format!(
                "keyspaceID {keyspace_id} is out of range, maximum is {MAX_KEYSPACE_ID}"
            )));
        }
        Ok(Self::Enable { keyspace_id })
    }

    pub fn api_version(&self) -> kvrpcpb::ApiVersion {
        match self {
            Keyspace::Disable => kvrpcpb::ApiVersion::V1,
            Keyspace::V1Ttl => kvrpcpb::ApiVersion::V1,
            Keyspace::Enable { .. } => kvrpcpb::ApiVersion::V2,
            Keyspace::ApiV2NoPrefix => kvrpcpb::ApiVersion::V2,
        }
    }

    /// Returns the source keyspace oneof value carried with a request.
    ///
    /// The no-prefix embedding mode still addresses the default (zero) V2
    /// keyspace; it merely leaves already-physical key bytes untouched. API
    /// V1/V1TTL use PD's all-ones null-keyspace sentinel.
    pub fn context_keyspace_id(&self) -> Option<u32> {
        match self {
            Self::Enable { keyspace_id } => Some(*keyspace_id),
            Self::ApiV2NoPrefix => Some(0),
            Self::Disable | Self::V1Ttl => Some(NULL_KEYSPACE_ID),
        }
    }

    pub(crate) fn response_codec(&self, mode: KeyMode) -> Option<ApiV2Codec> {
        match self {
            Self::Enable { keyspace_id } => Some(
                ApiV2Codec::new(mode, *keyspace_id)
                    .expect("enabled API V2 keyspaces are validated at client construction"),
            ),
            // This embedding-only mode must not decode or rewrite already-physical keys.
            Self::Disable | Self::V1Ttl | Self::ApiV2NoPrefix => None,
        }
    }

    pub(crate) fn v1_response_codec(&self, mode: KeyMode) -> Option<ApiV1Codec> {
        matches!(self, Self::Disable | Self::V1Ttl).then(|| ApiV1Codec::new(mode))
    }
}

/// Builds the API V2 numeric keyspace codec input from PD metadata.
///
/// Client-go's V2 codec only understands the numeric `id` arm. API V3 uses
/// a namespace/keyspace identity instead, which must be rejected rather than
/// silently becoming the default numeric keyspace (ID zero).
pub(crate) fn keyspace_from_pd_meta(meta: &keyspacepb::KeyspaceMeta) -> crate::Result<Keyspace> {
    let keyspace_id = match &meta.keyspace {
        Some(keyspacepb::keyspace_meta::Keyspace::Id(id)) => *id,
        Some(keyspacepb::keyspace_meta::Keyspace::KeyspaceIdentity(_)) => {
            return Err(crate::Error::StringError(
                "unsupported keyspace identity: codec V2 only supports the numeric keyspace ID"
                    .to_owned(),
            ));
        }
        // `KeyspaceMeta.GetId()` in client-go returns zero when the oneof is
        // absent. Preserve that compatibility for older PD responses.
        None => 0,
    };
    Keyspace::try_enable(keyspace_id)
}

/// Extracts a source-compatible numeric keyspace ID from loaded PD metadata.
///
/// This is deliberately stricter than codec construction: client-go's
/// `GetKeyspaceID` rejects disabled keyspaces and API V3 identities before
/// returning the legacy numeric ID arm.
pub(crate) fn keyspace_id_from_pd_meta(
    name: &str,
    meta: &keyspacepb::KeyspaceMeta,
) -> crate::Result<u32> {
    if meta.state != keyspacepb::KeyspaceState::Enabled as i32 {
        return Err(crate::Error::StringError(format!(
            "keyspace {name} not enabled"
        )));
    }
    match &meta.keyspace {
        Some(keyspacepb::keyspace_meta::Keyspace::KeyspaceIdentity(_)) => {
            Err(crate::Error::StringError(format!(
                "keyspace {name} uses an API V3 keyspace identity, which is not supported"
            )))
        }
        Some(keyspacepb::keyspace_meta::Keyspace::Id(id)) => Ok(*id),
        None => Ok(0),
    }
}

/// Canonicalizes an optional user keyspace name like client-go's `BuildKeyspaceName`.
pub fn build_keyspace_name(name: impl AsRef<str>) -> String {
    let name = name.as_ref();
    if name.is_empty() {
        DEFAULT_KEYSPACE_NAME.to_owned()
    } else {
        name.to_owned()
    }
}

/// Returns the API V2 key-mode prefixes in their source-defined sort order.
pub fn api_v2_prefixes() -> [[u8; 1]; 2] {
    [[RAW_KEY_PREFIX], [TXN_KEY_PREFIX]]
}

/// Returns prefixes excluded from API V1 mixed deployments.
///
/// This is intentionally a separate API even though the pinned source list
/// currently equals [`api_v2_prefixes`].
pub fn api_v1_excluded_prefixes() -> [[u8; 1]; 2] {
    api_v2_prefixes()
}

/// Extracts the numeric keyspace identifier from an API V2 physical key.
pub fn parse_keyspace_id(key: &[u8]) -> crate::Result<u32> {
    let prefix = api_v2_key_prefix(key)?;
    Ok(u32::from_be_bytes([0, prefix[1], prefix[2], prefix[3]]))
}

/// Splits an API-versioned physical key into its optional V2 prefix and logical key.
pub fn decode_api_key(
    key: &[u8],
    api_version: kvrpcpb::ApiVersion,
) -> crate::Result<(Option<[u8; KEYSPACE_PREFIX_LEN]>, Vec<u8>)> {
    match api_version {
        kvrpcpb::ApiVersion::V1 => Ok((None, key.to_vec())),
        kvrpcpb::ApiVersion::V2 => {
            let prefix = api_v2_key_prefix(key)?;
            Ok((Some(prefix), key[KEYSPACE_PREFIX_LEN..].to_vec()))
        }
        unsupported => Err(crate::Error::StringError(format!(
            "unsupported api version {}",
            unsupported.as_str_name()
        ))),
    }
}

/// Reports whether an error chain contains a malformed memcomparable region key.
///
/// This is the Rust counterpart of client-go's exported `IsDecodeError` helper.
/// Region-cache callers use the classification to stop retrying corrupt PD/TiKV
/// region metadata while allowing ordinary PD failures to retain their budget.
pub fn is_decode_error(mut error: &(dyn std::error::Error + 'static)) -> bool {
    loop {
        if let Some(error) = error.downcast_ref::<crate::Error>() {
            if crate_error_contains_decode(error) {
                return true;
            }
        }
        match error.source() {
            Some(source) => error = source,
            None => return false,
        }
    }
}

fn crate_error_contains_decode(error: &crate::Error) -> bool {
    match error {
        crate::Error::ApiCodecDecode(_) => true,
        crate::Error::Connection { source, .. }
        | crate::Error::UndeterminedError(source)
        | crate::Error::PessimisticLockError { inner: source, .. } => {
            crate_error_contains_decode(source)
        }
        crate::Error::ExtractedErrors(errors) | crate::Error::MultipleKeyErrors(errors) => {
            errors.iter().any(crate_error_contains_decode)
        }
        _ => false,
    }
}

fn decode_memcomparable_key(encoded_key: &[u8]) -> crate::Result<Vec<u8>> {
    let mut decoded = Vec::new();
    crate::kv::codec::decode_bytes(encoded_key, &mut decoded)
        .map_err(|error| crate::Error::ApiCodecDecode(Box::new(error)))?;
    Ok(decoded)
}

/// Writes the legacy numeric V2 arm of the pinned context keyspace oneof.
pub(crate) fn set_context_keyspace_id(context: &mut kvrpcpb::Context, keyspace_id: u32) {
    context.keyspace = Some(kvrpcpb::context::Keyspace::KeyspaceId(keyspace_id));
}

/// Reads the legacy numeric V2 arm without treating a V3 identity as ID zero.
#[cfg(test)]
pub(crate) fn context_keyspace_id(context: &kvrpcpb::Context) -> Option<u32> {
    match context.keyspace.as_ref() {
        Some(kvrpcpb::context::Keyspace::KeyspaceId(keyspace_id)) => Some(*keyspace_id),
        Some(kvrpcpb::context::Keyspace::KeyspaceIdentity(_)) | None => None,
    }
}

fn api_v2_key_prefix(key: &[u8]) -> crate::Result<[u8; KEYSPACE_PREFIX_LEN]> {
    if key.len() < KEYSPACE_PREFIX_LEN || !matches!(key[0], RAW_KEY_PREFIX | TXN_KEY_PREFIX) {
        return Err(crate::Error::StringError(format!(
            "invalid API V2 key {key:?}"
        )));
    }
    Ok(key[..KEYSPACE_PREFIX_LEN]
        .try_into()
        .expect("length checked above"))
}

/// A source-compatible API V1 key codec.
///
/// V1 raw keys are already region keys. V1 transactional region keys use the
/// memcomparable encoding while request keys themselves remain logical bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiV1Codec {
    mode: KeyMode,
}

impl ApiV1Codec {
    pub const fn new(mode: KeyMode) -> Self {
        Self { mode }
    }

    pub const fn mode(&self) -> KeyMode {
        self.mode
    }

    pub fn encode_key(&self, key: &[u8]) -> Vec<u8> {
        key.to_vec()
    }

    pub fn decode_key(&self, key: &[u8]) -> crate::Result<Vec<u8>> {
        Ok(key.to_vec())
    }

    pub fn encode_range(&self, start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
        (start.to_vec(), end.to_vec())
    }

    pub fn decode_range(&self, start: &[u8], end: &[u8]) -> crate::Result<(Vec<u8>, Vec<u8>)> {
        Ok((start.to_vec(), end.to_vec()))
    }

    pub fn encode_region_key(&self, key: &[u8]) -> Vec<u8> {
        match self.mode {
            KeyMode::Raw => key.to_vec(),
            KeyMode::Txn => {
                let mut encoded = Vec::new();
                crate::kv::codec::encode_bytes(&mut encoded, key);
                encoded
            }
        }
    }

    pub fn decode_region_key(&self, key: &[u8]) -> crate::Result<Vec<u8>> {
        if key.is_empty() || matches!(self.mode, KeyMode::Raw) {
            return Ok(key.to_vec());
        }
        decode_memcomparable_key(key)
    }

    pub fn encode_region_range(&self, start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let start = self.encode_region_key(start);
        let end = if end.is_empty() {
            Vec::new()
        } else {
            self.encode_region_key(end)
        };
        (start, end)
    }

    pub fn decode_region_range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> crate::Result<(Vec<u8>, Vec<u8>)> {
        Ok((self.decode_region_key(start)?, self.decode_region_key(end)?))
    }

    /// Decodes V1 region bounds in place, matching client-go's V1 response codec.
    pub fn decode_region_error(
        &self,
        error: &mut crate::proto::errorpb::Error,
    ) -> crate::Result<()> {
        if let Some(key_not_in_region) = &mut error.key_not_in_region {
            (key_not_in_region.start_key, key_not_in_region.end_key) =
                self.decode_region_range(&key_not_in_region.start_key, &key_not_in_region.end_key)?;
        }
        if let Some(epoch_not_match) = &mut error.epoch_not_match {
            for region in &mut epoch_not_match.current_regions {
                (region.start_key, region.end_key) =
                    self.decode_region_range(&region.start_key, &region.end_key)?;
            }
        }
        Ok(())
    }

    pub fn decode_bucket_keys(&self, keys: &[Vec<u8>]) -> crate::Result<Vec<Vec<u8>>> {
        keys.iter().map(|key| self.decode_region_key(key)).collect()
    }
}

/// A source-compatible API V2 key codec for one numeric keyspace and key mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiV2Codec {
    mode: KeyMode,
    keyspace_id: u32,
    prefix: [u8; KEYSPACE_PREFIX_LEN],
    end_key: [u8; KEYSPACE_PREFIX_LEN],
}

impl ApiV2Codec {
    pub fn new(mode: KeyMode, keyspace_id: u32) -> crate::Result<Self> {
        Keyspace::try_enable(keyspace_id)?;
        let prefix = keyspace_prefix(keyspace_id, mode);
        Ok(Self {
            mode,
            keyspace_id,
            prefix,
            end_key: keyspace_end_prefix(keyspace_id, mode),
        })
    }

    pub const fn mode(&self) -> KeyMode {
        self.mode
    }

    pub const fn keyspace_id(&self) -> u32 {
        self.keyspace_id
    }

    pub const fn prefix(&self) -> [u8; KEYSPACE_PREFIX_LEN] {
        self.prefix
    }

    pub const fn end_key(&self) -> [u8; KEYSPACE_PREFIX_LEN] {
        self.end_key
    }

    pub fn encode_key(&self, key: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(KEYSPACE_PREFIX_LEN + key.len());
        encoded.extend_from_slice(&self.prefix);
        encoded.extend_from_slice(key);
        encoded
    }

    pub fn decode_key(&self, encoded_key: &[u8]) -> crate::Result<Vec<u8>> {
        // client-go deliberately tolerates an absent optional key field.
        if encoded_key.is_empty() {
            return Ok(Vec::new());
        }
        if !encoded_key.starts_with(&self.prefix) {
            return Err(crate::Error::StringError(
                "given key does not belong to the keyspace".to_owned(),
            ));
        }
        Ok(encoded_key[KEYSPACE_PREFIX_LEN..].to_vec())
    }

    pub fn encode_range(&self, start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
        self.encode_range_with_reverse(start, end, false)
    }

    /// Clones and encodes a protobuf key range like client-go's `encodeKeyRange`.
    pub fn encode_key_range(
        &self,
        range: &crate::proto::kvrpcpb::KeyRange,
    ) -> crate::proto::kvrpcpb::KeyRange {
        let (start_key, end_key) = self.encode_range(&range.start_key, &range.end_key);
        crate::proto::kvrpcpb::KeyRange { start_key, end_key }
    }

    /// Clones and encodes protobuf key ranges like client-go's `encodeKeyRanges`.
    pub fn encode_key_ranges(
        &self,
        ranges: &[crate::proto::kvrpcpb::KeyRange],
    ) -> Vec<crate::proto::kvrpcpb::KeyRange> {
        ranges
            .iter()
            .map(|range| self.encode_key_range(range))
            .collect()
    }

    /// Clones and encodes raw KV pairs like client-go's `encodeParis` helper.
    pub fn encode_pairs(
        &self,
        pairs: &[crate::proto::kvrpcpb::KvPair],
    ) -> Vec<crate::proto::kvrpcpb::KvPair> {
        pairs
            .iter()
            .map(|pair| {
                let mut pair = pair.clone();
                pair.key = self.encode_key(&pair.key);
                pair
            })
            .collect()
    }

    /// Clones and encodes transactional mutations like client-go's `encodeMutations`.
    pub fn encode_mutations(
        &self,
        mutations: &[crate::proto::kvrpcpb::Mutation],
    ) -> Vec<crate::proto::kvrpcpb::Mutation> {
        mutations
            .iter()
            .map(|mutation| {
                let mut mutation = mutation.clone();
                mutation.key = self.encode_key(&mutation.key);
                mutation
            })
            .collect()
    }

    pub fn encode_cop_range(
        &self,
        range: &crate::proto::coprocessor::KeyRange,
    ) -> crate::proto::coprocessor::KeyRange {
        let (start, end) = self.encode_range(&range.start, &range.end);
        crate::proto::coprocessor::KeyRange { start, end }
    }

    pub fn encode_cop_ranges(
        &self,
        ranges: &[crate::proto::coprocessor::KeyRange],
    ) -> Vec<crate::proto::coprocessor::KeyRange> {
        ranges
            .iter()
            .map(|range| self.encode_cop_range(range))
            .collect()
    }

    pub fn decode_cop_range(
        &self,
        range: &mut crate::proto::coprocessor::KeyRange,
    ) -> crate::Result<()> {
        (range.start, range.end) = self.decode_range(&range.start, &range.end)?;
        Ok(())
    }

    /// Clones and encodes the per-region tasks carried by a batched
    /// coprocessor request, matching client-go's `encodeStoreBatchTasks`.
    pub fn encode_cop_store_batch_tasks(
        &self,
        tasks: &[crate::proto::coprocessor::StoreBatchTask],
    ) -> Vec<crate::proto::coprocessor::StoreBatchTask> {
        tasks
            .iter()
            .map(|task| {
                let mut task = task.clone();
                task.ranges = self.encode_cop_ranges(&task.ranges);
                task
            })
            .collect()
    }

    /// Clones and encodes one TiFlash region descriptor, matching client-go's
    /// `encodeRegionInfo` helper.
    pub fn encode_cop_region_info(
        &self,
        region: &crate::proto::coprocessor::RegionInfo,
    ) -> crate::proto::coprocessor::RegionInfo {
        let mut region = region.clone();
        region.ranges = self.encode_cop_ranges(&region.ranges);
        region
    }

    /// Clones and encodes TiFlash region descriptors, matching client-go's
    /// `encodeRegionInfos` helper.
    pub fn encode_cop_region_infos(
        &self,
        regions: &[crate::proto::coprocessor::RegionInfo],
    ) -> Vec<crate::proto::coprocessor::RegionInfo> {
        regions
            .iter()
            .map(|region| self.encode_cop_region_info(region))
            .collect()
    }

    /// Clones and encodes partition-table region descriptors, matching
    /// client-go's `encodeTableRegions` helper.
    pub fn encode_cop_table_regions(
        &self,
        tables: &[crate::proto::coprocessor::TableRegions],
    ) -> Vec<crate::proto::coprocessor::TableRegions> {
        tables
            .iter()
            .map(|table| {
                let mut table = table.clone();
                table.regions = self.encode_cop_region_infos(&table.regions);
                table
            })
            .collect()
    }

    /// Clones and encodes the API V2 fields of an ordinary coprocessor request.
    ///
    /// This is the Rust request-level counterpart to client-go's `CmdCop`
    /// branch: only request ranges and batched store-task ranges are keyspace
    /// encoded at this protocol version.
    pub fn encode_coprocessor_request(
        &self,
        request: &crate::proto::coprocessor::Request,
    ) -> crate::proto::coprocessor::Request {
        let mut request = request.clone();
        request.ranges = self.encode_cop_ranges(&request.ranges);
        request.tasks = self.encode_cop_store_batch_tasks(&request.tasks);
        request
    }

    /// Clones and encodes the key-bearing fields of a TiFlash batch
    /// coprocessor request, matching client-go's `CmdBatchCop` branch.
    pub fn encode_batch_coprocessor_request(
        &self,
        request: &crate::proto::coprocessor::BatchRequest,
    ) -> crate::proto::coprocessor::BatchRequest {
        let mut request = request.clone();
        request.regions = self.encode_cop_region_infos(&request.regions);
        request.table_regions = self.encode_cop_table_regions(&request.table_regions);
        request
    }

    /// Clones and encodes the key-bearing fields of an MPP dispatch request,
    /// matching client-go's `CmdMPPTask` branch.
    pub fn encode_mpp_dispatch_task_request(
        &self,
        request: &crate::proto::mpp::DispatchTaskRequest,
    ) -> crate::proto::mpp::DispatchTaskRequest {
        let mut request = request.clone();
        request.regions = self.encode_cop_region_infos(&request.regions);
        request.table_regions = self.encode_cop_table_regions(&request.table_regions);
        if let Some(meta) = &mut request.meta {
            meta.keyspace = Some(crate::proto::mpp::task_meta::Keyspace::KeyspaceId(
                self.keyspace_id,
            ));
            meta.api_version = crate::proto::kvrpcpb::ApiVersion::V2 as i32;
        }
        request
    }

    /// Encodes scan bounds using client-go's reversed-scan endpoint swapping.
    pub fn encode_range_with_reverse(
        &self,
        start: &[u8],
        end: &[u8],
        reverse: bool,
    ) -> (Vec<u8>, Vec<u8>) {
        if reverse {
            let (end, start) = self.encode_range_with_reverse(end, start, false);
            return (start, end);
        }
        let encoded_end = if end.is_empty() {
            self.end_key.to_vec()
        } else {
            self.encode_key(end)
        };
        (self.encode_key(start), encoded_end)
    }

    pub fn decode_range(&self, start: &[u8], end: &[u8]) -> crate::Result<(Vec<u8>, Vec<u8>)> {
        if start >= self.end_key.as_slice() || (!end.is_empty() && end <= self.prefix.as_slice()) {
            return Err(crate::Error::StringError(
                "given key does not belong to the keyspace".to_owned(),
            ));
        }

        let start = start
            .strip_prefix(&self.prefix)
            .map_or_else(Vec::new, ToOwned::to_owned);
        let end = end
            .strip_prefix(&self.prefix)
            .map_or_else(Vec::new, ToOwned::to_owned);
        Ok((start, end))
    }

    pub fn encode_region_key(&self, key: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::new();
        crate::kv::codec::encode_bytes(&mut encoded, &self.encode_key(key));
        encoded
    }

    pub fn decode_region_key(&self, encoded_key: &[u8]) -> crate::Result<Vec<u8>> {
        let decoded = decode_memcomparable_key(encoded_key)?;
        self.decode_key(&decoded)
    }

    pub fn encode_region_range(&self, start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let (start, end) = self.encode_range(start, end);
        let mut encoded_start = Vec::new();
        crate::kv::codec::encode_bytes(&mut encoded_start, &start);
        let mut encoded_end = Vec::new();
        crate::kv::codec::encode_bytes(&mut encoded_end, &end);
        (encoded_start, encoded_end)
    }

    pub fn decode_region_range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> crate::Result<(Vec<u8>, Vec<u8>)> {
        let decoded_start = if start.is_empty() {
            Vec::new()
        } else {
            decode_memcomparable_key(start)?
        };
        let decoded_end = if end.is_empty() {
            Vec::new()
        } else {
            decode_memcomparable_key(end)?
        };
        self.decode_range(&decoded_start, &decoded_end)
    }

    /// Decodes API V2 boundaries on every region returned by commands such as
    /// `SplitRegion`, matching client-go's `decodeRegions` helper.
    pub fn decode_regions(
        &self,
        regions: &mut [crate::proto::metapb::Region],
    ) -> crate::Result<()> {
        for region in regions {
            (region.start_key, region.end_key) =
                self.decode_region_range(&region.start_key, &region.end_key)?;
        }
        Ok(())
    }

    /// Decodes the API-v2 key fields carried by a region error in place.
    pub fn decode_region_error(
        &self,
        error: &mut crate::proto::errorpb::Error,
    ) -> crate::Result<()> {
        if let Some(key_not_in_region) = &mut error.key_not_in_region {
            key_not_in_region.key = self.decode_key(&key_not_in_region.key)?;
            (key_not_in_region.start_key, key_not_in_region.end_key) =
                self.decode_region_range(&key_not_in_region.start_key, &key_not_in_region.end_key)?;
        }

        if let Some(epoch_not_match) = &mut error.epoch_not_match {
            let mut decoded_regions = Vec::with_capacity(epoch_not_match.current_regions.len());
            for mut region in std::mem::take(&mut epoch_not_match.current_regions) {
                match self.decode_region_range(&region.start_key, &region.end_key) {
                    Ok((start_key, end_key)) => {
                        region.start_key = start_key;
                        region.end_key = end_key;
                        decoded_regions.push(region);
                    }
                    Err(error) if is_keyspace_out_of_bound(&error) => {
                        // client-go omits sibling regions entirely outside this keyspace.
                    }
                    Err(error) => return Err(error),
                }
            }
            epoch_not_match.current_regions = decoded_regions;
        }

        Ok(())
    }

    /// Decodes API V2 bucket boundary keys with client-go's edge suppression.
    pub fn decode_bucket_keys(&self, keys: &[Vec<u8>]) -> crate::Result<Vec<Vec<u8>>> {
        let mut decoded_keys = Vec::with_capacity(keys.len());
        for (index, key) in keys.iter().enumerate() {
            let decoded = if key.is_empty() {
                Vec::new()
            } else {
                decode_memcomparable_key(key)?
            };

            let outside_start = index == 0 && decoded.as_slice() < self.prefix.as_slice();
            let outside_end = index + 1 == keys.len()
                && (decoded.is_empty() || decoded.as_slice() >= self.end_key.as_slice());
            if outside_start || outside_end {
                decoded_keys.push(Vec::new());
            } else if let Some(raw) = decoded.strip_prefix(&self.prefix) {
                if raw.is_empty() && !decoded_keys.is_empty() && decoded_keys[0].is_empty() {
                    continue;
                }
                decoded_keys.push(raw.to_vec());
            }
        }
        Ok(decoded_keys)
    }

    /// Decodes every key-bearing member of a transactional key error in place.
    pub fn decode_key_error(
        &self,
        error: &mut crate::proto::kvrpcpb::KeyError,
    ) -> crate::Result<()> {
        if let Some(lock) = &mut error.locked {
            self.decode_lock_info(lock)?;
        }
        if let Some(conflict) = &mut error.conflict {
            conflict.key = self.decode_key(&conflict.key)?;
            conflict.primary = self.decode_key(&conflict.primary)?;
        }
        if let Some(already_exist) = &mut error.already_exist {
            already_exist.key = self.decode_key(&already_exist.key)?;
        }
        if let Some(deadlock) = &mut error.deadlock {
            deadlock.lock_key = self.decode_key(&deadlock.lock_key)?;
            deadlock.deadlock_key = self.decode_key(&deadlock.deadlock_key)?;
            for wait in &mut deadlock.wait_chain {
                wait.key = self.decode_key(&wait.key)?;
            }
        }
        if let Some(commit_ts_expired) = &mut error.commit_ts_expired {
            commit_ts_expired.key = self.decode_key(&commit_ts_expired.key)?;
        }
        if let Some(txn_not_found) = &mut error.txn_not_found {
            txn_not_found.primary_key = self.decode_key(&txn_not_found.primary_key)?;
        }
        if let Some(assertion_failed) = &mut error.assertion_failed {
            assertion_failed.key = self.decode_key(&assertion_failed.key)?;
        }
        if let Some(primary_mismatch) = &mut error.primary_mismatch {
            if let Some(lock_info) = &mut primary_mismatch.lock_info {
                self.decode_lock_info(lock_info)?;
            }
        }
        if let Some(txn_lock_not_found) = &mut error.txn_lock_not_found {
            txn_lock_not_found.key = self.decode_key(&txn_lock_not_found.key)?;
        }
        if let Some(debug_info) = &mut error.debug_info {
            for mvcc_info in &mut debug_info.mvcc_info {
                mvcc_info.key = self.decode_key(&mvcc_info.key)?;
                if let Some(mvcc) = &mut mvcc_info.mvcc {
                    self.decode_mvcc_info(mvcc)?;
                }
            }
        }
        Ok(())
    }

    pub fn decode_key_errors(
        &self,
        errors: &mut [crate::proto::kvrpcpb::KeyError],
    ) -> crate::Result<()> {
        for error in errors {
            self.decode_key_error(error)?;
        }
        Ok(())
    }

    /// Decodes per-pair errors without changing the pair key itself.
    ///
    /// Successful pair keys are still decoded by the existing high-level client
    /// conversion until every response command is migrated atomically.
    pub fn decode_pair_key_errors(
        &self,
        pairs: &mut [crate::proto::kvrpcpb::KvPair],
    ) -> crate::Result<()> {
        for pair in pairs {
            if let Some(error) = &mut pair.error {
                self.decode_key_error(error)?;
            }
        }
        Ok(())
    }

    /// Decodes complete key/value pairs for response paths that no longer need
    /// physical keys for pagination or region routing.
    pub fn decode_pairs(&self, pairs: &mut [crate::proto::kvrpcpb::KvPair]) -> crate::Result<()> {
        self.decode_pair_key_errors(pairs)?;
        for pair in pairs {
            if !pair.key.is_empty() {
                pair.key = self.decode_key(&pair.key)?;
            }
        }
        Ok(())
    }

    pub fn decode_lock_info(
        &self,
        lock: &mut crate::proto::kvrpcpb::LockInfo,
    ) -> crate::Result<()> {
        lock.key = self.decode_key(&lock.key)?;
        lock.primary_lock = self.decode_key(&lock.primary_lock)?;
        for secondary in &mut lock.secondaries {
            *secondary = self.decode_key(secondary)?;
        }
        for shared_lock in &mut lock.shared_lock_infos {
            self.decode_lock_info(shared_lock)?;
        }
        Ok(())
    }

    pub fn decode_mvcc_info(
        &self,
        info: &mut crate::proto::kvrpcpb::MvccInfo,
    ) -> crate::Result<()> {
        let Some(lock) = &mut info.lock else {
            return Ok(());
        };
        if !lock.primary.is_empty() {
            lock.primary = self.decode_key(&lock.primary)?;
        }
        for secondary in &mut lock.secondaries {
            if !secondary.is_empty() {
                *secondary = self.decode_key(secondary)?;
            }
        }
        Ok(())
    }
}

fn is_keyspace_out_of_bound(error: &crate::Error) -> bool {
    matches!(error, crate::Error::StringError(message) if message == "given key does not belong to the keyspace")
}

pub trait EncodeKeyspace {
    fn encode_keyspace(self, keyspace: Keyspace, key_mode: KeyMode) -> Self;
}

pub trait TruncateKeyspace {
    fn truncate_keyspace(self, keyspace: Keyspace) -> Self;
}

impl EncodeKeyspace for Key {
    fn encode_keyspace(mut self, keyspace: Keyspace, key_mode: KeyMode) -> Self {
        let Keyspace::Enable { keyspace_id } = keyspace else {
            return self;
        };
        let prefix = keyspace_prefix(keyspace_id, key_mode);

        prepend_bytes(&mut self.0, &prefix);

        self
    }
}

impl EncodeKeyspace for KvPair {
    fn encode_keyspace(mut self, keyspace: Keyspace, key_mode: KeyMode) -> Self {
        self.0 = self.0.encode_keyspace(keyspace, key_mode);
        self
    }
}

impl EncodeKeyspace for BoundRange {
    fn encode_keyspace(mut self, keyspace: Keyspace, key_mode: KeyMode) -> Self {
        self.from = match self.from {
            Bound::Included(key) => Bound::Included(key.encode_keyspace(keyspace, key_mode)),
            Bound::Excluded(key) => Bound::Excluded(key.encode_keyspace(keyspace, key_mode)),
            Bound::Unbounded => Bound::Included(Key::EMPTY.encode_keyspace(keyspace, key_mode)),
        };

        self.to = match self.to {
            Bound::Included(key) if !key.is_empty() => {
                Bound::Included(key.encode_keyspace(keyspace, key_mode))
            }
            Bound::Excluded(key) if !key.is_empty() => {
                Bound::Excluded(key.encode_keyspace(keyspace, key_mode))
            }
            _ => match keyspace {
                Keyspace::Enable { keyspace_id } => {
                    // The source codec increments the whole four-byte prefix, not
                    // only the numeric ID. That distinction matters at 0xFF_FFFF:
                    // `r FF FF FF` ends at `s 00 00 00`.
                    Bound::Excluded(Key::from(
                        keyspace_end_prefix(keyspace_id, key_mode).to_vec(),
                    ))
                }
                _ => Bound::Excluded(Key::EMPTY),
            },
        };
        self
    }
}

impl EncodeKeyspace for Mutation {
    fn encode_keyspace(self, keyspace: Keyspace, key_mode: KeyMode) -> Self {
        match self {
            Mutation::Put(key, val) => Mutation::Put(key.encode_keyspace(keyspace, key_mode), val),
            Mutation::Delete(key) => Mutation::Delete(key.encode_keyspace(keyspace, key_mode)),
        }
    }
}

impl TruncateKeyspace for Key {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        if !matches!(keyspace, Keyspace::Enable { .. }) {
            return self;
        }

        pretruncate_bytes::<KEYSPACE_PREFIX_LEN>(&mut self.0);

        self
    }
}

impl TruncateKeyspace for KvPair {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        self.0 = self.0.truncate_keyspace(keyspace);
        self
    }
}

impl TruncateKeyspace for Range<Key> {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        self.start = self.start.truncate_keyspace(keyspace);
        self.end = self.end.truncate_keyspace(keyspace);
        self
    }
}

impl TruncateKeyspace for Vec<Range<Key>> {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        if !matches!(keyspace, Keyspace::Enable { .. }) {
            return self;
        }
        for range in &mut self {
            take_mut::take(range, |range| range.truncate_keyspace(keyspace));
        }
        self
    }
}

impl TruncateKeyspace for Vec<KvPair> {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        if !matches!(keyspace, Keyspace::Enable { .. }) {
            return self;
        }
        for pair in &mut self {
            take_mut::take(pair, |pair| pair.truncate_keyspace(keyspace));
        }
        self
    }
}

/// Is this key field UNSET, as opposed to carrying the empty logical key?
///
/// An encoded key always carries its 4-byte keyspace prefix, so on the wire an empty
/// byte string means "not set" — never "the empty logical key", which encodes to the
/// bare prefix. Guarding on this keeps the codecs off `pretruncate_bytes`' length
/// assertion for fields a shared-lock wrapper leaves unset, and mirrors client-go's
/// `codecV2.DecodeKey`, which returns early on a zero-length key.
fn is_unset_key(key: &[u8]) -> bool {
    key.is_empty()
}

impl TruncateKeyspace for Vec<crate::proto::kvrpcpb::LockInfo> {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        if !matches!(keyspace, Keyspace::Enable { .. }) {
            return self;
        }
        for lock in &mut self {
            // Convert this lock's OWN key fields, then recurse into any shared-lock
            // members — the shape of client-go's `codecV2.decodeLockInfo`, which
            // decodes Key/PrimaryLock/Secondaries and *then* walks SharedLockInfos,
            // with no wrapper special case.
            //
            // Per TiKV's writer (`SharedLocks::into_lock_info`, txn_types/src/lock.rs)
            // a wrapper sets `lock_type`, `shared_lock_infos` and `key` — the key it
            // locks — and leaves `primary_lock`/`lock_version` at their defaults. Each
            // member is built from that SAME raw key plus its own primary and version.
            // So the wrapper's key must be converted like any other (skipping it would
            // hand `scan_locks` a physical key beside decoded member keys), while its
            // unset fields must be left alone — hence no wrapper special case, just the
            // per-field guard below.
            if !is_unset_key(&lock.key) {
                take_mut::take(&mut lock.key, |key| {
                    Key::from(key).truncate_keyspace(keyspace).into()
                });
            }
            if !is_unset_key(&lock.primary_lock) {
                take_mut::take(&mut lock.primary_lock, |primary| {
                    Key::from(primary).truncate_keyspace(keyspace).into()
                });
            }
            for secondary in lock.secondaries.iter_mut() {
                // Unlike `key`/`primary_lock` above, this guard is not an unset-field
                // case: an unset `secondaries` is an empty Vec, with no elements to
                // visit. A present-but-empty ELEMENT is malformed input (an encoded
                // key always carries its prefix); skipping it merely keeps a corrupt
                // entry from panicking the codec on `pretruncate_bytes`' length
                // assertion.
                if is_unset_key(secondary) {
                    continue;
                }
                take_mut::take(secondary, |secondary| {
                    Key::from(secondary).truncate_keyspace(keyspace).into()
                });
            }
            take_mut::take(&mut lock.shared_lock_infos, |members| {
                members.truncate_keyspace(keyspace)
            });
        }
        self
    }
}

impl EncodeKeyspace for Vec<crate::proto::kvrpcpb::LockInfo> {
    fn encode_keyspace(mut self, keyspace: Keyspace, key_mode: KeyMode) -> Self {
        if !matches!(keyspace, Keyspace::Enable { .. }) {
            return self;
        }
        for lock in &mut self {
            // Deliberately NOT symmetric with the TruncateKeyspace impl above. There,
            // empty bytes can only mean "unset", because an encoded key always carries
            // its 4-byte prefix. Here the input is a LOGICAL key, and the empty logical
            // key is valid in API v2 — it encodes to the bare prefix. Skipping empties
            // would strand a lock on the empty key with no prefix, and `scan_locks` ->
            // `resolve_locks` (transaction/client.rs) round-trips exactly that way, so
            // its region lookup would then use an empty physical key.
            //
            // Shared-lock wrappers do not need the unset-field guard here: resolution
            // refuses them (`reject_shared_locks`) before anything acts on the encoded
            // result.
            take_mut::take(&mut lock.key, |key| {
                Key::from(key).encode_keyspace(keyspace, key_mode).into()
            });
            take_mut::take(&mut lock.primary_lock, |primary| {
                Key::from(primary)
                    .encode_keyspace(keyspace, key_mode)
                    .into()
            });
            for secondary in lock.secondaries.iter_mut() {
                take_mut::take(secondary, |secondary| {
                    Key::from(secondary)
                        .encode_keyspace(keyspace, key_mode)
                        .into()
                });
            }
            take_mut::take(&mut lock.shared_lock_infos, |members| {
                members.encode_keyspace(keyspace, key_mode)
            });
        }
        self
    }
}

fn keyspace_prefix(keyspace_id: u32, key_mode: KeyMode) -> [u8; KEYSPACE_PREFIX_LEN] {
    assert!(
        keyspace_id <= MAX_KEYSPACE_ID,
        "keyspace ID {keyspace_id} exceeds the API V2 uint24 maximum {MAX_KEYSPACE_ID}"
    );
    let mut prefix = keyspace_id.to_be_bytes();
    prefix[0] = match key_mode {
        KeyMode::Raw => RAW_KEY_PREFIX,
        KeyMode::Txn => TXN_KEY_PREFIX,
    };
    prefix
}

/// Returns the exclusive byte boundary immediately after this mode/keyspace prefix.
fn keyspace_end_prefix(keyspace_id: u32, key_mode: KeyMode) -> [u8; KEYSPACE_PREFIX_LEN] {
    let prefix = keyspace_prefix(keyspace_id, key_mode);
    u32::from_be_bytes(prefix)
        .checked_add(1)
        .expect("API V2 keyspace prefix always has a successor")
        .to_be_bytes()
}

fn prepend_bytes<const N: usize>(vec: &mut Vec<u8>, prefix: &[u8; N]) {
    unsafe {
        vec.reserve_exact(N);
        std::ptr::copy(vec.as_ptr(), vec.as_mut_ptr().add(N), vec.len());
        std::ptr::copy_nonoverlapping(prefix.as_ptr(), vec.as_mut_ptr(), N);
        vec.set_len(vec.len() + N);
    }
}

fn pretruncate_bytes<const N: usize>(vec: &mut Vec<u8>) {
    assert!(vec.len() >= N);
    unsafe {
        std::ptr::copy(vec.as_ptr().add(N), vec.as_mut_ptr(), vec.len() - N);
        vec.set_len(vec.len() - N);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyspace_prefix() {
        let key_mode = KeyMode::Raw;
        assert_eq!(keyspace_prefix(0, key_mode), [b'r', 0, 0, 0]);
        assert_eq!(keyspace_prefix(1, key_mode), [b'r', 0, 0, 1]);
        assert_eq!(keyspace_prefix(0xFFFF, key_mode), [b'r', 0, 0xFF, 0xFF]);

        let key_mode = KeyMode::Txn;
        assert_eq!(keyspace_prefix(0, key_mode), [b'x', 0, 0, 0]);
        assert_eq!(keyspace_prefix(1, key_mode), [b'x', 0, 0, 1]);
        assert_eq!(keyspace_prefix(0xFFFF, key_mode), [b'x', 0, 0xFF, 0xFF]);
    }

    #[test]
    fn keyspace_id_is_limited_to_the_api_v2_uint24_namespace() {
        assert_eq!(DEFAULT_KEYSPACE_ID, 0);
        assert_eq!(
            Keyspace::try_enable(MAX_KEYSPACE_ID).unwrap(),
            Keyspace::Enable {
                keyspace_id: MAX_KEYSPACE_ID
            }
        );
        let error = Keyspace::try_enable(MAX_KEYSPACE_ID + 1).unwrap_err();
        assert_eq!(
            error.to_string(),
            "keyspaceID 16777216 is out of range, maximum is 16777215"
        );
    }

    #[test]
    fn source_v2_codec_rejects_api_v3_keyspace_identity() {
        let meta = keyspacepb::KeyspaceMeta {
            keyspace: Some(keyspacepb::keyspace_meta::Keyspace::KeyspaceIdentity(
                crate::proto::apipb::KeyspaceIdentity {
                    namespace_id: 1,
                    keyspace_id: 2,
                },
            )),
            ..Default::default()
        };
        assert_eq!(
            keyspace_from_pd_meta(&meta).unwrap_err().to_string(),
            "unsupported keyspace identity: codec V2 only supports the numeric keyspace ID"
        );
        assert_eq!(
            keyspace_from_pd_meta(&keyspacepb::KeyspaceMeta::default()).unwrap(),
            Keyspace::Enable { keyspace_id: 0 }
        );
    }

    #[test]
    fn source_get_keyspace_id_rejects_non_enabled_and_v3_metadata() {
        let disabled = keyspacepb::KeyspaceMeta {
            state: keyspacepb::KeyspaceState::Disabled as i32,
            keyspace: Some(keyspacepb::keyspace_meta::Keyspace::Id(7)),
            ..Default::default()
        };
        assert_eq!(
            keyspace_id_from_pd_meta("tenant", &disabled)
                .unwrap_err()
                .to_string(),
            "keyspace tenant not enabled"
        );

        let identity = keyspacepb::KeyspaceMeta {
            state: keyspacepb::KeyspaceState::Enabled as i32,
            keyspace: Some(keyspacepb::keyspace_meta::Keyspace::KeyspaceIdentity(
                crate::proto::apipb::KeyspaceIdentity {
                    namespace_id: 1,
                    keyspace_id: 2,
                },
            )),
            ..Default::default()
        };
        assert_eq!(
            keyspace_id_from_pd_meta("tenant", &identity)
                .unwrap_err()
                .to_string(),
            "keyspace tenant uses an API V3 keyspace identity, which is not supported"
        );

        let enabled = keyspacepb::KeyspaceMeta {
            state: keyspacepb::KeyspaceState::Enabled as i32,
            keyspace: Some(keyspacepb::keyspace_meta::Keyspace::Id(42)),
            ..Default::default()
        };
        assert_eq!(keyspace_id_from_pd_meta("tenant", &enabled).unwrap(), 42);
        assert_eq!(
            keyspace_id_from_pd_meta("tenant", &keyspacepb::KeyspaceMeta::default()).unwrap(),
            0
        );
    }

    #[test]
    fn empty_keyspace_name_uses_the_source_default_name() {
        assert_eq!(build_keyspace_name(""), DEFAULT_KEYSPACE_NAME);
        assert_eq!(build_keyspace_name("tenant"), "tenant");
    }

    #[test]
    fn unbounded_maximum_keyspace_range_carries_into_the_mode_byte() {
        let keyspace = Keyspace::try_enable(MAX_KEYSPACE_ID).unwrap();
        let range: BoundRange = (..).into();
        let raw_expected: BoundRange =
            (Key::from(vec![b'r', 0xFF, 0xFF, 0xFF])..Key::from(vec![b's', 0, 0, 0])).into();
        let txn_expected: BoundRange =
            (Key::from(vec![b'x', 0xFF, 0xFF, 0xFF])..Key::from(vec![b'y', 0, 0, 0])).into();

        assert_eq!(
            range.clone().encode_keyspace(keyspace, KeyMode::Raw),
            raw_expected
        );
        assert_eq!(range.encode_keyspace(keyspace, KeyMode::Txn), txn_expected);
    }

    #[test]
    fn api_v2_codec_matches_source_keyspace_and_range_boundaries() {
        let codec = ApiV2Codec::new(KeyMode::Raw, 4242).unwrap();
        assert_eq!(codec.prefix(), [b'r', 0, 16, 146]);
        assert_eq!(codec.end_key(), [b'r', 0, 16, 147]);
        assert_eq!(codec.encode_key(b"key"), b"r\0\x10\x92key");
        assert_eq!(codec.decode_key(b"r\0\x10\x92key").unwrap(), b"key");
        assert!(codec.decode_key(b"x\0\x10\x92key").is_err());

        assert_eq!(
            codec.encode_range(b"start", b""),
            (b"r\0\x10\x92start".to_vec(), b"r\0\x10\x93".to_vec())
        );
        assert_eq!(
            codec.encode_range_with_reverse(b"start", b"end", true),
            (b"r\0\x10\x92start".to_vec(), b"r\0\x10\x92end".to_vec())
        );
        assert_eq!(
            codec
                .decode_range(b"r\0\x10\x92start", b"r\0\x10\x93")
                .unwrap(),
            (b"start".to_vec(), Vec::new())
        );
        assert!(codec.decode_range(b"r\0\x10\x93", b"r\0\x10\x94").is_err());

        let ranges = vec![
            kvrpcpb::KeyRange::default(),
            kvrpcpb::KeyRange {
                end_key: b"z".to_vec(),
                ..Default::default()
            },
            kvrpcpb::KeyRange {
                start_key: b"a".to_vec(),
                ..Default::default()
            },
        ];
        assert_eq!(
            codec.encode_key_ranges(&ranges),
            vec![
                kvrpcpb::KeyRange {
                    start_key: b"r\0\x10\x92".to_vec(),
                    end_key: b"r\0\x10\x93".to_vec(),
                },
                kvrpcpb::KeyRange {
                    start_key: b"r\0\x10\x92".to_vec(),
                    end_key: b"r\0\x10\x92z".to_vec(),
                },
                kvrpcpb::KeyRange {
                    start_key: b"r\0\x10\x92a".to_vec(),
                    end_key: b"r\0\x10\x93".to_vec(),
                },
            ]
        );

        let pairs = vec![kvrpcpb::KvPair {
            key: b"pair".to_vec(),
            value: b"value".to_vec(),
            commit_ts: 9,
            ..Default::default()
        }];
        let mutations = vec![kvrpcpb::Mutation {
            key: b"mutation".to_vec(),
            value: b"value".to_vec(),
            ..Default::default()
        }];
        assert_eq!(codec.encode_pairs(&pairs)[0].key, b"r\0\x10\x92pair");
        assert_eq!(codec.encode_pairs(&pairs)[0].commit_ts, 9);
        assert_eq!(
            codec.encode_mutations(&mutations)[0].key,
            b"r\0\x10\x92mutation"
        );

        let mut cop_range = crate::proto::coprocessor::KeyRange {
            start: b"a".to_vec(),
            end: Vec::new(),
        };
        let encoded_cop_range = codec.encode_cop_range(&cop_range);
        assert_eq!(encoded_cop_range.start, b"r\0\x10\x92a");
        assert_eq!(encoded_cop_range.end, b"r\0\x10\x93");
        cop_range = encoded_cop_range;
        codec.decode_cop_range(&mut cop_range).unwrap();
        assert_eq!(cop_range.start, b"a");
        assert!(cop_range.end.is_empty());
    }

    #[test]
    fn api_v2_codec_region_bounds_round_trip_and_clip_like_client_go() {
        let codec = ApiV2Codec::new(KeyMode::Txn, 0x102).unwrap();
        let (start, end) = codec.encode_region_range(b"a", b"z");
        assert_eq!(
            codec.decode_region_range(&start, &end).unwrap(),
            (b"a".to_vec(), b"z".to_vec())
        );

        let before = ApiV2Codec::new(KeyMode::Txn, 0x101)
            .unwrap()
            .encode_region_key(b"");
        let after = ApiV2Codec::new(KeyMode::Txn, 0x103)
            .unwrap()
            .encode_region_key(b"");
        assert_eq!(
            codec.decode_region_range(&before, &after).unwrap(),
            (Vec::new(), Vec::new()),
            "a region spanning this keyspace is clipped to its logical bounds"
        );
    }

    #[test]
    fn api_v1_transaction_region_codec_is_memcomparable_but_raw_is_identity() {
        let txn = ApiV1Codec::new(KeyMode::Txn);
        let raw = ApiV1Codec::new(KeyMode::Raw);
        let encoded = txn.encode_region_key(b"key");
        assert_ne!(encoded, b"key");
        assert_eq!(txn.decode_region_key(&encoded).unwrap(), b"key");
        assert_eq!(raw.encode_region_key(b"key"), b"key");
        assert_eq!(raw.decode_region_key(b"key").unwrap(), b"key");
    }

    #[test]
    fn malformed_region_keys_retain_the_source_decode_error_classification() {
        let malformed = ApiV1Codec::new(KeyMode::Txn)
            .decode_region_key(b"not-memcomparable")
            .unwrap_err();
        assert!(is_decode_error(&malformed));

        let wrapped = crate::Error::Connection {
            source: Box::new(malformed),
            address: "127.0.0.1:20160".to_owned(),
            version: 7,
        };
        assert!(is_decode_error(&wrapped));
        assert!(!is_decode_error(&crate::Error::StringError(
            "ordinary PD failure".to_owned()
        )));

        let malformed_v2 = ApiV2Codec::new(KeyMode::Txn, 1)
            .unwrap()
            .decode_region_range(b"short", b"")
            .unwrap_err();
        assert!(is_decode_error(&malformed_v2));
    }

    #[test]
    fn api_v1_codec_decodes_transactional_region_error_ranges() {
        use crate::proto::{errorpb, metapb};

        let codec = ApiV1Codec::new(KeyMode::Txn);
        let (start, end) = codec.encode_region_range(b"a", b"z");
        let mut error = errorpb::Error {
            key_not_in_region: Some(errorpb::KeyNotInRegion {
                start_key: start.clone(),
                end_key: end.clone(),
                ..Default::default()
            }),
            epoch_not_match: Some(errorpb::EpochNotMatch {
                current_regions: vec![metapb::Region {
                    start_key: start,
                    end_key: end,
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };

        codec.decode_region_error(&mut error).unwrap();
        let key_not_in_region = error.key_not_in_region.unwrap();
        assert_eq!(key_not_in_region.start_key, b"a");
        assert_eq!(key_not_in_region.end_key, b"z");
        let region = &error.epoch_not_match.unwrap().current_regions[0];
        assert_eq!(region.start_key, b"a");
        assert_eq!(region.end_key, b"z");
    }

    #[test]
    fn bucket_key_decoders_preserve_v1_and_apply_v2_edge_suppression() {
        let v1 = ApiV1Codec::new(KeyMode::Txn);
        let v1_key = v1.encode_region_key(b"v1");
        assert_eq!(v1.decode_bucket_keys(&[v1_key]).unwrap(), [b"v1".to_vec()]);

        let v2 = ApiV2Codec::new(KeyMode::Raw, 0x102).unwrap();
        let before = ApiV2Codec::new(KeyMode::Raw, 0x101)
            .unwrap()
            .encode_region_key(b"");
        let prefix = v2.encode_region_key(b"");
        let inside = v2.encode_region_key(b"middle");
        let after = ApiV2Codec::new(KeyMode::Raw, 0x103)
            .unwrap()
            .encode_region_key(b"");
        let physical_bucket_keys = vec![
            before.clone(),
            prefix.clone(),
            inside.clone(),
            after.clone(),
        ];
        assert_eq!(
            v2.decode_bucket_keys(&physical_bucket_keys).unwrap(),
            [Vec::new(), b"middle".to_vec(), Vec::new()]
        );

        // Bucket mismatch keys are consumed by RegionCache as region keys.
        // The source response switch does not run DecodeBucketKeys on them.
        let mut region_error = crate::proto::errorpb::Error {
            bucket_version_not_match: Some(crate::proto::errorpb::BucketVersionNotMatch {
                keys: physical_bucket_keys.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };
        v2.decode_region_error(&mut region_error).unwrap();
        assert_eq!(
            region_error.bucket_version_not_match.unwrap().keys,
            physical_bucket_keys
        );
    }

    #[test]
    fn api_v2_codec_decodes_region_errors_and_filters_outside_siblings() {
        use crate::proto::{errorpb, metapb};

        let codec = ApiV2Codec::new(KeyMode::Raw, 0x102).unwrap();
        let (start, end) = codec.encode_region_range(b"a", b"z");
        let mut key_not_in_region = errorpb::Error {
            key_not_in_region: Some(errorpb::KeyNotInRegion {
                key: codec.encode_key(b"key"),
                start_key: start,
                end_key: end,
                ..Default::default()
            }),
            ..Default::default()
        };
        codec.decode_region_error(&mut key_not_in_region).unwrap();
        let key_not_in_region = key_not_in_region.key_not_in_region.unwrap();
        assert_eq!(key_not_in_region.key, b"key");
        assert_eq!(key_not_in_region.start_key, b"a");
        assert_eq!(key_not_in_region.end_key, b"z");

        let before = ApiV2Codec::new(KeyMode::Raw, 0x101)
            .unwrap()
            .encode_region_range(b"", b"");
        let inside = codec.encode_region_range(b"a", b"z");
        let mut epoch_not_match = errorpb::Error {
            epoch_not_match: Some(errorpb::EpochNotMatch {
                current_regions: vec![
                    metapb::Region {
                        start_key: before.0,
                        end_key: before.1,
                        ..Default::default()
                    },
                    metapb::Region {
                        start_key: inside.0,
                        end_key: inside.1,
                        ..Default::default()
                    },
                ],
            }),
            ..Default::default()
        };
        codec.decode_region_error(&mut epoch_not_match).unwrap();
        let regions = epoch_not_match.epoch_not_match.unwrap().current_regions;
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].start_key, b"a");
        assert_eq!(regions[0].end_key, b"z");
    }

    #[test]
    fn api_v2_codec_decodes_nested_lock_and_key_error_fields() {
        use crate::proto::{deadlock, kvrpcpb};

        let codec = ApiV2Codec::new(KeyMode::Txn, 7).unwrap();
        let mut error = kvrpcpb::KeyError {
            locked: Some(kvrpcpb::LockInfo {
                key: codec.encode_key(b"lock"),
                primary_lock: codec.encode_key(b"primary"),
                secondaries: vec![codec.encode_key(b"secondary")],
                shared_lock_infos: vec![kvrpcpb::LockInfo {
                    key: codec.encode_key(b"shared"),
                    primary_lock: codec.encode_key(b"shared-primary"),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            conflict: Some(kvrpcpb::WriteConflict {
                key: codec.encode_key(b"conflict"),
                primary: codec.encode_key(b"conflict-primary"),
                ..Default::default()
            }),
            deadlock: Some(kvrpcpb::Deadlock {
                lock_key: codec.encode_key(b"deadlock-lock"),
                deadlock_key: codec.encode_key(b"deadlock-key"),
                wait_chain: vec![deadlock::WaitForEntry {
                    key: codec.encode_key(b"wait"),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            txn_not_found: Some(kvrpcpb::TxnNotFound {
                primary_key: codec.encode_key(b"missing-primary"),
                ..Default::default()
            }),
            ..Default::default()
        };

        codec.decode_key_error(&mut error).unwrap();
        let lock = error.locked.unwrap();
        assert_eq!(lock.key, b"lock");
        assert_eq!(lock.primary_lock, b"primary");
        assert_eq!(lock.secondaries, [b"secondary"]);
        assert_eq!(lock.shared_lock_infos[0].key, b"shared");
        assert_eq!(error.conflict.unwrap().primary, b"conflict-primary");
        let deadlock = error.deadlock.unwrap();
        assert_eq!(deadlock.lock_key, b"deadlock-lock");
        assert_eq!(deadlock.wait_chain[0].key, b"wait");
        assert_eq!(error.txn_not_found.unwrap().primary_key, b"missing-primary");
    }

    #[test]
    fn api_key_utilities_match_v1_and_v2_source_contracts() {
        assert_eq!(api_v2_prefixes(), [*b"r", *b"x"]);
        assert_eq!(parse_keyspace_id(b"x\x01\x02\x03key").unwrap(), 0x010203);
        assert!(parse_keyspace_id(b"t\x01\x02\x03key").is_err());
        assert_eq!(
            decode_api_key(b"r\x01\x02\x03key", kvrpcpb::ApiVersion::V2).unwrap(),
            (Some([b'r', 1, 2, 3]), b"key".to_vec())
        );
        assert_eq!(
            decode_api_key(b"key", kvrpcpb::ApiVersion::V1).unwrap(),
            (None, b"key".to_vec())
        );
    }

    #[test]
    fn test_encode_version() {
        let keyspace = Keyspace::Enable {
            keyspace_id: 0xDEAD,
        };
        let key_mode = KeyMode::Raw;

        let key = Key::from(vec![0xBE, 0xEF]);
        let expected_key = Key::from(vec![b'r', 0, 0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(key.encode_keyspace(keyspace, key_mode), expected_key);

        let bound: BoundRange = (Key::from(vec![0xDE, 0xAD])..Key::from(vec![0xBE, 0xEF])).into();
        let expected_bound: BoundRange = (Key::from(vec![b'r', 0, 0xDE, 0xAD, 0xDE, 0xAD])
            ..Key::from(vec![b'r', 0, 0xDE, 0xAD, 0xBE, 0xEF]))
            .into();
        assert_eq!(bound.encode_keyspace(keyspace, key_mode), expected_bound);

        let bound: BoundRange = (..).into();
        let expected_bound: BoundRange =
            (Key::from(vec![b'r', 0, 0xDE, 0xAD])..Key::from(vec![b'r', 0, 0xDE, 0xAE])).into();
        assert_eq!(bound.encode_keyspace(keyspace, key_mode), expected_bound);

        let bound: BoundRange = (Key::from(vec![])..Key::from(vec![])).into();
        let expected_bound: BoundRange =
            (Key::from(vec![b'r', 0, 0xDE, 0xAD])..Key::from(vec![b'r', 0, 0xDE, 0xAE])).into();
        assert_eq!(bound.encode_keyspace(keyspace, key_mode), expected_bound);

        let bound: BoundRange = (Key::from(vec![])..=Key::from(vec![])).into();
        let expected_bound: BoundRange =
            (Key::from(vec![b'r', 0, 0xDE, 0xAD])..Key::from(vec![b'r', 0, 0xDE, 0xAE])).into();
        assert_eq!(bound.encode_keyspace(keyspace, key_mode), expected_bound);

        let mutation = Mutation::Put(Key::from(vec![0xBE, 0xEF]), vec![4, 5, 6]);
        let expected_mutation = Mutation::Put(
            Key::from(vec![b'r', 0, 0xDE, 0xAD, 0xBE, 0xEF]),
            vec![4, 5, 6],
        );
        assert_eq!(
            mutation.encode_keyspace(keyspace, key_mode),
            expected_mutation
        );

        let mutation = Mutation::Delete(Key::from(vec![0xBE, 0xEF]));
        let expected_mutation = Mutation::Delete(Key::from(vec![b'r', 0, 0xDE, 0xAD, 0xBE, 0xEF]));
        assert_eq!(
            mutation.encode_keyspace(keyspace, key_mode),
            expected_mutation
        );

        let key_mode = KeyMode::Txn;
        let lock = crate::proto::kvrpcpb::LockInfo {
            key: vec![b'k', b'1'],
            primary_lock: vec![b'p', b'1'],
            secondaries: vec![vec![b's', b'1'], vec![b's', b'2']],
            ..Default::default()
        };
        let locks = vec![lock].encode_keyspace(keyspace, key_mode);
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].key, vec![b'x', 0, 0xDE, 0xAD, b'k', b'1']);
        assert_eq!(locks[0].primary_lock, vec![b'x', 0, 0xDE, 0xAD, b'p', b'1']);
        assert_eq!(
            locks[0].secondaries,
            vec![
                vec![b'x', 0, 0xDE, 0xAD, b's', b'1'],
                vec![b'x', 0, 0xDE, 0xAD, b's', b'2']
            ]
        );
    }

    #[test]
    fn test_truncate_version() {
        let keyspace = Keyspace::Enable {
            keyspace_id: 0xDEAD,
        };

        let key = Key::from(vec![b'r', 0, 0xDE, 0xAD, 0xBE, 0xEF]);
        let expected_key = Key::from(vec![0xBE, 0xEF]);
        assert_eq!(key.truncate_keyspace(keyspace), expected_key);

        let key = Key::from(vec![b'x', 0, 0xDE, 0xAD, 0xBE, 0xEF]);
        let expected_key = Key::from(vec![0xBE, 0xEF]);
        assert_eq!(key.truncate_keyspace(keyspace), expected_key);

        let pair = KvPair(Key::from(vec![b'x', 0, 0xDE, 0xAD, b'k']), vec![b'v']);
        let expected_pair = KvPair(Key::from(vec![b'k']), vec![b'v']);
        assert_eq!(pair.truncate_keyspace(keyspace), expected_pair);

        let range = Range {
            start: Key::from(vec![b'x', 0, 0xDE, 0xAD, b'a']),
            end: Key::from(vec![b'x', 0, 0xDE, 0xAD, b'b']),
        };
        let expected_range = Range {
            start: Key::from(vec![b'a']),
            end: Key::from(vec![b'b']),
        };
        assert_eq!(range.truncate_keyspace(keyspace), expected_range);

        let ranges = vec![
            Range {
                start: Key::from(vec![b'x', 0, 0xDE, 0xAD, b'a']),
                end: Key::from(vec![b'x', 0, 0xDE, 0xAD, b'b']),
            },
            Range {
                start: Key::from(vec![b'x', 0, 0xDE, 0xAD, b'c']),
                end: Key::from(vec![b'x', 0, 0xDE, 0xAD, b'd']),
            },
        ];
        let expected_ranges = vec![
            Range {
                start: Key::from(vec![b'a']),
                end: Key::from(vec![b'b']),
            },
            Range {
                start: Key::from(vec![b'c']),
                end: Key::from(vec![b'd']),
            },
        ];
        assert_eq!(ranges.truncate_keyspace(keyspace), expected_ranges);

        let pairs = vec![
            KvPair(Key::from(vec![b'x', 0, 0xDE, 0xAD, b'k']), vec![b'v']),
            KvPair(
                Key::from(vec![b'x', 0, 0xDE, 0xAD, b'k', b'2']),
                vec![b'v', b'2'],
            ),
        ];
        let expected_pairs = vec![
            KvPair(Key::from(vec![b'k']), vec![b'v']),
            KvPair(Key::from(vec![b'k', b'2']), vec![b'v', b'2']),
        ];
        assert_eq!(pairs.truncate_keyspace(keyspace), expected_pairs);

        let lock = crate::proto::kvrpcpb::LockInfo {
            key: vec![b'x', 0, 0xDE, 0xAD, b'k'],
            primary_lock: vec![b'x', 0, 0xDE, 0xAD, b'p'],
            secondaries: vec![vec![b'x', 0, 0xDE, 0xAD, b's']],
            ..Default::default()
        };
        let expected_lock = crate::proto::kvrpcpb::LockInfo {
            key: vec![b'k'],
            primary_lock: vec![b'p'],
            secondaries: vec![vec![b's']],
            ..Default::default()
        };
        assert_eq!(vec![lock].truncate_keyspace(keyspace), vec![expected_lock]);
    }

    #[test]
    fn test_apiv2_no_prefix_api_version() {
        assert_eq!(
            Keyspace::ApiV2NoPrefix.api_version(),
            kvrpcpb::ApiVersion::V2
        );
    }

    #[test]
    fn test_v1ttl_uses_the_v1_codec_and_context_version() {
        assert_eq!(Keyspace::V1Ttl.api_version(), kvrpcpb::ApiVersion::V1);
        assert_eq!(
            Keyspace::V1Ttl.context_keyspace_id(),
            Some(NULL_KEYSPACE_ID)
        );
        assert_eq!(
            Keyspace::Disable.context_keyspace_id(),
            Some(NULL_KEYSPACE_ID)
        );
        assert!(Keyspace::V1Ttl.response_codec(KeyMode::Raw).is_none());
        assert!(Keyspace::V1Ttl.v1_response_codec(KeyMode::Raw).is_some());
        assert!(decode_api_key(b"raw-key", kvrpcpb::ApiVersion::V1ttl).is_err());
        assert!(decode_api_key(b"raw-key", kvrpcpb::ApiVersion::V3).is_err());
        assert_eq!(api_v1_excluded_prefixes(), api_v2_prefixes());
    }

    #[test]
    fn v3_context_identity_is_never_reinterpreted_as_a_numeric_v2_id() {
        let context = kvrpcpb::Context {
            api_version: kvrpcpb::ApiVersion::V3 as i32,
            keyspace: Some(kvrpcpb::context::Keyspace::KeyspaceIdentity(
                crate::proto::apipb::KeyspaceIdentity {
                    namespace_id: 11,
                    keyspace_id: 22,
                },
            )),
            ..Default::default()
        };
        assert_eq!(context_keyspace_id(&context), None);
    }

    #[test]
    fn pinned_v3_namespace_and_keyspace_schema_inputs_are_generated() {
        use crate::proto::{apipb, keyspacepb, mpp};

        let identity = apipb::KeyspaceIdentity {
            namespace_id: 11,
            keyspace_id: 22,
        };
        let namespace = keyspacepb::NamespaceRef {
            namespace: Some(keyspacepb::namespace_ref::Namespace::NamespaceId(11)),
        };
        let load = keyspacepb::LoadKeyspaceRequest {
            name: "tenant".to_owned(),
            namespace: Some(namespace.clone()),
            ..Default::default()
        };
        assert_eq!(load.namespace, Some(namespace.clone()));

        let by_id = keyspacepb::LoadKeyspaceByIdRequest {
            keyspace: Some(
                keyspacepb::load_keyspace_by_id_request::Keyspace::KeyspaceIdentity(
                    identity.clone(),
                ),
            ),
            ..Default::default()
        };
        assert!(matches!(
            by_id.keyspace,
            Some(keyspacepb::load_keyspace_by_id_request::Keyspace::KeyspaceIdentity(_))
        ));

        let all = keyspacepb::GetAllKeyspacesRequest {
            namespace: Some(namespace),
            start_keyspace: Some(
                keyspacepb::get_all_keyspaces_request::StartKeyspace::StartKeyspaceIdentity(
                    identity.clone(),
                ),
            ),
            ..Default::default()
        };
        assert!(matches!(
            all.start_keyspace,
            Some(keyspacepb::get_all_keyspaces_request::StartKeyspace::StartKeyspaceIdentity(_))
        ));
        assert_eq!(
            keyspacepb::LookupKeyspaceRequest {
                name: "tenant".to_owned(),
                ..Default::default()
            }
            .name,
            "tenant"
        );
        let _namespace_client = std::any::type_name::<
            keyspacepb::namespace_client::NamespaceClient<tonic::transport::Channel>,
        >();

        let compact = kvrpcpb::CompactRequest {
            api_version: kvrpcpb::ApiVersion::V3 as i32,
            keyspace: Some(kvrpcpb::compact_request::Keyspace::KeyspaceIdentity(
                identity.clone(),
            )),
            ..Default::default()
        };
        assert!(matches!(
            compact.keyspace,
            Some(kvrpcpb::compact_request::Keyspace::KeyspaceIdentity(_))
        ));
        let task = mpp::TaskMeta {
            api_version: kvrpcpb::ApiVersion::V3 as i32,
            keyspace: Some(mpp::task_meta::Keyspace::KeyspaceIdentity(identity)),
            ..Default::default()
        };
        assert!(matches!(
            task.keyspace,
            Some(mpp::task_meta::Keyspace::KeyspaceIdentity(_))
        ));
        assert!(kvrpcpb::ExecDetailsV2 {
            read_pool_task_details: Some(kvrpcpb::PoolTaskDetails {
                poll_count: 1,
                ..Default::default()
            }),
            ..Default::default()
        }
        .read_pool_task_details
        .is_some());
    }

    #[test]
    fn test_apiv2_no_prefix_encode_is_noop() {
        let keyspace = Keyspace::ApiV2NoPrefix;
        let key_mode = KeyMode::Txn;

        let key = Key::from(vec![b'x', 0, 0, 0, b'k']);
        assert_eq!(key.clone().encode_keyspace(keyspace, key_mode), key);

        let pair = KvPair(Key::from(vec![b'x', 0, 0, 0, b'k']), vec![b'v']);
        assert_eq!(pair.clone().encode_keyspace(keyspace, key_mode), pair);

        let bound: BoundRange =
            (Key::from(vec![b'x', 0, 0, 0, b'a'])..Key::from(vec![b'x', 0, 0, 0, b'b'])).into();
        assert_eq!(bound.clone().encode_keyspace(keyspace, key_mode), bound);

        let mutation = Mutation::Put(Key::from(vec![b'x', 0, 0, 0, b'k']), vec![1, 2, 3]);
        assert_eq!(
            mutation.clone().encode_keyspace(keyspace, key_mode),
            mutation
        );

        let lock = crate::proto::kvrpcpb::LockInfo {
            key: vec![b'x', 0, 0, 0, b'k'],
            primary_lock: vec![b'x', 0, 0, 0, b'p'],
            secondaries: vec![vec![b'x', 0, 0, 0, b's']],
            ..Default::default()
        };
        let locks = vec![lock];
        assert_eq!(locks.clone().encode_keyspace(keyspace, key_mode), locks);

        let lock = crate::proto::kvrpcpb::LockInfo {
            key: vec![b'k', b'1'],
            primary_lock: vec![b'p', b'1'],
            secondaries: vec![vec![b's', b'1']],
            ..Default::default()
        };
        let locks = vec![lock.clone()];
        assert_eq!(
            locks.clone().encode_keyspace(Keyspace::Disable, key_mode),
            locks
        );
    }

    #[test]
    fn test_apiv2_no_prefix_truncate_is_noop() {
        let keyspace = Keyspace::ApiV2NoPrefix;

        let key = Key::from(vec![b'x', 0, 0, 0, b'k']);
        assert_eq!(key.clone().truncate_keyspace(keyspace), key);

        let pair = KvPair(Key::from(vec![b'x', 0, 0, 0, b'k']), vec![b'v']);
        assert_eq!(pair.clone().truncate_keyspace(keyspace), pair);

        let range = Range {
            start: Key::from(vec![b'x', 0, 0, 0, b'a']),
            end: Key::from(vec![b'x', 0, 0, 0, b'b']),
        };
        assert_eq!(range.clone().truncate_keyspace(keyspace), range);

        let pairs = vec![pair];
        assert_eq!(pairs.clone().truncate_keyspace(keyspace), pairs);

        let lock = crate::proto::kvrpcpb::LockInfo {
            key: vec![b'x', 0, 0, 0, b'k'],
            primary_lock: vec![b'x', 0, 0, 0, b'p'],
            secondaries: vec![vec![b'x', 0, 0, 0, b's']],
            ..Default::default()
        };
        let locks = vec![lock];
        assert_eq!(locks.clone().truncate_keyspace(keyspace), locks);
    }

    /// A shared-lock wrapper carries the key it locks, and its members are built from
    /// that same raw key (TiKV's `SharedLocks::into_lock_info`). Both must be converted:
    /// skipping the wrapper would hand `scan_locks` a physical key beside decoded member
    /// keys. The wrapper's *other* fields are a different matter — see
    /// [`unset_lock_key_fields_survive_truncation`].
    #[test]
    fn shared_lock_wrapper_keys_are_converted_alongside_their_members() {
        use crate::proto::kvrpcpb::{LockInfo, Op};
        let keyspace = Keyspace::Enable { keyspace_id: 0 };

        let wrapper = LockInfo {
            key: vec![b'x', 0, 0, 0, b'k'],
            lock_type: Op::SharedLock as i32,
            shared_lock_infos: vec![LockInfo {
                key: vec![b'x', 0, 0, 0, b'm'],
                primary_lock: vec![b'x', 0, 0, 0, b'p'],
                lock_version: 8,
                ..Default::default()
            }],
            ..Default::default()
        };

        let out = vec![wrapper].truncate_keyspace(keyspace);
        assert_eq!(
            out[0].key,
            vec![b'k'],
            "the wrapper's own key must be decoded, not left physical"
        );
        assert_eq!(out[0].shared_lock_infos[0].key, vec![b'm']);
        assert_eq!(out[0].shared_lock_infos[0].primary_lock, vec![b'p']);

        let back = out.encode_keyspace(keyspace, KeyMode::Txn);
        assert_eq!(back[0].key, vec![b'x', 0, 0, 0, b'k']);
        assert_eq!(back[0].shared_lock_infos[0].key, vec![b'x', 0, 0, 0, b'm']);
    }

    /// The empty logical key is VALID in API v2: it encodes to the bare keyspace
    /// prefix. `scan_locks` -> `resolve_locks` round-trips locks through
    /// truncate-then-encode, so a lock on the empty key must regain its prefix —
    /// otherwise resolution would look up the region for an empty physical key.
    /// This is why the encode side does not share the truncate side's unset guard.
    #[test]
    fn a_lock_on_the_empty_logical_key_round_trips() {
        use crate::proto::kvrpcpb::LockInfo;
        let keyspace = Keyspace::Enable { keyspace_id: 0 };

        let physical = vec![LockInfo {
            key: vec![b'x', 0, 0, 0],
            primary_lock: vec![b'x', 0, 0, 0],
            ..Default::default()
        }];
        let logical = physical.clone().truncate_keyspace(keyspace);
        assert!(logical[0].key.is_empty(), "the empty logical key");

        let back = logical.encode_keyspace(keyspace, KeyMode::Txn);
        assert_eq!(
            back, physical,
            "a lock on the empty logical key must regain its keyspace prefix"
        );
    }

    /// TiKV's `SharedLocks::into_lock_info` leaves a wrapper's `primary_lock` at its
    /// default, so the codec meets genuinely unset fields in practice — they must be
    /// skipped, not run into `pretruncate_bytes`' length assertion.
    #[test]
    fn unset_lock_key_fields_survive_truncation() {
        use crate::proto::kvrpcpb::{LockInfo, Op};
        let keyspace = Keyspace::Enable { keyspace_id: 0 };

        // The writer's actual shape: key and members set, primary_lock left default.
        let realistic = LockInfo {
            key: vec![b'x', 0, 0, 0, b'k'],
            lock_type: Op::SharedLock as i32,
            shared_lock_infos: vec![LockInfo {
                key: vec![b'x', 0, 0, 0, b'k'],
                primary_lock: vec![b'x', 0, 0, 0, b'p'],
                ..Default::default()
            }],
            ..Default::default()
        };
        let out = vec![realistic].truncate_keyspace(keyspace);
        assert_eq!(out[0].key, vec![b'k']);
        assert!(
            out[0].primary_lock.is_empty(),
            "the wrapper's unset primary must be skipped, not truncated"
        );
        assert_eq!(out[0].shared_lock_infos[0].primary_lock, vec![b'p']);

        // Defensively, a wrapper with no key at all must not panic either.
        let keyless = LockInfo {
            lock_type: Op::SharedLock as i32,
            ..Default::default()
        };
        let out = vec![keyless].truncate_keyspace(keyspace);
        assert!(out[0].key.is_empty());
    }

    /// An empty element INSIDE `secondaries` is a different case from the unset
    /// fields above: an unset `secondaries` is an empty Vec with no elements, so a
    /// present-but-empty element can only be malformed input. It is tolerated —
    /// skipped rather than run into `pretruncate_bytes`' length assertion — while
    /// the well-formed elements beside it still convert.
    #[test]
    fn a_malformed_empty_secondary_is_tolerated_not_truncated() {
        use crate::proto::kvrpcpb::LockInfo;
        let keyspace = Keyspace::Enable { keyspace_id: 0 };

        let malformed = LockInfo {
            key: vec![b'x', 0, 0, 0, b'k'],
            secondaries: vec![vec![], vec![b'x', 0, 0, 0, b's']],
            ..Default::default()
        };
        let out = vec![malformed].truncate_keyspace(keyspace);
        assert_eq!(out[0].secondaries, vec![vec![], vec![b's']]);
    }
}
