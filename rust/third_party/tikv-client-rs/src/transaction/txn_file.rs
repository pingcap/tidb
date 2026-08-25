// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! File-based transaction support transcreated from client-go's
//! `txnkv/transaction/txn_file.go`.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

use futures::stream::{self, StreamExt, TryStreamExt};
use openssl::pkcs12::Pkcs12;
use openssl::pkey::PKey;
use openssl::x509::X509;
use reqwest::{Certificate, Client, Identity, StatusCode};
use serde_derive::Deserialize;

use crate::async_util::Cancellation;
use crate::config::{get_global_config, MAX_TXN_CHUNK_SIZE_IN_PARALLEL};
use crate::pd::PdClient;
use crate::proto::kvrpcpb;
use crate::region::RegionWithLeader;
use crate::request::Keyspace;
use crate::retry::{RetryBackoffer, BO_TIKV_RPC, BO_TIKV_SERVER_BUSY};
use crate::{Error, Key, Result};

use super::transaction::RequestSource;

pub const PRE_SPLIT_REGION_CHUNKS: usize = 4;
pub const DEFAULT_BUILD_TXN_FILE_MAX_BACKOFF_MS: u64 = 60_000;
const TXN_FILE_HTTP_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

static BUILD_TXN_FILE_MAX_BACKOFF_MS: AtomicU64 =
    AtomicU64::new(DEFAULT_BUILD_TXN_FILE_MAX_BACKOFF_MS);
static HTTP_CLIENT: LazyLock<RwLock<Option<Arc<Client>>>> = LazyLock::new(Default::default);

pub fn build_txn_file_max_backoff_ms() -> u64 {
    BUILD_TXN_FILE_MAX_BACKOFF_MS.load(Ordering::Acquire)
}

pub fn set_build_txn_file_max_backoff_ms(value: u64) {
    BUILD_TXN_FILE_MAX_BACKOFF_MS.store(value, Ordering::Release);
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TxnChunkRange {
    pub(crate) smallest: Vec<u8>,
    pub(crate) biggest: Vec<u8>,
    pub(crate) entries: u64,
}

impl TxnChunkRange {
    pub(crate) fn new(smallest: Vec<u8>, biggest: Vec<u8>, entries: u64) -> Self {
        Self {
            smallest,
            biggest,
            entries,
        }
    }
}

impl fmt::Display for TxnChunkRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "txnChunkRange[{},{}](entries={})",
            crate::redact::key(&self.smallest),
            crate::redact::key(&self.biggest),
            self.entries
        )
    }
}

/// Sorted, non-overlapping transaction chunks.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct TxnChunkSlice {
    pub(crate) chunk_ids: Vec<u64>,
    pub(crate) chunk_ranges: Vec<TxnChunkRange>,
}

impl TxnChunkSlice {
    pub(crate) fn len(&self) -> usize {
        self.chunk_ids.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.chunk_ids.is_empty()
    }

    pub(crate) fn smallest(&self) -> Option<&[u8]> {
        self.chunk_ranges
            .first()
            .map(|range| range.smallest.as_slice())
    }

    #[allow(dead_code)] // Retained for the source txnChunkSlice accessor surface.
    pub(crate) fn biggest(&self) -> Option<&[u8]> {
        self.chunk_ranges
            .last()
            .map(|range| range.biggest.as_slice())
    }

    pub(crate) fn push(&mut self, chunk_id: u64, chunk_range: TxnChunkRange) {
        self.chunk_ids.push(chunk_id);
        self.chunk_ranges.push(chunk_range);
    }

    pub(crate) fn append(&mut self, other: &Self) {
        self.chunk_ids.extend_from_slice(&other.chunk_ids);
        self.chunk_ranges.extend_from_slice(&other.chunk_ranges);
    }

    pub(crate) fn sort_and_dedup(&mut self) {
        if self.len() <= 1 {
            return;
        }
        let mut entries: Vec<_> = self
            .chunk_ids
            .drain(..)
            .zip(self.chunk_ranges.drain(..))
            .collect();
        entries.sort_by(|left, right| left.1.smallest.cmp(&right.1.smallest));
        entries.dedup_by(|right, left| right.0 == left.0);
        (self.chunk_ids, self.chunk_ranges) = entries.into_iter().unzip();
    }
}

impl fmt::Display for TxnChunkSlice {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[")?;
        for (index, (chunk_id, range)) in self.chunk_ids.iter().zip(&self.chunk_ranges).enumerate()
        {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            write!(
                formatter,
                "txnChunkSlice{{{}: [{}, {}]}}",
                chunk_id,
                crate::redact::key(&range.smallest),
                crate::redact::key(&range.biggest)
            )?;
        }
        formatter.write_str("]")
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ChunkBatch {
    pub(crate) chunks: TxnChunkSlice,
    pub(crate) region: RegionWithLeader,
    pub(crate) first_key: Vec<u8>,
    pub(crate) sample_data_keys: Vec<Vec<u8>>,
    pub(crate) is_primary: bool,
}

impl ChunkBatch {
    pub(crate) fn transaction_size(&self) -> u64 {
        self.chunks
            .chunk_ranges
            .iter()
            .map(|range| range.entries)
            .sum()
    }
}

/// Return the first mutation and first write mutation in `[start, end)`.
pub(crate) fn mutations_has_data_in_range<'a>(
    mutations: &'a [kvrpcpb::Mutation],
    start: &[u8],
    end: &[u8],
) -> Option<(&'a [u8], Option<&'a [u8]>)> {
    let mut position = mutations.partition_point(|mutation| mutation.key.as_slice() < start);
    if position == mutations.len() || (!end.is_empty() && mutations[position].key.as_slice() >= end)
    {
        return None;
    }
    let first_key = mutations[position].key.as_slice();
    let mut first_data_key = None;
    while position < mutations.len() && (end.is_empty() || mutations[position].key.as_slice() < end)
    {
        let operation = kvrpcpb::Op::try_from(mutations[position].op).unwrap_or(kvrpcpb::Op::Put);
        if !matches!(
            operation,
            kvrpcpb::Op::CheckNotExists | kvrpcpb::Op::Lock | kvrpcpb::Op::PessimisticLock
        ) {
            first_data_key = Some(mutations[position].key.as_slice());
            break;
        }
        position += 1;
    }
    Some((first_key, first_data_key))
}

impl TxnChunkSlice {
    pub(crate) async fn group_to_batches<PdC: PdClient>(
        &self,
        pd_client: &Arc<PdC>,
        mutations: &[kvrpcpb::Mutation],
    ) -> Result<Vec<ChunkBatch>> {
        // client-go deliberately ignores conf_ver so a configuration change
        // does not duplicate a region batch while grouping chunks.
        let mut grouped: BTreeMap<(u64, u64), ChunkBatch> = BTreeMap::new();
        for (chunk_id, chunk_range) in self.chunk_ids.iter().zip(&self.chunk_ranges) {
            let mut next = chunk_range.smallest.clone();
            while next.as_slice() <= chunk_range.biggest.as_slice() {
                let region = pd_client.region_for_key(&Key::from(next.clone())).await?;
                let range_start = chunk_range
                    .smallest
                    .as_slice()
                    .max(region.region.start_key.as_slice());
                let mut exclusive_biggest = chunk_range.biggest.clone();
                exclusive_biggest.push(0);
                let range_end = if region.region.end_key.is_empty()
                    || exclusive_biggest.as_slice() < region.region.end_key.as_slice()
                {
                    exclusive_biggest.as_slice()
                } else {
                    region.region.end_key.as_slice()
                };
                if let Some((first_key, first_data_key)) =
                    mutations_has_data_in_range(mutations, range_start, range_end)
                {
                    let epoch = region.region.region_epoch.as_ref();
                    let key = (region.region.id, epoch.map_or(0, |epoch| epoch.version));
                    let batch = grouped.entry(key).or_insert_with(|| ChunkBatch {
                        chunks: TxnChunkSlice::default(),
                        region: region.clone(),
                        first_key: first_key.to_vec(),
                        sample_data_keys: Vec::new(),
                        is_primary: false,
                    });
                    if batch.first_key.is_empty() {
                        batch.first_key = first_key.to_vec();
                    }
                    batch.chunks.push(*chunk_id, chunk_range.clone());
                    if let Some(first_data_key) = first_data_key {
                        batch.sample_data_keys.push(first_data_key.to_vec());
                    }
                }
                if region.region.end_key.is_empty() {
                    break;
                }
                if region.region.end_key.as_slice() <= next.as_slice() {
                    return Err(Error::StringError(
                        "txn file: located region does not advance".to_owned(),
                    ));
                }
                next = region.region.end_key;
            }
        }
        let mut batches: Vec<_> = grouped.into_values().collect();
        batches.sort_by(|left, right| {
            left.chunks
                .smallest()
                .cmp(&right.chunks.smallest())
                .then_with(|| {
                    left.region
                        .region
                        .start_key
                        .cmp(&right.region.region.start_key)
                })
        });
        Ok(batches)
    }
}

pub(crate) fn txn_file_max_chunks_in_parallel(txn_chunk_max_size: u64) -> usize {
    if txn_chunk_max_size == 0 {
        return 1;
    }
    usize::try_from((MAX_TXN_CHUNK_SIZE_IN_PARALLEL / txn_chunk_max_size).max(1))
        .unwrap_or(usize::MAX)
}

pub(crate) fn request_source_allows_txn_file(
    request_source: &RequestSource,
    whitelist: &[String],
) -> bool {
    !request_source.internal || whitelist.contains(&request_source.source_type)
}

/// Returns whether the source is admitted by the current txn-file whitelist.
pub fn is_request_source_use_txn_file(request_source: &RequestSource) -> bool {
    let config = get_global_config();
    request_source_allows_txn_file(
        request_source,
        &config.tikv_client.txn_file_request_source_whitelist,
    )
}

pub(crate) fn txn_file_pre_split_keys(batches: &[ChunkBatch]) -> Vec<Vec<u8>> {
    let mut split_keys = Vec::new();
    for batch in batches {
        for index in (PRE_SPLIT_REGION_CHUNKS..batch.chunks.len()).step_by(PRE_SPLIT_REGION_CHUNKS)
        {
            split_keys.push(batch.chunks.chunk_ranges[index].smallest.clone());
        }
    }
    split_keys
}

pub(crate) fn keyspace_id(keyspace: Keyspace) -> u32 {
    keyspace
        .context_keyspace_id()
        .unwrap_or(crate::request::NULL_KEYSPACE_ID)
}

fn build_http_client() -> Result<Client> {
    let config = get_global_config();
    let mut builder = Client::builder()
        .timeout(Duration::from_millis(build_txn_file_max_backoff_ms()))
        .pool_idle_timeout(TXN_FILE_HTTP_IDLE_TIMEOUT)
        .pool_max_idle_per_host(20);
    if !config.security.cluster_ssl_ca.is_empty() {
        let ca = fs::read(&config.security.cluster_ssl_ca)?;
        let certificate = Certificate::from_pem(&ca)
            .map_err(|error| Error::StringError(format!("failed to append ca certs: {error}")))?;
        builder = builder.add_root_certificate(certificate);
        if !config.security.cluster_ssl_cert.is_empty()
            && !config.security.cluster_ssl_key.is_empty()
        {
            let certificate = fs::read(&config.security.cluster_ssl_cert)?;
            let key = fs::read(&config.security.cluster_ssl_key)?;
            let certificate = X509::from_pem(&certificate).map_err(|error| {
                Error::StringError(format!("could not load client certificate: {error}"))
            })?;
            let key = PKey::private_key_from_pem(&key).map_err(|error| {
                Error::StringError(format!("could not load client private key: {error}"))
            })?;
            let identity = Pkcs12::builder()
                .name("")
                .pkey(&key)
                .cert(&certificate)
                .build2("")
                .and_then(|identity| identity.to_der())
                .map_err(|error| {
                    Error::StringError(format!("could not build client identity: {error}"))
                })?;
            builder =
                builder.identity(Identity::from_pkcs12_der(&identity, "").map_err(|error| {
                    Error::StringError(format!("could not load client key pair: {error}"))
                })?);
        }
    }
    builder
        .build()
        .map_err(|error| Error::StringError(error.to_string()))
}

fn shared_http_client() -> Result<Arc<Client>> {
    if let Some(client) = HTTP_CLIENT.read().unwrap().as_ref() {
        return Ok(client.clone());
    }
    let mut slot = HTTP_CLIENT.write().unwrap();
    if let Some(client) = slot.as_ref() {
        return Ok(client.clone());
    }
    let client = Arc::new(build_http_client()?);
    *slot = Some(client.clone());
    Ok(client)
}

/// Drop the shared uploader's idle pool without interrupting requests that
/// already cloned the client.
pub fn close_txn_file_idle_connections() {
    HTTP_CLIENT.write().unwrap().take();
}

#[derive(Clone)]
struct ChunkWriterClient {
    client: Arc<Client>,
    service_address: String,
}

#[derive(Deserialize)]
struct ChunkWriterResponse {
    chunk_id: u64,
}

impl ChunkWriterClient {
    fn new(keyspace_id: u32) -> Result<Self> {
        let config = get_global_config();
        let scheme = if config.security.cluster_ssl_ca.is_empty() {
            "http"
        } else {
            "https"
        };
        Ok(Self {
            client: shared_http_client()?,
            service_address: format!(
                "{scheme}://{}/txn_chunk?keyspace_id={keyspace_id}",
                config.tikv_client.txn_chunk_writer_addr
            ),
        })
    }

    async fn build_chunk(&self, mut payload: Vec<u8>, cancellation: Cancellation) -> Result<u64> {
        let checksum = crc32fast::hash(&payload);
        payload.extend_from_slice(&checksum.to_le_bytes());
        let mut backoffer = RetryBackoffer::new(cancellation, build_txn_file_max_backoff_ms());
        loop {
            let response = match self
                .client
                .post(&self.service_address)
                .header("content-type", "application/octet-stream")
                .body(payload.clone())
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    backoffer
                        .backoff(BO_TIKV_RPC, format!("request failed: {error}"))
                        .await
                        .map_err(|error| Error::StringError(error.to_string()))?;
                    continue;
                }
            };
            if response.status() != StatusCode::OK {
                let status = response.status();
                backoffer
                    .backoff(
                        BO_TIKV_SERVER_BUSY,
                        format!("service error, http status {status}"),
                    )
                    .await
                    .map_err(|error| Error::StringError(error.to_string()))?;
                continue;
            }
            return response
                .json::<ChunkWriterResponse>()
                .await
                .map(|response| response.chunk_id)
                .map_err(|error| Error::StringError(format!("unmarshal response: {error}")));
        }
    }
}

#[derive(Clone)]
struct UnbuiltChunk {
    payload: Vec<u8>,
    range: TxnChunkRange,
}

pub(crate) async fn build_txn_chunks(
    mutations: &[kvrpcpb::Mutation],
    keyspace: Keyspace,
    cancellation: Cancellation,
) -> Result<TxnChunkSlice> {
    if mutations.is_empty() {
        return Ok(TxnChunkSlice::default());
    }
    let config = get_global_config();
    let max_chunk_size = usize::try_from(config.tikv_client.txn_chunk_max_size)
        .map_err(|_| Error::StringError("txn chunk max size exceeds usize".to_owned()))?;
    if max_chunk_size == 0 {
        return Err(Error::StringError(
            "txn-chunk-max-size should be greater than 0".to_owned(),
        ));
    }
    let mut sorted = mutations.to_vec();
    sorted.sort_by(|left, right| left.key.cmp(&right.key));
    let capacity = max_chunk_size.min(
        sorted
            .iter()
            .map(|mutation| mutation.key.len() + mutation.value.len() + 7)
            .sum::<usize>()
            .saturating_add(4),
    );
    let mut chunks = Vec::new();
    let mut payload = Vec::with_capacity(capacity);
    let mut smallest = sorted[0].key.clone();
    let mut entries = 0_u64;
    for (index, mutation) in sorted.iter().enumerate() {
        let entry_size = mutation.key.len() + mutation.value.len() + 7;
        if !payload.is_empty()
            && payload.len().saturating_add(entry_size).saturating_add(4) > max_chunk_size
        {
            chunks.push(UnbuiltChunk {
                payload,
                range: TxnChunkRange::new(smallest, sorted[index - 1].key.clone(), entries),
            });
            payload = Vec::with_capacity(capacity);
            smallest = mutation.key.clone();
            entries = 0;
        }
        let key_len = u16::try_from(mutation.key.len())
            .map_err(|_| Error::StringError("txn file mutation key exceeds uint16".to_owned()))?;
        let value_len = u32::try_from(mutation.value.len())
            .map_err(|_| Error::StringError("txn file mutation value exceeds uint32".to_owned()))?;
        payload.extend_from_slice(&key_len.to_le_bytes());
        payload.extend_from_slice(&mutation.key);
        payload.push(mutation.op as u8);
        payload.extend_from_slice(&value_len.to_le_bytes());
        payload.extend_from_slice(&mutation.value);
        entries += 1;
    }
    chunks.push(UnbuiltChunk {
        payload,
        range: TxnChunkRange::new(smallest, sorted.last().unwrap().key.clone(), entries),
    });

    let writer = ChunkWriterClient::new(keyspace_id(keyspace))?;
    let concurrency = usize::try_from(config.tikv_client.txn_chunk_writer_concurrency)
        .unwrap_or(usize::MAX)
        .max(1)
        .min(chunks.len());
    let mut built: Vec<_> = stream::iter(chunks.into_iter().map(|chunk| {
        let writer = writer.clone();
        let cancellation = cancellation.clone();
        async move {
            let chunk_id = writer.build_chunk(chunk.payload, cancellation).await?;
            Ok::<_, Error>((chunk_id, chunk.range))
        }
    }))
    .buffer_unordered(concurrency)
    .try_collect()
    .await?;
    built.sort_by(|left, right| left.1.smallest.cmp(&right.1.smallest));
    let mut result = TxnChunkSlice::default();
    for (chunk_id, range) in built {
        result.push(chunk_id, range);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn mutation(key: &str, operation: kvrpcpb::Op) -> kvrpcpb::Mutation {
        kvrpcpb::Mutation {
            op: operation as i32,
            key: key.as_bytes().to_vec(),
            value: key.as_bytes().to_vec(),
            ..Default::default()
        }
    }

    #[test]
    fn source_mutations_has_data_in_range_matrix() {
        let mutations = (10..20)
            .step_by(2)
            .map(|index| {
                mutation(
                    &format!("{index:04}"),
                    if index % 4 == 2 {
                        kvrpcpb::Op::CheckNotExists
                    } else {
                        kvrpcpb::Op::Put
                    },
                )
            })
            .collect::<Vec<_>>();
        let key = |index: i32| {
            (index >= 0)
                .then(|| format!("{index:04}").into_bytes())
                .unwrap_or_default()
        };
        let cases = [
            (-1, -1, Some((10, Some(12)))),
            (-1, 5, None),
            (0, 10, None),
            (0, 11, Some((10, None))),
            (0, 30, Some((10, Some(12)))),
            (0, -1, Some((10, Some(12)))),
            (10, 20, Some((10, Some(12)))),
            (15, 16, None),
            (15, 17, Some((16, Some(16)))),
            (15, -1, Some((16, Some(16)))),
            (20, 30, None),
            (21, 30, None),
            (21, -1, None),
        ];
        for (start, end, expected) in cases {
            let actual = mutations_has_data_in_range(&mutations, &key(start), &key(end));
            match (actual, expected) {
                (None, None) => {}
                (Some((first, data)), Some((expected_first, expected_data))) => {
                    assert_eq!(first, key(expected_first));
                    assert_eq!(data, expected_data.map(key).as_deref());
                }
                other => panic!("range [{start}, {end}) mismatch: {other:?}"),
            }
        }
    }

    #[test]
    fn source_chunk_slice_sort_and_dedup_preserves_ranges() {
        let mut chunks = TxnChunkSlice::default();
        for id in [7, 1, 5, 1, 2, 7] {
            chunks.push(
                id,
                TxnChunkRange::new(
                    format!("k{id:04}").into_bytes(),
                    format!("k{id:04}_end").into_bytes(),
                    id + 1,
                ),
            );
        }
        chunks.sort_and_dedup();
        assert_eq!(chunks.chunk_ids, [1, 2, 5, 7]);
        for (index, id) in chunks.chunk_ids.iter().enumerate() {
            assert_eq!(chunks.chunk_ranges[index].entries, id + 1);
        }
    }

    #[test]
    fn source_txn_file_parallel_budget_boundaries() {
        assert_eq!(txn_file_max_chunks_in_parallel(128 * 1024 * 1024), 32);
        assert_eq!(
            txn_file_max_chunks_in_parallel(MAX_TXN_CHUNK_SIZE_IN_PARALLEL),
            1
        );
        assert_eq!(
            txn_file_max_chunks_in_parallel(MAX_TXN_CHUNK_SIZE_IN_PARALLEL + 1),
            1
        );
        assert_eq!(txn_file_max_chunks_in_parallel(0), 1);
    }

    #[test]
    fn source_txn_file_http_client_idle_connection_timeout_is_90_seconds() {
        assert_eq!(TXN_FILE_HTTP_IDLE_TIMEOUT, Duration::from_secs(90));
    }

    #[test]
    fn source_request_source_whitelist() {
        let external = RequestSource::default();
        assert!(request_source_allows_txn_file(&external, &[]));
        let internal = RequestSource {
            internal: true,
            source_type: "ddl_modify_column".to_owned(),
            explicit_source_type: String::new(),
        };
        assert!(request_source_allows_txn_file(
            &internal,
            &["ddl_modify_column".to_owned()]
        ));
        assert!(!request_source_allows_txn_file(
            &internal,
            &["ddl_alter_partition".to_owned()]
        ));
    }

    #[test]
    fn close_before_http_client_initialization_is_safe() {
        close_txn_file_idle_connections();
    }

    #[test]
    fn source_close_idle_connections_replaces_the_shared_pool() {
        close_txn_file_idle_connections();
        let first = shared_http_client().unwrap();
        close_txn_file_idle_connections();
        let second = shared_http_client().unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn source_build_txn_files_counts_entries_and_matches_wire_format() {
        close_txn_file_idle_connections();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            let (header_end, content_length) = loop {
                let mut part = [0_u8; 1024];
                let read = stream.read(&mut part).await.unwrap();
                assert!(read > 0);
                received.extend_from_slice(&part[..read]);
                if let Some(header_end) = received.windows(4).position(|part| part == b"\r\n\r\n") {
                    let header_end = header_end + 4;
                    let headers = std::str::from_utf8(&received[..header_end]).unwrap();
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>())
                        })
                        .unwrap()
                        .unwrap();
                    break (header_end, content_length);
                }
            };
            while received.len() < header_end + content_length {
                let mut part = [0_u8; 1024];
                let read = stream.read(&mut part).await.unwrap();
                assert!(read > 0);
                received.extend_from_slice(&part[..read]);
            }
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{\"chunk_id\":42}",
                )
                .await
                .unwrap();
            (
                String::from_utf8(received[..header_end].to_vec()).unwrap(),
                received[header_end..header_end + content_length].to_vec(),
            )
        });
        let restore = crate::config::update_global(|config| {
            config.tikv_client.txn_chunk_writer_addr = address.to_string();
            config.tikv_client.txn_chunk_writer_concurrency = 1;
            config.tikv_client.txn_chunk_max_size = 1024;
        });
        let mutations = vec![
            mutation("a", kvrpcpb::Op::Put),
            mutation("b", kvrpcpb::Op::Del),
        ];
        let chunks = build_txn_chunks(&mutations, Keyspace::Disable, Cancellation::default())
            .await
            .unwrap();
        restore();
        close_txn_file_idle_connections();

        assert_eq!(chunks.chunk_ids, [42]);
        assert_eq!(chunks.chunk_ranges[0].entries, 2);
        assert_eq!(chunks.chunk_ranges[0].smallest, b"a");
        assert_eq!(chunks.chunk_ranges[0].biggest, b"b");
        let (headers, payload) = server.await.unwrap();
        assert!(headers.starts_with(&format!(
            "POST /txn_chunk?keyspace_id={} HTTP/1.1\r\n",
            crate::request::NULL_KEYSPACE_ID
        )));
        assert!(headers
            .to_ascii_lowercase()
            .contains("content-type: application/octet-stream"));
        let (serialized, checksum) = payload.split_at(payload.len() - 4);
        assert_eq!(
            u32::from_le_bytes(checksum.try_into().unwrap()),
            crc32fast::hash(serialized)
        );

        let mut cursor = 0;
        for expected in &mutations {
            let key_len =
                u16::from_le_bytes(serialized[cursor..cursor + 2].try_into().unwrap()) as usize;
            cursor += 2;
            assert_eq!(&serialized[cursor..cursor + key_len], expected.key);
            cursor += key_len;
            assert_eq!(serialized[cursor], expected.op as u8);
            cursor += 1;
            let value_len =
                u32::from_le_bytes(serialized[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            assert_eq!(&serialized[cursor..cursor + value_len], expected.value);
            cursor += value_len;
        }
        assert_eq!(cursor, serialized.len());
    }
}
