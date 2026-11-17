// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `reader.go`: reads the pairs of a key range from many data files.

use std::sync::atomic::{AtomicUsize, Ordering};

use std::sync::{Arc, Mutex};
use std::time::Instant;

use tidb_gocontext::Context;
use tidb_ingestor_simplesst::{KVPair, KVReader, CONCURRENT_READER_BUFFER_SIZE_PER_CONC, DEFAULT_READ_BUFFER_SIZE};
use tidb_objstore::storeapi::Storage;

use crate::engine::MemKVsAndBuffers;
use crate::{int_field, str_field, Result};

/// Go `readAllDataConcThreshold`: a file range needs this many concurrent
/// reader buffers before it is read concurrently.
const READ_ALL_DATA_CONC_THRESHOLD: u64 = 4;

/// Go `readAllData`: reads the pairs in `[start_key, end_key)` of every data
/// file, from `start_offsets[i]` of file `i`, into `output.kvs_per_file`.
/// On failure the output is emptied.
///
/// # Errors
/// A file could not be read.
#[allow(clippy::too_many_arguments)]
pub(crate) fn read_all_data(
    ctx: &Context,
    store: &Arc<dyn Storage>,
    data_files: &[String],
    stats_files: &[String],
    start_key: &[u8],
    end_key: &[u8],
    start_offsets: &[u64],
    estimated_end_offsets: &[u64],
    output: &mut MemKVsAndBuffers,
) -> Result<()> {
    let task = tidb_lightning_log::begin_logutil_task(tidb_logutil::bg_logger(), "read all data");
    tidb_log::info(
        "read all data arguments",
        &[
            int_field("data-file-count", data_files.len() as i64),
            int_field("stat-file-count", stats_files.len() as i64),
            str_field("start-key", &hex::encode(start_key)),
            str_field("end-key", &hex::encode(end_key)),
        ],
    );
    let result = read_all_data_inner(
        ctx,
        store,
        data_files,
        stats_files,
        start_key,
        end_key,
        start_offsets,
        estimated_end_offsets,
        output,
    );
    if result.is_err() {
        *output = MemKVsAndBuffers::default();
    }
    task.end(tidb_log::Level::Error, result.as_ref().err().map(|e| e.as_ref() as &(dyn std::error::Error + 'static)), &[]);
    result
}

#[allow(clippy::too_many_arguments)]
fn read_all_data_inner(
    ctx: &Context,
    store: &Arc<dyn Storage>,
    data_files: &[String],
    stats_files: &[String],
    start_key: &[u8],
    end_key: &[u8],
    start_offsets: &[u64],
    estimated_end_offsets: &[u64],
    output: &mut MemKVsAndBuffers,
) -> Result<()> {
    let buf_size = CONCURRENT_READER_BUFFER_SIZE_PER_CONC.load(Ordering::SeqCst) as u64;
    let mut concurrences = vec![1u64; stats_files.len()];
    let mut total_file_size = 0u64;
    for i in 0..stats_files.len() {
        let size = estimated_end_offsets[i].saturating_sub(start_offsets[i]);
        total_file_size += size;
        let expected_conc = size / buf_size + 1;
        // Only a large range is worth the concurrent reader's buffers.
        if expected_conc >= READ_ALL_DATA_CONC_THRESHOLD {
            concurrences[i] = expected_conc;
        }
        if expected_conc > 1 {
            tidb_log::info(
                "found hotspot file in readAllData",
                &[
                    str_field("filename", &stats_files[i]),
                    int_field("startOffset", start_offsets[i] as i64),
                    int_field("endOffset", estimated_end_offsets[i] as i64),
                    int_field("expectedConc", expected_conc as i64),
                    int_field("concurrency", concurrences[i] as i64),
                ],
            );
        }
    }
    tidb_log::info(
        "estimated file size of this range group",
        &[str_field("totalSize", &tidb_config_configtypes::bytes_size(total_file_size as f64))],
    );

    let read_conn = 1000.min(data_files.len());
    let (worker_ctx, cancel) = Context::with_cancel(ctx);
    let next = AtomicUsize::new(0);
    let first_err: Mutex<Option<crate::Error>> = Mutex::new(None);
    let shared = Mutex::new(std::mem::take(output));
    std::thread::scope(|scope| {
        for _ in 0..read_conn {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::SeqCst);
                if i >= data_files.len() || worker_ctx.err().is_some() {
                    return;
                }
                let result = read_one_file(
                    &worker_ctx,
                    store,
                    &data_files[i],
                    start_key,
                    end_key,
                    start_offsets[i],
                    concurrences[i],
                    &shared,
                );
                if let Err(e) = result {
                    let e = crate::other(format!("failed to read file {}: {e}", data_files[i]));
                    first_err.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get_or_insert(e);
                    cancel.cancel();
                    return;
                }
            });
        }
    });
    *output = shared.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(e) = first_err.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner) {
        return Err(e);
    }
    if let Some(e) = ctx.err() {
        return Err(crate::other(e.to_string()));
    }
    Ok(())
}

/// Go `readOneFile`.
#[allow(clippy::too_many_arguments)]
fn read_one_file(
    ctx: &Context,
    store: &Arc<dyn Storage>,
    data_file: &str,
    start_key: &[u8],
    end_key: &[u8],
    start_offset: u64,
    concurrency: u64,
    output: &Mutex<MemKVsAndBuffers>,
) -> Result<()> {
    let ts = Instant::now();
    let mut rd = KVReader::new(ctx, data_file, store.as_ref(), start_offset, 64 * 1024).map_err(crate::sst)?;
    let result = (|| {
        if concurrency > 1 {
            rd.enable_concurrent_read(
                Arc::clone(store),
                data_file,
                concurrency as usize,
                CONCURRENT_READER_BUFFER_SIZE_PER_CONC.load(Ordering::SeqCst),
            );
            rd.switch_concurrent_mode(true).map_err(crate::sst)?;
        }
        let mut kvs = Vec::with_capacity(1024);
        let (mut size, mut dropped_size) = (0usize, 0usize);
        loop {
            if let Some(e) = ctx.err() {
                return Err(crate::other(e.to_string()));
            }
            let (k, v) = match rd.next_kv() {
                Ok(kv) => kv,
                Err(e) if e.is_eof() => break,
                Err(e) => return Err(crate::sst(e)),
            };
            if k.as_slice() < start_key {
                dropped_size += k.len() + v.len();
                continue;
            }
            if k.as_slice() >= end_key {
                break;
            }
            size += k.len() + v.len();
            kvs.push(KVPair { key: k, value: v });
        }
        tidb_metrics::globalsort::READ_FROM_CLOUD_STORAGE_DURATION
            .with_label_values(&["read_one_file"])
            .observe(ts.elapsed().as_secs_f64());
        let mut out = output.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        out.kvs_per_file.push(kvs);
        out.size += size;
        out.dropped_size_per_file.push(dropped_size);
        Ok(())
    })();
    let _ = rd.close();
    result
}

/// Go `ReadKVFilesAsync`: the pairs of `files`, in order, from a reader
/// thread. The thread's result is its join handle's.
#[must_use]
pub fn read_kv_files_async(
    ctx: &Context,
    store: Arc<dyn Storage>,
    files: Vec<String>,
) -> (crossbeam_channel::Receiver<KVPair>, std::thread::JoinHandle<Result<()>>) {
    let (tx, rx) = crossbeam_channel::bounded(0);
    let ctx = ctx.clone();
    let handle = std::thread::spawn(move || {
        for file in &files {
            // Go `readOneKVFile2Ch`.
            let mut reader = KVReader::new(&ctx, file, store.as_ref(), 0, 3 * DEFAULT_READ_BUFFER_SIZE).map_err(crate::sst)?;
            let result = loop {
                let (key, value) = match reader.next_kv() {
                    Ok(kv) => kv,
                    Err(e) if e.is_eof() => break Ok(()),
                    Err(e) => break Err(crate::sst(e)),
                };
                if let Some(e) = ctx.err() {
                    break Err(crate::other(e.to_string()));
                }
                // The receiver is gone: the reader stops like a canceled Go one.
                if tx.send(KVPair { key, value }).is_err() {
                    break Ok(());
                }
            };
            let _ = reader.close();
            result?;
        }
        Ok(())
    });
    (rx, handle)
}
