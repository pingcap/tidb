// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `util.go`: overlap estimation, duplicate removal, the read offsets of
//! job keys, and listing a writer's files.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use tidb_gocontext::Context;
use tidb_objstore::storeapi::{Storage, WalkOption};

use crate::kv_reader::StatsReader;
use crate::writer::is_valid_partition;
use crate::Result;

/// Go `EndpointTp`: at equal keys, ends sort before starts before
/// inclusive ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum EndpointTp {
    /// Go `ExclusiveEnd`.
    ExclusiveEnd = 0,
    /// Go `InclusiveStart`.
    InclusiveStart = 1,
    /// Go `InclusiveEnd`.
    InclusiveEnd = 2,
}

/// Go `Endpoint`: one end of an interval, for [`get_max_overlapping`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    /// Go `Key`.
    pub key: Vec<u8>,
    /// Go `Tp`.
    pub tp: EndpointTp,
    /// Go `Weight`: positive for every type.
    pub weight: i64,
}

/// Go `GetMaxOverlapping`: the largest weight sum of intervals overlapping
/// at one point. Sorts `points`.
pub fn get_max_overlapping(points: &mut [Endpoint]) -> i64 {
    tidb_goruntime::go_pdqsort::go_sort_func_by(points, |i, j| {
        i.key.cmp(&j.key).then((i.tp as i64).cmp(&(j.tp as i64)))
    });
    let (mut max_weight, mut cur_weight) = (0, 0);
    for p in points.iter() {
        match p.tp {
            EndpointTp::InclusiveStart => cur_weight += p.weight,
            EndpointTp::ExclusiveEnd | EndpointTp::InclusiveEnd => cur_weight -= p.weight,
        }
        max_weight = max_weight.max(cur_weight);
    }
    max_weight
}

/// Go `RemoveDuplicates`: drops every pair of a key that appears more than
/// once in the sorted `input`. Returns the kept pairs, the removed ones when
/// `record_removed`, and the number of pairs with a duplicate key.
pub fn remove_duplicates<E: Clone>(
    input: Vec<E>,
    key_getter: impl Fn(&E) -> Vec<u8>,
    record_removed: bool,
) -> (Vec<E>, Vec<E>, usize) {
    do_remove_duplicates(input, key_getter, 0, record_removed)
}

/// Go `removeDuplicatesMoreThanTwo`: keeps the first 2 pairs of a duplicate
/// key and records the rest.
pub(crate) fn remove_duplicates_more_than_two<E: Clone>(
    input: Vec<E>,
    key_getter: impl Fn(&E) -> Vec<u8>,
) -> (Vec<E>, Vec<E>, usize) {
    do_remove_duplicates(input, key_getter, 2, true)
}

/// Go `doRemoveDuplicates`.
fn do_remove_duplicates<E: Clone>(
    mut input: Vec<E>,
    key_getter: impl Fn(&E) -> Vec<u8>,
    kept_dup_cnt: usize,
    record_removed: bool,
) -> (Vec<E>, Vec<E>, usize) {
    debug_assert!(
        kept_dup_cnt == 0 || kept_dup_cnt == 2,
        "keptDupCnt must be 0 or 2"
    );
    if input.len() <= 1 {
        return (input, Vec::new(), 0);
    }
    let (mut pivot_idx, mut fill_idx, mut total_dup) = (0, 0, 0);
    let mut pivot = key_getter(&input[pivot_idx]);
    let mut removed = Vec::new();
    for idx in 1..=input.len() {
        let mut key = Vec::new();
        if idx < input.len() {
            key = key_getter(&input[idx]);
            if pivot == key {
                continue;
            }
        }
        let dup_count = idx - pivot_idx;
        if dup_count >= 2 {
            total_dup += dup_count;
            // The removed pairs are copied before the kept ones move; the
            // kept ones only move to slots before `pivot_idx + kept`.
            if record_removed {
                removed.extend_from_slice(&input[pivot_idx + kept_dup_cnt..idx]);
            }
            for start_idx in pivot_idx..pivot_idx + kept_dup_cnt {
                if start_idx != fill_idx {
                    input[fill_idx] = input[start_idx].clone();
                }
                fill_idx += 1;
            }
        } else {
            if pivot_idx != fill_idx {
                input[fill_idx] = input[pivot_idx].clone();
            }
            fill_idx += 1;
        }
        pivot_idx = idx;
        pivot = key;
    }
    input.truncate(fill_idx);
    (input, removed, total_dup)
}

/// Go `getReadRangeFromPropsConcurrency`.
const GET_READ_RANGE_FROM_PROPS_CONCURRENCY: usize = 64;

/// Go `GetReadRangeFromProps`: for each job key and stat file, the offset of
/// the data file to start reading from so no key at or after the job key is
/// skipped. `job_keys` must be sorted.
///
/// # Errors
/// A stat file could not be read.
pub fn get_read_range_from_props(
    ctx: &Context,
    job_keys: &[Vec<u8>],
    paths: &[String],
    ex_storage: &dyn Storage,
) -> Result<Vec<Vec<u64>>> {
    let task =
        tidb_lightning_log::begin_logutil_task(tidb_logutil::bg_logger(), "seek props offsets");
    let result = read_range_from_props(ctx, job_keys, paths, ex_storage);
    let err = result.as_ref().err();
    task.end(
        tidb_log::Level::Error,
        err.map(|e| e as &(dyn std::error::Error + 'static)),
        &[],
    );
    result
}

fn read_range_from_props(
    ctx: &Context,
    starts: &[Vec<u8>],
    paths: &[String],
    ex_storage: &dyn Storage,
) -> Result<Vec<Vec<u64>>> {
    if starts.is_empty() {
        return Ok(Vec::new());
    }
    let columns: Vec<Mutex<Vec<u64>>> = paths
        .iter()
        .map(|_| Mutex::new(vec![0; starts.len()]))
        .collect();
    let next = AtomicUsize::new(0);
    let first_err: Mutex<Option<crate::Error>> = Mutex::new(None);
    let workers = GET_READ_RANGE_FROM_PROPS_CONCURRENCY.min(paths.len());
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::SeqCst);
                if i >= paths.len() || first_err.lock().expect("lock").is_some() {
                    return;
                }
                let mut column = columns[i].lock().expect("lock");
                if let Err(e) = read_one_stat_file(ctx, starts, &paths[i], ex_storage, &mut column)
                {
                    first_err.lock().expect("lock").get_or_insert(e);
                }
            });
        }
    });
    if let Some(e) = first_err.into_inner().expect("lock") {
        return Err(e);
    }
    let columns: Vec<Vec<u64>> = columns
        .into_iter()
        .map(|c| c.into_inner().expect("lock"))
        .collect();
    Ok((0..starts.len())
        .map(|k| columns.iter().map(|c| c[k]).collect())
        .collect())
}

/// Go `GetReadRangeFromProps`'s per-file goroutine; `offsets[k]` is job key
/// `k`'s offset in this file.
fn read_one_stat_file(
    ctx: &Context,
    starts: &[Vec<u8>],
    path: &str,
    ex_storage: &dyn Storage,
    offsets: &mut [u64],
) -> Result<()> {
    let mut r = match StatsReader::new(ctx, ex_storage, path, 250 * 1024) {
        Ok(r) => r,
        Err(e) if e.is_eof() => return Ok(()),
        Err(e) => return Err(e),
    };
    let result = (|| {
        let mut key_idx = 0;
        let mut cur_key = &starts[key_idx];
        let mut p = r.next_prop();
        loop {
            let prop = match p {
                Ok(prop) => prop,
                Err(e) if e.is_eof() => {
                    let off = offsets[key_idx];
                    for o in &mut offsets[key_idx + 1..] {
                        *o = off;
                    }
                    return Ok(());
                }
                Err(e) => return Err(e),
            };
            while prop.first_key.as_slice() > cur_key.as_slice() {
                key_idx += 1;
                if key_idx >= starts.len() {
                    return Ok(());
                }
                offsets[key_idx] = offsets[key_idx - 1];
                cur_key = &starts[key_idx];
            }
            offsets[key_idx] = prop.offset;
            p = r.next_prop();
        }
    })();
    let _ = r.close();
    result
}

/// Go `GetAllFileNames`: the sorted paths whose first directory, or the
/// second behind a partition prefix, is one of `non_partitioned_dirs`.
///
/// # Errors
/// The storage could not be listed.
pub fn get_all_file_names(
    ctx: &Context,
    store: &dyn Storage,
    non_partitioned_dirs: &[&str],
) -> Result<Vec<String>> {
    if non_partitioned_dirs.is_empty() {
        return Ok(Vec::new());
    }
    let dirs: HashSet<&str> = non_partitioned_dirs.iter().copied().collect();
    let mut data = Vec::new();
    store.walk_dir(ctx, Some(&WalkOption::default()), &mut |path, _| {
        let Some(first_idx) = path.find('/') else {
            return Ok(());
        };
        let first_dir = &path[..first_idx];
        if dirs.contains(first_dir) {
            data.push(path.to_owned());
            return Ok(());
        }
        if !is_valid_partition(first_dir.as_bytes()) {
            return Ok(());
        }
        let Some(second_idx) = path[first_idx + 1..].find('/') else {
            return Ok(());
        };
        if dirs.contains(&path[first_idx + 1..first_idx + 1 + second_idx]) {
            data.push(path.to_owned());
        }
        Ok(())
    })?;
    data.sort();
    Ok(data)
}
