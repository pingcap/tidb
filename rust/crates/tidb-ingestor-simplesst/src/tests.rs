// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Byte-for-byte checks against Go: `testdata/sst_go.jsonl` was written by
//! a Go master probe that ran simplesst's `Writer` and `OneFileWriter`
//! over a local store, then read the files back with `NewMergeKVIter`,
//! `NewMergePropIter`, `GetReadRangeFromProps` and `GetAllFileNames`.

use std::sync::{Arc, Mutex};

use base64::Engine;
use serde_json::{json, Value};
use tidb_gocontext::Context;
use tidb_ingestor_engineapi::OnDuplicateKey;
use tidb_objstore::storeapi::Storage;
use tidb_objstore::LocalStorage;

use super::*;

/// The probe's `kvs`: keys written out of order, and with `dup_every`,
/// 1 to 3 duplicates of every key divisible by it.
fn kvs(n: usize, dup_every: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut ret = Vec::new();
    for i in 0..n {
        let k = (i * 7) % n;
        let key = format!("key-{k:05}").into_bytes();
        ret.push((
            key.clone(),
            format!("value-{i}-{}", "v".repeat(i % 13)).into_bytes(),
        ));
        if dup_every > 0 && k.is_multiple_of(dup_every) {
            for j in 0..1 + k % 3 {
                ret.push((key.clone(), format!("dup-{i}-{j}").into_bytes()));
            }
        }
    }
    ret
}

/// Go encoding/json of a `[]byte`.
fn go_bytes(b: &[u8]) -> Value {
    if b.is_empty() {
        Value::Null
    } else {
        Value::String(base64::engine::general_purpose::STANDARD.encode(b))
    }
}

/// Go encoding/json of a slice: `null` when empty (the probe's slices are
/// nil until appended to).
fn go_slice(v: Vec<Value>) -> Value {
    if v.is_empty() {
        Value::Null
    } else {
        Value::Array(v)
    }
}

/// Go encoding/json of `*WriterSummary`, whose fields have no tags.
fn summary_json(s: &WriterSummary) -> Value {
    json!({
        "WriterID": s.writer_id,
        "GroupOffset": s.group_offset,
        "Seq": s.seq,
        "Min": go_bytes(&s.min),
        "Max": go_bytes(&s.max),
        "TotalSize": s.total_size,
        "TotalCnt": s.total_cnt,
        "KVFileCount": s.kv_file_count,
        "MultipleFilesStats": serde_json::to_value(&s.multiple_files_stats).unwrap(),
        "ConflictInfo": serde_json::to_value(&s.conflict_info).unwrap(),
    })
}

fn dump(ctx: &Context, store: &dyn Storage) -> Value {
    let mut files = Vec::new();
    store
        .walk_dir(ctx, None, &mut |path, _| {
            files.push((path.to_owned(), hex::encode(store.read_file(ctx, path)?)));
            Ok(())
        })
        .unwrap();
    files.sort();
    go_slice(
        files
            .into_iter()
            .map(|(path, hex)| json!({"path": path, "hex": hex}))
            .collect(),
    )
}

/// The probe's `readBack`.
fn read_back(
    ctx: &Context,
    store: &Arc<dyn Storage>,
    summaries: &[WriterSummary],
    o: &mut serde_json::Map<String, Value>,
) {
    let mut stats = Vec::new();
    let (mut data_files, mut stat_files) = (Vec::new(), Vec::new());
    for s in summaries {
        for m in &s.multiple_files_stats {
            stats.push(m.clone());
            for f in &m.filenames {
                data_files.push(f[0].clone());
                stat_files.push(f[1].clone());
            }
        }
    }
    o.insert(
        "max_overlap".into(),
        json!(get_max_overlapping_total(&stats)),
    );
    let names = get_all_file_names(ctx, store.as_ref(), &["30001"]).unwrap();
    o.insert(
        "all_file_names".into(),
        go_slice(names.into_iter().map(Value::String).collect()),
    );
    if data_files.is_empty() {
        return;
    }
    let mut it = MergeKVIter::new(
        ctx,
        &data_files,
        &vec![0; data_files.len()],
        Arc::clone(store),
        64,
        false,
        0,
    )
    .unwrap();
    let mut merged = Vec::new();
    while it.next() {
        merged.push(json!([
            String::from_utf8_lossy(it.key()),
            String::from_utf8_lossy(it.value())
        ]));
    }
    assert!(it.error().is_none(), "{:?}", it.error());
    it.close().unwrap();
    o.insert("merged_kvs".into(), go_slice(merged));

    let mut pit = MergePropIter::new(ctx, &mut stats, Arc::clone(store)).unwrap();
    let mut props = Vec::new();
    while pit.next() {
        let p = pit.curr_property();
        let (a, b) = pit.reader_index();
        props.push(Value::String(format!(
            "{}|{}|{}|{}|{}|{a}|{b}",
            String::from_utf8_lossy(&p.first_key),
            String::from_utf8_lossy(&p.last_key),
            p.offset,
            p.size,
            p.keys
        )));
    }
    assert!(pit.error().is_none(), "{:?}", pit.error());
    pit.close().unwrap();
    o.insert("merged_props".into(), go_slice(props));

    let keys: Vec<Vec<u8>> = ["key-00000", "key-00010", "key-00031", "key-00057", "zzz"]
        .iter()
        .map(|k| k.as_bytes().to_vec())
        .collect();
    let ranges = get_read_range_from_props(ctx, &keys, &stat_files, store.as_ref()).unwrap();
    o.insert("read_ranges".into(), json!(ranges));
}

fn run_case(one_file: bool, on_dup: OnDuplicateKey, n: usize) -> Value {
    let ctx = Context::background();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Storage> =
        Arc::new(LocalStorage::new(dir.path().to_str().unwrap()).unwrap());
    let summaries = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&summaries);
    let b = WriterBuilder::new()
        .set_prop_size_distance(100)
        .set_prop_keys_distance(4)
        .set_memory_size_limit(600)
        .set_block_size(300)
        .set_on_dup(on_dup)
        .set_on_close_func(Arc::new(move |s: &WriterSummary| {
            sink.lock().unwrap().push(s.clone())
        }));
    let result = if one_file {
        let mut w = b.build_one_file(Arc::clone(&store), "30001/7", "w1");
        w.init_part_size_and_logger(&ctx, 5 << 20);
        kvs(n, 5)
            .iter()
            .try_for_each(|(k, v)| w.write_row(&ctx, k, v))
            .and_then(|()| w.close(&ctx))
    } else {
        let mut w = b.build(Arc::clone(&store), "30001/7", "w1");
        kvs(n, 5)
            .iter()
            .try_for_each(|(k, v)| w.write_row(&ctx, k, v))
            .and_then(|()| w.close(&ctx))
    };
    let summaries = summaries.lock().unwrap().clone();
    let mut o = serde_json::Map::new();
    o.insert(
        "case".into(),
        json!(format!("onefile={one_file}/ondup={on_dup}/n={n}")),
    );
    o.insert("files".into(), dump(&ctx, store.as_ref()));
    o.insert(
        "summaries".into(),
        go_slice(summaries.iter().map(summary_json).collect()),
    );
    if let Err(e) = &result {
        o.insert("err".into(), json!(e.to_string()));
    }
    for key in [
        "merged_kvs",
        "merged_props",
        "read_ranges",
        "all_file_names",
    ] {
        o.insert(key.into(), Value::Null);
    }
    o.insert("max_overlap".into(), json!(0));
    if result.is_ok() {
        read_back(&ctx, &store, &summaries, &mut o);
    }
    Value::Object(o)
}

#[test]
fn writers_and_readers_match_go() {
    let expected: Vec<Value> = include_str!("../testdata/sst_go.jsonl")
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let mut cases = Vec::new();
    for one_file in [false, true] {
        for on_dup in [
            OnDuplicateKey::Ignore,
            OnDuplicateKey::Record,
            OnDuplicateKey::Remove,
            OnDuplicateKey::Error,
        ] {
            for n in [0, 1, 60] {
                cases.push(run_case(one_file, on_dup, n));
            }
        }
    }
    assert_eq!(cases.len(), expected.len());
    for (got, want) in cases.iter().zip(&expected) {
        assert_eq!(got, want, "case {}", want["case"]);
    }
}

#[test]
fn remove_duplicates_keeps_order() {
    let key = |v: &(u8, u8)| vec![v.0];
    let input = vec![(1, 0), (2, 0), (2, 1), (2, 2), (3, 0), (4, 0), (4, 1)];
    assert_eq!(
        remove_duplicates(input.clone(), key, true),
        (
            vec![(1, 0), (3, 0)],
            input[1..4].iter().chain(&input[5..]).copied().collect(),
            5
        )
    );
    assert_eq!(
        util::remove_duplicates_more_than_two(input.clone(), key),
        (
            vec![(1, 0), (2, 0), (2, 1), (3, 0), (4, 0), (4, 1)],
            vec![(2, 2)],
            5
        )
    );
}

#[test]
fn partitions() {
    assert!(is_valid_partition(b"p01010101"));
    assert!(!is_valid_partition(b"p0101010"));
    assert!(!is_valid_partition(b"q01010101"));
    assert!(!is_valid_partition(b"p0101010a"));
}
