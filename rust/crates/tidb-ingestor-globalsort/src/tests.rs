// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Checks against Go: `testdata/globalsort_go.jsonl` was printed by a Go
//! master probe (`zzprobe/e05gs`). It writes 3 overlapping writers' files,
//! then prints their merged meta, range splits with 4 limit sets, a merge
//! sort, and the external engine's loads of every split group, for each
//! `OnDuplicateKey`.
//!
//! Go sorts a loaded batch with an unstable parallel sort, so which
//! duplicate an error names, and the order of a key's recorded duplicates,
//! vary between Go runs. The test compares recorded duplicates as sorted
//! lists and checks that an error names a duplicated key.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tidb_dxf_execute::{Collector, SubtaskSummary};
use tidb_gocontext::Context;
use tidb_ingestor_engineapi::{Engine as _, OnDuplicateKey};
use tidb_ingestor_simplesst::{get_max_overlapping_total, KVReader, WriterBuilder, WriterSummary};
use tidb_objstore::storeapi::Storage;
use tidb_objstore::LocalStorage;

use super::*;

fn go_lines() -> Vec<Value> {
    include_str!("../testdata/globalsort_go.jsonl").lines().map(|l| serde_json::from_str(l).unwrap()).collect()
}

fn go_slice(v: Vec<Value>) -> Value {
    if v.is_empty() {
        Value::Null
    } else {
        Value::Array(v)
    }
}

fn strs(v: &[Vec<u8>]) -> Value {
    go_slice(v.iter().map(|k| Value::String(String::from_utf8_lossy(k).into_owned())).collect())
}

#[test]
fn pure_functions_match_go() {
    let lines = go_lines();
    for line in &lines {
        if let Some(d) = line.get("divide") {
            let (n, nodes, conc) = (d["Files"].as_u64().unwrap() as usize, d["Nodes"].as_u64().unwrap() as usize, d["Conc"].as_u64().unwrap() as usize);
            let files: Vec<String> = (0..n).map(|i| format!("f{i}")).collect();
            let (groups, err) = match divide_merge_sort_data_files(&files, nodes, conc) {
                Ok(g) => (go_slice(g.iter().map(|g| json!(g.len())).collect()), String::new()),
                Err(e) => (Value::Null, e.to_string()),
            };
            assert_eq!(json!({"Files": n, "Nodes": nodes, "Conc": conc, "Groups": groups, "Err": err}), *d);
        }
        if let Some(c) = line.get("cal_range_size") {
            let c: Vec<i64> = serde_json::from_value(c.clone()).unwrap();
            assert_eq!(cal_range_size(c[0], c[1], c[2]), (c[3], c[4]), "{c:?}");
        }
    }
}

fn write_group(ctx: &Context, store: &Arc<dyn Storage>) -> SortedKVMeta {
    let meta = Arc::new(Mutex::new(SortedKVMeta::default()));
    for w in 0..3 {
        let sink = Arc::clone(&meta);
        let mut writer = WriterBuilder::new()
            .set_prop_size_distance(64)
            .set_prop_keys_distance(3)
            .set_memory_size_limit(512)
            .set_block_size(256)
            .set_on_dup(OnDuplicateKey::Ignore)
            .set_on_close_func(Arc::new(move |s: &WriterSummary| sink.lock().unwrap().merge_summary(s)))
            .build(Arc::clone(store), "7/30", &format!("w{w}"));
        for i in 0..90 {
            let k = (i * 11 + w * 5) % 120;
            writer.write_row(ctx, format!("key-{k:05}").as_bytes(), format!("v-{w}-{i}").as_bytes()).unwrap();
        }
        writer.close(ctx).unwrap();
    }
    let meta = meta.lock().unwrap().clone();
    meta
}

fn read_file(ctx: &Context, store: &Arc<dyn Storage>, path: &str) -> Vec<(String, String)> {
    let mut r = KVReader::new(ctx, path, store.as_ref(), 0, 1024).unwrap();
    let mut out = Vec::new();
    while let Ok((k, v)) = r.next_kv() {
        out.push((String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap()));
    }
    r.close().unwrap();
    out
}

fn kvs_json(kvs: &[(String, String)]) -> Value {
    go_slice(kvs.iter().map(|(k, v)| json!({"K": k, "V": v})).collect())
}

fn split_all(ctx: &Context, store: &Arc<dyn Storage>, meta: &SortedKVMeta, a: [i64; 6]) -> Vec<SplitGroup> {
    let mut s = RangeSplitter::new(ctx, meta.stats().to_vec(), Arc::clone(store), a[0], a[1], a[2], a[3], a[4], a[5]).unwrap();
    let mut out = Vec::new();
    loop {
        let g = s.split_one_ranges_group().unwrap();
        let last = g.end_key_of_group.is_empty();
        out.push(g);
        if last {
            s.close().unwrap();
            return out;
        }
    }
}

fn group_json(g: &SplitGroup) -> Value {
    json!({
        "End": String::from_utf8_lossy(&g.end_key_of_group),
        "Data": go_slice(g.data_files.iter().map(|f| json!(f)).collect()),
        "Stat": go_slice(g.stat_files.iter().map(|f| json!(f)).collect()),
        "JobKeys": strs(&g.interior_range_job_keys),
        "SplitKeys": strs(&g.interior_region_split_keys),
    })
}

struct SummaryCollector(Arc<SubtaskSummary>);

impl Collector for SummaryCollector {
    fn accepted(&self, b: i64) {
        MergeCollector::new(Some(Arc::clone(&self.0))).accepted(b);
    }
    fn processed(&self, b: i64, r: i64) {
        MergeCollector::new(Some(Arc::clone(&self.0))).processed(b, r);
    }
}

/// Replaces the key and value an error names with `*`, after checking the
/// key is one of `dup_keys`.
fn normalize_dup_err(err: &str, dup_keys: &BTreeSet<String>) -> String {
    let hex_of = |s: &str| String::from_utf8(hex::decode(s).unwrap()).unwrap();
    if let Some(rest) = err.strip_prefix("duplicate key found: ") {
        assert!(dup_keys.contains(&hex_of(rest)), "{err}");
        return "duplicate key found: *".to_owned();
    }
    if let Some(rest) = err.strip_prefix("[Lightning:Restore:ErrFoundDuplicateKey]found duplicate key '") {
        let key = &rest[..rest.find('\'').unwrap()];
        assert!(dup_keys.contains(&hex_of(key)), "{err}");
        return "[Lightning:Restore:ErrFoundDuplicateKey]found duplicate key *".to_owned();
    }
    err.to_owned()
}

/// Go's recorded duplicates sorted, and errors normalized.
fn normalize(v: &mut Value, dup_keys: &BTreeSet<String>) {
    for load in v["loads"].as_array_mut().unwrap() {
        if let Some(Value::String(e)) = load.get("err") {
            load["err"] = json!(normalize_dup_err(e, dup_keys));
        }
        if let Some(Value::Array(kvs)) = load.get_mut("dup_kvs") {
            kvs.sort_by_key(|kv| (kv["K"].as_str().unwrap().to_owned(), kv["V"].as_str().unwrap().to_owned()));
        }
    }
}

fn run_sort_case(on_dup: OnDuplicateKey) -> Value {
    let ctx = Context::background();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Storage> = Arc::new(LocalStorage::new(dir.path().to_str().unwrap()).unwrap());
    let meta = write_group(&ctx, &store);
    let mut out = serde_json::Map::new();
    out.insert("on_dup".into(), json!(on_dup.to_string()));
    out.insert("meta".into(), serde_json::to_value(&meta).unwrap());
    let max = i64::MAX;
    let splits: Vec<Vec<SplitGroup>> = [
        [max, max, max, max, max, max],
        [300, max, 200, max, 400, max],
        [max, 40, max, 15, max, 25],
        [1000, 70, 100, 9, 350, 31],
    ]
    .into_iter()
    .map(|a| split_all(&ctx, &store, &meta, a))
    .collect();
    out.insert("splits".into(), json!(splits.iter().map(|s| s.iter().map(group_json).collect::<Vec<_>>()).collect::<Vec<_>>()));
    out.insert("max_overlap".into(), json!(get_max_overlapping_total(meta.stats())));

    let merged = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&merged);
    let summary = Arc::new(SubtaskSummary::default());
    let op = MergeOperator::new(
        Arc::clone(&store),
        64 << 20,
        "7/40",
        1024,
        Arc::new(move |s: &WriterSummary| sink.lock().unwrap().push(s.clone())),
        Some(Arc::new(SummaryCollector(Arc::clone(&summary)))),
        1,
        false,
        on_dup,
    );
    match merge_overlapping_files(&ctx, &meta.get_data_files(), &op) {
        Err(e) => {
            out.insert("merge_err".into(), json!(e.to_string()));
        }
        Ok(()) => {
            let mut list = Vec::new();
            for s in merged.lock().unwrap().iter() {
                let mut m = serde_json::Map::new();
                m.insert("min".into(), json!(String::from_utf8_lossy(&s.min)));
                m.insert("max".into(), json!(String::from_utf8_lossy(&s.max)));
                m.insert("size".into(), json!(s.total_size));
                m.insert("cnt".into(), json!(s.total_cnt));
                m.insert("dups".into(), json!(s.conflict_info.count));
                for st in &s.multiple_files_stats {
                    for f in &st.filenames {
                        m.insert("kvs".into(), kvs_json(&read_file(&ctx, &store, &f[0])));
                        m.insert("overlap".into(), json!(st.max_overlapping_num));
                    }
                }
                if let Some(f) = s.conflict_info.files.first() {
                    m.insert("dup_kvs".into(), kvs_json(&read_file(&ctx, &store, f)));
                }
                list.push(Value::Object(m));
            }
            out.insert("merged".into(), go_slice(list));
            out.insert(
                "merge_processed".into(),
                json!([
                    summary.processed.load(std::sync::atomic::Ordering::SeqCst),
                    summary.row_cnt.load(std::sync::atomic::Ordering::SeqCst)
                ]),
            );
        }
    }

    let groups = &splits[3];
    let mut loads = Vec::new();
    for (gi, g) in groups.iter().enumerate() {
        let start = if gi == 0 { meta.start_key.clone() } else { groups[gi - 1].end_key_of_group.clone() };
        let end = if g.end_key_of_group.is_empty() { meta.end_key.clone() } else { g.end_key_of_group.clone() };
        let mut job_keys = vec![start.clone()];
        job_keys.extend(g.interior_range_job_keys.iter().cloned());
        job_keys.push(end.clone());
        let mut eng = Engine::new(
            EngineConfig {
                ext_store: Arc::clone(&store),
                data_files: g.data_files.clone(),
                stat_files: g.stat_files.clone(),
                start_key: start,
                end_key: end,
                job_keys: job_keys.clone(),
                split_keys: job_keys,
                total_file_size: 1,
                total_kv_count: 0,
                check_hotspot: false,
                mem_capacity: 1 << 30,
                on_dup,
                file_prefix: format!("7/50/{gi}"),
            },
            2,
            42,
        );
        let mut batches = Vec::new();
        let result = eng.load_ingest_data(&ctx, &mut |d| {
            let ranges = go_slice(
                d.sorted_ranges
                    .iter()
                    .map(|r| json!(format!("{}..{}", String::from_utf8_lossy(&r.start), String::from_utf8_lossy(&r.end))))
                    .collect(),
            );
            let mut it = d.data.new_iter(&[], &[])?;
            let mut kvs = Vec::new();
            while let Some((k, v)) = it.next_pair()? {
                kvs.push((String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap()));
            }
            drop(it);
            let (f, l) = d.data.get_first_and_last_key(b"key-00030", b"key-00060")?.unwrap_or_default();
            batches.push(json!({
                "ranges": ranges,
                "kvs": kvs_json(&kvs),
                "ts": d.data.get_ts(),
                "first_last": [String::from_utf8_lossy(&f), String::from_utf8_lossy(&l)],
            }));
            d.data.finish(kvs.len() as i64, kvs.len() as i64);
            Ok(())
        });
        let mut load = serde_json::Map::new();
        load.insert("group".into(), json!(gi));
        if let Err(e) = result {
            load.insert("err".into(), json!(e.to_string()));
        }
        load.insert("batches".into(), go_slice(batches));
        let ci = eng.conflict_info();
        load.insert("conflict".into(), serde_json::to_value(&ci).unwrap());
        if let Some(f) = ci.files.first() {
            load.insert("dup_kvs".into(), kvs_json(&read_file(&ctx, &store, f)));
        }
        load.insert("loaded".into(), json!(eng.get_total_loaded_kvs_count()));
        loads.push(Value::Object(load));
        eng.close().unwrap();
    }
    out.insert("loads".into(), json!(loads));
    let mut count = 0;
    store.walk_dir(&ctx, None, &mut |_, _| {
        count += 1;
        Ok(())
    })
    .unwrap();
    out.insert("file_count".into(), json!(count));
    clean_up_files(&ctx, store.as_ref(), &["7"]).unwrap();
    let mut left = Vec::new();
    store.walk_dir(&ctx, None, &mut |p, _| {
        left.push(json!(p));
        Ok(())
    })
    .unwrap();
    out.insert("left_after_cleanup".into(), go_slice(left));
    Value::Object(out)
}

#[test]
fn sort_merge_split_and_engine_match_go() {
    let want: Vec<Value> = go_lines().into_iter().filter(|l| l.get("on_dup").is_some()).collect();
    // Every key written more than once: keys `k` with 2 or 3 of the 270
    // `(i * 11 + w * 5) % 120` values.
    let mut seen = std::collections::BTreeMap::<String, usize>::new();
    for w in 0..3 {
        for i in 0..90 {
            *seen.entry(format!("key-{:05}", (i * 11 + w * 5) % 120)).or_default() += 1;
        }
    }
    let dup_keys: BTreeSet<String> = seen.into_iter().filter(|(_, n)| *n > 1).map(|(k, _)| k).collect();
    let on_dups = [OnDuplicateKey::Ignore, OnDuplicateKey::Record, OnDuplicateKey::Remove, OnDuplicateKey::Error];
    assert_eq!(want.len(), on_dups.len());
    for (on_dup, want) in on_dups.into_iter().zip(want) {
        let (mut got, mut want) = (run_sort_case(on_dup), want);
        normalize(&mut got, &dup_keys);
        normalize(&mut want, &dup_keys);
        for key in want.as_object().unwrap().keys() {
            assert_eq!(got[key], want[key], "on_dup {on_dup} key {key}");
        }
        assert_eq!(got, want, "on_dup {on_dup}");
    }
}

#[test]
fn sorted_kv_meta_merge_keeps_go_nil_slices() {
    let empty = SortedKVMeta::default();
    assert_eq!(
        serde_json::to_string(&empty).unwrap(),
        r#"{"start-key":null,"end-key":null,"total-kv-size":0,"total-kv-cnt":0,"multiple-files-stats":null,"conflict-info":{}}"#
    );
    let mut m = SortedKVMeta::default();
    m.merge(&empty);
    assert_eq!(m, empty);
    assert_eq!(subtask_meta_path(3, 9), "3/9/meta.json");
    assert_eq!(plan_meta_path(3, "merge-sort", 1), "3/plan/merge-sort/1/meta.json");
    assert_eq!(kv_group_2_index_id("12").unwrap(), 12);
    assert!(kv_group_2_index_id("data").is_err());
}
