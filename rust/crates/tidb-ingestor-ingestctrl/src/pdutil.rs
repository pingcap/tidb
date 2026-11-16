// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `br/pkg/pdutil`'s `PauseSchedulersByKeyRange`: a `schedule=deny`
//! region label on a key range, refreshed until the caller is done and then
//! deleted.

use std::sync::Arc;
use std::time::Duration;

use tidb_gocontext::Context;
use tikv_client::pd_http::{LabelRule, LabelRulePatch, RegionLabel};

use crate::region_job::ImportClient;

/// Go `pauseTimeout`.
const PAUSE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Go `KeyRangeRule`.
#[derive(serde::Serialize)]
struct KeyRangeRule {
    #[serde(rename = "start_key")]
    start_key_hex: String,
    #[serde(rename = "end_key")]
    end_key_hex: String,
}

/// Go `time.Duration.String()` for whole seconds, or whole milliseconds
/// under a second.
fn go_duration_string(d: Duration) -> String {
    let secs = d.as_secs();
    if secs == 0 {
        let ms = d.as_millis();
        return if ms == 0 { "0s".to_owned() } else { format!("{ms}ms") };
    }
    let (h, m, s) = (secs / 3600, secs % 3600 / 60, secs % 60);
    if h > 0 {
        format!("{h}h{m}m{s}s")
    } else if m > 0 {
        format!("{m}m{s}s")
    } else {
        format!("{s}s")
    }
}

/// Go `pauseSchedulerByKeyRangeWithTTL`'s rule for one key range.
fn key_range_rule(id: String, start_key: &[u8], end_key: &[u8], ttl: Duration) -> LabelRule {
    LabelRule {
        id,
        index: 0,
        labels: vec![RegionLabel {
            key: "schedule".to_owned(),
            value: "deny".to_owned(),
            ttl: go_duration_string(ttl),
            start_at: String::new(),
        }],
        rule_type: "key-range".to_owned(),
        data: serde_json::to_value(vec![KeyRangeRule {
            start_key_hex: hex::encode(start_key),
            end_key_hex: hex::encode(end_key),
        }])
        .unwrap_or_default(),
    }
}

/// Go `PauseSchedulersByKeyRange`: pauses PD's schedulers on
/// `[start_key, end_key)` (memcomparable-encoded keys) until `ctx` is done.
/// The returned thread, when there is one, finishes after the rule is
/// deleted; the caller cancels `ctx` and joins it (Go `<-done`). An empty
/// start key pauses nothing.
///
/// # Errors
/// PD refused the first label rule.
pub fn pause_schedulers_by_key_range(
    ctx: &Context,
    client: Arc<dyn ImportClient>,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
) -> crate::Result<Option<std::thread::JoinHandle<()>>> {
    let done = pause_scheduler_by_key_range_with_ttl(ctx, client, &start_key, &end_key, PAUSE_TIMEOUT);
    // Go waits for the rule to take effect, since PD processes operators
    // asynchronously (pingcap/tidb#49477).
    std::thread::sleep(Duration::from_millis(20));
    done
}

/// Go `pauseSchedulerByKeyRangeWithTTL`.
fn pause_scheduler_by_key_range_with_ttl(
    ctx: &Context,
    client: Arc<dyn ImportClient>,
    start_key: &[u8],
    end_key: &[u8],
    ttl: Duration,
) -> crate::Result<Option<std::thread::JoinHandle<()>>> {
    // Go: no table to restore, return an empty rule.
    if start_key.is_empty() {
        return Ok(None);
    }
    let rule = key_range_rule(uuid::Uuid::new_v4().to_string(), start_key, end_key, ttl);
    client.set_region_label_rule(&rule)?;
    let ctx = ctx.clone();
    Ok(Some(std::thread::spawn(move || {
        while !ctx.sleep(ttl / 3) {
            if let Err(e) = client.set_region_label_rule(&rule) {
                tidb_log::warn(
                    "pause scheduler by key range failed, ignore it and wait next time pause",
                    &[tidb_log::Field::new("error", tidb_log::Value::Str(e.to_string()))],
                );
            }
        }
        let patch = LabelRulePatch { set_rules: Vec::new(), delete_rules: vec![rule.id.clone()] };
        if let Err(e) = client.patch_region_label_rules(&patch) {
            tidb_log::warn(
                "failed to delete region label rule, the rule will be removed after ttl expires",
                &[
                    tidb_log::Field::new("rule-id", tidb_log::Value::Str(rule.id.clone())),
                    tidb_log::Field::new("error", tidb_log::Value::Str(e.to_string())),
                ],
            );
        }
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tidb_proto::import_sstpb as sst;

    #[derive(Default)]
    struct Pd {
        calls: Mutex<Vec<String>>,
    }

    impl ImportClient for Pd {
        fn get_region(&self, _: &[u8]) -> crate::Result<crate::region_job::RegionInfo> {
            unreachable!()
        }
        fn invalidate_region(&self, _: &crate::region_job::RegionInfo) {}
        fn write(&self, _: u64, _: Vec<sst::WriteRequest>) -> crate::Result<sst::WriteResponse> {
            unreachable!()
        }
        fn multi_ingest(&self, _: u64, _: sst::MultiIngestRequest) -> crate::Result<sst::IngestResponse> {
            unreachable!()
        }
        fn ingest(&self, _: u64, _: sst::IngestRequest) -> crate::Result<sst::IngestResponse> {
            unreachable!()
        }
        fn duplicate_detect(
            &self,
            _: u64,
            _: sst::DuplicateDetectRequest,
            _: &mut (dyn FnMut(sst::DuplicateDetectResponse) -> crate::Result<()> + Send),
        ) -> crate::Result<()> {
            unreachable!()
        }
        fn api_version(&self) -> i32 {
            0
        }
        fn encode_key(&self, key: &[u8]) -> Vec<u8> {
            key.to_vec()
        }
        fn set_region_label_rule(&self, rule: &LabelRule) -> crate::Result<()> {
            self.calls.lock().unwrap().push(format!("set {}", serde_json::to_string(rule).unwrap().replace(&rule.id, "ID")));
            Ok(())
        }
        fn patch_region_label_rules(&self, patch: &LabelRulePatch) -> crate::Result<()> {
            self.calls.lock().unwrap().push(format!("delete {}", patch.delete_rules.len()));
            Ok(())
        }
    }

    /// Go `pauseSchedulerByKeyRangeWithTTL`: the rule PD gets (Go's
    /// `pdhttp.LabelRule` JSON), its refresh every TTL/3, and its deletion
    /// once the caller is done.
    #[test]
    fn pause_sets_refreshes_and_deletes_the_rule() {
        let pd = Arc::new(Pd::default());
        let (ctx, cancel) = Context::with_cancel(&Context::background());
        let done = pause_scheduler_by_key_range_with_ttl(&ctx, pd.clone(), b"\x01a", b"\x01b", Duration::from_millis(300))
            .unwrap()
            .unwrap();
        std::thread::sleep(Duration::from_millis(250));
        cancel.cancel();
        done.join().unwrap();
        let calls = pd.calls.lock().unwrap().clone();
        // `data` is a `serde_json::Value`, whose object keys serialize
        // sorted; Go writes `start_key` first. PD reads it as an object.
        let set = r#"set {"id":"ID","index":0,"labels":[{"key":"schedule","value":"deny"}],"rule_type":"key-range","data":[{"end_key":"0162","start_key":"0161"}]}"#;
        assert!(calls.len() >= 3, "{calls:?}");
        assert!(calls[..calls.len() - 1].iter().all(|c| c == &set.replace(r#""value":"deny""#, r#""value":"deny","ttl":"300ms""#)), "{calls:?}");
        assert_eq!(calls.last().unwrap(), "delete 1");
        assert!(pause_scheduler_by_key_range_with_ttl(&ctx, pd, b"", b"", PAUSE_TIMEOUT).unwrap().is_none());
    }

    /// Go `time.Duration.String()`.
    #[test]
    fn go_duration_strings() {
        assert_eq!(go_duration_string(PAUSE_TIMEOUT), "5m0s");
        assert_eq!(go_duration_string(Duration::from_secs(3661)), "1h1m1s");
        assert_eq!(go_duration_string(Duration::from_secs(7)), "7s");
        assert_eq!(go_duration_string(Duration::from_millis(300)), "300ms");
        assert_eq!(go_duration_string(Duration::ZERO), "0s");
    }
}
