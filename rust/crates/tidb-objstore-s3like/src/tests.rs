//! `S3BackendOptions.Apply` against Go master.
//!
//! `testdata/sse_kms_go.jsonl` was printed by an uncommitted probe in a Go
//! master worktree (`GOTOOLCHAIN=go1.27.3 go test ./zzprobe/e07ssekms`) that
//! ran `S3BackendOptions.Apply` over SSE-KMS contexts (escapes, surrogates,
//! HTML-escaped characters, duplicates, non-string values, malformed JSON)
//! with and without `sse=aws:kms` and an S3-compatible provider, and over
//! endpoints.

use super::*;
use serde_json::Value;

#[test]
fn apply_matches_go_capture() {
    let mut n = 0;
    for line in include_str!("../testdata/sse_kms_go.jsonl").lines() {
        let rec: Value = serde_json::from_str(line).unwrap();
        let text = |k: &str| rec[k].as_str().unwrap_or_default().to_owned();
        let mut opts = S3BackendOptions::default();
        let (field, want): (fn(&S3) -> &str, _) = if rec.get("endpoint").is_some() {
            opts.endpoint = text("endpoint");
            (|s| &s.endpoint, text("out"))
        } else {
            opts.sse_kms_context = text("in");
            opts.provider = text("provider");
            opts.sse = text("sse");
            (|s| &s.sse_kms_encryption_context, text("out"))
        };
        let mut s3 = S3::default();
        let result = opts.apply(&mut s3);
        match rec.get("err").and_then(Value::as_str) {
            Some(err) => assert_eq!(result.unwrap_err(), err, "{line}"),
            None => {
                assert!(result.is_ok(), "{line}: {result:?}");
                assert_eq!(field(&s3), want, "{line}");
            }
        }
        n += 1;
    }
    assert_eq!(n, 129);
}
