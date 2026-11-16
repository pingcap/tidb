// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! BR's retry strategies and error handling against a Go master capture:
//! each strategy's delays and remaining attempts over each error kind.

use std::sync::Arc;
use std::time::Duration;

use tidb_proto::backup;

use super::backoff::*;
use super::error_handling::*;
use super::retry::*;
use crate::errors;
use crate::test_util::go_records;

fn error(name: &str) -> errors::Error {
    match name {
        "epoch" => errors::annotate(errors::new(&errors::ERR_KV_EPOCH_NOT_MATCH), "x"),
        "unavail" => errors::grpc(tonic::Status::unavailable("down")),
        "grpc_cancel" => errors::grpc(tonic::Status::cancelled("grpc canceled")),
        "ctx_cancel" => Arc::new(tidb_gocontext::CtxError::Canceled),
        "deadline" => Arc::new(tidb_gocontext::CtxError::DeadlineExceeded),
        "plain" => errors::errorf("foo"),
        "refused" => errors::errorf("dial: connection refused"),
        "eof" => Arc::new(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "EOF")),
        "range_empty" => errors::new(&errors::ERR_KV_RANGE_IS_EMPTY),
        "disk_full" => errors::annotate(errors::new(&errors::ERR_KV_DISK_FULL), "y"),
        "pd_invalid" => errors::new(&errors::ERR_PD_INVALID_RESPONSE),
        "notfound_io" => errors::errorf("Io(Os { code: 2, kind: NotFound })"),
        "total_kv" => errors::new(&errors::ERR_RESTORE_TOTAL_KV_MISMATCH),
        other => panic!("{other}"),
    }
}

fn is_eof() -> ErrFn {
    Arc::new(|e| Sentinel::IoEof.matches(e))
}

fn strategy(name: &str) -> Box<dyn BackoffStrategy> {
    let s = Duration::from_secs(1);
    match name {
        "import" => Box::new(new_import_sst_backoff_strategy()),
        "download" => Box::new(new_download_sst_backoff_strategy()),
        "peer_download" => Box::new(new_peer_download_sst_backoff_strategy()),
        "backup" => Box::new(new_backup_sst_backoff_strategy()),
        "pd" => Box::new(new_aggressive_pd_backoff_strategy()),
        "disk" => Box::new(new_disk_check_backoff_strategy()),
        "checksum" => Box::new(new_checksum_backoff_strategy()),
        "flashback" => Box::new(new_flashback_backoff_strategy()),
        "raw" => Box::new(new_raw_client_backoff_strategy()),
        "all" => Box::new(new_backoff_retry_all_error_strategy(3, s, 3 * s)),
        "except_eof" => Box::new(new_backoff_retry_all_except_strategy(3, s, 3 * s, is_eof())),
        "state" => Box::new(RetryState::new(4, Duration::from_millis(10), Duration::from_millis(50))),
        "giveup_eof" => {
            Box::new(give_up_retry_on(new_backoff_retry_all_error_strategy(3, s, 3 * s), vec![Sentinel::IoEof]))
        }
        other => panic!("{other}"),
    }
}

/// Go's `BackoffStrategy` implementations, step by step.
#[test]
fn backoff_steps_match_go() {
    let records = go_records("backoff");
    assert_eq!(records.len(), 169);
    for r in records {
        let name = r["strategy"].as_str().unwrap();
        let err = error(r["err"].as_str().unwrap());
        assert_eq!(strategy(name).remaining_attempts(), r["initial"].as_i64().unwrap() as i32, "{r}");
        let mut bo = strategy(name);
        let mut steps = Vec::new();
        while steps.len() < 6 && bo.remaining_attempts() > 0 {
            let d = bo.next_backoff(&err);
            steps.push(format!("{}/{}", d.as_millis(), bo.remaining_attempts()));
        }
        let want: Vec<String> = r["steps"].as_array().unwrap().iter().map(|s| s.as_str().unwrap().to_owned()).collect();
        assert_eq!(steps, want, "{r}");
    }
}

/// Go `WithRetry`: every attempt's error, joined by multierr.
#[test]
fn with_retry_joins_errors() {
    let r = &go_records("with_retry")[0];
    let mut bo = new_backoff_retry_all_error_strategy(3, Duration::from_millis(1), Duration::from_millis(1));
    let err = with_retry(&tidb_gocontext::Context::background(), || Err(errors::errorf("boom")), &mut bo).unwrap_err();
    assert_eq!(err.to_string(), r["err"].as_str().unwrap());
    assert_eq!(new_aggressive_pd_backoff_strategy().remaining_attempts(), r["remaining"].as_i64().unwrap() as i32);
}

/// Go `HandleBackupError`, one error context through every case.
#[test]
fn handle_backup_error_matches_go() {
    let ec = ErrorContext::new("test", 2);
    let msg = |m: &str| Some(backup::Error { msg: m.into(), detail: None });
    let cases = vec![
        None,
        Some(backup::Error { msg: String::new(), detail: Some(backup::error::Detail::KvError(Default::default())) }),
        Some(backup::Error { msg: String::new(), detail: Some(backup::error::Detail::RegionError(Default::default())) }),
        Some(backup::Error { msg: String::new(), detail: Some(backup::error::Detail::ClusterIdError(Default::default())) }),
        msg("Io(Os { code: 2, kind: NotFound, message: \"No such file\" })"),
        msg("failed: PermissionDenied"),
        msg("credential info not found"),
        msg("Context Canceled"),
        msg("S3 put object timeout"),
        msg("weird"),
        msg("weird"),
        msg("weird"),
        Some(backup::Error::default()),
    ];
    let records = go_records("handle");
    assert_eq!(records.len(), cases.len());
    for (c, r) in cases.iter().zip(records) {
        let res = handle_backup_error(c.as_ref(), 7, &ec);
        let strategy = match res.strategy {
            ErrorHandlingStrategy::Retry => 0,
            ErrorHandlingStrategy::GiveUp => 1,
            ErrorHandlingStrategy::Unknown => 2,
        };
        assert_eq!(strategy, r["strategy"].as_i64().unwrap(), "{r}");
        assert_eq!(res.reason, r["reason"].as_str().unwrap(), "{r}");
    }
}
