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

//! Go-derived regressions for the materialized-view session-variable drift in
//! `pkg/sessionctx/{vardef,variable}` (Go master `94a9cbedab`): the new
//! `tidb_mview_*` sysvars, the shared `normalizeIsolationReadEnginesValue`
//! validation, and the `MViewExecutionSessionVars` capture/apply/restore
//! machinery.

use super::vars::{
    apply_m_view_execution_session_vars_with_config, capture_applied_m_view_execution_session_vars,
    capture_m_view_execution_session_vars, get_isolation_read_engines_string,
    MViewExecutionSessionVars, MViewExecutionSessionVarsApplyConfig, SessionVars, VarError,
};

/// Go `sysvar_test.go::TestTiDBMViewEnable`: the variable registers with Go's
/// default and round-trips ON/OFF through the session SET path. Go reads the
/// typed `EnableMView` field; the Rust carrier reads the validated text.
#[test]
fn mview_enable_roundtrip() {
    let mut vars = SessionVars::new();
    assert_eq!(
        vars.get_system("tidb_mview_enable").expect("registered"),
        "OFF",
        "Go `DefTiDBMViewEnable` is false"
    );
    vars.set_system("tidb_mview_enable", "on".to_owned())
        .expect("set on");
    assert_eq!(vars.get_system("tidb_mview_enable").unwrap(), "ON");
    vars.set_system("tidb_mview_enable", "off".to_owned())
        .expect("set off");
    assert_eq!(vars.get_system("tidb_mview_enable").unwrap(), "OFF");
}

/// Go's `tidb_mview_maintain_mem_quota` validation: a positive value below
/// 128 clamps to 128 with the truncated-value warning; other values pass.
#[test]
fn mview_maintain_mem_quota_clamps_below_128() {
    let mut vars = SessionVars::new();
    let truncated = vars
        .set_system("tidb_mview_maintain_mem_quota", "64".to_owned())
        .expect("64 validates with a warning");
    assert!(truncated, "Go appends ErrTruncatedWrongValue");
    assert_eq!(
        vars.get_system("tidb_mview_maintain_mem_quota").unwrap(),
        "128"
    );

    let truncated = vars
        .set_system("tidb_mview_maintain_mem_quota", "200".to_owned())
        .expect("200 is in range");
    assert!(!truncated);
    assert_eq!(
        vars.get_system("tidb_mview_maintain_mem_quota").unwrap(),
        "200"
    );

    // The minimum is -1 (Go `MinValue: -1`), so -1 is accepted un-clamped.
    vars.set_system("tidb_mview_maintain_mem_quota", "-1".to_owned())
        .expect("-1 is the auto sentinel");
    assert_eq!(
        vars.get_system("tidb_mview_maintain_mem_quota").unwrap(),
        "-1"
    );
}

/// Go's shared `normalizeIsolationReadEnginesValue`: engines trim and
/// canonicalize case-insensitively; an empty or unknown engine refuses the
/// SET with `ErrWrongValueForVar`, on BOTH the existing
/// `tidb_isolation_read_engines` and the new
/// `tidb_mview_maintain_isolation_read_engines`.
#[test]
fn isolation_read_engines_normalize_is_shared() {
    let mut vars = SessionVars::new();
    for name in [
        "tidb_isolation_read_engines",
        "tidb_mview_maintain_isolation_read_engines",
    ] {
        vars.set_system(name, " TIKV , TiFlash ".to_owned())
            .expect("case-insensitive engines canonicalize");
        assert_eq!(vars.get_system(name).unwrap(), "tikv,tiflash");

        let error = vars
            .set_system(name, "tikv,,tidb".to_owned())
            .expect_err("an empty engine refuses the SET");
        assert!(
            matches!(error, VarError::WrongValueForVar(_, _)),
            "Go returns ErrWrongValueForVar, got {error:?}"
        );

        let error = vars
            .set_system(name, "tikv,foo".to_owned())
            .expect_err("an unknown engine refuses the SET");
        assert!(matches!(error, VarError::WrongValueForVar(_, _)));
    }
}

/// Go's `tidb_mview_maintain_import_disk_quota` validation: empty passes;
/// a positive go-units size passes; zero or unparsable refuses.
#[test]
fn mview_maintain_import_disk_quota_validation() {
    let mut vars = SessionVars::new();
    vars.set_system("tidb_mview_maintain_import_disk_quota", String::new())
        .expect("empty disables the quota");
    vars.set_system("tidb_mview_maintain_import_disk_quota", "10GB".to_owned())
        .expect("10GB is a positive size");
    assert_eq!(
        vars.get_system("tidb_mview_maintain_import_disk_quota")
            .unwrap(),
        "10GB"
    );
    assert!(vars
        .set_system("tidb_mview_maintain_import_disk_quota", "abc".to_owned())
        .is_err());
    assert!(vars
        .set_system("tidb_mview_maintain_import_disk_quota", "0KB".to_owned())
        .is_err());
}

/// Go's `tidb_mview_maintain_import_threads` bounds: [0, 256].
#[test]
fn mview_maintain_import_threads_bounds() {
    let mut vars = SessionVars::new();
    vars.set_system("tidb_mview_maintain_import_threads", "8".to_owned())
        .expect("8 is in range");
    assert_eq!(
        vars.get_system("tidb_mview_maintain_import_threads")
            .unwrap(),
        "8"
    );
    let truncated = vars
        .set_system("tidb_mview_maintain_import_threads", "300".to_owned())
        .expect("300 clamps to the 256 maximum");
    assert!(truncated);
    assert_eq!(
        vars.get_system("tidb_mview_maintain_import_threads")
            .unwrap(),
        "256"
    );
}

/// Go `CaptureMViewExecutionSessionVars` on a fresh session: every knob at
/// its registry default, and `GetIsolationReadEnginesString` falls back to
/// the catalog default when the session never loaded the variable.
#[test]
fn capture_on_fresh_session_reads_defaults() {
    let vars = SessionVars::new();
    let captured = capture_m_view_execution_session_vars(&vars);
    assert_eq!(captured.maintain_mem_quota, 2 * 1024 * 1024 * 1024);
    assert_eq!(captured.isolation_read_engines, "tikv,tiflash,tidb");
    assert_eq!(captured.import_threads, 0);
    assert_eq!(captured.import_disk_quota, "");
    assert_eq!(
        get_isolation_read_engines_string(&vars),
        "tikv,tiflash,tidb"
    );
}

/// Go `AddMViewExecutionSessionVarsToJob`'s capture side: the image carries
/// the canonical sysvar names mapped to the session's LIVE values, so a
/// submitted MV job inherits the creator's settings rather than the
/// defaults. The DDL statement context installs exactly this image.
#[test]
fn session_vars_image_carries_live_values() {
    let mut vars = SessionVars::new();
    vars.set_system("tidb_mview_maintain_import_threads", "8".to_owned())
        .expect("seed import threads");
    vars.set_system("tidb_isolation_read_engines", "tikv".to_owned())
        .expect("seed the isolation engines");
    let image = super::vars::m_view_execution_session_vars_image(&vars);
    assert_eq!(image.len(), 12, "the twelve MV-execution variables");
    assert_eq!(
        image
            .get("tidb_mview_maintain_import_threads")
            .map(String::as_str),
        Some("8")
    );
    assert_eq!(
        image
            .get("tidb_mview_maintain_isolation_read_engines")
            .map(String::as_str),
        Some("tikv")
    );
    // Untouched knobs stay at their registry defaults.
    assert_eq!(
        image
            .get("tidb_mview_maintain_mem_quota")
            .map(String::as_str),
        Some("2147483648")
    );

    // The DDL statement context carries the image forward.
    let session = crate::Session::new();
    let context = session.ddl_statement_context();
    let carried = context
        .session_vars_image()
        .expect("the DDL context carries the image");
    assert_eq!(
        carried
            .get("tidb_mview_maintain_import_threads")
            .map(String::as_str),
        Some("0")
    );
}

/// Go `ApplyMViewExecutionSessionVarsWithConfig` with the mem-quota name
/// pointed at `tidb_mem_quota_query` (the maintenance-session shape): the
/// target values land, the restore handle puts them back, and capture-applied
/// reads the landed values.
#[test]
fn apply_and_restore_mview_execution_vars() {
    let mut vars = SessionVars::new();
    vars.set_system(
        "tidb_mview_maintain_isolation_read_engines",
        "tikv,tiflash".to_owned(),
    )
    .expect("seed the isolation engines");
    let mut cfg = MViewExecutionSessionVarsApplyConfig::new();
    cfg.maintain_mem_quota_var_name = "tidb_mem_quota_query".to_owned();
    let target = MViewExecutionSessionVars {
        maintain_mem_quota: 4 * 1024 * 1024 * 1024,
        isolation_read_engines: "tikv".to_owned(),
        ti_flash_max_threads: 16,
        import_threads: 4,
        import_disk_quota: "8GB".to_owned(),
        ..Default::default()
    };

    let restore = apply_m_view_execution_session_vars_with_config(&mut vars, &target, &cfg)
        .expect("all assignments apply");
    assert_eq!(
        vars.get_system("tidb_mem_quota_query").unwrap(),
        "4294967296"
    );
    assert_eq!(
        vars.get_system("tidb_mview_maintain_isolation_read_engines")
            .unwrap(),
        "tikv"
    );
    assert_eq!(vars.get_system("tidb_max_tiflash_threads").unwrap(), "16");
    assert_eq!(
        vars.get_system("tidb_mview_maintain_import_threads")
            .unwrap(),
        "4"
    );
    assert_eq!(
        vars.get_system("tidb_mview_maintain_import_disk_quota")
            .unwrap(),
        "8GB"
    );
    let applied = capture_applied_m_view_execution_session_vars(&vars);
    assert_eq!(applied.maintain_mem_quota, 4 * 1024 * 1024 * 1024);

    restore.restore(&mut vars);
    assert_eq!(
        vars.get_system("tidb_mem_quota_query").unwrap(),
        "1073741824",
        "restore puts the query quota back"
    );
    assert_eq!(
        vars.get_system("tidb_max_tiflash_threads").unwrap(),
        "-1",
        "restore puts Go's auto (-1) tiflash threads default back"
    );
}

/// Go's early return: when the session already matches the target the apply
/// is a no-op that still yields a usable restore handle.
#[test]
fn apply_mview_is_noop_when_origin_equals_target() {
    let mut vars = SessionVars::new();
    // Go's default capture is `CaptureAppliedMViewExecutionSessionVars`; a
    // target equal to that snapshot makes the apply a no-op.
    let current = capture_applied_m_view_execution_session_vars(&vars);
    let cfg = MViewExecutionSessionVarsApplyConfig::new();
    let restore = apply_m_view_execution_session_vars_with_config(&mut vars, &current, &cfg)
        .expect("no-op apply");
    restore.restore(&mut vars);
    assert_eq!(
        capture_applied_m_view_execution_session_vars(&vars),
        current
    );
}

/// Go's best-effort path: a failing assignment is reported through
/// `OnApplyError` and the remaining assignments still apply.
#[test]
fn apply_mview_best_effort_reports_and_continues() {
    let mut vars = SessionVars::new();
    let mut cfg = MViewExecutionSessionVarsApplyConfig::new();
    cfg.best_effort = true;
    let failures = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let sink = failures.clone();
    cfg.inject_apply_error = Some(Box::new(|name| {
        (name == "tidb_max_tiflash_threads").then(|| "injected".to_owned())
    }));
    cfg.on_apply_error = Some(Box::new(move |name, value, error| {
        sink.borrow_mut()
            .push((name.to_owned(), value.to_owned(), error.to_owned()));
    }));
    let target = MViewExecutionSessionVars {
        isolation_read_engines: "tikv".to_owned(),
        ti_flash_max_threads: 16,
        fine_grained_stream_count: 8,
        ..Default::default()
    };
    let restore = apply_m_view_execution_session_vars_with_config(&mut vars, &target, &cfg)
        .expect("best-effort apply succeeds");
    let reported = failures.borrow();
    assert_eq!(reported.len(), 1);
    assert_eq!(reported[0].0, "tidb_max_tiflash_threads");
    assert_eq!(reported[0].1, "16");
    assert_eq!(reported[0].2, "injected");
    assert_eq!(
        vars.get_system("tiflash_fine_grained_shuffle_stream_count")
            .unwrap(),
        "8",
        "the assignments after the failure still applied"
    );
    restore.restore(&mut vars);
}

/// Go's strict path: a failing assignment aborts the apply, restores the
/// already-applied values, and annotates the error with Go's failure message.
#[test]
fn apply_mview_strict_fails_and_restores() {
    let mut vars = SessionVars::new();
    let mut cfg = MViewExecutionSessionVarsApplyConfig::new();
    cfg.maintain_mem_quota_var_name = "tidb_mem_quota_query".to_owned();
    cfg.inject_apply_error = Some(Box::new(|name| {
        (name == "tidb_max_tiflash_threads").then(|| "injected".to_owned())
    }));
    let target = MViewExecutionSessionVars {
        maintain_mem_quota: 4 * 1024 * 1024 * 1024,
        isolation_read_engines: "tikv".to_owned(),
        ti_flash_max_threads: 16,
        ..Default::default()
    };
    let error = match apply_m_view_execution_session_vars_with_config(&mut vars, &target, &cfg) {
        Ok(_) => panic!("the strict apply must abort on the injected failure"),
        Err(error) => error,
    };
    assert!(error.contains("mv execution: failed to apply tidb_max_tiflash_threads"));
    assert!(error.contains("injected"));
    assert_eq!(
        vars.get_system("tidb_mem_quota_query").unwrap(),
        "1073741824",
        "the mem-quota assignment that applied before the failure was restored"
    );
}

/// Go `SessionVars.InMViewMaintenance`: the programmatic flag round-trips
/// without any sysvar text.
#[test]
fn in_mview_maintenance_flag_roundtrip() {
    let mut vars = SessionVars::new();
    assert!(!vars.in_mview_maintenance());
    vars.set_in_mview_maintenance(true);
    assert!(vars.in_mview_maintenance());
    vars.set_in_mview_maintenance(false);
    assert!(!vars.in_mview_maintenance());
}
