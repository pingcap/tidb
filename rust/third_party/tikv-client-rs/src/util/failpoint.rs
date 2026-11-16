// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, Ordering};

use thiserror::Error;

const FAILPOINT_PREFIX: &str = "tikvclient/";
static FAILPOINTS_ENABLED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("failpoints are disabled")]
pub struct FailpointsDisabled;

pub fn enable_failpoints() {
    FAILPOINTS_ENABLED.store(true, Ordering::Release);
}

pub fn eval_failpoint<R>(
    name: &str,
    action: impl FnOnce(Option<String>) -> R,
) -> Result<Option<R>, FailpointsDisabled> {
    if !FAILPOINTS_ENABLED.load(Ordering::Acquire) {
        return Err(FailpointsDisabled);
    }
    Ok(fail::eval(&format!("{FAILPOINT_PREFIX}{name}"), action))
}

/// client-go `util.EvalFailpoint(name)` in async code. Go's `pause` parks
/// only the calling goroutine; `fail`'s blocks the OS thread, which inside
/// a task is a runtime worker, and enough of those stall every future on
/// the runtime, PD timestamps included. So a paused failpoint parks the
/// calling task until it is changed, then evaluates.
pub async fn eval_failpoint_async<R>(
    name: &str,
    action: impl FnOnce(Option<String>) -> R,
) -> Result<Option<R>, FailpointsDisabled> {
    if !FAILPOINTS_ENABLED.load(Ordering::Acquire) {
        return Err(FailpointsDisabled);
    }
    let name = format!("{FAILPOINT_PREFIX}{name}");
    while fail::list().iter().any(|(point, actions)| *point == name && actions == "pause") {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    Ok(fail::eval(&name, action))
}

/// A failpoint's value as client-go's `EvalFailpoint` returns it.
pub struct FailpointValue(pub Option<String>);

impl FailpointValue {
    /// Go `val.(string)`: a quoted `return("...")` value, unquoted.
    pub fn as_string(&self) -> Option<&str> {
        self.0.as_deref()?.strip_prefix('"')?.strip_suffix('"')
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

    /// A paused failpoint parks only its task, as Go's parks only its
    /// goroutine: another task on the same one-thread runtime still runs,
    /// and the paused one evaluates once the failpoint changes.
    #[test]
    #[serial]
    fn a_paused_failpoint_parks_only_its_task() {
        enable_failpoints();
        let _scenario = fail::FailScenario::setup();
        fail::cfg("tikvclient/pausedTask", "pause").unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        runtime.block_on(async {
            let paused = tokio::spawn(eval_failpoint_async("pausedTask", |_| 1));
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert!(!paused.is_finished());
            fail::cfg("tikvclient/pausedTask", "return").unwrap();
            assert_eq!(paused.await.unwrap(), Ok(Some(1)));
        });
    }

    #[test]
    #[serial]
    fn source_prefix_and_enable_gate() {
        FAILPOINTS_ENABLED.store(false, Ordering::Release);
        assert_eq!(eval_failpoint("gate", |_| 1), Err(FailpointsDisabled));
        enable_failpoints();
        let _scenario = fail::FailScenario::setup();
        fail::cfg("tikvclient/gate", "return(value)").unwrap();
        assert_eq!(
            eval_failpoint("gate", |value| value.unwrap()),
            Ok(Some("value".to_owned()))
        );
    }
}
