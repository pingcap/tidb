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

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

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
