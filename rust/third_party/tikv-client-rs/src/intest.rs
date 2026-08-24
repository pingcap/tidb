// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Build-time internal-test configuration.

use std::sync::atomic::{AtomicBool, Ordering};

static IN_TEST: AtomicBool = AtomicBool::new(cfg!(feature = "internal-tests"));

/// Returns whether internal-test-specific behavior is enabled.
pub fn in_test() -> bool {
    IN_TEST.load(Ordering::Relaxed)
}

/// Changes whether internal-test-specific behavior is enabled.
///
/// This matches client-go's mutable `InTest` variable while avoiding a data race in Rust tests.
pub fn set_in_test(enabled: bool) {
    IN_TEST.store(enabled, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_test_matches_the_build_feature_and_remains_mutable() {
        let initial = cfg!(feature = "internal-tests");
        assert_eq!(in_test(), initial);
        set_in_test(!initial);
        assert_eq!(in_test(), !initial);
        set_in_test(initial);
    }
}
