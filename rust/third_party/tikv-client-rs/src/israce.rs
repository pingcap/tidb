// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Build-time race-detector test configuration.

/// Whether race-detector-specific test behavior is enabled.
///
/// Enable this together with the Rust race detector, such as ThreadSanitizer, by passing the
/// `race-tests` Cargo feature.
pub const RACE_ENABLED: bool = cfg!(feature = "race-tests");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn race_enabled_matches_the_build_feature() {
        assert_eq!(RACE_ENABLED, cfg!(feature = "race-tests"));
    }
}
