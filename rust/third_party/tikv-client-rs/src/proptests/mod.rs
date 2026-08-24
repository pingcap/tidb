// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// Note: This module exists and includes some integration tests because the `/tests/`
// directory tests don't have access to `cfg(tests)` functions and we don't want to force
// users to depend on proptest or manually enable features to test.

//  Temporarily disabled
//
// use proptest::strategy::Strategy;
// use std::env::var;
//
// mod raw;
// pub(crate) const ENV_PD_ADDRS: &str = "PD_ADDRS";
// pub(crate) const PROPTEST_BATCH_SIZE_MAX: usize = 16;
//
// pub fn arb_batch<T: core::fmt::Debug>(
// single_strategy: impl Strategy<Value = T>,
// max_batch_size: impl Into<Option<usize>>,
// ) -> impl Strategy<Value = Vec<T>> {
// let max_batch_size = max_batch_size.into().unwrap_or(PROPTEST_BATCH_SIZE_MAX);
// proptest::collection::vec(single_strategy, 0..max_batch_size)
// }
//
// pub fn pd_addrs() -> Vec<String> {
// var(ENV_PD_ADDRS)
// .expect(&format!("Expected {}:", ENV_PD_ADDRS))
// .split(",")
// .map(From::from)
// .collect()
// }
