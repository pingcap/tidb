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

use super::*;

fn counter(name: &str) -> Counter {
    Counter::with_opts(prometheus::Opts::new(name, "test counter")).unwrap()
}

#[test]
#[deny(unused_must_use)]
fn return_values_may_be_ignored_like_go() {
    new_default_factory();
    new_noop_registry();
    new_default_registry();
}

// Go TestNoopRegistry.
#[test]
fn noop_registry() {
    let registry = new_noop_registry();

    registry
        .register(Box::new(counter("noop_counter")))
        .unwrap();
    registry
        .register(Box::new(counter("noop_counter")))
        .unwrap();
    assert!(registry.unregister(Box::new(counter("noop_counter"))));
    let gauge_vec = GaugeVec::new(prometheus::Opts::new("noop_gauge", "test gauge"), &[]).unwrap();
    assert!(registry.unregister(Box::new(gauge_vec)));
}
