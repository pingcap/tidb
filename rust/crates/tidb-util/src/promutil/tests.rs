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
