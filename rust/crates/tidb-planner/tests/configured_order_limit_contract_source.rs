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

//! Source-shaped tests for the shared configured ORDER BY LIMIT contract.

#[path = "../src/configured_order_limit_contract.rs"]
mod configured_order_limit_contract;

use configured_order_limit_contract::{
    ConfiguredLimitWindow, ConfiguredLimitWindowError, ConfiguredOrderDirection,
    ConfiguredOrderKey, ConfiguredOrderLimitSpec, ConfiguredOrderLimitSpecError,
};

#[test]
fn by_items_preserve_source_order_offset_and_direction() {
    let keys = [
        ConfiguredOrderKey::new(4, ConfiguredOrderDirection::from_descending(true)),
        ConfiguredOrderKey::new(0, ConfiguredOrderDirection::from_descending(false)),
        ConfiguredOrderKey::new(4, ConfiguredOrderDirection::Descending),
    ];

    assert_eq!(keys[0].full_offset(), 4);
    assert_eq!(keys[1].full_offset(), 0);
    assert!(keys[0].direction().is_descending());
    assert!(!keys[1].direction().is_descending());
    assert_eq!(keys[0], keys[2], "duplicate ByItems remain source-valid");
}

#[test]
fn limit_window_computes_one_checked_exclusive_end() {
    let window = ConfiguredLimitWindow::new(7, 5).expect("checked window");
    assert_eq!(window.offset(), 7);
    assert_eq!(window.count(), 5);
    assert_eq!(window.end_exclusive(), 12);
    assert!(!window.is_empty());

    let empty = ConfiguredLimitWindow::new(usize::MAX, 0).expect("zero count cannot overflow");
    assert_eq!(empty.end_exclusive(), usize::MAX);
    assert!(empty.is_empty());
}

#[test]
fn limit_window_rejects_offset_count_overflow() {
    assert_eq!(
        ConfiguredLimitWindow::new(usize::MAX, 1),
        Err(ConfiguredLimitWindowError::EndOverflow {
            offset: usize::MAX,
            count: 1,
        })
    );
    assert_eq!(
        ConfiguredLimitWindow::new(usize::MAX - 1, 2),
        Err(ConfiguredLimitWindowError::EndOverflow {
            offset: usize::MAX - 1,
            count: 2,
        })
    );
}

#[test]
fn combined_spec_preserves_identity_and_rejects_limit_only_aliasing() {
    let window = ConfiguredLimitWindow::new(2, 3).expect("checked window");
    let keys = vec![
        ConfiguredOrderKey::new(3, ConfiguredOrderDirection::Descending),
        ConfiguredOrderKey::new(1, ConfiguredOrderDirection::Ascending),
    ];
    let spec = ConfiguredOrderLimitSpec::new(keys.clone(), window).expect("ordered TopN spec");

    assert_eq!(spec.order_keys(), keys);
    assert_eq!(spec.limit(), window);
    assert_ne!(
        spec,
        ConfiguredOrderLimitSpec::new(
            vec![
                ConfiguredOrderKey::new(3, ConfiguredOrderDirection::Ascending),
                ConfiguredOrderKey::new(1, ConfiguredOrderDirection::Ascending),
            ],
            window,
        )
        .expect("direction is part of source identity")
    );
    assert_eq!(
        ConfiguredOrderLimitSpec::new(Vec::new(), window),
        Err(ConfiguredOrderLimitSpecError::EmptyOrderKeys)
    );
}
