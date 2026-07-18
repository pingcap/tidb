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

//! Source-backed tests for SHOW PLACEMENT label aggregation.

use tidb_exec::placement_labels::{PlacementLabels, StoreLabel};

#[test]
fn placement_labels_deduplicate_and_sort_rows() {
    // Source: pkg/executor/show_placement.go:43-110.
    // Direct Go coverage: pkg/executor/show_placement_labels_test.go:26
    // (TestShowPlacementLabelsBuilder).
    let mut labels = PlacementLabels::default();
    labels.append_store_labels(None);
    assert!(labels.build_rows().is_empty());

    let stores = [
        vec![
            StoreLabel::new("zone", "z1"),
            StoreLabel::new("rack", "r3"),
            StoreLabel::new("host", "h1"),
        ],
        vec![
            StoreLabel::new("zone", "z1"),
            StoreLabel::new("rack", "r1"),
            StoreLabel::new("host", "h2"),
        ],
        vec![
            StoreLabel::new("zone", "z1"),
            StoreLabel::new("rack", "r2"),
            StoreLabel::new("host", "h2"),
        ],
        vec![
            StoreLabel::new("zone", "z2"),
            StoreLabel::new("rack", "r1"),
            StoreLabel::new("host", "h2"),
        ],
        vec![StoreLabel::new("k1", "v1")],
    ];
    for store in &stores {
        labels.append_store_labels(Some(store));
    }

    assert_eq!(
        labels.build_rows(),
        vec![
            ("host".to_owned(), vec!["h1".to_owned(), "h2".to_owned()]),
            ("k1".to_owned(), vec!["v1".to_owned()]),
            (
                "rack".to_owned(),
                vec!["r1".to_owned(), "r2".to_owned(), "r3".to_owned()]
            ),
            ("zone".to_owned(), vec!["z1".to_owned(), "z2".to_owned()]),
        ]
    );
}

#[test]
fn placement_labels_preserve_empty_strings_as_values() {
    let mut labels = PlacementLabels::default();
    labels.append_store_labels(Some(&[
        StoreLabel::new("key", ""),
        StoreLabel::new("key", ""),
    ]));
    assert_eq!(
        labels.build_rows(),
        vec![("key".to_owned(), vec![String::new()])]
    );
}
