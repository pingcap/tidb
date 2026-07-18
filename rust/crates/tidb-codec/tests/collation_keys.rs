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

//! Key-collation rows from `pkg/util/codec/collation_test.go` plus the default
//! `utf8mb4_bin` padding behavior owned by `pkg/util/collate/bin.go`.

use tidb_codec::{encode_key, Encoder};
use tidb_datatype::{Collation, Datum};

#[test]
fn default_utf8mb4_bin_key_ignores_trailing_spaces() {
    let plain = encode_key(&[Datum::new_string("abc")]).unwrap();
    let padded = encode_key(&[Datum::new_string("abc ")]).unwrap();
    assert_eq!(padded, plain);

    let enabled = Encoder::new(true);
    assert!(enabled.use_new_collation());
    assert_eq!(
        enabled.encode_key(&[Datum::new_string("abc ")]).unwrap(),
        plain
    );

    let disabled = Encoder::new(false);
    assert!(!disabled.use_new_collation());
    assert_ne!(
        disabled.encode_key(&[Datum::new_string("abc ")]).unwrap(),
        plain
    );

    let binary = Datum::new_collation_string("abc ", Collation::Binary);
    assert_eq!(
        enabled.encode_key(std::slice::from_ref(&binary)).unwrap(),
        disabled.encode_key(std::slice::from_ref(&binary)).unwrap()
    );
}
