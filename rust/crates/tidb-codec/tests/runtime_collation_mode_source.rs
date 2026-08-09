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

//! Process-mode boundary for Go's global new-collation switch.
//!
//! This file intentionally contains one test so no sibling test can observe
//! the temporary process-global mode. Cargo runs each integration-test file in
//! its own process.

use tidb_codec::{convert_by_collation, encode_hash_datum, COMPACT_BYTES_FLAG};
use tidb_datatype::{
    new_collation_enabled, set_new_collation_enabled, Datum, FieldType, FieldTypeCode,
};

struct RestoreCollationMode(bool);

impl Drop for RestoreCollationMode {
    fn drop(&mut self) {
        set_new_collation_enabled(self.0);
    }
}

#[test]
fn operational_keys_follow_exact_name_and_process_mode() {
    let _restore = RestoreCollationMode(new_collation_enabled());
    let canonical =
        FieldType::new(FieldTypeCode::Varchar).with_collation_name("utf8mb4_general_ci");

    set_new_collation_enabled(true);
    assert_eq!(
        convert_by_collation(b"a", &canonical),
        convert_by_collation(b"A ", &canonical)
    );
    assert_eq!(
        encode_hash_datum(&Datum::new_string("a"), &canonical).unwrap(),
        encode_hash_datum(&Datum::new_string("A "), &canonical).unwrap()
    );

    set_new_collation_enabled(false);
    assert_eq!(convert_by_collation(b"A ", &canonical), b"A ".to_vec());
    assert_ne!(
        convert_by_collation(b"a", &canonical),
        convert_by_collation(b"A ", &canonical)
    );
    assert_eq!(
        encode_hash_datum(&Datum::new_string("A "), &canonical).unwrap(),
        (COMPACT_BYTES_FLAG, b"A ".to_vec())
    );

    for exact_name in ["UTF8MB4_GENERAL_CI", "unknown_collation"] {
        let field_type = FieldType::new(FieldTypeCode::Varchar).with_collation_name(exact_name);
        assert_eq!(
            convert_by_collation(&[0xff, 0xfe], &field_type),
            vec![0xff, 0xfe]
        );
    }
}
