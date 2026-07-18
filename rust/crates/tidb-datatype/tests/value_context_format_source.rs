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

//! Smoke target for the consolidated datatype ownership bundle.
//!
//! The Go sources do not define one operation joining these authorities. Each
//! test therefore follows its real source path independently; this target must
//! never grow a synthetic FieldType/Datum/conversion/format pipeline.

use std::sync::Mutex;

use tidb_datatype::{
    output_format, parse_hex_str, ConversionContext, ConversionLocation, ConversionWarningAppender,
    Datum, DatumKind, FieldType, FieldTypeCode, STRICT_FLAGS,
};
use tidb_error::terror::TerrorError;

#[derive(Default)]
struct WarningStore(Mutex<Vec<TerrorError>>);

impl ConversionWarningAppender for WarningStore {
    fn append_conversion_warning(&self, warning: TerrorError) {
        self.0.lock().unwrap().push(warning);
    }
}

/// `FieldType.String` is the real consumer of `OutputFormat` for ENUM/SET
/// declaration elements. Arbitrary Datum strings are not part of this path.
#[test]
fn field_type_metadata_owns_output_format() {
    let field_type = FieldType::parser(FieldTypeCode::Enum).with_elems(["a'\n", "nul\0"]);

    assert_eq!(field_type.source_string(), "enum('a''\\n','nul\\0')");
    assert_eq!(output_format("'\0abc\n\rdef"), "''\\0abc\\n\\rdef");
}

/// `Context.HandleTruncate` consumes the error produced by the actual binary
/// literal conversion. The value, error, and warning cannot be paired with an
/// unrelated FieldType or Datum.
#[test]
fn conversion_generated_truncation_uses_context_warning_port() {
    let literal = parse_hex_str("0x1010ffff8080ff12ff").unwrap();
    let warnings = WarningStore::default();
    let context = ConversionContext::new(
        STRICT_FLAGS.with_truncate_as_warning(true),
        ConversionLocation::UTC,
        &warnings,
    );

    let (value, error) = literal.to_int_with_context(&context);
    assert_eq!(value, u64::MAX);
    assert!(error.is_none());
    let retained = warnings.0.lock().unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].rfc_code(), "types:1292");
}

/// Datum rendering and sentinel ordering remain Datum-owned. No invented
/// FieldType compatibility predicate is involved.
#[test]
fn datum_scalar_and_range_sentinel_paths_remain_distinct() {
    let scalar = Datum::new_string("'nul\0\n\r\\");
    assert_eq!(scalar.sql_string().unwrap(), "'nul\0\n\r\\");

    let sentinel = Datum::max_value();
    assert_eq!(sentinel.kind(), DatumKind::MaxValue);
    assert!(sentinel.sql_string().is_err());
    assert!(Datum::default().compare_sentinel_order(&sentinel).is_some());
}
