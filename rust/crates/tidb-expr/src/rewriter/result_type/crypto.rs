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

use tidb_datatype::FieldType;

use super::{set_binary_charset, MAX_BLOB_WIDTH};

pub(in crate::rewriter) fn returns_binary_string(name: &str) -> bool {
    matches!(
        name,
        "unhex"
            | "from_base64"
            | "inet6_aton"
            | "weight_string"
            | "uuid_to_bin"
            | "compress"
            | "uncompress"
            | "aes_encrypt"
            | "aes_decrypt"
            | "random_bytes"
    )
}

/// Go `randomBytesFunctionClass`: binary `VAR_STRING(1024)`.
pub(super) fn random_bytes_return_type(mut field_type: FieldType) -> FieldType {
    field_type.set_flen(1024);
    set_binary_charset(&mut field_type);
    field_type
}

/// Go `compressFunctionClass`: zlib's public upper bound, capped at the
/// maximum blob width, with binary charset/collation.
pub(super) fn compress_return_type(source: i64, mut field_type: FieldType) -> FieldType {
    let bound = source + (source >> 12) + (source >> 14) + (source >> 25) + 13;
    field_type.set_flen(bound.min(MAX_BLOB_WIDTH));
    set_binary_charset(&mut field_type);
    field_type
}

#[cfg(test)]
mod tests {
    use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};

    use super::{random_bytes_return_type, returns_binary_string};

    #[test]
    fn random_bytes_is_binary_var_string_1024() {
        let field_type = random_bytes_return_type(FieldType::new(FieldTypeCode::VarString));
        assert_eq!(field_type.code(), FieldTypeCode::VarString);
        assert_eq!(field_type.flen(), 1024);
        assert_eq!(field_type.charset_name(), "binary");
        assert_eq!(field_type.collation_name(), "binary");
        assert!(field_type.has_flag(FieldTypeFlags::BINARY));
        assert!(returns_binary_string("random_bytes"));
    }
}
