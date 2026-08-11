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
    )
}

/// Go `compressFunctionClass`: zlib's public upper bound, capped at the
/// maximum blob width, with binary charset/collation.
pub(super) fn compress_return_type(source: i64, mut field_type: FieldType) -> FieldType {
    let bound = source + (source >> 12) + (source >> 14) + (source >> 25) + 13;
    field_type.set_flen(bound.min(MAX_BLOB_WIDTH));
    set_binary_charset(&mut field_type);
    field_type
}
