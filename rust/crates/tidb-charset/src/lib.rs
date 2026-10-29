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
// See the License for the specific language governing permissions and
// limitations under the License.


//! Go `pkg/parser/charset`: the character sets, their collations' registry
//! and the encodings the parser and every later layer read.

pub mod charset_errors;
pub mod ascii_encoding;
pub mod charset;
pub mod encoding_base;
mod encoding_table;
mod multibyte_encoding;
pub mod utf8_encoding;

pub use charset::{
    add_charset, add_collation, add_supported_collation, charset_known, get_charset_info,
    get_charset_info_by_id, get_collation_by_id, get_collation_by_name,
    get_default_charset_and_collate, get_default_collation, get_default_collation_legacy,
    get_supported_charsets, get_supported_collations, is_new_collation_supported, remove_charset,
    valid_charset_and_collation, Charset, CharsetError, CharsetInfo, CharsetName, Collation,
    CollationInfo, PAD_NONE, PAD_SPACE, TIFLASH_SUPPORTED_CHARSETS,
};
pub use encoding_base::{TransformOp, TransformResult};
pub use encoding_table::{lookup_encoding, HtmlEncoding, HtmlEncodingError};
pub use multibyte_encoding::{
    count_valid_bytes, count_valid_bytes_decode, find_encoding, find_encoding_take_utf8_as_noop,
    is_supported_encoding, Encoding, EncodingError, EncodingResult, EncodingType,
};
pub use utf8_encoding::{
    Utf8Encoding, Utf8Mb3StrictEncoding, Utf8Op, Utf8TransformError, Utf8TransformResult,
    UTF8_ENCODING, UTF8_MB3_STRICT_ENCODING,
};
