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

//! Complete transcreation of `pkg/util/encrypt`.
//!
//! The package's three production files remain separate modules. Its AES mode
//! framing, random-access CTR layer, legacy SQL codec, original source tests,
//! benchmark, build metadata, and leak-check disposition move together.
//! Rust's test harness creates no package-owned background workers, so Go's
//! `goleak.VerifyTestMain` has no runtime analogue.

mod aes;
mod aes_layer;
mod crypt;

pub use aes::{
    aes_decrypt_with_cbc, aes_decrypt_with_cfb, aes_decrypt_with_ctr, aes_decrypt_with_ecb,
    aes_decrypt_with_ofb, aes_encrypt_with_cbc, aes_encrypt_with_cfb, aes_encrypt_with_ctr,
    aes_encrypt_with_ecb, aes_encrypt_with_ofb, derive_key_mysql, pkcs7_pad, pkcs7_unpad,
    EncryptError,
};
pub use aes_layer::{CtrCipher, Reader, Writer, DEFAULT_ENCRYPT_BLOCK_SIZE};
pub use crypt::{sql_decode, sql_encode};
