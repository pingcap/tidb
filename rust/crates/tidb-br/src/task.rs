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

//! Go `br/pkg/task`: local backup metadata loading. Physical task runners
//! are not yet ported; this module must not be used to claim backup success.

pub mod backup;
pub mod common;
pub mod restore;
pub mod show;

use prost::Message;
use tidb_proto::backup::{BackupMeta, CipherInfo};

/// Go `task.ReadBackupMeta`, for the local storage backend.
pub fn read_backup_meta(
    storage: &str,
    cipher: &CipherInfo,
) -> Result<crate::metautil::MetaReader, String> {
    let storage = crate::local_storage::LocalStorage::from_url(storage)?;
    let data = storage
        .read_file("backupmeta")
        .map_err(|err| format!("load backupmeta failed: {err}"))?;
    let (iv, content) =
        if cipher.cipher_type == tidb_proto::encryptionpb::EncryptionMethod::Plaintext as i32 {
            (&[][..], data.as_slice())
        } else {
            if data.len() < 16 {
                return Err("decrypt failed with wrong key: truncated backupmeta IV".into());
            }
            data.split_at(16)
        };
    let data = crate::utils::decrypt(content, Some(cipher), iv)
        .map_err(|err| format!("decrypt failed with wrong key: {err}"))?;
    let meta = BackupMeta::decode(data.as_slice())
        .map_err(|err| format!("parse backupmeta failed because of wrong aes cipher: {err}"))?;
    // Go show.Config.lameTaskConfig leaves CheckRequirements false.
    Ok(crate::metautil::MetaReader::new(
        meta,
        storage,
        cipher.clone(),
    ))
}
