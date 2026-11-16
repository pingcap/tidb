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

/// Go `taskcommon.GetStorage`: the backend of `storage_name` and its
/// storage.
///
/// # Errors
/// The URL cannot be parsed, or the storage cannot be created.
pub fn get_storage(
    ctx: &tidb_gocontext::Context,
    storage_name: &str,
    backend_options: &tidb_objstore::BackendOptions,
    no_creds: bool,
    send_creds: bool,
) -> Result<(tidb_proto::backup::StorageBackend, crate::metautil::Storage), String> {
    let u = tidb_objstore::parse_backend(storage_name, Some(backend_options))?;
    let opts = tidb_objstore::storeapi::Options { no_credentials: no_creds, send_credentials: send_creds, ..Default::default() };
    let s = tidb_objstore::new(ctx, &u, Some(&opts)).map_err(|e| format!("create storage failed: {e}"))?;
    Ok((u, s))
}

/// Go `metautil.DecryptFullBackupMetaIfNeeded`: the backupmeta file is the
/// IV followed by the encrypted backupmeta under an effective cipher.
///
/// # Errors
/// The content cannot be decrypted.
pub fn decrypt_full_backup_meta_if_needed(meta_data: &[u8], cipher: &CipherInfo) -> Result<Vec<u8>, String> {
    if cipher.cipher_type == tidb_proto::encryptionpb::EncryptionMethod::Plaintext as i32
        || cipher.cipher_type == tidb_proto::encryptionpb::EncryptionMethod::Unknown as i32
    {
        return Ok(meta_data.to_vec());
    }
    if meta_data.len() < 16 {
        return Err("decrypt failed with wrong key: truncated backupmeta IV".into());
    }
    let (iv, content) = meta_data.split_at(16);
    crate::utils::decrypt(content, Some(cipher), iv).map_err(|err| format!("decrypt failed with wrong key: {err}"))
}

/// Go `task.ReadBackupMeta` for `SHOW BACKUP METADATA`'s task config
/// (`show.Config.lameTaskConfig`: default backend options and
/// `CheckRequirements` off). Go's GCS object-not-found retry under the
/// parent prefix is not ported.
pub fn read_backup_meta(
    storage: &str,
    cipher: &CipherInfo,
) -> Result<crate::metautil::MetaReader, String> {
    let ctx = tidb_gocontext::Context::background();
    let (_, storage) = get_storage(&ctx, storage, &tidb_objstore::BackendOptions::default(), false, false)?;
    let data = storage
        .read_file(&ctx, crate::metautil::META_FILE)
        .map_err(|err| format!("load backupmeta failed: {err}"))?;
    let data = decrypt_full_backup_meta_if_needed(&data, cipher)?;
    let meta = BackupMeta::decode(data.as_slice())
        .map_err(|err| format!("parse backupmeta failed because of wrong aes cipher: {err}"))?;
    Ok(crate::metautil::MetaReader::new(meta, storage, cipher.clone()))
}
