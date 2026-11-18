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

//! Go `pkg/planner/extstore` (`extstore.go`): the process-wide external
//! storage the plan replayer, optimizer trace and extract task files live
//! in. It is an `objstore` storage (`tidb_objstore::new` over the parsed
//! URL, the keyspace name joined to its path), as Go's `NewExtStorage` is
//! `objstore.New`. A classic kernel, or an empty `tidb_cloud_storage_uri`,
//! uses a local directory: the log file's directory when its `replayer`
//! subdirectory is writable, else the configured temp dir.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tidb_gocontext::Context;
use tidb_objstore::storeapi::Storage;

/// Go `storeapi.Storage`, the type every caller of this package holds.
pub type ExtStorage = Arc<dyn Storage>;

/// Go `globalExtStorage`.
static GLOBAL_EXT_STORAGE: Mutex<Option<ExtStorage>> = Mutex::new(None);

/// Go `GetGlobalExtStorage`: created once, then shared.
pub fn get_global_ext_storage() -> Result<ExtStorage, String> {
    let mut global = GLOBAL_EXT_STORAGE.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(storage) = global.as_ref() {
        return Ok(Arc::clone(storage));
    }
    let storage = create_global_ext_storage(&Context::background())?;
    *global = Some(Arc::clone(&storage));
    Ok(storage)
}

/// Go `SetGlobalExtStorageForTest`.
pub fn set_global_ext_storage_for_test(storage: Option<ExtStorage>) {
    *GLOBAL_EXT_STORAGE.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = storage;
}

/// Go `createGlobalExtStorage`.
fn create_global_ext_storage(ctx: &Context) -> Result<ExtStorage, String> {
    let keyspace_name = tidb_keyspace::get_keyspace_name_by_settings();
    let mut uri = tidb_vardef::CLOUD_STORAGE_URI.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
    // When classic kernel or cloud storage URI is not set, use local directory.
    if tidb_config_kerneltype::is_classic() || uri.is_empty() {
        let local_path = get_local_path_dir_name();
        let local_path = std::path::absolute(&local_path).unwrap_or(local_path);
        tidb_logutil::bg_logger().warn(
            "using default local storage",
            &[
                tidb_log::Field::new("category", tidb_log::Value::Str("extstore".to_owned())),
                tidb_log::Field::new("localPath", tidb_log::Value::Str(local_path.display().to_string())),
                tidb_log::Field::new("keyspaceName", tidb_log::Value::Str(keyspace_name.clone())),
            ],
        );
        uri = format!("file://{}", local_path.display());
    }
    let storage = new_ext_storage(ctx, &uri, &keyspace_name).inspect_err(|error| {
        // Go hides credentials through `ast.RedactURL`.
        tidb_logutil::bg_logger().warn(
            "failed to create global ext storage",
            &[
                tidb_log::Field::new("category", tidb_log::Value::Str("extstore".to_owned())),
                tidb_log::Field::new("uri", tidb_log::Value::Str(tidb_ast::redact_url(&uri))),
                tidb_log::Field::new("error", tidb_log::Value::Str(error.clone())),
            ],
        );
    })?;
    tidb_logutil::bg_logger().info(
        "initialized global ext storage",
        &[
            tidb_log::Field::new("category", tidb_log::Value::Str("extstore".to_owned())),
            tidb_log::Field::new("storage", tidb_log::Value::Str(storage.uri())),
        ],
    );
    Ok(storage)
}

/// Go `NewExtStorage`: `objstore.ParseRawURL`, the namespace joined to the
/// URL's path (`filepath.Join`), `ParseBackendFromURL` and `objstore.New`.
pub fn new_ext_storage(ctx: &Context, raw_url: &str, namespace: &str) -> Result<ExtStorage, String> {
    let mut u = tidb_objstore::parse_raw_url(raw_url)?;
    if !namespace.is_empty() {
        let path = String::from_utf8_lossy(&u.path).into_owned();
        u.path = tidb_objstore::gopath::join(&[&path, namespace]).into_bytes();
    }
    let backend = tidb_objstore::parse_backend_from_url(&mut u, None)?;
    tidb_objstore::new(ctx, &backend, None).map_err(|error| error.to_string())
}

/// Go `objstore.ValidateCloudStorageURI`, the validator
/// `tidb_cloud_storage_uri` installs.
pub fn validate_cloud_storage_uri(ctx: &Context, uri: &str) -> Result<(), String> {
    tidb_objstore::validate_cloud_storage_uri(ctx, uri).map_err(|error| error.to_string())
}

/// Go `getLocalPathDirName`: the log file's directory when a file can be
/// written in its `replayer` subdirectory, else the configured temp dir.
pub fn get_local_path_dir_name() -> PathBuf {
    let config = tidb_config::config_tree::config::get_global_config();
    // Go `filepath.Clean(filepath.Dir(filename))`.
    let log_dir = PathBuf::from(tidb_objstore::gopath::dir(&config.log.file.filename));
    if can_write_to_replayer_dir_file(&log_dir) {
        tidb_logutil::bg_logger().info(
            "use log dir as local path",
            &[tidb_log::Field::new("dir", tidb_log::Value::Str(log_dir.display().to_string()))],
        );
        return log_dir;
    }
    tidb_logutil::bg_logger().info(
        "use temp dir as local path",
        &[tidb_log::Field::new("dir", tidb_log::Value::Str(config.temp_dir.clone()))],
    );
    PathBuf::from(&config.temp_dir)
}

/// Go `canWriteToReplayerDirFile`: opens, writes one byte to and removes
/// `<dir>/replayer/test_<time>.txt` (Go does not create the directory).
fn can_write_to_replayer_dir_file(dir: &Path) -> bool {
    let time = chrono::Local::now().format("%Y%m%d%H%M%S");
    let path = dir.join("replayer").join(format!("test_{time}.txt"));
    let written = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .and_then(|mut file| std::io::Write::write_all(&mut file, &[0]));
    if written.is_err() {
        tidb_logutil::bg_logger().warn(
            "cannot write to file",
            &[tidb_log::Field::new("path", tidb_log::Value::Str(path.display().to_string()))],
        );
    }
    let _ = std::fs::remove_file(&path);
    written.is_ok()
}

#[cfg(test)]
mod tests;
