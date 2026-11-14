//! Go `LoadDataController.InitDataFiles`: server-disk path and glob discovery.
use super::file_match::filepath_match;
use crate::DriverError;
use std::path::{Path, PathBuf};
use tidb_lightning_mydump::RawFile;

/// Go `LoadDataController.InitDataFiles`'s local source validation and file walk.
/// Format/compression sampling is applied by the controller after discovery.
pub(super) fn find_local_data_files(
    path: &str,
    in_import_into: bool,
) -> Result<(PathBuf, Vec<RawFile>), DriverError> {
    if !in_import_into {
        return Err(DriverError::from(
            tidb_exeerrors::ERR_LOAD_DATA_FROM_SERVER_DISK
                .gen_with_stack_by_args(&[tidb_ast::redact_url(path).as_str().into()]),
        ));
    }
    let p = Path::new(path);
    if !p.is_absolute() {
        return Err(invalid_uri(
            "file location should be absolute path when import from server disk",
        ));
    }
    let ext = path
        .rsplit('/')
        .next()
        .unwrap_or("")
        .rsplit_once('.')
        .map(|(_, ext)| ext)
        .unwrap_or("")
        .to_ascii_lowercase();
    if !matches!(
        ext.as_str(),
        "csv" | "sql" | "parquet" | "gz" | "gzip" | "zstd" | "zst" | "snappy"
    ) {
        return Err(invalid_uri(
            "the file suffix is not supported when import from server disk",
        ));
    }
    let base = p.parent().unwrap_or(Path::new("/"));
    std::fs::metadata(base).map_err(|e| invalid_uri(&io_message("stat", base, &e)))?;
    let file_name = p.file_name().unwrap_or_default().to_string_lossy();
    let escaped = tidb_stringutil::escape_glob_question_mark(file_name.as_bytes());
    filepath_match(escaped.as_bytes(), b"")
        .map_err(|e| invalid_uri(&format!("Glob pattern error: {e}")))?;
    // InitDataStore changes URL.Path, preserving the query and fragment; the
    // empty-scheme backend then uses URL.String as its filesystem base.
    let mut storage_base = String::new();
    for byte in base.to_string_lossy().bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~$&+,/:;=@".contains(&byte) {
            storage_base.push(byte as char);
        } else {
            use std::fmt::Write;
            write!(storage_base, "%{byte:02X}").unwrap();
        }
    }
    if let Some(index) = path.find(['?', '#']) {
        storage_base.push_str(&path[index..].replace('+', "%2B"));
    }
    let storage_base = PathBuf::from(storage_base);
    let source_path = storage_base.join(file_name.as_ref());
    if !file_name.contains(['*', '[']) {
        let file = std::fs::File::open(&source_path).map_err(|e| {
            cant_read(
                &io_message("open", &source_path, &e),
                "Please check the file location is correct",
            )
        })?;
        use std::io::Seek;
        let size = (&file).seek(std::io::SeekFrom::End(0)).map_err(|e| {
            cant_read(
                &io_message("seek", &source_path, &e),
                "failed to read file size by seek",
            )
        })?;
        return Ok((
            storage_base,
            vec![RawFile {
                path: file_name.into_owned(),
                size: size as i64,
            }],
        ));
    }
    let entries = std::fs::read_dir(&storage_base)
        .map_err(|e| cant_read(&io_message("open", &storage_base, &e), "failed to walk dir"))?;
    let mut entries = entries
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| cant_read(&io_message("readdir", base, &e), "failed to walk dir"))?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    let mut files = Vec::new();
    for entry in entries {
        let path = entry.path();
        let meta = match std::fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(cant_read(
                    &io_message("lstat", &path, &e),
                    "failed to walk dir",
                ))
            }
        };
        if meta.is_dir() {
            // filepath.Walk reads a directory before invoking its callback, even
            // when the callback returns SkipDir. Thus inaccessible subdirs fail.
            std::fs::read_dir(&path)
                .map_err(|e| cant_read(&io_message("open", &path, &e), "failed to walk dir"))?;
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if filepath_match(escaped.as_bytes(), name.as_bytes()).unwrap_or(false) {
            let size = if meta.is_file() {
                meta.len() as i64
            } else {
                std::fs::metadata(&path)
                    .map(|m| m.len() as i64)
                    .unwrap_or(0)
            };
            files.push(RawFile { path: name, size });
        }
    }
    Ok((storage_base, files))
}

/// Go `exeerrors.ErrLoadDataInvalidURI` for the source data URI.
fn invalid_uri(reason: &str) -> DriverError {
    DriverError::from(
        tidb_exeerrors::ERR_LOAD_DATA_INVALID_URI
            .gen_with_stack_by_args(&["data source".into(), reason.into()]),
    )
}
/// Go `exeerrors.ErrLoadDataCantRead`.
fn cant_read(reason: &str, hint: &str) -> DriverError {
    DriverError::from(
        tidb_exeerrors::ERR_LOAD_DATA_CANT_READ
            .gen_with_stack_by_args(&[reason.into(), hint.into()]),
    )
}
/// Go `os.PathError.Error` with URL redaction before including the location.
fn io_message(operation: &str, path: &Path, error: &std::io::Error) -> String {
    let reason = match error.kind() {
        std::io::ErrorKind::NotFound => "no such file or directory".to_owned(),
        std::io::ErrorKind::PermissionDenied => "permission denied".to_owned(),
        _ => error.to_string(),
    };
    format!(
        "{operation} {}: {reason}",
        tidb_ast::redact_url(&path.to_string_lossy())
    )
}
