//! Go `pkg/lightning/mydump/loader.go`: source-file metadata.
use crate::{Compression, SourceType};

/// Go `RawFile`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RawFile {
    pub path: String,
    pub size: i64,
}

/// Go `SourceFileMeta`: file identity and sizes used by import planning.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SourceFileMeta {
    pub path: String,
    pub r#type: SourceType,
    pub compression: Compression,
    pub sort_key: String,
    pub file_size: i64,
    pub real_size: i64,
    pub rows: i64,
}
