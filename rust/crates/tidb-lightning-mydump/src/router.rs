//! Go `pkg/lightning/mydump/router.go`: source and compression types.

/// Go `SourceType`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(i32)]
pub enum SourceType {
    #[default]
    Ignore = 0,
    SchemaSchema,
    TableSchema,
    SQL,
    CSV,
    Parquet,
    ViewSchema,
}
impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Ignore => "ignore",
            Self::SchemaSchema => "schema-schema",
            Self::TableSchema => "table-schema",
            Self::SQL => "sql",
            Self::CSV => "csv",
            Self::Parquet => "parquet",
            Self::ViewSchema => "view-schema",
        })
    }
}
/// Go `Compression`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
pub enum Compression {
    #[default]
    None = 0,
    GZ,
    LZ4,
    ZStd,
    XZ,
    LZO,
    Snappy,
}

/// Go `ParseCompressionOnFileExtension`.
pub fn parse_compression_on_file_extension(filename: &str) -> Compression {
    let base = filename.rsplit('/').next().unwrap_or(filename);
    match base
        .rsplit_once('.')
        .map(|(_, ext)| ext.to_ascii_lowercase())
        .as_deref()
    {
        Some("gz" | "gzip") => Compression::GZ,
        Some("lz4") => Compression::LZ4,
        Some("zstd" | "zst") => Compression::ZStd,
        Some("xz") => Compression::XZ,
        Some("lzo") => Compression::LZO,
        Some("snappy") => Compression::Snappy,
        _ => Compression::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `TestImportSourceTypes` probe of `SourceType.String` and
    /// `ParseCompressionOnFileExtension`.
    #[test]
    fn test_import_source_types() {
        for line in include_str!("../testdata/source_types_go.jsonl").lines() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            if row[0] == "source" {
                let all = [
                    SourceType::Ignore,
                    SourceType::SchemaSchema,
                    SourceType::TableSchema,
                    SourceType::SQL,
                    SourceType::CSV,
                    SourceType::Parquet,
                    SourceType::ViewSchema,
                ];
                assert_eq!(
                    all[row[1].as_u64().unwrap() as usize].to_string(),
                    row[2].as_str().unwrap()
                );
            } else {
                assert_eq!(
                    parse_compression_on_file_extension(row[1].as_str().unwrap()) as i32,
                    row[2].as_i64().unwrap() as i32
                );
            }
        }
    }
}
