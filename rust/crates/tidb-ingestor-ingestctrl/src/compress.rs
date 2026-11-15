// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/compress.go`.
use std::io::{Read, Write};
/// Go `gzipCompressor`.
pub struct GzipCompressor;
impl GzipCompressor {
    /// Go `gzipCompressor.Type`.
    pub fn type_name(&self) -> &'static str {
        "gzip"
    }
    /// Go `gzipCompressor.Do`.
    pub fn compress(&self, output: impl Write, input: &[u8]) -> std::io::Result<()> {
        let mut writer = flate2::write::GzEncoder::new(output, flate2::Compression::default());
        writer.write_all(input)?;
        writer.finish()?;
        Ok(())
    }
}
/// Go `gzipDecompressor`.
pub struct GzipDecompressor;
impl GzipDecompressor {
    /// Go `gzipDecompressor.Type`.
    pub fn type_name(&self) -> &'static str {
        "gzip"
    }
    /// Go `gzipDecompressor.Do`.
    pub fn decompress(&self, input: impl Read) -> std::io::Result<Vec<u8>> {
        let mut output = Vec::new();
        flate2::read::MultiGzDecoder::new(input).read_to_end(&mut output)?;
        Ok(output)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    /// Go `TestGzipCompressor input fixture`.
    fn input() -> Vec<u8> {
        let mut n = 1u64;
        (0..1 << 20)
            .map(|_| {
                n ^= n << 13;
                n ^= n >> 7;
                n ^= n << 17;
                n as u8
            })
            .collect()
    }
    /// Go `TestGzipCompressor`.
    #[test]
    fn test_gzip_compressor() {
        let input = input();
        assert_eq!(GzipCompressor.type_name(), "gzip");
        let mut compressed = Vec::new();
        GzipCompressor.compress(&mut compressed, &input).unwrap();
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(&compressed[..])
            .read_to_end(&mut decoded)
            .unwrap();
        assert_eq!(input, decoded);
    }
    /// Go `TestGzipDecompressor`.
    #[test]
    fn test_gzip_decompressor() {
        assert_eq!(GzipDecompressor.type_name(), "gzip");
        let input = input();
        let mut compressed = Vec::new();
        flate2::read::GzEncoder::new(&input[..], flate2::Compression::default())
            .read_to_end(&mut compressed)
            .unwrap();
        assert_eq!(GzipDecompressor.decompress(&compressed[..]).unwrap(), input);
        assert!(GzipDecompressor.decompress(&b"invalid"[..]).is_err());
        compressed.pop();
        assert!(GzipDecompressor.decompress(&compressed[..]).is_err());
    }
}
