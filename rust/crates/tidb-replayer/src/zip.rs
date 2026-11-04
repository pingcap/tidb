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

//! Go `archive/zip`'s `Writer` as the plan replayer uses it: `Create(name)`
//! entries (Deflate, no modification time, sizes in a trailing data
//! descriptor), then `Close` writes the central directory. Go's own deflate
//! bytes differ from flate2's; any zip reader inflates both.

use std::io::{self, Write};

/// Go `zip.Writer`.
pub struct ZipWriter<W: Write> {
    writer: W,
    offset: u64,
    entries: Vec<Entry>,
    current: Option<Current>,
    closed: bool,
}

struct Entry {
    name: String,
    flags: u16,
    crc32: u32,
    compressed: u64,
    uncompressed: u64,
    offset: u64,
}

struct Current {
    entry: Entry,
    encoder: flate2::write::DeflateEncoder<Vec<u8>>,
    hasher: crc32fast::Hasher,
}

/// Go `zipVersion20`.
const ZIP_VERSION_20: u16 = 20;
/// Go `Deflate`.
const DEFLATE: u16 = 8;

impl<W: Write> ZipWriter<W> {
    /// Go `zip.NewWriter`.
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            offset: 0,
            entries: Vec::new(),
            current: None,
            closed: false,
        }
    }

    /// Go `Writer.Create(name)` followed by writing `contents`.
    pub fn create_file(&mut self, name: &str, contents: &[u8]) -> io::Result<()> {
        self.create(name)?;
        self.write_current(contents)
    }

    /// Go `Writer.Create`: starts a Deflate entry with a data descriptor
    /// (flag 0x8) and the UTF-8 flag (0x800) for a non-ASCII name.
    pub fn create(&mut self, name: &str) -> io::Result<()> {
        self.finish_current()?;
        let mut flags = 0x8;
        if !name.is_ascii() {
            flags |= 0x800;
        }
        let entry = Entry {
            name: name.to_owned(),
            flags,
            crc32: 0,
            compressed: 0,
            uncompressed: 0,
            offset: self.offset,
        };
        // Local file header: sizes and CRC follow in the data descriptor.
        let mut header = Vec::with_capacity(30 + name.len());
        header.extend_from_slice(&0x0403_4b50_u32.to_le_bytes());
        header.extend_from_slice(&ZIP_VERSION_20.to_le_bytes());
        header.extend_from_slice(&flags.to_le_bytes());
        header.extend_from_slice(&DEFLATE.to_le_bytes());
        header.extend_from_slice(&0_u16.to_le_bytes()); // modified time
        header.extend_from_slice(&0_u16.to_le_bytes()); // modified date
        header.extend_from_slice(&0_u32.to_le_bytes()); // crc32
        header.extend_from_slice(&0_u32.to_le_bytes()); // compressed size
        header.extend_from_slice(&0_u32.to_le_bytes()); // uncompressed size
        header.extend_from_slice(&(name.len() as u16).to_le_bytes());
        header.extend_from_slice(&0_u16.to_le_bytes()); // extra length
        header.extend_from_slice(name.as_bytes());
        self.write_raw(&header)?;
        self.current = Some(Current {
            entry,
            encoder: flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default()),
            hasher: crc32fast::Hasher::new(),
        });
        Ok(())
    }

    /// Writes to the entry `create` opened.
    pub fn write_current(&mut self, bytes: &[u8]) -> io::Result<()> {
        let current = self
            .current
            .as_mut()
            .ok_or_else(|| io::Error::other("zip: write to closed file"))?;
        current.hasher.update(bytes);
        current.entry.uncompressed += bytes.len() as u64;
        current.encoder.write_all(bytes)
    }

    fn write_raw(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all(bytes)?;
        self.offset += bytes.len() as u64;
        Ok(())
    }

    fn finish_current(&mut self) -> io::Result<()> {
        let Some(current) = self.current.take() else {
            return Ok(());
        };
        let mut entry = current.entry;
        let compressed = current.encoder.finish()?;
        entry.crc32 = current.hasher.finalize();
        entry.compressed = compressed.len() as u64;
        self.write_raw(&compressed)?;
        // Data descriptor.
        let mut descriptor = Vec::with_capacity(16);
        descriptor.extend_from_slice(&0x0807_4b50_u32.to_le_bytes());
        descriptor.extend_from_slice(&entry.crc32.to_le_bytes());
        descriptor.extend_from_slice(&(entry.compressed as u32).to_le_bytes());
        descriptor.extend_from_slice(&(entry.uncompressed as u32).to_le_bytes());
        self.write_raw(&descriptor)?;
        self.entries.push(entry);
        Ok(())
    }

    /// Go `Writer.Close`: finishes the open entry and writes the central
    /// directory and its end record.
    pub fn close(&mut self) -> io::Result<()> {
        if self.closed {
            return Err(io::Error::other("zip: writer closed twice"));
        }
        self.closed = true;
        self.finish_current()?;
        let start = self.offset;
        let entries = std::mem::take(&mut self.entries);
        for entry in &entries {
            let mut header = Vec::with_capacity(46 + entry.name.len());
            header.extend_from_slice(&0x0201_4b50_u32.to_le_bytes());
            header.extend_from_slice(&ZIP_VERSION_20.to_le_bytes()); // creator version
            header.extend_from_slice(&ZIP_VERSION_20.to_le_bytes()); // reader version
            header.extend_from_slice(&entry.flags.to_le_bytes());
            header.extend_from_slice(&DEFLATE.to_le_bytes());
            header.extend_from_slice(&0_u16.to_le_bytes());
            header.extend_from_slice(&0_u16.to_le_bytes());
            header.extend_from_slice(&entry.crc32.to_le_bytes());
            header.extend_from_slice(&(entry.compressed as u32).to_le_bytes());
            header.extend_from_slice(&(entry.uncompressed as u32).to_le_bytes());
            header.extend_from_slice(&(entry.name.len() as u16).to_le_bytes());
            header.extend_from_slice(&0_u16.to_le_bytes()); // extra length
            header.extend_from_slice(&0_u16.to_le_bytes()); // comment length
            header.extend_from_slice(&0_u16.to_le_bytes()); // disk number
            header.extend_from_slice(&0_u16.to_le_bytes()); // internal attributes
            header.extend_from_slice(&0_u32.to_le_bytes()); // external attributes
            header.extend_from_slice(&(entry.offset as u32).to_le_bytes());
            header.extend_from_slice(entry.name.as_bytes());
            self.write_raw(&header)?;
        }
        let size = self.offset - start;
        let mut end = Vec::with_capacity(22);
        end.extend_from_slice(&0x0605_4b50_u32.to_le_bytes());
        end.extend_from_slice(&0_u16.to_le_bytes()); // disk number
        end.extend_from_slice(&0_u16.to_le_bytes()); // directory disk
        end.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        end.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        end.extend_from_slice(&(size as u32).to_le_bytes());
        end.extend_from_slice(&(start as u32).to_le_bytes());
        end.extend_from_slice(&0_u16.to_le_bytes()); // comment length
        self.write_raw(&end)?;
        self.writer.flush()
    }

    /// The underlying writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

/// The entries of a zip written by [`ZipWriter`] (or any Deflate/Store zip
/// with a central directory), each name with its inflated bytes -- what
/// Go's `zip.NewReader` gives `PLAN REPLAYER LOAD`.
pub fn read_zip(bytes: &[u8]) -> io::Result<Vec<(String, Vec<u8>)>> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "zip: not a valid zip file");
    let end = (0..bytes.len().saturating_sub(21))
        .rev()
        .find(|&at| bytes[at..at + 4] == 0x0605_4b50_u32.to_le_bytes())
        .ok_or_else(invalid)?;
    let u16_at = |at: usize| u16::from_le_bytes([bytes[at], bytes[at + 1]]) as usize;
    let u32_at = |at: usize| u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]]) as usize;
    let count = u16_at(end + 10);
    let mut at = u32_at(end + 16);
    let mut files = Vec::with_capacity(count);
    for _ in 0..count {
        if bytes.get(at..at + 4) != Some(&0x0201_4b50_u32.to_le_bytes()[..]) {
            return Err(invalid());
        }
        let method = u16_at(at + 10);
        let crc32 = u32_at(at + 16) as u32;
        let compressed = u32_at(at + 20);
        let name_len = u16_at(at + 28);
        let extra_len = u16_at(at + 30);
        let comment_len = u16_at(at + 32);
        let local = u32_at(at + 42);
        let name = String::from_utf8_lossy(bytes.get(at + 46..at + 46 + name_len).ok_or_else(invalid)?).into_owned();
        at += 46 + name_len + extra_len + comment_len;
        let data_start = local + 30 + u16_at(local + 26) + u16_at(local + 28);
        let data = bytes.get(data_start..data_start + compressed).ok_or_else(invalid)?;
        let contents = match method {
            0 => data.to_vec(),
            8 => {
                let mut out = Vec::new();
                std::io::Read::read_to_end(&mut flate2::read::DeflateDecoder::new(data), &mut out)?;
                out
            }
            _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "zip: unsupported compression algorithm")),
        };
        // Go `checksumReader`: `zip: checksum error`.
        if crc32fast::hash(&contents) != crc32 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "zip: checksum error"));
        }
        files.push((name, contents));
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Entries written through Go's `Create` shape read back in order with
    /// their bytes, and an entry after another closes the first.
    #[test]
    fn a_written_zip_reads_back_its_entries() {
        let mut zip = ZipWriter::new(Vec::new());
        zip.create_file("sql/sql0.sql", b"select 1").unwrap();
        zip.create("explain.txt").unwrap();
        zip.write_current(b"TableDual_1\t1.00\n").unwrap();
        zip.write_current(b"second line\n").unwrap();
        zip.create_file("debug_trace/debug_trace0.json", b"").unwrap();
        zip.close().unwrap();
        assert!(zip.close().is_err());
        // A corrupted byte in the first entry's data fails its checksum.
        let mut corrupted = zip.into_inner();
        corrupted[30 + "sql/sql0.sql".len()] ^= 0xff;
        assert!(read_zip(&corrupted).is_err());
        let mut zip = ZipWriter::new(Vec::new());
        zip.create_file("sql/sql0.sql", b"select 1").unwrap();
        zip.create("explain.txt").unwrap();
        zip.write_current(b"TableDual_1\t1.00\n").unwrap();
        zip.write_current(b"second line\n").unwrap();
        zip.create_file("debug_trace/debug_trace0.json", b"").unwrap();
        zip.close().unwrap();
        let bytes = zip.into_inner();
        if let Ok(path) = std::env::var("TIDB_REPLAYER_ZIP_OUT") {
            std::fs::write(path, &bytes).unwrap();
        }
        assert_eq!(&bytes[..4], &0x0403_4b50_u32.to_le_bytes());
        assert_eq!(
            read_zip(&bytes).unwrap(),
            [
                ("sql/sql0.sql".to_owned(), b"select 1".to_vec()),
                ("explain.txt".to_owned(), b"TableDual_1\t1.00\nsecond line\n".to_vec()),
                ("debug_trace/debug_trace0.json".to_owned(), Vec::new()),
            ]
        );
    }
}
