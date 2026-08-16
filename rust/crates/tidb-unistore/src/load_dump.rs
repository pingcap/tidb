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

//! Go `load_dump.go`: persisting a [`MemStore`] to a file and reading it back.
//!
//! The wire format is Go's, byte for byte: a little-endian `u32` length
//! followed by that many bytes, first for the caller's meta blob and then for
//! each key and value in ascending key order.

use std::fs::File;
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::Path;

use crate::lockstore::MemStore;

impl MemStore {
    /// Go `LoadFromFile`: loads entries from `file_name` and returns the meta
    /// blob stored ahead of them. A missing file yields `Ok(None)`, as Go's
    /// `os.IsNotExist` branch returns `nil, nil`.
    ///
    /// narrowing: Go's `defer func() { err = f.Close() }()` overwrites the
    /// named return value, so every one of its error paths actually reports
    /// `nil, nil` — a mid-file decode failure is silently indistinguishable
    /// from an absent file. That is a bug, not a contract, and it is not
    /// reproduced: a decode failure here returns the error.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error when the file exists but cannot be
    /// opened or decoded. A truncated record is [`ErrorKind::UnexpectedEof`].
    pub fn load_from_file(
        &mut self,
        file_name: impl AsRef<Path>,
    ) -> std::io::Result<Option<Vec<u8>>> {
        let f = match File::open(file_name) {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let mut reader = BufReader::new(f);
        let Some(meta) = read_item(&mut reader)? else {
            return Err(std::io::Error::from(ErrorKind::UnexpectedEof));
        };
        while let Some(key) = read_item(&mut reader)? {
            let Some(val) = read_item(&mut reader)? else {
                return Err(std::io::Error::from(ErrorKind::UnexpectedEof));
            };
            self.put(&key, &val);
        }
        Ok(Some(meta))
    }

    /// Go `DumpToFile`: writes `meta` and every entry to `file_name`, through
    /// a `.tmp` sibling that is renamed into place once flushed and synced.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error from creating, writing, syncing, or
    /// renaming the temporary file.
    pub fn dump_to_file(&self, file_name: impl AsRef<Path>, meta: &[u8]) -> std::io::Result<()> {
        let file_name = file_name.as_ref();
        let mut tmp_file_name = file_name.as_os_str().to_owned();
        tmp_file_name.push(".tmp");
        let f = File::create(&tmp_file_name)?;
        let mut writer = BufWriter::new(f);
        write_item(&mut writer, meta)?;
        let mut it = self.new_iterator();
        it.seek_to_first();
        while it.valid() {
            write_item(&mut writer, it.key())?;
            write_item(&mut writer, it.value())?;
            it.next();
        }
        let f = writer
            .into_inner()
            .map_err(std::io::IntoInnerError::into_error)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp_file_name, file_name)
    }
}

/// Go `readItem`: `Ok(None)` stands for Go's `io.EOF` at a record boundary.
///
/// Go threads a caller-owned scratch buffer through this to avoid an
/// allocation per record; Rust returns an owned `Vec` because the value is
/// handed straight to `Put`, which copies it into the arena anyway.
fn read_item(reader: &mut impl Read) -> std::io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let l = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; l];
    reader.read_exact(&mut buf)?;
    Ok(Some(buf))
}

/// Go `writeItem`.
fn write_item(writer: &mut impl Write, data: &[u8]) -> std::io::Result<()> {
    writer.write_all(&(data.len() as u32).to_le_bytes())?;
    writer.write_all(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::num_to_key;

    /// Not a Go test: the Go package ships no coverage for `LoadFromFile` /
    /// `DumpToFile`, but the on-disk format is a compatibility surface — a
    /// unistore restart reads back what a previous process wrote.
    #[test]
    fn test_dump_load_round_trip() {
        let dir = std::env::temp_dir().join(format!(
            "tidb-unistore-lockstore-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("lockstore");

        let mut src = MemStore::new(1 << 12);
        for i in 0..500usize {
            let key = num_to_key(i);
            let val = key.repeat(3);
            src.put(&key, &val);
        }
        src.dump_to_file(&path, b"the-meta").unwrap();

        let mut dst = MemStore::new(1 << 12);
        let meta = dst.load_from_file(&path).unwrap();
        assert_eq!(meta.as_deref(), Some(b"the-meta".as_slice()));
        assert_eq!(dst.len(), src.len());

        let mut a = src.new_iterator();
        let mut b = dst.new_iterator();
        a.seek_to_first();
        b.seek_to_first();
        while a.valid() {
            assert!(b.valid());
            assert_eq!(a.key(), b.key());
            assert_eq!(a.value(), b.value());
            a.next();
            b.next();
        }
        assert!(!b.valid());

        // An absent file is not an error; Go returns `nil, nil` for it.
        let mut empty = MemStore::new(1 << 12);
        assert!(empty.load_from_file(dir.join("nope")).unwrap().is_none());

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
