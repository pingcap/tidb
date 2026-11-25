// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `kv_reader.go` and `stat_reader.go`.

use std::sync::Arc;

use tidb_gocontext::Context;
use tidb_objstore::storeapi::Storage;

use crate::byte_reader::{open_store_reader_and_seek, ByteReader};
use crate::codec::{decode_prop, RangeProperty};
use crate::{no_eof, Result};

/// Go `DefaultReadBufferSize`: 64 KiB, a third of it for the user and two
/// thirds prefetched from storage.
pub const DEFAULT_READ_BUFFER_SIZE: usize = 64 * 1024;

/// Go `KVReader`: reads a data file's `<key-len><value-len><key><value>`
/// blocks.
pub struct KVReader {
    pub(crate) byte_reader: ByteReader,
}

impl KVReader {
    /// Go `NewKVReader`.
    ///
    /// # Errors
    /// The file could not be opened or read.
    pub fn new(
        ctx: &Context,
        name: &str,
        store: &dyn Storage,
        init_file_offset: u64,
        buf_size: usize,
    ) -> Result<Self> {
        // Some tests use a buffer size under 3.
        let one_third = (buf_size / 3).max(1);
        let sr = open_store_reader_and_seek(ctx, store, name, init_file_offset, one_third * 2)?;
        Ok(Self {
            byte_reader: ByteReader::new(ctx, sr, one_third)?,
        })
    }

    /// Go `getFileSize`.
    pub(crate) fn get_file_size(&self) -> Result<i64> {
        Ok(self.byte_reader.storage_reader.get_file_size()?)
    }

    /// Reads the next key and value into owned buffers.
    ///
    /// # Errors
    /// [`crate::Error::Eof`] at the end, or a truncated or unreadable file.
    pub fn next_kv(&mut self) -> Result<(Vec<u8>, Vec<u8>)> {
        let (key, value) = self.next_kv_ref()?;
        Ok((key.to_vec(), value.to_vec()))
    }

    /// Go `NextKV`. Borrows the next key and value until the reader advances.
    ///
    /// # Errors
    /// [`crate::Error::Eof`] at the end, or a truncated or unreadable file.
    #[inline]
    pub fn next_kv_ref(&mut self) -> Result<(&[u8], &[u8])> {
        let key_len = u64::from_be_bytes(
            self.byte_reader
                .read_n_bytes(8)?
                .try_into()
                .expect("8 bytes"),
        ) as usize;
        let val_len = u64::from_be_bytes(
            self.byte_reader
                .read_n_bytes(8)
                .map_err(no_eof)?
                .try_into()
                .expect("8 bytes"),
        ) as usize;
        let kv = self
            .byte_reader
            .read_n_bytes(key_len + val_len)
            .map_err(no_eof)?;
        Ok(kv.split_at(key_len))
    }

    /// Go `EnableConcurrentRead`.
    pub fn enable_concurrent_read(
        &mut self,
        store: Arc<dyn Storage>,
        filename: &str,
        concurrency: usize,
        buf_size_per_conc: usize,
    ) {
        self.byte_reader
            .enable_concurrent_read(store, filename, concurrency, buf_size_per_conc);
    }

    /// Go `SwitchConcurrentMode`.
    ///
    /// # Errors
    /// The file could not be re-positioned or read.
    pub fn switch_concurrent_mode(&mut self, use_concurrent: bool) -> Result<()> {
        self.byte_reader.switch_concurrent_mode(use_concurrent)
    }

    /// Go `Close`.
    ///
    /// # Errors
    /// The storage reader failed to close.
    pub fn close(&mut self) -> Result<()> {
        self.byte_reader.close()
    }
}

/// Go `StatsReader`: reads a stat file's range properties.
pub struct StatsReader {
    byte_reader: ByteReader,
}

impl StatsReader {
    /// Go `NewStatsReader`.
    ///
    /// # Errors
    /// The file could not be opened or read; [`crate::Error::Eof`] when it
    /// is empty.
    pub fn new(ctx: &Context, store: &dyn Storage, name: &str, buf_size: usize) -> Result<Self> {
        let sr = open_store_reader_and_seek(ctx, store, name, 0, 250 * 1024)?;
        Ok(Self {
            byte_reader: ByteReader::new(ctx, sr, buf_size)?,
        })
    }

    /// Go `NextProp`.
    ///
    /// # Errors
    /// [`crate::Error::Eof`] at the end, or a truncated or unreadable file.
    pub fn next_prop(&mut self) -> Result<RangeProperty> {
        let len = u32::from_be_bytes(
            self.byte_reader
                .read_n_bytes(4)?
                .try_into()
                .expect("4 bytes"),
        ) as usize;
        let bytes = self.byte_reader.read_n_bytes(len).map_err(no_eof)?;
        Ok(decode_prop(bytes))
    }

    /// Go `Close`.
    ///
    /// # Errors
    /// The storage reader failed to close.
    pub fn close(&mut self) -> Result<()> {
        self.byte_reader.close()
    }
}
