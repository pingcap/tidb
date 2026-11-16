// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `file.go`: a data file being written, with its range properties.

use tidb_gocontext::Context;
use tidb_objstore::objectio::Writer as ObjWriter;

use crate::writer::RangePropertiesCollector;

/// Go `statSuffix`.
pub(crate) const STAT_SUFFIX: &str = "_stat";
/// Go `dupSuffix`.
pub(crate) const DUP_SUFFIX: &str = "_dup";
/// Go `LengthBytes`: a key or value length is a big-endian `u64`.
pub const LENGTH_BYTES: usize = 8;

/// Go `KeyValueStore`: writes KV pairs to a data file and keeps its range
/// properties in a [`RangePropertiesCollector`].
pub struct KeyValueStore<'a> {
    data_writer: &'a mut dyn ObjWriter,
    rc: Option<&'a mut RangePropertiesCollector>,
    ctx: Context,
    pub(crate) offset: u64,
}

impl<'a> KeyValueStore<'a> {
    /// Go `NewKeyValueStore`.
    pub fn new(
        ctx: &Context,
        data_writer: &'a mut dyn ObjWriter,
        rc: Option<&'a mut RangePropertiesCollector>,
    ) -> Self {
        Self {
            data_writer,
            rc,
            ctx: ctx.clone(),
            offset: 0,
        }
    }

    /// Go `NewKeyValueStore` continuing a file already `offset` bytes long.
    pub fn resume(
        ctx: &Context,
        data_writer: &'a mut dyn ObjWriter,
        rc: Option<&'a mut RangePropertiesCollector>,
        offset: u64,
    ) -> Self {
        Self {
            data_writer,
            rc,
            ctx: ctx.clone(),
            offset,
        }
    }

    /// Go `addEncodedData`: `data` is `keyLen + valueLen + key + value`;
    /// keys must ascend strictly over a store's calls.
    ///
    /// # Errors
    /// The data file could not be written.
    pub fn add_encoded_data(&mut self, data: &[u8]) -> crate::Result<()> {
        self.data_writer.write(&self.ctx, data)?;
        self.offset += data.len() as u64;
        if let Some(rc) = self.rc.as_deref_mut() {
            rc.on_next_encoded_data(data, self.offset);
        }
        Ok(())
    }

    /// Go `Offset`.
    #[must_use]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Go `AddRawKV`.
    ///
    /// # Errors
    /// The data file could not be written.
    pub fn add_raw_kv(&mut self, key: &[u8], val: &[u8]) -> crate::Result<()> {
        let mut buf = vec![0; key.len() + val.len() + LENGTH_BYTES * 2];
        encode_to_buf(&mut buf, key, val);
        self.add_encoded_data(&buf)
    }

    /// Go `Finish`: appends the last range property.
    pub fn finish(&mut self) {
        if let Some(rc) = self.rc.as_deref_mut() {
            rc.on_file_end();
        }
    }
}

/// Go `encodeToBuf`: `buf` is exactly `16 + len(key) + len(value)` long.
pub(crate) fn encode_to_buf(buf: &mut [u8], key: &[u8], value: &[u8]) {
    debug_assert_eq!(buf.len(), LENGTH_BYTES * 2 + key.len() + value.len());
    buf[..LENGTH_BYTES].copy_from_slice(&(key.len() as u64).to_be_bytes());
    buf[LENGTH_BYTES..2 * LENGTH_BYTES].copy_from_slice(&(value.len() as u64).to_be_bytes());
    buf[2 * LENGTH_BYTES..2 * LENGTH_BYTES + key.len()].copy_from_slice(key);
    buf[2 * LENGTH_BYTES + key.len()..].copy_from_slice(value);
}
