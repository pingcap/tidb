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

//! `pkg/util/chunk/row_in_disk.go`.
//!
//! PORTED HERE: [`ReaderWithCache`], the positional reader that lets a spill
//! file be READ WHILE STILL BEING WRITTEN. Without it a reader would see only
//! what the checksum writer has flushed in whole 1KiB blocks and would miss
//! the tail sitting in the writer's buffer.
//!
//! NOT PORTED (named, not silently missing): `DataInDiskByRows`,
//! `rowInDisk`/`diskFormatRow`, and the `RowPtr`-addressed random access built
//! on them. The chunk-addressed container in `crate::chunk_in_disk` is what
//! the sort's spill uses; the row-addressed one lands with `RowContainer`.

use tidb_util::layered_io::{ReadAt, ReadAtResult};

/// Go `ReaderWithCache`: reads through to `reader`, then tops the result up
/// from the writer's not-yet-flushed cache.
pub struct ReaderWithCache<R> {
    reader: R,
    cache_off: i64,
    cache: Vec<u8>,
}

impl<R: ReadAt> ReaderWithCache<R> {
    /// Go `NewReaderWithCache`.
    ///
    /// Go shares the writer's cache slice by reference and sees later writes
    /// through it; a snapshot is taken here instead, because the Rust writer
    /// owns its buffer mutably. Each read builds a fresh reader from the
    /// writer's current cache, so a reader never serves a stale tail.
    #[must_use]
    pub fn new(reader: R, cache: &[u8], cache_off: i64) -> Self {
        ReaderWithCache {
            reader,
            cache_off,
            cache: cache.to_vec(),
        }
    }
}

impl<R: ReadAt> ReadAt for ReaderWithCache<R> {
    fn read_at(&self, destination: &mut [u8], offset: i64) -> ReadAtResult {
        let result = self.reader.read_at(destination, offset);
        let Some(error) = &result.error else {
            return result;
        };
        if !error.is_eof() {
            return result;
        }
        let read_cnt = result.n;
        if read_cnt >= destination.len() {
            return result;
        }

        // The caller's buffer is not full, so the rest must come from cache.
        let remaining = &mut destination[read_cnt..];
        let begin = (offset - self.cache_off).max(0) as usize;
        let begin = begin.min(self.cache.len());
        let mut end = begin + remaining.len();
        let mut hit_eof = false;
        if end > self.cache.len() {
            hit_eof = true;
            end = self.cache.len();
        }
        let copied = end - begin;
        remaining[..copied].copy_from_slice(&self.cache[begin..end]);
        let total = read_cnt + copied;
        if hit_eof {
            ReadAtResult::eof(total)
        } else {
            ReadAtResult::ok(total)
        }
    }
}
