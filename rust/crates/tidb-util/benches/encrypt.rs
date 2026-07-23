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

//! Executable translation of `pkg/util/encrypt.BenchmarkReadAt`.

#![allow(non_snake_case)]

use std::hint::black_box;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tidb_util::checksum;
use tidb_util::encrypt;
use tidb_util::layered_io::{CloseWrite, ReadAt, ReadAtResult};

const ITERATIONS: usize = 1_000_000;

#[derive(Clone, Default)]
struct MemoryFile(Arc<Mutex<Vec<u8>>>);

impl Write for MemoryFile {
    fn write(&mut self, source: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("memory file")
            .extend_from_slice(source);
        Ok(source.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl CloseWrite for MemoryFile {
    fn close(self) -> io::Result<()> {
        Ok(())
    }
}

impl ReadAt for MemoryFile {
    fn read_at(&self, destination: &mut [u8], offset: i64) -> ReadAtResult {
        let bytes = self.0.lock().expect("memory file");
        if offset < 0 || offset as usize > bytes.len() {
            return ReadAtResult::eof(0);
        }
        let copied = destination
            .len()
            .min(bytes.len().saturating_sub(offset as usize));
        destination[..copied].copy_from_slice(&bytes[offset as usize..offset as usize + copied]);
        if copied < destination.len() {
            ReadAtResult::eof(copied)
        } else {
            ReadAtResult::ok(copied)
        }
    }
}

fn measure(name: &str, logical_len: usize, reader: impl ReadAt) {
    let started = Instant::now();
    let mut buffer = [0_u8; 10];
    for index in 0..ITERATIONS {
        let result = reader.read_at(&mut buffer, (index % logical_len) as i64);
        if let Some(error) = result.error {
            assert!(error.is_eof());
        }
        black_box(&buffer[..result.n]);
    }
    println!("{name}: {:?}", started.elapsed());
}

fn BenchmarkReadAt() {
    let data = b"0123456789".repeat(1_020);

    let mut plain_file = MemoryFile::default();
    plain_file.write_all(&data).expect("write plain benchmark");
    measure("data->file", data.len(), plain_file);

    let checksum_file = MemoryFile::default();
    let mut checksum_writer = checksum::Writer::new(checksum_file.clone());
    checksum_writer
        .write_all(&data)
        .expect("write checksum benchmark");
    checksum_writer.close().expect("close checksum benchmark");
    measure(
        "data->checksum->file",
        data.len(),
        checksum::Reader::new(checksum_file),
    );

    let cipher = encrypt::CtrCipher::new().expect("create benchmark cipher");
    let encrypted_file = MemoryFile::default();
    let encrypted_writer = encrypt::Writer::new(encrypted_file.clone(), &cipher);
    let mut checksum_over_encrypt = checksum::Writer::new(encrypted_writer);
    checksum_over_encrypt
        .write_all(&data)
        .expect("write encrypted checksum benchmark");
    checksum_over_encrypt
        .close()
        .expect("close encrypted checksum benchmark");
    measure(
        "data->checksum->encrypt->file",
        data.len(),
        checksum::Reader::new(encrypt::Reader::new(encrypted_file, &cipher)),
    );
}

fn main() {
    BenchmarkReadAt();
}
