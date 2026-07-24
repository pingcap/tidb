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

//! Complete transcreation of Go `pkg/util/prefetch` (`reader.go`).
//!
//! A reader that prefetches from an underlying reader on a background thread,
//! using two ping-pong buffers handed across an *unbuffered* rendezvous channel
//! so that exactly one buffer is filled ahead of the consumer.
//!
//! Go/Rust idiom adaptations, all behavior-preserving:
//! - Go's goroutine + `sync.WaitGroup` become a [`std::thread`] + `JoinHandle`.
//! - Go's unbuffered `chan []byte` + `select { <-closedCh; bufCh<-buf }` become
//!   a [`std::sync::mpsc::sync_channel`] of bound 0; cancellation is dropping the
//!   receiver, which makes the producer's blocked `send` fail.
//! - Go signals end-of-stream with an `io.EOF` *error*; Rust's [`Read`] signals
//!   it with `Ok(0)`. The converted "`ErrUnexpectedEOF` at exactly `rangeSize`"
//!   case therefore becomes a clean `Ok(0)`, while a genuine underlying
//!   `UnexpectedEof` propagates as an `Err`.
//! - Go reuses two ping-pong buffers; Rust hands ownership of a fresh `Vec` per
//!   chunk across the channel. The rendezvous still fills exactly one buffer
//!   ahead, so the observable prefetch depth is identical; only the allocation
//!   is not reused.
//! - Go's `io.ReadCloser.Close` returns the underlying `Close` error; Rust's
//!   `Read` has no `Close`, so the underlying reader is dropped when the
//!   producer thread ends and [`PrefetchReader::close`] returns `Ok(())`.

use std::io::{self, Cursor, Read};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

/// The terminal state of the producer, read by the consumer once the channel
/// closes. `Eof` maps to `Read` returning `Ok(0)`.
enum Terminal {
    Pending,
    Eof,
    Failed(io::ErrorKind, String),
}

/// Outcome of one `read_full` attempt.
enum Fill {
    Full,
    Eof,
    Failed(io::Error),
}

/// Reads until `buf` is full, mirroring Go's `io.ReadFull`: a clean EOF before
/// any byte yields [`Fill::Eof`]; an EOF after a partial fill yields a
/// [`Fill::Failed`] carrying `UnexpectedEof`; any underlying error is returned
/// as-is.
fn read_full(reader: &mut impl Read, buf: &mut [u8]) -> (usize, Fill) {
    let mut n = 0;
    while n < buf.len() {
        match reader.read(&mut buf[n..]) {
            Ok(0) => {
                return if n == 0 {
                    (0, Fill::Eof)
                } else {
                    (
                        n,
                        Fill::Failed(io::Error::from(io::ErrorKind::UnexpectedEof)),
                    )
                };
            }
            Ok(m) => n += m,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return (n, Fill::Failed(e)),
        }
    }
    (n, Fill::Full)
}

fn run<R: Read>(
    mut reader: R,
    sender: SyncSender<Vec<u8>>,
    shared: Arc<Mutex<Terminal>>,
    range_size: i64,
    half: usize,
) {
    let mut read_size: i64 = 0;
    loop {
        let mut buffer = vec![0u8; half];
        let (n, fill) = read_full(&mut reader, &mut buffer);
        buffer.truncate(n);
        read_size += n as i64;

        // Hand the chunk to the consumer. A send error means the receiver was
        // dropped (the reader was closed), so stop.
        if sender.send(buffer).is_err() {
            return;
        }

        match fill {
            Fill::Full => {}
            Fill::Eof => {
                *shared.lock().unwrap() = Terminal::Eof;
                return;
            }
            Fill::Failed(e) => {
                // Because we are prefetching, the buffer may be larger than the
                // caller needs, so `io.ReadFull`'s partial-EOF at exactly the
                // range size is a clean end-of-stream. A premature underlying
                // `UnexpectedEof` (read size below the range) is a real error.
                let terminal =
                    if e.kind() == io::ErrorKind::UnexpectedEof && read_size == range_size {
                        Terminal::Eof
                    } else {
                        Terminal::Failed(e.kind(), e.to_string())
                    };
                *shared.lock().unwrap() = terminal;
                return;
            }
        }
    }
}

/// A reader that prefetches data from an underlying reader.
pub struct PrefetchReader {
    receiver: Option<Receiver<Vec<u8>>>,
    cur: Option<Cursor<Vec<u8>>>,
    err: Arc<Mutex<Terminal>>,
    handle: Option<JoinHandle<()>>,
    closed: bool,
}

/// Creates a new [`PrefetchReader`] over `reader`, prefetching `prefetch_size`
/// bytes total (split across two ping-pong buffers). `range_size` is the total
/// size of the data range being read.
pub fn new_reader<R: Read + Send + 'static>(
    reader: R,
    range_size: i64,
    prefetch_size: usize,
) -> PrefetchReader {
    let (sender, receiver) = sync_channel::<Vec<u8>>(0);
    let err = Arc::new(Mutex::new(Terminal::Pending));
    let shared = Arc::clone(&err);
    let half = prefetch_size / 2;
    let handle = thread::spawn(move || run(reader, sender, shared, range_size, half));

    PrefetchReader {
        receiver: Some(receiver),
        cur: None,
        err,
        handle: Some(handle),
        closed: false,
    }
}

impl PrefetchReader {
    /// Closes the reader, stopping the background thread. Should not be called
    /// concurrently with [`Read::read`].
    pub fn close(&mut self) -> io::Result<()> {
        if self.closed {
            return Ok(());
        }
        // Dropping the receiver unblocks a producer waiting on `send` (the
        // equivalent of Go closing `closedCh`).
        self.receiver = None;
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        self.closed = true;
        Ok(())
    }
}

impl Read for PrefetchReader {
    fn read(&mut self, data: &mut [u8]) -> io::Result<usize> {
        let mut total = 0;
        let mut data = data;
        loop {
            if self.cur.is_none() {
                match self.receiver.as_ref().expect("reader is closed").recv() {
                    Ok(chunk) => self.cur = Some(Cursor::new(chunk)),
                    Err(_) => {
                        // Channel closed by the producer.
                        if total > 0 {
                            return Ok(total);
                        }
                        return match &*self.err.lock().unwrap() {
                            Terminal::Failed(kind, msg) => Err(io::Error::new(*kind, msg.clone())),
                            Terminal::Eof | Terminal::Pending => Ok(0),
                        };
                    }
                }
            }

            let expected = data.len();
            let (n, exhausted) = {
                let cur = self.cur.as_mut().unwrap();
                let n = cur.read(data)?;
                let exhausted = cur.position() as usize >= cur.get_ref().len();
                (n, exhausted)
            };
            total += n;
            if n == expected {
                return Ok(total);
            }
            data = &mut data[n..];
            if exhausted {
                self.cur = None;
            }
        }
    }
}

impl Drop for PrefetchReader {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::{new_reader, PrefetchReader};
    use std::io::{Cursor, ErrorKind, Read};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn eventually(cond: impl Fn() -> bool) {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(1) {
            if cond() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(cond(), "condition not met within 1s");
    }

    // Go `TestBasic`. Go's terminal `io.EOF` error becomes Rust's `Ok(0)`.
    #[test]
    fn basic() {
        let source = Cursor::new(b"01234567890".to_vec());
        let mut r = new_reader(source, 11, 3);

        let mut buf1 = [0u8; 1];
        assert_eq!(r.read(&mut buf1).unwrap(), 1);
        assert_eq!(&buf1[..1], b"0");

        let mut buf2 = [0u8; 2];
        assert_eq!(r.read(&mut buf2).unwrap(), 2);
        assert_eq!(&buf2[..2], b"12");

        let mut buf3 = [0u8; 3];
        assert_eq!(r.read(&mut buf3).unwrap(), 3);
        assert_eq!(&buf3[..3], b"345");

        let mut buf4 = [0u8; 4];
        assert_eq!(r.read(&mut buf4).unwrap(), 4);
        assert_eq!(&buf4[..4], b"6789");
        assert_eq!(r.read(&mut buf4).unwrap(), 1);
        assert_eq!(&buf4[..1], b"0");
        assert_eq!(r.read(&mut buf4).unwrap(), 0); // Go: io.EOF

        let source = Cursor::new(b"01234567890".to_vec());
        let mut r = new_reader(source, 11, 3);
        let mut buf = [0u8; 11];
        assert_eq!(r.read(&mut buf).unwrap(), 11);
        assert_eq!(r.read(&mut buf).unwrap(), 0); // Go: io.EOF

        let source = Cursor::new(b"01234".to_vec());
        let mut r = new_reader(source, 5, 100);
        let mut buf = [0u8; 11];
        assert_eq!(r.read(&mut buf).unwrap(), 5);
        assert_eq!(r.read(&mut buf).unwrap(), 0); // Go: io.EOF
    }

    struct UnexpectedEofReader;

    impl Read for UnexpectedEofReader {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::from(ErrorKind::UnexpectedEof))
        }
    }

    // Go `TestConvertUnexpectedEOF`: a genuine underlying UnexpectedEof is not
    // converted to end-of-stream.
    #[test]
    fn convert_unexpected_eof() {
        let mut r = new_reader(UnexpectedEofReader, 10, 10);
        let mut buf = [0u8; 10];
        let err = r.read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }

    // Go `TestCloseBeforeDrainRead`.
    #[test]
    fn close_before_drain_read() {
        let source = Cursor::new(vec![0u8; 1024]);
        let mut r: PrefetchReader = new_reader(source, 1024, 2);
        assert!(r.close().is_ok());
    }

    struct FragmentReader {
        data: Vec<u8>,
        i: Arc<AtomicUsize>,
        frag_size: usize,
    }

    impl Read for FragmentReader {
        fn read(&mut self, p: &mut [u8]) -> std::io::Result<usize> {
            let i = self.i.load(Ordering::SeqCst);
            if i >= self.data.len() {
                return Ok(0); // Go: io.EOF
            }
            let copy_size = p.len().min(self.frag_size).min(self.data.len() - i);
            p[..copy_size].copy_from_slice(&self.data[i..i + copy_size]);
            self.i.fetch_add(copy_size, Ordering::SeqCst);
            Ok(copy_size)
        }
    }

    // Go `TestFillPrefetchBuffer`.
    #[test]
    fn fill_prefetch_buffer() {
        // First, exercise FragmentReader itself.
        let mut frag = FragmentReader {
            data: b"0123456789".to_vec(),
            i: Arc::new(AtomicUsize::new(0)),
            frag_size: 3,
        };
        let mut buf = [0u8; 5];
        assert_eq!(frag.read(&mut buf).unwrap(), 3);
        assert_eq!(&buf[..3], b"012");
        let mut buf = [0u8; 1];
        assert_eq!(frag.read(&mut buf).unwrap(), 1);
        assert_eq!(&buf[..1], b"3");

        let i = Arc::new(AtomicUsize::new(0));
        let frag = FragmentReader {
            data: b"0123456789".to_vec(),
            i: Arc::clone(&i),
            frag_size: 3,
        };
        // prefetch = 10B, so the two ping-pong buffers are 5B each.
        let mut prefetch_reader = new_reader(frag, 10, 10);
        // With no read yet, one ping-pong buffer is fully filled.
        eventually(|| i.load(Ordering::SeqCst) == 5);
        // After a small read, one buffer serves the read and the other fills.
        let mut buf = [0u8; 2];
        assert_eq!(prefetch_reader.read(&mut buf).unwrap(), 2);
        assert_eq!(&buf[..2], b"01");
        eventually(|| i.load(Ordering::SeqCst) == 10);
    }
}
