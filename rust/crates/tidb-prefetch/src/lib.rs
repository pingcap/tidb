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

//! Background-prefetching reader for `pkg/util/prefetch`.
//!
//! The reader and worker exchange two buffers. The worker fills the next
//! buffer after the reader takes the current one.
//! [`new_reader`] accepts the separate close operation needed to represent
//! Go's `io.ReadCloser` with Rust's standard I/O traits.

use std::io::{self, Cursor, Read};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

/// The producer's final status, exposed after the last buffer is consumed.
enum Terminal {
    Eof,
    Failed(io::ErrorKind, String),
}

#[derive(Default)]
struct ExchangeState {
    ready: Option<Vec<u8>>,
    spare: Option<Vec<u8>>,
    terminal: Option<Terminal>,
    finished: bool,
    closed: bool,
}

#[derive(Default)]
struct Exchange {
    state: Mutex<ExchangeState>,
    ready: Condvar,
    spare: Condvar,
    flags: AtomicU8,
}

const BUFFER_READY: u8 = 1;
const BUFFER_RETURNED: u8 = 2;
const CLOSED: u8 = 4;
const FINISHED: u8 = 8;
const SPIN_ROUNDS: usize = 512;

impl Exchange {
    fn publish(&self, buffer: Vec<u8>, terminal: Option<Terminal>) -> Option<Vec<u8>> {
        let terminal_ready = terminal.is_some();
        {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return None;
            }
            state.ready = Some(buffer);
            state.terminal = terminal;
            self.refresh_flags(&state);
        }
        self.ready.notify_one();
        if terminal_ready {
            return None;
        }
        self.spin_until(BUFFER_RETURNED | CLOSED);
        let mut state = self.state.lock().unwrap();
        while !state.closed && state.spare.is_none() {
            state = self.spare.wait(state).unwrap();
        }
        let spare = state.spare.take();
        self.refresh_flags(&state);
        spare
    }

    fn receive(&self, spare: &mut Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        self.spin_until(BUFFER_READY | FINISHED | CLOSED);
        let mut state = self.state.lock().unwrap();
        while state.ready.is_none() && state.terminal.is_none() && !state.finished && !state.closed
        {
            state = self.ready.wait(state).unwrap();
        }
        if let Some(buffer) = state.ready.take() {
            let returned = state.terminal.is_none();
            if returned {
                state.spare = Some(std::mem::take(spare));
            }
            self.refresh_flags(&state);
            drop(state);
            if returned {
                self.spare.notify_one();
            }
            return Ok(Some(buffer));
        }
        match &state.terminal {
            Some(Terminal::Failed(kind, message)) => Err(io::Error::new(*kind, message.clone())),
            _ => Ok(None),
        }
    }

    fn finish(&self) {
        let mut state = self.state.lock().unwrap();
        state.finished = true;
        self.refresh_flags(&state);
        drop(state);
        self.ready.notify_one();
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        state.ready = None;
        state.spare = None;
        self.refresh_flags(&state);
        drop(state);
        self.spare.notify_one();
    }

    fn wait(&self) {
        self.spin_until(FINISHED);
        let mut state = self.state.lock().unwrap();
        while !state.finished {
            state = self.ready.wait(state).unwrap();
        }
    }

    fn refresh_flags(&self, state: &ExchangeState) {
        let ready = u8::from(state.ready.is_some()) * BUFFER_READY;
        let spare = u8::from(state.spare.is_some()) * BUFFER_RETURNED;
        let closed = u8::from(state.closed) * CLOSED;
        let finished = u8::from(state.finished) * FINISHED;
        self.flags
            .store(ready | spare | closed | finished, Ordering::Relaxed);
    }

    fn spin_until(&self, flags: u8) {
        // A short in-memory handoff can finish before either thread parks.
        // The mutex still protects the buffers and all wakeup conditions.
        for _ in 0..SPIN_ROUNDS {
            if self.flags.load(Ordering::Relaxed) & flags != 0 {
                return;
            }
            std::hint::spin_loop();
        }
    }
}

struct Job {
    run: Box<dyn FnOnce(&Exchange) + Send>,
    exchange: Arc<Exchange>,
}

// A live reader gets its own worker. Only idle workers count toward this
// cache, so an undrained reader cannot delay another reader's startup.
const MAX_IDLE_WORKERS: usize = 8;
static IDLE_WORKERS: Mutex<Vec<SyncSender<Job>>> = Mutex::new(Vec::new());

fn start(job: Job) {
    let idle = IDLE_WORKERS.lock().unwrap().pop();
    if let Some(idle) = idle {
        idle.send(job).expect("idle prefetch worker stopped");
    } else {
        let (sender, receiver) = sync_channel(1);
        thread::spawn(move || worker(job, &sender, &receiver));
    }
}

fn worker(mut job: Job, sender: &SyncSender<Job>, receiver: &Receiver<Job>) {
    loop {
        let Job { run, exchange } = job;
        // A panicking source disconnects its reader without losing the
        // cached worker or preventing Close from completing.
        let _ = catch_unwind(AssertUnwindSafe(|| run(&exchange)));
        let reuse = {
            let mut idle = IDLE_WORKERS.lock().unwrap();
            if idle.len() < MAX_IDLE_WORKERS {
                idle.push(sender.clone());
                true
            } else {
                false
            }
        };
        exchange.finish();
        if !reuse {
            return;
        }
        job = next_job(receiver);
    }
}

fn next_job(receiver: &Receiver<Job>) -> Job {
    for _ in 0..SPIN_ROUNDS {
        if let Ok(job) = receiver.try_recv() {
            return job;
        }
        std::hint::spin_loop();
    }
    receiver.recv().expect("prefetch worker owns a sender")
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

fn run<R: Read>(mut reader: R, exchange: &Exchange, range_size: i64, half: usize) {
    let mut read_size: i64 = 0;
    let mut buffer = Vec::new();
    loop {
        buffer.resize(half, 0);
        let (n, fill) = read_full(&mut reader, &mut buffer);
        buffer.truncate(n);
        read_size += n as i64;

        let terminal = match fill {
            Fill::Full => None,
            Fill::Eof => Some(Terminal::Eof),
            Fill::Failed(e) => {
                // Because we are prefetching, the buffer may be larger than the
                // caller needs, so `io.ReadFull`'s partial-EOF at exactly the
                // range size is a clean end-of-stream. A premature underlying
                // `UnexpectedEof` (read size below the range) is a real error.
                Some(
                    if e.kind() == io::ErrorKind::UnexpectedEof && read_size == range_size {
                        Terminal::Eof
                    } else {
                        Terminal::Failed(e.kind(), e.to_string())
                    },
                )
            }
        };
        let Some(spare) = exchange.publish(buffer, terminal) else {
            return;
        };
        buffer = spare;
    }
}

/// A reader that prefetches data from an underlying reader.
pub struct PrefetchReader {
    exchange: Option<Arc<Exchange>>,
    cur: Option<Cursor<Vec<u8>>>,
    close_source: Option<Box<dyn FnOnce() -> io::Result<()> + Send>>,
}

/// Go `NewReader`. The callback represents the source `io.ReadCloser.Close`:
/// it is invoked before the producer is joined and may unblock an in-flight
/// source read.
pub fn new_reader<R, C>(
    reader: R,
    range_size: i64,
    prefetch_size: usize,
    close_source: C,
) -> PrefetchReader
where
    R: Read + Send + 'static,
    C: FnOnce() -> io::Result<()> + Send + 'static,
{
    let exchange = Arc::new(Exchange::default());
    let half = prefetch_size / 2;
    start(Job {
        run: Box::new(move |exchange| run(reader, exchange, range_size, half)),
        exchange: Arc::clone(&exchange),
    });

    PrefetchReader {
        exchange: Some(exchange),
        cur: None,
        close_source: Some(Box::new(close_source)),
    }
}

impl PrefetchReader {
    /// Closes the reader, stopping the background thread. Should not be called
    /// concurrently with [`Read::read`].
    pub fn close(&mut self) -> io::Result<()> {
        let Some(exchange) = &self.exchange else {
            return Ok(());
        };
        let close_result = self.close_source.take().map_or(Ok(()), |close| close());
        exchange.close();
        exchange.wait();
        self.exchange = None;
        close_result
    }
}

impl Drop for PrefetchReader {
    fn drop(&mut self) {
        if let Some(exchange) = &self.exchange {
            exchange.close();
        }
    }
}

impl Read for PrefetchReader {
    fn read(&mut self, data: &mut [u8]) -> io::Result<usize> {
        let mut total = 0;
        let mut data = data;
        let mut spare = Vec::new();
        loop {
            if self.cur.is_none() {
                let exchange = self.exchange.as_ref().expect("reader is closed");
                match exchange.receive(&mut spare) {
                    Ok(Some(chunk)) => self.cur = Some(Cursor::new(chunk)),
                    Ok(None) => return Ok(total),
                    Err(_) if total > 0 => return Ok(total),
                    Err(err) => return Err(err),
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
                spare = self.cur.take().unwrap().into_inner();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PrefetchReader, new_reader};
    use std::io::{Cursor, ErrorKind, Read};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, mpsc};
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
        let mut r = new_reader(source, 11, 3, || Ok(()));

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
        let mut r = new_reader(source, 11, 3, || Ok(()));
        let mut buf = [0u8; 11];
        assert_eq!(r.read(&mut buf).unwrap(), 11);
        assert_eq!(r.read(&mut buf).unwrap(), 0); // Go: io.EOF

        let source = Cursor::new(b"01234".to_vec());
        let mut r = new_reader(source, 5, 100, || Ok(()));
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
        let mut r = new_reader(UnexpectedEofReader, 10, 10, || Ok(()));
        let mut buf = [0u8; 10];
        let err = r.read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }

    // Go `TestCloseBeforeDrainRead`.
    #[test]
    fn close_before_drain_read() {
        let source = Cursor::new(vec![0u8; 1024]);
        let mut r: PrefetchReader = new_reader(source, 1024, 2, || Ok(()));
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
        let mut prefetch_reader = new_reader(frag, 10, 10, || Ok(()));
        // With no read yet, one ping-pong buffer is fully filled.
        eventually(|| i.load(Ordering::SeqCst) == 5);
        // After a small read, one buffer serves the read and the other fills.
        let mut buf = [0u8; 2];
        assert_eq!(prefetch_reader.read(&mut buf).unwrap(), 2);
        assert_eq!(&buf[..2], b"01");
        eventually(|| i.load(Ordering::SeqCst) == 10);
    }

    #[test]
    fn returns_only_the_two_prefetch_buffers_to_the_source() {
        struct Source {
            marker: u8,
            previous: Arc<Mutex<Vec<u8>>>,
        }
        impl Read for Source {
            fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
                if self.marker == 5 {
                    return Ok(0);
                }
                self.previous.lock().unwrap().push(output[0]);
                output.fill(self.marker);
                self.marker += 1;
                Ok(output.len())
            }
        }

        let previous = Arc::new(Mutex::new(Vec::new()));
        let source = Source {
            marker: 1,
            previous: Arc::clone(&previous),
        };
        let mut reader = new_reader(source, 16, 8, || Ok(()));
        let mut output = [0; 16];
        reader.read_exact(&mut output).unwrap();
        assert_eq!(output, [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4]);
        assert_eq!(*previous.lock().unwrap(), [0, 0, 1, 2]);
        reader.close().unwrap();
    }

    #[test]
    fn returns_partial_data_before_the_terminal_error() {
        let mut reader = new_reader(Cursor::new(b"abcdef"), 7, 8, || Ok(()));
        let mut output = [0; 8];
        let n = reader.read(&mut output).unwrap();
        assert_eq!(&output[..n], b"abcdef");
        for _ in 0..2 {
            assert_eq!(
                reader.read(&mut output).unwrap_err().kind(),
                ErrorKind::UnexpectedEof
            );
        }
        reader.close().unwrap();
    }

    #[test]
    fn close_unblocks_the_source_and_waits_for_it() {
        struct BlockingSource {
            entered: mpsc::Sender<()>,
            unblock: mpsc::Receiver<()>,
            finished: mpsc::Sender<()>,
        }
        impl Read for BlockingSource {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                self.entered.send(()).unwrap();
                self.unblock.recv().unwrap();
                self.finished.send(()).unwrap();
                Ok(0)
            }
        }

        let (entered, entered_rx) = mpsc::channel();
        let (unblock, unblock_rx) = mpsc::channel();
        let (finished, finished_rx) = mpsc::channel();
        let mut reader = new_reader(
            BlockingSource {
                entered,
                unblock: unblock_rx,
                finished,
            },
            0,
            8,
            move || {
                unblock.send(()).unwrap();
                Err(std::io::Error::other("source close failed"))
            },
        );
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            reader.close().unwrap_err().to_string(),
            "source close failed"
        );
        assert!(finished_rx.try_recv().is_ok());
        assert!(reader.close().is_ok());
    }

    #[test]
    fn undrained_readers_do_not_hold_up_other_readers() {
        struct Source {
            index: u8,
            first_read: Option<mpsc::Sender<u8>>,
        }
        impl Read for Source {
            fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
                let Some(first_read) = self.first_read.take() else {
                    return Ok(0);
                };
                first_read.send(self.index).unwrap();
                output[0] = self.index;
                Ok(1)
            }
        }

        let (first_read, reported) = mpsc::channel();
        let mut readers: Vec<_> = (0..12)
            .map(|index| {
                let source = Source {
                    index,
                    first_read: Some(first_read.clone()),
                };
                new_reader(source, 1, 2, || Ok(()))
            })
            .collect();
        let mut ready: Vec<_> = (0..readers.len())
            .map(|_| reported.recv_timeout(Duration::from_secs(1)).unwrap())
            .collect();
        ready.sort_unstable();
        assert_eq!(ready, (0..12).collect::<Vec<_>>());
        for (index, reader) in readers.iter_mut().enumerate() {
            let mut byte = [0];
            reader.read_exact(&mut byte).unwrap();
            assert_eq!(byte, [index as u8]);
            reader.close().unwrap();
        }
    }
}
