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

//! Executable package contract for `pkg/util/channel`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use tidb_util::channel::clear;

struct CountDrop(Arc<AtomicUsize>);

impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn clear_drains_values_and_waits_for_disconnect() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let (sender, receiver) = mpsc::channel();
    sender.send(CountDrop(Arc::clone(&dropped))).unwrap();
    sender.send(CountDrop(Arc::clone(&dropped))).unwrap();

    let (finished, completion) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        clear(receiver);
        finished.send(()).unwrap();
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    while dropped.load(Ordering::SeqCst) != 2 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    assert!(completion.try_recv().is_err());

    sender.send(CountDrop(Arc::clone(&dropped))).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while dropped.load(Ordering::SeqCst) != 3 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(dropped.load(Ordering::SeqCst), 3);
    assert!(completion.try_recv().is_err());

    drop(sender);
    completion
        .recv_timeout(Duration::from_secs(5))
        .expect("clear must return when the final sender disconnects");
    worker.join().unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 3);
}

#[test]
fn clear_accepts_a_borrowed_receive_only_view() {
    let (sender, receiver) = crossbeam_channel::unbounded();
    sender.send(1).unwrap();
    sender.send(2).unwrap();
    drop(sender);

    clear(&receiver);
    assert!(receiver.is_empty());
    assert_eq!(
        receiver.try_recv(),
        Err(crossbeam_channel::TryRecvError::Disconnected)
    );
}
