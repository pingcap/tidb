// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Continuation and shutdown: only a successful page creates the next
//! attempt, closing before the first pull stops every unsent attempt, and a
//! remotely cancelled task closes its exact channel generation before the same
//! task is resent.

#![allow(missing_docs)]

use crate::direct_unary_client_fixture::*;

mod concurrent {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc::{self, Receiver, Sender};
    use tidb_distsql::query_runtime::QuerySelectResult;

    type Attempt = (
        u64,
        CompletionRequest<DirectUnaryResponse, DirectUnaryClientError>,
    );

    #[derive(Clone)]
    struct Client {
        started: Sender<Attempt>,
        cancelled: Arc<AtomicUsize>,
    }

    impl AsyncRequestDispatcher for Client {
        type Pending = CompletionPull<DirectUnaryResponse, DirectUnaryClientError>;

        fn begin(
            &mut self,
            _: &str,
            _: Option<&str>,
            request: &DirectUnaryRequest,
            _: &UnaryCallContext,
        ) -> Result<Self::Pending, DirectUnaryClientError> {
            let cancelled = Arc::clone(&self.cancelled);
            let (response, pending) = completion_pair(CompletionRunLoop::new(), move || {
                cancelled.fetch_add(1, Ordering::SeqCst);
            });
            self.started
                .send((request.context.region_id, response))
                .unwrap();
            Ok(pending)
        }
    }

    impl DirectUnaryClient for Client {
        fn send_request(
            &mut self,
            _: &str,
            _: &DirectUnaryRequest,
            _: Duration,
        ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
            panic!("the async transport must own this request")
        }
        fn send_request_with_context(
            &mut self,
            _: &str,
            _: &DirectUnaryRequest,
            _: &UnaryCallContext,
        ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
            panic!("the async transport must own this request")
        }
        fn close_address(&mut self, _: &str) -> Result<(), DirectUnaryClientError> {
            Ok(())
        }
        fn close_address_version(&mut self, _: &str, _: u64) -> Result<(), DirectUnaryClientError> {
            Ok(())
        }
        fn liveness(&self, _: &str, _: Duration) -> Result<StoreLiveness, DirectUnaryClientError> {
            Ok(StoreLiveness::Reachable)
        }
        fn close(&mut self) -> Result<(), DirectUnaryClientError> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct NoLock;
    impl tidb_distsql::LockedResponseDelegate<Client, Loader> for NoLock {
        fn handle_locked_response(
            &self,
            _: &tidb_txnkv::SharedReadRuntime<Client, Loader>,
            _: tidb_distsql::LockedResponseObservation,
        ) -> Result<tidb_distsql::LockedResponseAction, String> {
            Err("unexpected lock in cop progress fixture".to_owned())
        }
    }

    struct Loader(VecDeque<RegionLocation>);
    impl RegionLoader for Loader {
        fn cluster_id(&self) -> u64 {
            9001
        }
        fn load_region(&mut self, _: &[u8]) -> Result<RegionLocation, RegionLoadError> {
            self.0
                .pop_front()
                .ok_or_else(|| RegionLoadError::new("empty", "no region"))
        }
    }
    impl RegionRecoveryLoader for Loader {
        fn hydrate_region(
            &mut self,
            metadata: &RegionMetadata,
            _: u64,
            _: &mut std::collections::BTreeMap<u64, Option<tidb_txnkv::region::StoreMetadata>>,
        ) -> Result<RegionLocation, RegionLoadError> {
            self.load_region(&metadata.encoded_start_key)
        }
    }

    type Response =
        tidb_distsql::CopIterator<tidb_distsql::DirectUnaryQueryResponse<Client, Loader>>;
    type Runtime = InjectedQueryRuntime<DirectUnaryQueryTransport<Client, Loader>>;

    fn runtime(
        regions: impl IntoIterator<Item = RegionLocation>,
    ) -> (Runtime, Receiver<Attempt>, Arc<AtomicUsize>) {
        let (started, incoming) = mpsc::channel();
        let cancelled = Arc::new(AtomicUsize::new(0));
        let client = Client {
            started,
            cancelled: Arc::clone(&cancelled),
        };
        let transport = DirectUnaryQueryTransport::with_locked_response_delegate(
            tidb_txnkv::SharedReadRuntime::new_injected(
                client,
                RegionCache::new(Loader(regions.into_iter().collect())),
            ),
            DirectUnaryRuntimeConfig::default(),
            Arc::new(NoLock),
        )
        .unwrap()
        .with_async_dispatch();
        let transport = transport.with_concurrent_workers().unwrap();
        (InjectedQueryRuntime::new(transport), incoming, cancelled)
    }

    fn select(
        runtime: &mut Runtime,
        metadata: KvRequestMetadata,
        token: Arc<AtomicBool>,
    ) -> QuerySelectResult<Response> {
        let mut metadata = metadata;
        metadata.concurrency = 2;
        metadata.tikv_client_read_timeout_ms = 5000;
        runtime
            .select_with_runtime_stats(
                &transport_request(metadata).with_cop_lite_worker(token),
                SelectInput::default(),
                QueryResultContext::new(Vec::<FieldType>::new(), WarningCollector::new()),
                vec![],
                0,
                false,
            )
            .unwrap()
    }

    fn started(incoming: &Receiver<Attempt>) -> Attempt {
        incoming
            .recv_timeout(Duration::from_secs(2))
            .expect("cop request must progress without another consumer Next")
    }

    fn answer(attempt: Attempt, bytes: Vec<u8>) {
        attempt.1.schedule(Ok(DirectUnaryResponse::new(
            bytes,
            &format!("tikv-{}:20160", attempt.0),
            1,
        )));
    }

    /// Go TestQueryWithConcurrentSmallCop: opening a second single-task
    /// reader must start it while the first reader still owns the lite slot.
    #[test]
    fn go_concurrent_small_cop_overlaps_other_reader() {
        let token = Arc::new(AtomicBool::new(false));
        let (mut first_runtime, first_calls, _) = runtime([location(1, "a", "z", "tikv-1:20160")]);
        let first = select(&mut first_runtime, metadata("a", "z"), Arc::clone(&token));
        assert!(token.load(Ordering::Acquire));
        assert!(first_calls.try_recv().is_err());
        let (mut second_runtime, second_calls, _) =
            runtime([location(2, "a", "z", "tikv-2:20160")]);
        let mut second = select(&mut second_runtime, metadata("a", "z"), Arc::clone(&token));
        answer(started(&second_calls), response(b"second"));
        let first_read = std::thread::spawn(move || {
            let mut first = first;
            assert_eq!(first.next_raw().unwrap(), Some(b"first".to_vec()));
            first
        });
        answer(started(&first_calls), response(b"first"));
        let first = first_read.join().unwrap();
        assert!(!token.load(Ordering::Acquire));
        let (mut third_runtime, third_calls, _) = runtime([location(3, "a", "z", "tikv-3:20160")]);
        let third = select(&mut third_runtime, metadata("a", "z"), Arc::clone(&token));
        assert!(token.load(Ordering::Acquire));
        drop(first);
        assert!(
            token.load(Ordering::Acquire),
            "an old iterator must not release the next owner's slot"
        );
        drop(third);
        assert!(!token.load(Ordering::Acquire));
        assert!(third_calls.try_recv().is_err());
        assert_eq!(second.next_raw().unwrap(), Some(b"second".to_vec()));
        assert_eq!(second.next_raw().unwrap(), None);

        // Go TestBasicSmallTaskConc / optRowHint: small tasks have their
        // own workers, but internal requests keep the normal pool.
        for internal in [false, true] {
            let (mut runtime, incoming, _) = runtime([
                location(1, "a", "g", "tikv-1:20160"),
                location(2, "g", "m", "tikv-2:20160"),
                location(3, "m", "z", "tikv-3:20160"),
            ]);
            let mut request = metadata("a", "z");
            request.request_source.internal = internal;
            request.key_ranges = Some(RequestKeyRanges::new_non_partitioned_with_hints(
                vec![range("a", "g"), range("g", "m"), range("m", "z")],
                vec![64, 64, 1],
            ));
            let mut result = select(&mut runtime, request, Arc::new(AtomicBool::new(false)));
            let mut attempts = vec![started(&incoming), started(&incoming)];
            if !internal {
                attempts.push(started(&incoming));
            } else {
                assert!(matches!(
                    incoming.recv_timeout(Duration::from_millis(100)),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ));
            }
            for attempt in attempts {
                answer(attempt, response(b"row"));
            }
            if internal {
                answer(started(&incoming), response(b"row"));
            }
            for _ in 0..3 {
                assert_eq!(result.next_raw().unwrap(), Some(b"row".to_vec()));
            }
            assert_eq!(result.next_raw().unwrap(), None);
        }
    }

    /// Go liteSendReq/Next switches remaining work to the concurrent worker
    /// before returning its first result, even while the caller stops reading.
    #[test]
    fn go_lite_continuation_progresses_before_another_next() {
        for ordered in [true, false] {
            for fail in [false, true] {
                let token = Arc::new(AtomicBool::new(false));
                let (mut runtime, incoming, _) = runtime([location(1, "a", "z", "tikv-1:20160")]);
                let mut metadata = metadata("a", "z");
                metadata.keep_order = ordered;
                metadata.paging.enabled = true;
                metadata.paging.min_size = 2;
                metadata.paging.max_size = 8;
                let mut result = select(&mut runtime, metadata, Arc::clone(&token));
                let first_read = std::thread::spawn(move || {
                    assert_eq!(result.next_raw().unwrap(), Some(b"one".to_vec()));
                    result
                });
                answer(
                    started(&incoming),
                    CoprocessorResponse {
                        data: b"one".to_vec(),
                        range: Some(CoprocessorKeyRange {
                            start: b"a".to_vec(),
                            end: b"m".to_vec(),
                        }),
                        ..CoprocessorResponse::default()
                    }
                    .encode_to_vec(),
                );
                let mut result = first_read.join().unwrap();
                let next = started(&incoming);
                assert!(!token.load(Ordering::Acquire));
                if fail {
                    next.1.schedule(Err(DirectUnaryClientError::InvalidRequest(
                        "continuation failed".to_owned(),
                    )));
                    let error = result.next_raw().unwrap_err().to_string();
                    assert!(error.contains("continuation failed"), "{error}");
                } else {
                    answer(next, response(b"two"));
                    assert_eq!(result.next_raw().unwrap(), Some(b"two".to_vec()));
                    assert_eq!(result.next_raw().unwrap(), None);
                }
            }
        }
    }

    /// Go TestDMLWithLiteCopWorker: rebuilding a single task after a split
    /// must start its remaining range before the caller asks for another row.
    #[test]
    fn go_split_switches_the_lite_reader_to_concurrent_progress() {
        for ordered in [true, false] {
            let (mut runtime, incoming, _) = runtime([
                location(1, "a", "z", "tikv-1:20160"),
                location(10, "a", "m", "tikv-10:20160"),
                location(11, "m", "z", "tikv-11:20160"),
            ]);
            let mut metadata = metadata("a", "z");
            metadata.keep_order = ordered;
            let mut result = select(&mut runtime, metadata, Arc::new(AtomicBool::new(false)));
            let first_read = std::thread::spawn(move || {
                assert_eq!(result.next_raw().unwrap(), Some(b"left".to_vec()));
                result
            });
            answer(started(&incoming), region_not_found(1));
            let left = started(&incoming);
            assert_eq!(left.0, 10);
            assert!(matches!(
                incoming.recv_timeout(Duration::from_millis(100)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ));
            answer(left, response(b"left"));
            let mut result = first_read.join().unwrap();
            let right = started(&incoming);
            assert_eq!(right.0, 11);
            answer(right, response(b"right"));
            assert_eq!(result.next_raw().unwrap(), Some(b"right".to_vec()));
            assert_eq!(result.next_raw().unwrap(), None);
        }

        // Go rebuilds without response channels; lite fallback creates two
        // slots for each rebuilt task, including tasks with paging enabled.
        let (mut runtime, incoming, _) = runtime([
            location(1, "a", "z", "tikv-1:20160"),
            location(10, "a", "m", "tikv-10:20160"),
            location(11, "m", "z", "tikv-11:20160"),
        ]);
        let mut request = metadata("a", "z");
        request.paging.enabled = true;
        request.paging.min_size = 2;
        request.paging.max_size = 8;
        let mut result = select(&mut runtime, request, Arc::new(AtomicBool::new(false)));
        let first_read = std::thread::spawn(move || {
            assert_eq!(result.next_raw().unwrap(), Some(b"left".to_vec()));
            result
        });
        answer(started(&incoming), region_not_found(1));
        answer(started(&incoming), response(b"left"));
        let mut result = first_read.join().unwrap();
        for page in 1..=3 {
            let right = started(&incoming);
            assert_eq!(right.0, 11);
            answer(
                right,
                CoprocessorResponse {
                    data: vec![page],
                    range: Some(CoprocessorKeyRange {
                        start: b"m".to_vec(),
                        end: vec![b'm', page],
                    }),
                    ..CoprocessorResponse::default()
                }
                .encode_to_vec(),
            );
        }
        assert!(matches!(
            incoming.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        for page in 1..=3 {
            assert_eq!(result.next_raw().unwrap(), Some(vec![page]));
        }
        answer(started(&incoming), response(b"right"));
        assert_eq!(result.next_raw().unwrap(), Some(b"right".to_vec()));
        assert_eq!(result.next_raw().unwrap(), None);
    }

    #[test]
    fn go_multiple_cop_tasks_start_on_open_and_close_cancels_pending_work() {
        for ordered in [true, false] {
            let (mut runtime, incoming, cancelled) = runtime([
                location(1, "a", "m", "tikv-1:20160"),
                location(2, "m", "z", "tikv-2:20160"),
            ]);
            let mut metadata = metadata("a", "z");
            metadata.keep_order = ordered;
            let mut result = select(&mut runtime, metadata, Arc::new(AtomicBool::new(false)));
            let first = started(&incoming);
            let second = started(&incoming);
            let mut regions = [first.0, second.0];
            regions.sort();
            assert_eq!(regions, [1, 2]);
            result.close();
            assert_eq!(cancelled.load(Ordering::SeqCst), 2);
            answer(first, response(b"late-one"));
            answer(second, response(b"late-two"));
            assert_eq!(result.next_raw().unwrap(), None);
        }
    }

    // Go handleTask progresses each task independently; ordered channels
    // affect publication order, not another region's paging or retry work.
    #[test]
    fn go_ordered_worker_pages_while_the_head_rpc_is_pending() {
        let (mut runtime, incoming, _) = runtime([
            location(1, "a", "m", "tikv-1:20160"),
            location(2, "m", "z", "tikv-2:20160"),
        ]);
        let mut metadata = metadata("a", "z");
        metadata.paging.enabled = true;
        metadata.paging.min_size = 2;
        metadata.paging.max_size = 8;
        let mut result = select(&mut runtime, metadata, Arc::new(AtomicBool::new(false)));
        let mut attempts = [started(&incoming), started(&incoming)];
        attempts.sort_by_key(|attempt| attempt.0);
        let [head, mut later] = attempts;
        // Go buildCopTasks gives paging tasks 18 response slots. With a
        // blocked head, the later worker can fill all slots and fetch one
        // more response before it waits for the consumer.
        for page in 1..=18 {
            answer(
                later,
                CoprocessorResponse {
                    data: vec![page],
                    range: Some(CoprocessorKeyRange {
                        start: b"m".to_vec(),
                        end: vec![b'm', page],
                    }),
                    ..CoprocessorResponse::default()
                }
                .encode_to_vec(),
            );
            later = started(&incoming);
            assert_eq!(later.0, 2);
        }
        answer(later, response(b"last"));
        answer(head, response(b"head"));
        assert_eq!(result.next_raw().unwrap(), Some(b"head".to_vec()));
        for page in 1..=18 {
            assert_eq!(result.next_raw().unwrap(), Some(vec![page]));
        }
        assert_eq!(result.next_raw().unwrap(), Some(b"last".to_vec()));
        assert_eq!(result.next_raw().unwrap(), None);
    }

    // Go copIteratorTaskSender permits 2*concurrency ordered tasks until
    // Next consumes a task's channel close, even with an unfinished head.
    #[test]
    fn go_ordered_send_window_advances_but_stays_bounded_without_next() {
        let (mut runtime, incoming, _) = runtime([
            location(1, "a", "e", "tikv-1:20160"),
            location(2, "e", "i", "tikv-2:20160"),
            location(3, "i", "m", "tikv-3:20160"),
            location(4, "m", "q", "tikv-4:20160"),
            location(5, "q", "z", "tikv-5:20160"),
        ]);
        let mut result = select(
            &mut runtime,
            metadata("a", "z"),
            Arc::new(AtomicBool::new(false)),
        );
        let mut attempts = [started(&incoming), started(&incoming)];
        attempts.sort_by_key(|attempt| attempt.0);
        let [head, later] = attempts;
        answer(later, response(b"two"));
        let third = started(&incoming);
        assert_eq!(third.0, 3);
        answer(third, response(b"three"));
        let fourth = started(&incoming);
        assert_eq!(fourth.0, 4);
        answer(fourth, response(b"four"));
        assert!(matches!(
            incoming.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        answer(head, response(b"one"));
        assert_eq!(result.next_raw().unwrap(), Some(b"one".to_vec()));
        assert_eq!(result.next_raw().unwrap(), Some(b"two".to_vec()));
        let fifth = started(&incoming);
        assert_eq!(fifth.0, 5);
        answer(fifth, response(b"five"));
        for row in [b"three".as_slice(), b"four", b"five"] {
            assert_eq!(result.next_raw().unwrap(), Some(row.to_vec()));
        }
        assert_eq!(result.next_raw().unwrap(), None);

        // Go's sender cannot skip a full normal-task channel to feed a
        // later small task. Once a normal worker takes its queued task,
        // both channels can advance within the shared admission window.
        let (mut runtime, incoming, _) = self::runtime([
            location(1, "a", "e", "tikv-1:20160"),
            location(2, "e", "i", "tikv-2:20160"),
            location(3, "i", "m", "tikv-3:20160"),
            location(4, "m", "q", "tikv-4:20160"),
            location(5, "q", "z", "tikv-5:20160"),
        ]);
        let mut request = metadata("a", "z");
        request.request_source.internal = false;
        request.key_ranges = Some(RequestKeyRanges::new_non_partitioned_with_hints(
            vec![
                range("a", "e"),
                range("e", "i"),
                range("i", "m"),
                range("m", "q"),
                range("q", "z"),
            ],
            vec![64, 64, 64, 64, 1],
        ));
        let mut result = select(&mut runtime, request, Arc::new(AtomicBool::new(false)));
        let mut attempts = [started(&incoming), started(&incoming)];
        attempts.sort_by_key(|attempt| attempt.0);
        let [first, second] = attempts;
        assert_eq!((first.0, second.0), (1, 2));
        assert!(matches!(
            incoming.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        answer(first, response(b"row"));
        let mut next = [started(&incoming), started(&incoming)];
        next.sort_by_key(|attempt| attempt.0);
        assert_eq!((next[0].0, next[1].0), (3, 5));
        for attempt in next {
            answer(attempt, response(b"row"));
        }
        let fourth = started(&incoming);
        assert_eq!(fourth.0, 4);
        answer(fourth, response(b"row"));
        answer(second, response(b"row"));
        for _ in 0..5 {
            assert_eq!(result.next_raw().unwrap(), Some(b"row".to_vec()));
        }
        assert_eq!(result.next_raw().unwrap(), None);
    }

    // Go's workers may wait on either RPC completion or sendToRespCh.
    // Close must release both owners without admitting the next task.
    #[test]
    fn go_close_joins_rpc_and_response_channel_waiters() {
        for ordered in [true, false] {
            let (mut runtime, incoming, cancelled) = runtime([
                location(1, "a", "m", "tikv-1:20160"),
                location(2, "m", "y", "tikv-2:20160"),
                location(3, "y", "z", "tikv-3:20160"),
            ]);
            let mut metadata = metadata("a", "z");
            metadata.keep_order = ordered;
            metadata.paging.enabled = true;
            metadata.paging.min_size = 2;
            metadata.paging.max_size = 8;
            let mut result = select(&mut runtime, metadata, Arc::new(AtomicBool::new(false)));
            let mut attempts = [started(&incoming), started(&incoming)];
            attempts.sort_by_key(|attempt| attempt.0);
            let [head, mut later] = attempts;
            for index in 0..19 {
                answer(
                    later,
                    CoprocessorResponse {
                        data: b"later".to_vec(),
                        range: Some(CoprocessorKeyRange {
                            start: b"m".to_vec(),
                            end: vec![b'm', index + 1],
                        }),
                        ..CoprocessorResponse::default()
                    }
                    .encode_to_vec(),
                );
                if !ordered || index == 18 {
                    break;
                }
                later = started(&incoming);
                assert_eq!(later.0, 2);
            }
            assert!(matches!(
                incoming.recv_timeout(Duration::from_millis(100)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ));
            result.close();
            assert_eq!(cancelled.load(Ordering::SeqCst), 1);
            answer(head, response(b"late-head"));
            assert_eq!(result.next_raw().unwrap(), None);
            assert!(incoming.try_recv().is_err());
        }
    }
}

#[test]
fn only_successful_paging_creates_a_continuation_attempt() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let first = CoprocessorResponse {
        data: b"page-one".to_vec(),
        range: Some(CoprocessorKeyRange {
            start: b"a".to_vec(),
            end: b"m".to_vec(),
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec();
    let mut metadata = metadata("a", "z");
    metadata.paging.enabled = true;
    metadata.paging.min_size = 2;
    metadata.paging.max_size = 8;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [Ok(first), Ok(response(b"page-two"))],
        [location(1, "a", "z", "tikv-1:20160")],
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata));

    assert_eq!(result.next_raw().unwrap(), Some(b"page-one".to_vec()));
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(result.next_raw().unwrap(), Some(b"page-two".to_vec()));
    assert_eq!(calls.borrow().len(), 2);
    assert_eq!(result.next_raw().unwrap(), None);
}

#[test]
fn unordered_paging_delivers_each_page_once() {
    unordered_paging_with_completion_modes(Some([true, true]));
}

#[test]
fn unordered_paging_with_synchronous_continuation_delivers_each_page_once() {
    unordered_paging_with_completion_modes(None);
}

fn unordered_paging_with_completion_modes(completion_modes: Option<[bool; 2]>) {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let first = CoprocessorResponse {
        data: b"page-one".to_vec(),
        range: Some(CoprocessorKeyRange {
            start: b"a".to_vec(),
            end: b"m".to_vec(),
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec();
    let mut metadata = metadata("a", "z");
    metadata.keep_order = false;
    metadata.concurrency = 1;
    metadata.paging.enabled = true;
    metadata.paging.min_size = 2;
    metadata.paging.max_size = 8;
    let responses = [Ok(first), Ok(response(b"page-two"))];
    let regions = [location(1, "a", "z", "tikv-1:20160")];
    let source = match completion_modes {
        Some(modes) => batch_first_transport(Rc::clone(&calls), responses, regions, modes),
        None => transport(Rc::clone(&calls), responses, regions),
    };
    let mut runtime = InjectedQueryRuntime::new(source);
    let mut result = select_result(&mut runtime, &transport_request(metadata));

    assert_eq!(result.next_raw().unwrap(), Some(b"page-one".to_vec()));
    assert_eq!(result.next_raw().unwrap(), Some(b"page-two".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(calls.borrow().len(), 2);
}

#[test]
fn synchronous_unordered_paging_keeps_one_ready_token_per_task() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let first = CoprocessorResponse {
        data: b"page-one".to_vec(),
        range: Some(CoprocessorKeyRange {
            start: b"a".to_vec(),
            end: b"m".to_vec(),
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec();
    let mut metadata = metadata("a", "z");
    metadata.keep_order = false;
    metadata.concurrency = 1;
    metadata.paging.enabled = true;
    metadata.paging.min_size = 2;
    metadata.paging.max_size = 8;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [Ok(first), Ok(response(b"page-two"))],
        [location(1, "a", "z", "tikv-1:20160")],
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata));

    assert_eq!(result.next_raw().unwrap(), Some(b"page-one".to_vec()));
    assert_eq!(result.next_raw().unwrap(), Some(b"page-two".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(calls.borrow().len(), 2);
}

#[test]
fn close_before_pull_stops_every_unsent_attempt() {
    let cancel = std::sync::Arc::new(tidb_distsql::CancelHandle::default());
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [Ok(response(b"never"))],
        [location(1, "a", "z", "tikv-1:20160")],
    ));
    let request = TransportRequest::new(metadata("a", "z"), std::sync::Arc::clone(&cancel));
    let mut result = select_result(&mut runtime, &request);
    result.close();
    result.close();
    assert!(
        !cancel.is_cancelled(),
        "closing one response must not cancel the outer execution"
    );
    assert_eq!(result.next_raw().unwrap(), None);
    assert!(calls.borrow().is_empty());
}

#[test]
fn remote_canceled_closes_exact_generation_before_resending_the_same_task() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Arc::new(RecordingRetryControl::default());
    let mut runtime = InjectedQueryRuntime::new(transport_with_transport_failures(
        Rc::clone(&calls),
        [
            Err(connection_failure(
                "tikv-1:20160",
                41,
                DirectUnaryTransportClass::RemoteGrpc,
                Some(DirectUnaryGrpcCode::Canceled),
            )),
            Ok(response(b"retried")),
        ],
        [Ok(StoreLiveness::Unreachable)],
        Rc::clone(&events),
        [location_with_second_peer(
            1,
            "a",
            "z",
            "tikv-1:20160",
            "tikv-2:20160",
        )],
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(result.next_raw().unwrap(), Some(b"retried".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(calls.borrow().len(), 2);
    assert_eq!(calls.borrow()[0].region_id, calls.borrow()[1].region_id);
    assert_eq!(calls.borrow()[0].data, calls.borrow()[1].data);
    assert_eq!(
        events.borrow().as_slice(),
        [
            ClientEvent::Send("tikv-1:20160".to_owned()),
            ClientEvent::CloseGeneration {
                address: "tikv-1:20160".to_owned(),
                version: 41,
            },
            ClientEvent::Liveness {
                address: "tikv-1:20160".to_owned(),
                timeout: Duration::from_secs(1),
            },
            ClientEvent::Send("tikv-2:20160".to_owned()),
        ]
    );
    assert_eq!(retry_control.sleeps.lock().unwrap().len(), 1);
}
