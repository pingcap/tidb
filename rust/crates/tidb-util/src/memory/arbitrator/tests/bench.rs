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

//! The arbitrator throughput test (Go `TestBench` /`bench_test.go` in
//! `pkg/util/memory`). Split out of `arbitrator.rs`'s `tests` module,
//! whose helpers it reaches through `use super::*`.

use super::*;

fn bench_round(m: &Arc<MemArbitrator>, n: usize, priority_mode: bool) -> (i64, i64) {
    assert!({
        m.weak_wake();
        m.async_run(DEF_TASK_TICK_DUR)
    });
    let cancel_pool = Arc::new(AtomicI64::new(0));
    let killed_pool = Arc::new(AtomicI64::new(0));
    let start = Arc::new(std::sync::Barrier::new(n));
    let mut handles = Vec::with_capacity(n);
    for i in 0..n {
        let m = Arc::clone(m);
        let cancel_pool = Arc::clone(&cancel_pool);
        let killed_pool = Arc::clone(&killed_pool);
        let start = Arc::clone(&start);
        handles.push(
            std::thread::Builder::new()
                .stack_size(256 * 1024)
                .spawn(move || {
                    start.wait();
                    let root = m.emplace_root_pool(i as u64).unwrap();
                    let (h, ch) = HelperForTest::new_with_channel();
                    let cancel_event = Arc::new(AtomicI64::new(0));
                    let killed = Arc::new(AtomicBool::new(false));
                    {
                        let ce = Arc::clone(&cancel_event);
                        let kd = Arc::clone(&killed);
                        h.set_kill_cb(move || {
                            ce.fetch_add(1, SeqCst);
                            kd.store(true, SeqCst);
                        });
                        let ce = Arc::clone(&cancel_event);
                        h.set_cancel_cb(move || {
                            ce.fetch_add(1, SeqCst);
                        });
                        h.set_heap_used_cb(|| 0);
                    }
                    let (prio, wait_averse) = if priority_mode {
                        let p = i % 6;
                        let prio = if p < 2 {
                            ArbitrationPriority::Low
                        } else if p < 4 {
                            ArbitrationPriority::Medium
                        } else {
                            ArbitrationPriority::High
                        };
                        (prio, p % 2 != 0)
                    } else {
                        (ArbitrationPriority::High, false)
                    };
                    let ctx = ArbitrationContext::new(
                        Some(ch),
                        Some(Arc::clone(&h) as Arc<dyn ArbitrateHelper>),
                        prio,
                        wait_averse,
                        true,
                    );
                    assert!(m.restart_entry_by_context(root.clone(), Some(ctx)));

                    let b = ConcurrentBudget::new(Arc::clone(root.entry.as_ref().unwrap().pool()));
                    for _ in 0..200 {
                        if b.used.fetch_add(m.limit() / 150, SeqCst) + m.limit() / 150
                            > b.capacity()
                        {
                            let _ = b.pull_from_upstream();
                        }
                        if cancel_event.load(SeqCst) != 0 {
                            if killed.load(SeqCst) {
                                killed_pool.fetch_add(1, SeqCst);
                            } else {
                                cancel_pool.fetch_add(1, SeqCst);
                            }
                            break;
                        }
                    }
                    m.reset_root_pool_by_id(i as u64, b.used.load(SeqCst), true);
                })
                .unwrap(),
        );
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(m.root_pool_num.load(SeqCst), n as i64);
    for i in 0..n {
        m.remove_root_pool_by_id(i as u64);
    }
    m.stop();
    check_entries(m, &[]);
    (cancel_pool.load(SeqCst), killed_pool.load(SeqCst))
}

#[test]
fn bench() {
    const N: usize = 3000;
    let m = MemArbitrator::new(
        4 * BYTE_SIZE_GB,
        DEF_POOL_STATUS_SHARDS,
        DEF_POOL_QUOTA_SHARDS,
        64 * BYTE_SIZE_KB,
        Box::new(RecorderForTest {
            load_fn: Arc::new(Mutex::new(Box::new(|| Ok(None)))),
            store_fn: Arc::new(Mutex::new(Box::new(|_| Ok(())))),
        }),
    );
    m.set_work_mode(ArbitratorWorkMode::Standard);
    m.init_await_free_pool(4, 4);
    {
        let (cancel_pool, killed_pool) = bench_round(&m, N, false);
        assert_eq!(killed_pool, 0);
        assert_eq!(cancel_pool, N as i64);
        assert_eq!(m.exec_metrics().task.pair.fail, N as i64);
        assert_eq!(m.exec_metrics().cancel.standard_mode, N as i64);
        reset_exec_metrics_for_test(&m);
    }

    m.set_work_mode(ArbitratorWorkMode::Priority);
    {
        let (cancel_pool, killed_pool) = bench_round(&m, N, true);
        assert_eq!(killed_pool, 0);
        assert_ne!(cancel_pool, 0);
        let em = m.exec_metrics();
        assert!(em.task.pair.fail >= (N / 2) as i64);
        assert_eq!(
            cancel_pool,
            em.cancel.wait_averse
                + em.cancel.priority_mode[0]
                + em.cancel.priority_mode[1]
                + em.cancel.priority_mode[2]
        );
        assert_eq!(em.cancel.wait_averse, (N / 2) as i64);
        // under priority mode, the arbitrator may cancel pools which are
        // not waiting for alloc
        assert!(cancel_pool >= em.task.pair.fail);
        reset_exec_metrics_for_test(&m);
    }
}
