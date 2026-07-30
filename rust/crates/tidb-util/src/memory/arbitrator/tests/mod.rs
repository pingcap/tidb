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

//! Test fixtures and helpers for the `MemArbitrator` (Go
//! `pkg/util/memory/arbitrator_test.go`), shared with the two large cases
//! that live in sibling files.

use super::*;
use crate::memory::pool::{new_pool_uid, DEF_MAX_UNUSED_BLOCKS};
use std::sync::atomic::AtomicI64;

mod bench;
mod full_flow;

const NO_WAIT_AVERSE: bool = false;
const REQUIRE_PRIVILEGE: bool = true;

type CbMap = Arc<Mutex<HashMap<u64, i32>>>;

struct HelperForTest {
    cancel: Mutex<Option<CancelHandle>>,
    kill_cb: Mutex<Option<Box<dyn Fn() + Send>>>,
    heap_used_cb: Mutex<Option<Box<dyn Fn() -> i64 + Send>>>,
    cancel_cb: Mutex<Option<Box<dyn Fn() + Send>>>,
}

impl HelperForTest {
    fn new_with_channel() -> (Arc<HelperForTest>, CancelChannel) {
        let (handle, ch) = cancel_channel();
        (
            Arc::new(HelperForTest {
                cancel: Mutex::new(Some(handle)),
                kill_cb: Mutex::new(None),
                heap_used_cb: Mutex::new(None),
                cancel_cb: Mutex::new(None),
            }),
            ch,
        )
    }
    fn cancel_self(&self) {
        let _ = self.cancel.lock().unwrap().take();
    }
    fn set_kill_cb(&self, f: impl Fn() + Send + 'static) {
        *self.kill_cb.lock().unwrap() = Some(Box::new(f));
    }
    fn set_heap_used_cb(&self, f: impl Fn() -> i64 + Send + 'static) {
        *self.heap_used_cb.lock().unwrap() = Some(Box::new(f));
    }
    fn set_cancel_cb(&self, f: impl Fn() + Send + 'static) {
        *self.cancel_cb.lock().unwrap() = Some(Box::new(f));
    }
}

impl ArbitrateHelper for HelperForTest {
    fn stop(&self, reason: ArbitratorStopReason) -> bool {
        let _ = self.cancel.lock().unwrap().take();
        if reason == ArbitratorStopReason::OomRiskKill {
            if let Some(f) = self.kill_cb.lock().unwrap().as_ref() {
                f();
            }
        } else if let Some(f) = self.cancel_cb.lock().unwrap().as_ref() {
            f();
        }
        true
    }
    fn heap_inuse(&self) -> i64 {
        match self.heap_used_cb.lock().unwrap().as_ref() {
            Some(f) => f(),
            None => 0,
        }
    }
    fn finish(&self) {}
}

type StoreFn = Box<dyn Fn(&RuntimeMemStateV1) -> Result<(), String> + Send>;
type LoadFn = Box<dyn Fn() -> Result<Option<RuntimeMemStateV1>, String> + Send>;

#[derive(Clone)]
struct RecorderForTest {
    load_fn: Arc<Mutex<LoadFn>>,
    store_fn: Arc<Mutex<StoreFn>>,
}

impl RecorderForTest {
    fn set_store(&self, f: impl Fn(&RuntimeMemStateV1) -> Result<(), String> + Send + 'static) {
        *self.store_fn.lock().unwrap() = Box::new(f);
    }
}

impl RecordMemState for RecorderForTest {
    fn load(&self) -> Result<Option<RuntimeMemStateV1>, String> {
        (self.load_fn.lock().unwrap())()
    }
    fn store(&self, s: &RuntimeMemStateV1) -> Result<(), String> {
        (self.store_fn.lock().unwrap())(s)
    }
}

fn new_arbitrator_for_test(shard_count: u64, limit: i64) -> (Arc<MemArbitrator>, RecorderForTest) {
    let load_event = Arc::new(AtomicI64::new(0));
    let le = Arc::clone(&load_event);
    let recorder = RecorderForTest {
        load_fn: Arc::new(Mutex::new(Box::new(move || {
            le.fetch_add(1, SeqCst);
            Ok(None)
        }))),
        store_fn: Arc::new(Mutex::new(Box::new(|_| Ok(())))),
    };
    let m = MemArbitrator::new(limit, shard_count, 3, 0, Box::new(recorder.clone()));
    assert_eq!(load_event.load(SeqCst), 1);
    m.init_await_free_pool(4, 4);
    (m, recorder)
}

fn set_actions(
    m: &MemArbitrator,
    info: impl Fn(&str) + Send + Sync + 'static,
    warn: impl Fn(&str) + Send + Sync + 'static,
    error: impl Fn(&str) + Send + Sync + 'static,
    update: Option<Box<dyn Fn() + Send + Sync>>,
    gc: Option<Box<dyn Fn() + Send + Sync>>,
) {
    *m.actions.lock().unwrap() = Arc::new(MemArbitratorActions {
        info: Box::new(info),
        warn: Box::new(warn),
        error: Box::new(error),
        update_runtime_mem_stats: update,
        gc,
    });
}

fn new_resource_pool_for_test(name: &str, alloc_align_size: i64) -> Arc<ResourcePool> {
    ResourcePool::new(
        new_pool_uid(),
        name,
        0,
        alloc_align_size,
        DEF_MAX_UNUSED_BLOCKS,
        Default::default(),
    )
}

fn set_limit_for_test(m: &MemArbitrator, v: i64) {
    let _g = m.mu.lock().unwrap();
    m.mu_limit.store(v, SeqCst);
}

fn cleanup_notifer(m: &MemArbitrator) {
    m.notifer.wake();
    m.notifer.wait();
}

fn new_ctx_with_helper(
    mem_priority: ArbitrationPriority,
    wait_averse: bool,
    prefer_privilege: bool,
) -> (Arc<ArbitrationContext>, Arc<HelperForTest>) {
    let (h, ch) = HelperForTest::new_with_channel();
    let ctx = ArbitrationContext::new(
        Some(ch),
        Some(Arc::clone(&h) as Arc<dyn ArbitrateHelper>),
        mem_priority,
        wait_averse,
        prefer_privilege,
    );
    (ctx, h)
}

fn new_def_ctx(mem_priority: ArbitrationPriority) -> Arc<ArbitrationContext> {
    ArbitrationContext::new(None, None, mem_priority, NO_WAIT_AVERSE, false)
}

fn add_root_pool_for_test(
    m: &Arc<MemArbitrator>,
    p: Arc<ResourcePool>,
    ctx: Option<Arc<ArbitrationContext>>,
) -> Arc<RootPoolEntry> {
    let entry = m.add_root_pool(p).unwrap();
    assert!(m.restart_entry_by_context(
        RootPoolWrap {
            entry: Some(Arc::clone(&entry))
        },
        ctx
    ));
    entry
}

fn add_entry_for_test(
    m: &Arc<MemArbitrator>,
    ctx: Option<Arc<ArbitrationContext>>,
) -> Arc<RootPoolEntry> {
    let p = new_resource_pool_for_test("test", 1);
    add_root_pool_for_test(m, p, ctx)
}

fn new_pool_with_helper(
    m: &Arc<MemArbitrator>,
    prefix: &str,
    mem_priority: ArbitrationPriority,
    wait_averse: bool,
    prefer_privilege: bool,
) -> (Arc<RootPoolEntry>, Arc<HelperForTest>) {
    let (ctx, h) = new_ctx_with_helper(mem_priority, wait_averse, prefer_privilege);
    let pool = new_resource_pool_for_test("", 1);
    let pool = ResourcePool::new(
        pool.uid(),
        &format!("{prefix}-{}", pool.uid()),
        0,
        1,
        DEF_MAX_UNUSED_BLOCKS,
        Default::default(),
    );
    let e = add_root_pool_for_test(m, pool, Some(ctx));
    (e, h)
}

fn get_all_entries(m: &MemArbitrator) -> HashMap<u64, Arc<RootPoolEntry>> {
    let mut res = HashMap::new();
    let mut empty_cnt = 0i64;
    for shard in &m.entry_map.shards {
        for (uid, v) in shard.entries.read().unwrap().iter() {
            res.insert(*uid, Arc::clone(v));
            let st = v.arbitrator_mu.lock().unwrap();
            if st.quota == 0 {
                empty_cnt += 1;
                assert!(st.quota_shard.is_none());
            } else {
                assert!(st.quota_shard.is_some());
            }
        }
    }
    let mut cnt = res.len() as i64;
    assert_eq!(m.root_pool_num(), cnt);
    assert_eq!(m.entry_map.context_cache_num.load(SeqCst), cnt);

    for prio in PRIORITIES {
        for (pos, shard) in m.entry_map.quota_shards[prio as usize].iter().enumerate() {
            for (uid, v) in shard.lock().unwrap().entries.iter() {
                let e = res.get(uid).expect("entry in quota shard must exist");
                assert!(Arc::ptr_eq(e, v));
                assert_eq!(
                    v.arbitrator_mu.lock().unwrap().quota_shard,
                    Some((prio, pos))
                );
                cnt -= 1;
            }
        }
    }
    cnt -= empty_cnt;
    assert_eq!(cnt, 0);
    res
}

fn check_entries(m: &MemArbitrator, expect: &[&Arc<RootPoolEntry>]) {
    let s = get_all_entries(m);
    assert_eq!(expect.len(), s.len());
    let mut sum_quota = 0i64;
    for entry in expect {
        let uid = entry.pool.uid();
        let e = s.get(&uid).expect("expected entry present");
        assert!(Arc::ptr_eq(e, entry));
        assert!(!e.arbitrator_mu.lock().unwrap().destroyed);
        assert!(entry.request.result_rx.try_recv().is_err());

        let st = e.arbitrator_mu.lock().unwrap();
        if st.quota == 0 {
            continue;
        }
        sum_quota += st.quota;
        assert_eq!(st.quota, entry.pool.approx_cap());
        let shard = (
            entry.mem_priority(),
            get_quota_shard(st.quota, m.entry_map.max_quota_shard_index),
        );
        assert_eq!(st.quota_shard, Some(shard));
    }
    sum_quota += m.await_free_pool_cap();
    assert_eq!(sum_quota, m.allocated());
}

fn check_entry_quota_by_priority(
    m: &MemArbitrator,
    e: &Arc<RootPoolEntry>,
    expected: ArbitrationPriority,
    quota: i64,
) {
    assert_eq!(expected, e.mem_priority());
    let st = e.arbitrator_mu.lock().unwrap();
    assert_eq!(quota, st.quota);
    if st.quota != 0 {
        let pos = get_quota_shard(st.quota, m.entry_map.max_quota_shard_index);
        assert_eq!(st.quota_shard, Some((expected, pos)));
        assert!(m.entry_map.quota_shards[expected as usize][pos]
            .lock()
            .unwrap()
            .entries
            .contains_key(&e.pool.uid()));
    }
}

fn delete_entries_for_test(m: &Arc<MemArbitrator>, entries: &[&Arc<RootPoolEntry>]) {
    for e in entries {
        m.remove_root_pool_by_id(e.pool.uid());
    }
    m.do_execute_cleanup_tasks();
    cleanup_notifer(m);
}

fn reset_entries_for_test(m: &Arc<MemArbitrator>, entries: &[&Arc<RootPoolEntry>]) {
    for e in entries {
        m.reset_root_pool_by_id(e.pool.uid(), 0, false);
    }
    m.do_execute_cleanup_tasks();
    cleanup_notifer(m);
}

fn find_task_by_mode(
    m: &MemArbitrator,
    e: &Arc<RootPoolEntry>,
    prio: ArbitrationPriority,
    wait_averse: bool,
) -> bool {
    let tasks = m.tasks.lock().unwrap();
    let mut found = false;
    for p in PRIORITIES {
        for (ele, v) in tasks.fifo_by_priority[p as usize].iter_live() {
            if Arc::ptr_eq(&v, e) {
                assert!(!found);
                assert_eq!(prio, p);
                found = true;
                assert_eq!(e.mem_priority(), p);
                assert_eq!(e.ctx.wait_averse.load(SeqCst), wait_averse);
                assert_eq!(e.task_mu.lock().unwrap().fifo_by_priority, ele);

                match e.load_ctx() {
                    None => {
                        assert_eq!(p, ArbitrationPriority::Medium);
                        assert!(!wait_averse);
                    }
                    Some(ctx) => {
                        assert_eq!(p, ctx.mem_priority);
                        assert_eq!(wait_averse, ctx.wait_averse);
                    }
                }

                if wait_averse {
                    assert!(!e.ctx.prefer_privilege.load(SeqCst));
                    let mut found2 = false;
                    for (ele2, v2) in tasks.fifo_wait_averse.iter_live() {
                        if Arc::ptr_eq(&v2, e) {
                            assert!(!found2);
                            assert_eq!(e.task_mu.lock().unwrap().fifo_wait_averse, ele2);
                            found2 = true;
                        }
                    }
                    assert!(found2);
                }
                {
                    let mut found3 = false;
                    for (ele3, v3) in tasks.fifo_tasks.iter_live() {
                        if Arc::ptr_eq(&v3, e) {
                            assert!(!found3);
                            assert_eq!(e.task_mu.lock().unwrap().fifo, ele3);
                            found3 = true;
                        }
                    }
                    assert!(found3);
                }
            }
        }
    }
    found
}

fn tasks_count(m: &MemArbitrator) -> i64 {
    let mut sz = 0i64;
    for p in PRIORITIES {
        sz += m.task_num_by_priority(p);
    }
    assert_eq!(sz, m.tasks.lock().unwrap().fifo_tasks.size());
    assert!(!(sz == 0 && m.waiting_alloc_size() != 0));
    sz
}

fn front_task_entry_for_test(m: &MemArbitrator) -> Option<Arc<RootPoolEntry>> {
    let entry = m.front_task_entry()?;
    assert!(entry.task_mu.lock().unwrap().fifo.valid());
    Some(entry)
}

fn check_task_exec(
    m: &MemArbitrator,
    task: PairSuccessFail,
    cancel_by_standard: i64,
    cancel: NumByPattern,
) {
    let em = m.exec_metrics();
    assert_eq!(em.task.pair, task);
    let s: i64 = em.task.succ_by_priority.iter().sum();
    if m.work_mode() == ArbitratorWorkMode::Priority {
        assert_eq!(s, task.succ);
    } else {
        assert_eq!(s, 0);
    }
    assert_eq!(
        em.cancel,
        ExecMetricsCancel {
            standard_mode: cancel_by_standard,
            priority_mode: [cancel[0], cancel[1], cancel[2]],
            wait_averse: cancel[3],
        }
    );
}

fn reset_exec_metrics_for_test(m: &MemArbitrator) {
    m.exec_metrics.reset();
    m.exec_mu.lock().unwrap().blocked_state = BlockedState::default();
    m.set_buffer_size(0);
    for (lock, elem) in m.buffer_timed_map.iter().zip(m.buffer_timed_elems.iter()) {
        let _g = lock.write().unwrap();
        elem.ts.store(0, SeqCst);
        elem.size.store(0, SeqCst);
        elem.quota.store(0, SeqCst);
    }
    let n = m.digest_shards.lock().unwrap().len() as u64;
    m.reset_digest_profile_cache(n);
    m.reset_statistics();
    m.mu_last_gc.store(m.mu_released.load(SeqCst), SeqCst);
}

fn reset_await_free_for_test(m: &MemArbitrator) {
    let af = m.await_free_state().unwrap();
    for b in &af.shards {
        b.budget.used.store(0, SeqCst);
        b.heap_inuse.store(0, SeqCst);
        b.budget.set_last_used_time_sec(0);
    }
    assert_eq!(m.await_free_pool_used(), MemPoolQuotaUsage::default());
    m.shrink_await_free_pool(0, now_unix_milli());
    assert_eq!(m.await_free_pool_cap(), 0);
}

fn check_await_free(m: &MemArbitrator) {
    let af = m.await_free_state().unwrap();
    let s: i64 = af
        .shards
        .iter()
        .map(|b| b.budget.capacity.load(SeqCst))
        .sum();
    assert_eq!(s, af.pool.allocated());
}

#[test]
fn mem_arbitrator_switch_mode() {
    let (m, _recorder) = new_arbitrator_for_test(1, -1);
    assert_eq!(m.work_mode(), ArbitratorWorkMode::Disable);
    reset_exec_metrics_for_test(&m);
    let new_limit = 1000i64;
    set_limit_for_test(&m, new_limit);
    assert_eq!(m.limit(), new_limit);
    let action_cancel: CbMap = Arc::new(Mutex::new(HashMap::new()));

    let gen_test_pool = |mem_priority, wait_averse, prefer_privilege| {
        let (e, h) = new_pool_with_helper(&m, "test", mem_priority, wait_averse, prefer_privilege);
        let uid = e.pool.uid();
        let ac = Arc::clone(&action_cancel);
        h.set_cancel_cb(move || {
            *ac.lock().unwrap().entry(uid).or_insert(0) += 1;
        });
        (e, h)
    };

    let gen_test_ctx = |e: &Arc<RootPoolEntry>, mem_priority, wait_averse, prefer_privilege| {
        let (c, h) = new_ctx_with_helper(mem_priority, wait_averse, prefer_privilege);
        let uid = e.pool.uid();
        let ac = Arc::clone(&action_cancel);
        h.set_cancel_cb(move || {
            *ac.lock().unwrap().entry(uid).or_insert(0) += 1;
        });
        (c, h)
    };

    let (entry1, _h1) = gen_test_pool(ArbitrationPriority::Medium, NO_WAIT_AVERSE, false);
    {
        // Disable mode
        entry1.ctx.prefer_privilege.store(true, SeqCst);
        assert_eq!(entry1.exec_state(), EntryExecState::Running);
        assert!(!entry1.state_mu.stop.load(SeqCst));
        let request_size = new_limit * 2;

        m.prepare_alloc(&entry1, request_size);
        m.notifer.wait();
        assert_eq!(tasks_count(&m), 1);
        assert_eq!(m.waiting_alloc_size(), request_size);
        assert!(m.waiting_alloc_size() > m.limit());

        assert!(Arc::ptr_eq(
            &front_task_entry_for_test(&m).unwrap(),
            &entry1
        ));
        assert!(Arc::ptr_eq(
            &m.get_root_pool_entry(entry1.pool.uid()).unwrap(),
            &entry1
        ));
        assert_eq!(m.task_num_by_pattern(), [0, 1, 0, 0]);
        check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Medium, 0);
        assert_eq!(m.run_one_round(), -1);
        assert_eq!(m.exec_mu.lock().unwrap().blocked_state.allocated, 0);
        assert_eq!(m.wait_alloc(&entry1), ArbitrateResult::Ok);
        assert_eq!(tasks_count(&m), 0);

        assert!(!find_task_by_mode(
            &m,
            &entry1,
            ArbitrationPriority::Medium,
            NO_WAIT_AVERSE
        ));
        check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Medium, request_size);
        check_task_exec(&m, PairSuccessFail { succ: 1, fail: 0 }, 0, [0; 4]);

        assert!(m.privileged_entry.lock().unwrap().is_none());
        assert_eq!(entry1.exec_state(), EntryExecState::Running);
        assert!(!entry1.state_mu.stop.load(SeqCst));

        assert_eq!(m.allocated(), request_size);
        assert!(m.allocated() > m.limit());
        check_entries(&m, &[&entry1]);

        {
            // illegal operation
            let e0 = add_entry_for_test(&m, None);
            m.prepare_alloc(&e0, 1);
            assert_eq!(m.root_pool_num(), 2);
            assert_eq!(m.run_one_round(), -1);
            assert_eq!(m.wait_alloc(&e0), ArbitrateResult::Ok);
            assert_eq!(e0.arbitrator_mu.lock().unwrap().quota, 1);

            m.prepare_alloc(&e0, 1);
            assert_eq!(
                e0.task_mu.lock().unwrap().fifo_priority,
                Some(ArbitrationPriority::Medium)
            );
            m.reset_root_pool_entry(&e0);
            let (c, _h) = new_ctx_with_helper(ArbitrationPriority::Low, NO_WAIT_AVERSE, false);
            assert!(m.restart_entry_by_context(
                RootPoolWrap {
                    entry: Some(Arc::clone(&e0))
                },
                Some(c)
            ));
            assert_eq!(e0.mem_priority(), ArbitrationPriority::Low);
            assert_eq!(
                e0.task_mu.lock().unwrap().fifo_priority,
                Some(ArbitrationPriority::Medium)
            );
            if m.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Disable {
                m.implicit_run();
            }
            assert_eq!(m.wait_alloc(&e0), ArbitrateResult::Ok);
            assert_eq!(e0.arbitrator_mu.lock().unwrap().quota, 2);
            assert_eq!(
                e0.arbitrator_mu.lock().unwrap().quota,
                e0.pool.capacity() + e0.state_mu.quota_to_reclaim.load(SeqCst)
            );
            assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 1);

            m.reset_root_pool_entry(&e0);
            assert!(m.remove_root_pool_entry(&e0));
            assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 3);
            m.prepare_alloc(&e0, 1);
            assert_eq!(m.run_one_round(), -1);
            assert_eq!(m.wait_alloc(&e0), ArbitrateResult::Fail);
            assert!(m.get_root_pool_entry(e0.pool.uid()).is_none());
            assert_eq!(m.root_pool_num(), 1);
            m.prepare_alloc(&e0, 1);
            assert_eq!(m.run_one_round(), -1);
            assert_eq!(m.wait_alloc(&e0), ArbitrateResult::Fail);

            let e1 = add_entry_for_test(&m, None);
            assert!(m.remove_root_pool_entry(&e1));
            assert!(!m.reset_root_pool_entry(&e1));
            assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 1);
            m.prepare_alloc(&e1, 1);
            if m.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Disable {
                m.implicit_run();
            }
            assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
            m.do_execute_cleanup_tasks();
        }
    }

    let (entry2, _h2) = gen_test_pool(ArbitrationPriority::Medium, NO_WAIT_AVERSE, false);
    reset_exec_metrics_for_test(&m);
    cleanup_notifer(&m);
    {
        // Disable mode -> Standard mode
        assert_eq!(m.allocated(), 2 * new_limit);
        let request_size = 1i64;

        m.prepare_alloc(&entry2, request_size);
        m.notifer.wait();
        assert_eq!(tasks_count(&m), 1);
        assert_eq!(m.waiting_alloc_size(), request_size);
        assert!(Arc::ptr_eq(
            &front_task_entry_for_test(&m).unwrap(),
            &entry2
        ));
        assert_eq!(entry2.request.quota.load(SeqCst), request_size);
        assert!(find_task_by_mode(
            &m,
            &entry2,
            ArbitrationPriority::Medium,
            NO_WAIT_AVERSE
        ));
        assert_eq!(m.task_num_by_pattern(), [0, 1, 0, 0]);
        check_entry_quota_by_priority(&m, &entry2, ArbitrationPriority::Medium, 0);

        m.set_work_mode(ArbitratorWorkMode::Standard);
        assert_eq!(m.work_mode(), ArbitratorWorkMode::Standard);

        assert_eq!(m.run_one_round(), 0);
        assert_eq!(
            m.exec_mu.lock().unwrap().blocked_state.allocated,
            2 * new_limit
        );

        check_task_exec(&m, PairSuccessFail::default(), 1, [0; 4]);
        assert_eq!(action_cancel.lock().unwrap()[&entry2.pool.uid()], 1);
        assert_eq!(m.wait_alloc(&entry2), ArbitrateResult::Fail);

        assert!(!find_task_by_mode(
            &m,
            &entry2,
            ArbitrationPriority::Medium,
            NO_WAIT_AVERSE
        ));
        check_entry_quota_by_priority(&m, &entry2, ArbitrationPriority::Medium, 0);
        check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Medium, new_limit * 2);
        check_task_exec(&m, PairSuccessFail { succ: 0, fail: 1 }, 1, [0; 4]);

        assert_eq!(tasks_count(&m), 0);
        assert_eq!(entry1.exec_state(), EntryExecState::Running);
        assert_eq!(entry2.exec_state(), EntryExecState::Running);
        check_entries(&m, &[&entry1, &entry2]);

        let mut entries: Vec<Arc<RootPoolEntry>> = Vec::new();
        let mut helpers = Vec::new();
        for prio in PRIORITIES {
            for wait_averse in [false, true] {
                let (e, h) = gen_test_pool(prio, wait_averse, true);
                m.prepare_alloc(&e, m.limit());
                entries.push(e);
                helpers.push(h);
            }
        }
        {
            let mut all: Vec<&Arc<RootPoolEntry>> = entries.iter().collect();
            all.push(&entry1);
            all.push(&entry2);
            check_entries(&m, &all);
        }
        assert!(m.privileged_entry.lock().unwrap().is_none());
        assert_eq!(m.run_one_round(), 0);
        assert_eq!(
            m.exec_mu.lock().unwrap().blocked_state.allocated,
            2 * new_limit
        );
        assert!(m.privileged_entry.lock().unwrap().is_none());

        for e in &entries {
            assert_eq!(m.wait_alloc(e), ArbitrateResult::Fail);
        }
        check_task_exec(&m, PairSuccessFail { succ: 0, fail: 7 }, 7, [0; 4]);
        {
            let refs: Vec<&Arc<RootPoolEntry>> = entries.iter().collect();
            delete_entries_for_test(&m, &refs);
        }
        check_entries(&m, &[&entry1, &entry2]);
    }

    reset_exec_metrics_for_test(&m);
    cleanup_notifer(&m);
    action_cancel.lock().unwrap().clear();
    {
        // Standard mode -> Priority mode: wait until cancel self
        assert_eq!(m.allocated(), 2 * new_limit);
        let request_size = 1i64;
        reset_entries_for_test(&m, &[&entry2]);
        let (c, h2b) = gen_test_ctx(&entry2, ArbitrationPriority::Medium, NO_WAIT_AVERSE, false);
        assert!(m.restart_entry_by_context(
            RootPoolWrap {
                entry: Some(Arc::clone(&entry2))
            },
            Some(c)
        ));
        m.prepare_alloc(&entry2, request_size);
        m.notifer.wait();
        assert_eq!(tasks_count(&m), 1);
        assert_eq!(m.waiting_alloc_size(), request_size);
        let e = front_task_entry_for_test(&m).unwrap();
        assert!(Arc::ptr_eq(&e, &entry2));

        assert!(find_task_by_mode(
            &m,
            &e,
            ArbitrationPriority::Medium,
            NO_WAIT_AVERSE
        ));
        assert_eq!(m.task_num_by_pattern(), [0, 1, 0, 0]);
        check_entry_quota_by_priority(&m, &e, ArbitrationPriority::Medium, 0);
        check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Medium, new_limit * 2);

        m.set_work_mode(ArbitratorWorkMode::Priority);
        assert_eq!(m.work_mode(), ArbitratorWorkMode::Priority);

        assert_eq!(m.run_one_round(), 0);
        assert_eq!(tasks_count(&m), 1);
        assert_eq!(
            m.exec_mu.lock().unwrap().blocked_state.allocated,
            2 * new_limit
        );
        assert_eq!(m.waiting_alloc_size(), request_size);
        check_task_exec(&m, PairSuccessFail::default(), 0, [0; 4]);

        assert!(find_task_by_mode(
            &m,
            &e,
            ArbitrationPriority::Medium,
            NO_WAIT_AVERSE
        ));

        h2b.cancel_self();
        assert_eq!(m.wait_alloc(&entry2), ArbitrateResult::Fail);
        check_task_exec(&m, PairSuccessFail { succ: 0, fail: 1 }, 0, [0; 4]);
        assert!(action_cancel.lock().unwrap().is_empty());

        assert_eq!(tasks_count(&m), 0);
        check_entries(&m, &[&entry1, &entry2]);
    }

    reset_exec_metrics_for_test(&m);
    cleanup_notifer(&m);
    {
        // Priority mode: interrupt lower priority tasks
        assert!(action_cancel.lock().unwrap().is_empty());
        assert_eq!(m.allocated(), 2 * new_limit);
        let request_size = 1i64;

        reset_entries_for_test(&m, &[&entry2]);
        assert!(m.restart_entry_by_context(
            RootPoolWrap {
                entry: Some(Arc::clone(&entry2))
            },
            Some(new_def_ctx(ArbitrationPriority::High))
        ));
        m.prepare_alloc(&entry2, request_size);
        m.notifer.wait();
        assert_eq!(tasks_count(&m), 1);
        assert_eq!(m.waiting_alloc_size(), request_size);
        let e = front_task_entry_for_test(&m).unwrap();
        assert!(Arc::ptr_eq(
            &m.get_root_pool_entry(e.pool.uid()).unwrap(),
            &entry2
        ));
        assert_eq!(entry2.request.quota.load(SeqCst), request_size);
        assert!(find_task_by_mode(
            &m,
            &entry2,
            ArbitrationPriority::High,
            NO_WAIT_AVERSE
        ));
        assert_eq!(m.task_num_by_pattern(), [0, 0, 1, 0]);
        check_entry_quota_by_priority(&m, &entry2, ArbitrationPriority::High, 0);
        check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Medium, new_limit * 2);

        assert_eq!(m.run_one_round(), 0);
        assert_eq!(
            m.exec_mu.lock().unwrap().blocked_state.allocated,
            2 * new_limit
        );
        check_task_exec(&m, PairSuccessFail::default(), 0, [0, 1, 0, 0]);

        assert!(find_task_by_mode(
            &m,
            &entry2,
            ArbitrationPriority::High,
            NO_WAIT_AVERSE
        ));
        assert_eq!(m.task_num_by_pattern(), [0, 0, 1, 0]);

        assert_eq!(action_cancel.lock().unwrap()[&entry1.pool.uid()], 1);
        m.reset_root_pool_entry(&entry1);

        assert!(entry1.arbitrator_mu.lock().unwrap().under_cancel.start);
        {
            let st = entry1.arbitrator_mu.lock().unwrap();
            assert_eq!(st.under_cancel.reclaim, st.quota);
        }
        {
            let uc = m.under_cancel.lock().unwrap();
            assert_eq!(uc.num, 1);
            assert!(uc.entries.contains_key(&entry1.pool.uid()));
        }

        assert_eq!(m.run_one_round(), 1);
        assert_eq!(m.wait_alloc(&entry2), ArbitrateResult::Ok);
        check_task_exec(&m, PairSuccessFail { succ: 1, fail: 0 }, 0, [0, 1, 0, 0]);
        {
            let uc = m.under_cancel.lock().unwrap();
            assert_eq!(uc.num, 0);
            assert!(uc.entries.is_empty());
        }
        assert_eq!(tasks_count(&m), 0);

        assert!(!find_task_by_mode(
            &m,
            &entry2,
            ArbitrationPriority::High,
            NO_WAIT_AVERSE
        ));
        check_entry_quota_by_priority(&m, &entry2, ArbitrationPriority::High, request_size);
        check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Medium, 0);

        assert_eq!(entry1.exec_state(), EntryExecState::Idle);
        assert_eq!(entry2.exec_state(), EntryExecState::Running);
        check_entries(&m, &[&entry1, &entry2]);
        reset_entries_for_test(&m, &[&entry1, &entry2]);
    }

    reset_exec_metrics_for_test(&m);
    cleanup_notifer(&m);
    {
        // Priority mode -> Disable mode
        assert_eq!(m.allocated(), 0);
        m.consume_quota_from_await_free_pool(0, 99);

        let request_size = new_limit + 1;
        assert!(m.restart_entry_by_context(
            RootPoolWrap {
                entry: Some(Arc::clone(&entry1))
            },
            Some(new_def_ctx(ArbitrationPriority::Low))
        ));
        m.prepare_alloc(&entry1, request_size);
        assert!(m.restart_entry_by_context(
            RootPoolWrap {
                entry: Some(Arc::clone(&entry2))
            },
            Some(new_def_ctx(ArbitrationPriority::Low))
        ));
        m.prepare_alloc(&entry2, request_size);

        m.notifer.wait();
        assert_eq!(tasks_count(&m), 2);
        assert_eq!(m.waiting_alloc_size(), request_size * 2);
        assert!(find_task_by_mode(
            &m,
            &entry1,
            ArbitrationPriority::Low,
            NO_WAIT_AVERSE
        ));
        assert!(find_task_by_mode(
            &m,
            &entry2,
            ArbitrationPriority::Low,
            NO_WAIT_AVERSE
        ));
        assert_eq!(m.task_num_by_pattern(), [2, 0, 0, 0]);

        assert_eq!(m.run_one_round(), 0);
        assert_eq!(m.exec_mu.lock().unwrap().blocked_state.allocated, 100);
        check_task_exec(&m, PairSuccessFail::default(), 0, [0; 4]);

        assert_eq!(tasks_count(&m), 2);
        assert_eq!(m.waiting_alloc_size(), request_size * 2);

        m.set_work_mode(ArbitratorWorkMode::Disable);
        assert_eq!(m.run_one_round(), -1);
        assert_eq!(m.exec_mu.lock().unwrap().blocked_state.allocated, 0);

        assert_eq!(m.wait_alloc(&entry1), ArbitrateResult::Ok);
        assert_eq!(m.wait_alloc(&entry2), ArbitrateResult::Ok);
        check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Low, request_size);
        check_entry_quota_by_priority(&m, &entry2, ArbitrationPriority::Low, request_size);
        assert_eq!(m.allocated(), request_size * 2 + m.await_free_pool_cap());
        assert_eq!(tasks_count(&m), 0);
        m.consume_quota_from_await_free_pool(0, -99);
        reset_await_free_for_test(&m);
        check_entries(&m, &[&entry1, &entry2]);
        check_task_exec(&m, PairSuccessFail { succ: 2, fail: 0 }, 0, [0; 4]);
        reset_entries_for_test(&m, &[&entry1, &entry2]);
        assert_eq!(m.allocated(), 0);
    }

    m.set_work_mode(ArbitratorWorkMode::Priority);
    reset_exec_metrics_for_test(&m);
    action_cancel.lock().unwrap().clear();
    {
        // Priority mode: mixed task mode with wait-averse
        let alloc_unit = 4000i64;
        let (c1, _h1b) = gen_test_ctx(&entry1, ArbitrationPriority::Low, NO_WAIT_AVERSE, false);
        assert!(m.restart_entry_by_context(
            RootPoolWrap {
                entry: Some(Arc::clone(&entry1))
            },
            Some(c1)
        ));
        let (c2, _h2c) = gen_test_ctx(&entry2, ArbitrationPriority::Medium, NO_WAIT_AVERSE, false);
        assert!(m.restart_entry_by_context(
            RootPoolWrap {
                entry: Some(Arc::clone(&entry2))
            },
            Some(c2)
        ));
        let (entry3, _h3) = gen_test_pool(ArbitrationPriority::High, NO_WAIT_AVERSE, false);
        let (entry4, _h4) = gen_test_pool(ArbitrationPriority::Low, NO_WAIT_AVERSE, false);
        let (entry5, _h5) = gen_test_pool(ArbitrationPriority::High, true, false);
        check_entries(&m, &[&entry1, &entry2, &entry3, &entry4, &entry5]);

        set_limit_for_test(&m, 8 * alloc_unit);

        m.prepare_alloc(&entry1, alloc_unit);
        m.prepare_alloc(&entry2, alloc_unit);
        m.prepare_alloc(&entry3, alloc_unit);
        m.prepare_alloc(&entry4, alloc_unit * 4);
        m.prepare_alloc(&entry5, alloc_unit);

        assert!(find_task_by_mode(
            &m,
            &entry1,
            ArbitrationPriority::Low,
            NO_WAIT_AVERSE
        ));
        assert!(find_task_by_mode(
            &m,
            &entry2,
            ArbitrationPriority::Medium,
            NO_WAIT_AVERSE
        ));
        assert!(find_task_by_mode(
            &m,
            &entry3,
            ArbitrationPriority::High,
            NO_WAIT_AVERSE
        ));
        assert!(find_task_by_mode(
            &m,
            &entry4,
            ArbitrationPriority::Low,
            NO_WAIT_AVERSE
        ));
        assert!(find_task_by_mode(
            &m,
            &entry5,
            ArbitrationPriority::High,
            true
        ));

        assert_eq!(tasks_count(&m), 5);
        assert_eq!(m.waiting_alloc_size(), 8 * alloc_unit);
        assert_eq!(m.task_num_by_pattern(), [2, 1, 2, 1]);

        assert_eq!(m.run_one_round(), 5);
        assert_eq!(m.exec_mu.lock().unwrap().blocked_state.allocated, 0);
        assert_eq!(m.allocated(), 8 * alloc_unit);

        assert_eq!(m.wait_alloc(&entry1), ArbitrateResult::Ok);
        assert_eq!(m.wait_alloc(&entry2), ArbitrateResult::Ok);
        assert_eq!(m.wait_alloc(&entry3), ArbitrateResult::Ok);
        assert_eq!(m.wait_alloc(&entry4), ArbitrateResult::Ok);
        assert_eq!(m.wait_alloc(&entry5), ArbitrateResult::Ok);

        check_task_exec(&m, PairSuccessFail { succ: 5, fail: 0 }, 0, [0; 4]);
        check_entry_quota_by_priority(&m, &entry1, ArbitrationPriority::Low, alloc_unit);
        check_entry_quota_by_priority(&m, &entry2, ArbitrationPriority::Medium, alloc_unit);
        check_entry_quota_by_priority(&m, &entry3, ArbitrationPriority::High, alloc_unit);
        check_entry_quota_by_priority(&m, &entry4, ArbitrationPriority::Low, alloc_unit * 4);
        check_entry_quota_by_priority(&m, &entry5, ArbitrationPriority::High, alloc_unit);

        {
            let s4 = entry4.arbitrator_mu.lock().unwrap().quota_shard;
            let s1 = entry1.arbitrator_mu.lock().unwrap().quota_shard;
            assert_ne!(s4, s1);
        }

        m.prepare_alloc(&entry1, alloc_unit);
        m.prepare_alloc(&entry2, alloc_unit);
        m.prepare_alloc(&entry3, alloc_unit);
        m.prepare_alloc(&entry4, alloc_unit);
        m.prepare_alloc(&entry5, alloc_unit);

        assert_eq!(tasks_count(&m), 5);
        assert_eq!(m.waiting_alloc_size(), 5 * alloc_unit);
        assert_eq!(m.task_num_by_pattern(), [2, 1, 2, 1]);

        assert_eq!(m.under_cancel.lock().unwrap().num, 0);
        assert_eq!(m.run_one_round(), 0);
        assert_eq!(
            m.exec_mu.lock().unwrap().blocked_state.allocated,
            8 * alloc_unit
        );

        assert_eq!(m.wait_alloc(&entry5), ArbitrateResult::Fail);
        assert_eq!(m.wait_alloc(&entry4), ArbitrateResult::Fail);
        assert_eq!(action_cancel.lock().unwrap()[&entry5.pool.uid()], 1);
        assert!(!entry5.arbitrator_mu.lock().unwrap().under_cancel.start);
        assert_eq!(action_cancel.lock().unwrap()[&entry4.pool.uid()], 1);
        assert!(entry4.arbitrator_mu.lock().unwrap().under_cancel.start);
        assert_eq!(m.under_cancel.lock().unwrap().num, 1);
        assert!(m
            .under_cancel
            .lock()
            .unwrap()
            .entries
            .contains_key(&entry4.pool.uid()));
        check_task_exec(&m, PairSuccessFail { succ: 5, fail: 2 }, 0, [1, 0, 0, 1]);
        assert_eq!(m.task_num_by_pattern(), [1, 1, 1, 0]);
        assert_eq!(tasks_count(&m), 3);
        assert_eq!(m.waiting_alloc_size(), 3 * alloc_unit);

        let ori_metrics = m.exec_metrics();
        assert_eq!(m.run_one_round(), 0);
        assert_eq!(
            m.exec_mu.lock().unwrap().blocked_state.allocated,
            8 * alloc_unit
        );
        assert_eq!(m.exec_metrics(), ori_metrics);

        reset_entries_for_test(&m, &[&entry5]);
        assert_eq!(m.run_one_round(), 1);
        assert_eq!(m.wait_alloc(&entry3), ArbitrateResult::Ok);
        check_task_exec(&m, PairSuccessFail { succ: 6, fail: 2 }, 0, [1, 0, 0, 1]);
        assert_eq!(tasks_count(&m), 2);
        assert_eq!(m.waiting_alloc_size(), 2 * alloc_unit);
        set_limit_for_test(&m, alloc_unit);
        reset_entries_for_test(&m, &[&entry3, &entry4]);
        assert_eq!(m.under_cancel.lock().unwrap().num, 0);

        assert_eq!(m.run_one_round(), 0);
        assert_eq!(
            m.exec_mu.lock().unwrap().blocked_state.allocated,
            2 * alloc_unit
        );

        check_task_exec(&m, PairSuccessFail { succ: 6, fail: 2 }, 0, [2, 0, 0, 1]);
        assert_eq!(m.under_cancel.lock().unwrap().num, 1);
        assert!(m
            .under_cancel
            .lock()
            .unwrap()
            .entries
            .contains_key(&entry1.pool.uid()));
        assert!(entry1.arbitrator_mu.lock().unwrap().under_cancel.start);
        assert_eq!(m.wait_alloc(&entry1), ArbitrateResult::Fail);
        check_task_exec(&m, PairSuccessFail { succ: 6, fail: 3 }, 0, [2, 0, 0, 1]);
        assert_eq!(action_cancel.lock().unwrap()[&entry1.pool.uid()], 1);

        set_limit_for_test(&m, 3 * alloc_unit);
        assert_eq!(m.run_one_round(), 1);
        assert_eq!(m.wait_alloc(&entry2), ArbitrateResult::Ok);
        check_task_exec(&m, PairSuccessFail { succ: 7, fail: 3 }, 0, [2, 0, 0, 1]);
        assert_eq!(tasks_count(&m), 0);

        reset_entries_for_test(&m, &[&entry1, &entry2]);
        reset_exec_metrics_for_test(&m);
        action_cancel.lock().unwrap().clear();

        let restart = |e: &Arc<RootPoolEntry>, prio| {
            let (c, h) = gen_test_ctx(e, prio, NO_WAIT_AVERSE, false);
            assert!(m.restart_entry_by_context(
                RootPoolWrap {
                    entry: Some(Arc::clone(e))
                },
                Some(c)
            ));
            h
        };
        let _ra = restart(&entry1, ArbitrationPriority::Low);
        let _rb = restart(&entry2, ArbitrationPriority::Medium);
        let _rc = restart(&entry3, ArbitrationPriority::High);
        let _rd = restart(&entry4, ArbitrationPriority::Low);
        let _re = restart(&entry5, ArbitrationPriority::Low);

        set_limit_for_test(&m, BASE_QUOTA_UNIT * 8);

        m.prepare_alloc(&entry1, 1);
        m.prepare_alloc(&entry3, BASE_QUOTA_UNIT * 4);
        m.prepare_alloc(&entry4, BASE_QUOTA_UNIT);
        m.prepare_alloc(&entry5, BASE_QUOTA_UNIT * 2);

        assert_eq!(tasks_count(&m), 4);
        assert_eq!(m.task_num_by_pattern(), [3, 0, 1, 0]);
        assert_eq!(m.run_one_round(), 4);

        let alloced = entry1.arbitrator_mu.lock().unwrap().quota
            + entry3.arbitrator_mu.lock().unwrap().quota
            + entry4.arbitrator_mu.lock().unwrap().quota
            + entry5.arbitrator_mu.lock().unwrap().quota;
        assert_eq!(m.allocated(), alloced);
        assert_eq!(m.task_num_by_pattern(), [0, 0, 0, 0]);

        assert_eq!(m.wait_alloc(&entry1), ArbitrateResult::Ok);
        assert_eq!(m.wait_alloc(&entry3), ArbitrateResult::Ok);
        assert_eq!(m.wait_alloc(&entry4), ArbitrateResult::Ok);
        assert_eq!(m.wait_alloc(&entry5), ArbitrateResult::Ok);
        assert_eq!(tasks_count(&m), 0);
        check_task_exec(&m, PairSuccessFail { succ: 4, fail: 0 }, 0, [0; 4]);

        m.prepare_alloc(&entry2, BASE_QUOTA_UNIT * 3);
        assert_eq!(tasks_count(&m), 1);

        assert_eq!(m.run_one_round(), 0);
        assert_eq!(action_cancel.lock().unwrap().len(), 2);
        assert_eq!(action_cancel.lock().unwrap()[&entry4.pool.uid()], 1);
        assert_eq!(action_cancel.lock().unwrap()[&entry5.pool.uid()], 1);
        check_task_exec(&m, PairSuccessFail { succ: 4, fail: 0 }, 0, [2, 0, 0, 0]);
        assert_eq!(m.task_num_by_pattern(), [0, 1, 0, 0]);
        assert_eq!(tasks_count(&m), 1);
        assert_eq!(m.under_cancel.lock().unwrap().num, 2);
        assert!(entry4.arbitrator_mu.lock().unwrap().under_cancel.start);
        assert!(entry5.arbitrator_mu.lock().unwrap().under_cancel.start);

        m.reset_root_pool_by_id(entry4.pool.uid(), 0, false);

        let (self_cancel_handle, self_cancel_ch) = cancel_channel();
        let prio4 = entry4.mem_priority();
        assert!(m.restart_entry_by_context(
            RootPoolWrap {
                entry: Some(Arc::clone(&entry4))
            },
            Some(ArbitrationContext::new(
                Some(self_cancel_ch),
                None,
                prio4,
                NO_WAIT_AVERSE,
                false
            ))
        ));
        m.prepare_alloc(&entry4, BASE_QUOTA_UNIT * 1000);
        assert_eq!(tasks_count(&m), 2);

        assert!(
            !entry4.state_mu.stop.load(SeqCst)
                && entry4.exec_state() != EntryExecState::Idle
                && entry4.state_mu.quota_to_reclaim.load(SeqCst) > 0
        );
        assert!(entry4.not_running());

        assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 1);
        assert!(Arc::ptr_eq(
            &m.cleanup_fifo.lock().unwrap().front().unwrap(),
            &entry4
        ));
        assert_eq!(m.run_one_round(), 0);
        assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 0);
        assert_eq!(m.task_num_by_pattern(), [1, 1, 0, 0]);
        assert!(!entry4.arbitrator_mu.lock().unwrap().under_cancel.start);
        assert!(!entry4.not_running());

        check_task_exec(&m, PairSuccessFail { succ: 4, fail: 0 }, 0, [2, 0, 0, 0]);
        {
            let uc = m.under_cancel.lock().unwrap();
            assert_eq!(uc.num, 1);
            assert!(uc.entries.contains_key(&entry5.pool.uid()));
        }

        self_cancel_handle.close();
        assert!(m.remove_task(&entry4));
        assert_eq!(m.task_num_by_pattern(), [0, 1, 0, 0]);

        let ori_fail_cnt = m.exec_metrics.task_fail.load(SeqCst);
        let m2 = Arc::clone(&m);
        let e4 = Arc::clone(&entry4);
        let h = std::thread::spawn(move || {
            while ori_fail_cnt == m2.exec_metrics.task_fail.load(SeqCst) {
                std::thread::yield_now();
            }
            e4.wind_up(0, ArbitrateResult::Ok);
        });
        assert_eq!(m.wait_alloc(&entry4), ArbitrateResult::Fail);
        h.join().unwrap();
        check_task_exec(&m, PairSuccessFail { succ: 4, fail: 1 }, 0, [2, 0, 0, 0]);
        assert!(m.remove_root_pool_entry(&entry5));

        assert!(entry5.not_running());
        assert!(entry5.state_mu.stop.load(SeqCst));
        assert!(!entry5.arbitrator_mu.lock().unwrap().destroyed);
        assert!(entry5.arbitrator_mu.lock().unwrap().under_cancel.start);
        assert_ne!(entry5.arbitrator_mu.lock().unwrap().quota, 0);

        assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 1);
        assert_eq!(m.run_one_round(), 1);
        assert_eq!(m.under_cancel.lock().unwrap().num, 0);
        assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 0);
        assert_eq!(m.exec_metrics().cancel.priority_mode, [2, 0, 0]);
        assert_eq!(m.exec_metrics().cancel.wait_averse, 0);

        assert!(entry5.arbitrator_mu.lock().unwrap().destroyed);
        assert!(!entry5.arbitrator_mu.lock().unwrap().under_cancel.start);
        assert_eq!(entry5.arbitrator_mu.lock().unwrap().quota, 0);

        assert_eq!(m.allocated(), alloced);
        assert_eq!(m.wait_alloc(&entry2), ArbitrateResult::Ok);
        assert_eq!(
            m.exec_metrics().task.pair,
            PairSuccessFail { succ: 5, fail: 1 }
        );
        cleanup_notifer(&m);
    }

    // drain the result channel state left by the raw wind_up above
    let _ = entry1;
}

fn set_mem_stats_for_test(
    m: &MemArbitrator,
    alloc: i64,
    heap_inuse: i64,
    total_alloc: i64,
    mem_off_heap: i64,
) {
    let last_gc = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    m.set_runtime_mem_stats(MemStats {
        heap_alloc: alloc,
        heap_inuse,
        total_free: total_alloc - alloc,
        mem_off_heap,
        last_gc,
    });
}

fn clean_digest_profile_for_test(m: &MemArbitrator) {
    let n = m.digest_num.load(SeqCst);
    assert_eq!(m.shrink_digest_profile(DEF_MAX, 0, 0), n);
    assert_eq!(m.digest_num.load(SeqCst), 0);
}

struct MockClock {
    t: Arc<Mutex<SystemTime>>,
}
impl MockClock {
    fn install(t0: SystemTime) -> MockClock {
        let t = Arc::new(Mutex::new(t0));
        let t2 = Arc::clone(&t);
        *test_time::MOCK_NOW_DYN.lock().unwrap() = Some(Box::new(move || *t2.lock().unwrap()));
        MockClock { t }
    }
    fn set(&self, v: SystemTime) {
        *self.t.lock().unwrap() = v;
    }
}
impl Drop for MockClock {
    fn drop(&mut self) {
        *test_time::MOCK_NOW_DYN.lock().unwrap() = None;
    }
}

// Serialize the two clock-mocking tests.
static CLOCK_TEST_GUARD: Mutex<()> = Mutex::new(());
