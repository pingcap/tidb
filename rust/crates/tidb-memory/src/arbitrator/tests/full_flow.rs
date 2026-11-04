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

//! The big end-to-end `MemArbitrator` test (Go `TestMemArbitrator` in
//! `pkg/util/memory/arbitrator_test.go`). Split out of `arbitrator.rs`'s
//! `tests` module, whose helpers it reaches through `use super::*`.

use super::*;

#[test]
#[allow(unused_assignments)] // `tl_pre`/`tl_now` mirror Go's `nextTime()` sequencing
fn mem_arbitrator() {
    let _guard = CLOCK_TEST_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    let (m, recorder) = new_arbitrator_for_test(3, -1);
    let new_limit = 1_000_000_000i64;
    m.set_work_mode(ArbitratorWorkMode::Standard);
    assert!(m.notifer.is_awake());
    m.notifer.wait();
    assert_eq!(m.work_mode(), ArbitratorWorkMode::Standard);

    // Standard mode
    {
        assert_eq!(m.allocated(), 0);
        assert_eq!(m.limit(), DEF_MAX_LIMIT);
        assert!(!m.notifer.is_awake());
        set_limit_for_test(&m, new_limit);
        assert_eq!(m.limit(), new_limit);
        assert_eq!(tasks_count(&m), 0);

        let expect_shard_count = 4usize; // next pow2 of 3
        assert_eq!(m.entry_map.shards.len(), expect_shard_count);
        assert_eq!(m.entry_map.shards_mask, (expect_shard_count - 1) as u64);
        for p in PRIORITIES {
            assert_eq!(
                m.entry_map.quota_shards[p as usize].len(),
                m.entry_map.max_quota_shard_index
            );
        }
        assert_eq!(m.run_one_round(), 0);
        check_entries(&m, &[]);

        let pool1_align = 4i64;
        let pool1 = new_resource_pool_for_test("root1", pool1_align);
        let uid1 = pool1.uid();
        let mut pb1 = pool1.create_budget();
        {
            // no out-of-memory action
            assert_eq!(pool1.limit(), DEF_MAX_LIMIT);
            assert!(pb1.grow(1).is_err());
            pb1.grow(0).unwrap();
            assert!(m.get_root_pool_entry(uid1).is_none());
            assert!(!m.notifer.is_awake());
            check_task_exec(&m, PairSuccessFail::default(), 0, [0; 4]);
        }

        let entry1 = add_root_pool_for_test(&m, Arc::clone(&pool1), None);
        {
            // with out-of-memory action
            check_entries(&m, &[&entry1]);
            assert!(!m.notifer.is_awake());
            assert_eq!(tasks_count(&m), 0);

            let m2 = Arc::clone(&m);
            let e1 = Arc::clone(&entry1);
            let u1 = uid1;
            let h = std::thread::spawn(move || {
                m2.notifer.wait();
                assert_eq!(tasks_count(&m2), 1);
                let e = front_task_entry_for_test(&m2).unwrap();
                assert!(Arc::ptr_eq(&e, &e1));
                assert!(Arc::ptr_eq(&m2.get_root_pool_entry(u1).unwrap(), &e1));
                check_entry_quota_by_priority(&m2, &e, ArbitrationPriority::Medium, 0);
                assert!(find_task_by_mode(
                    &m2,
                    &e,
                    ArbitrationPriority::Medium,
                    NO_WAIT_AVERSE
                ));
                assert_eq!(m2.run_one_round(), 1);
            });

            let grow_size = 1i64;
            pb1.grow(grow_size).unwrap();
            h.join().unwrap();

            assert_eq!(m.allocated(), pool1_align);
            assert_eq!(pb1.used(), grow_size);
            assert_eq!(pb1.capacity(), pool1_align);
            assert_eq!(pool1.capacity(), pool1_align);
            assert_eq!(entry1.arbitrator_mu.lock().unwrap().quota, pool1_align);
            assert!(!m.notifer.is_awake());
            assert_eq!(
                m.exec_metrics().task.pair,
                PairSuccessFail { succ: 1, fail: 0 }
            );
            cleanup_notifer(&m);
            check_entries(&m, &[&entry1]);

            let m2 = Arc::clone(&m);
            let e1 = Arc::clone(&entry1);
            let h = std::thread::spawn(move || {
                m2.notifer.wait();
                assert_eq!(tasks_count(&m2), 1);
                let e = front_task_entry_for_test(&m2).unwrap();
                assert!(Arc::ptr_eq(&e, &e1));
                assert_eq!(m2.run_one_round(), 1);
            });
            let grow_size = pool1_align - grow_size + 1;
            pb1.grow(grow_size).unwrap();
            h.join().unwrap();

            assert_eq!(m.allocated(), 2 * pool1_align);
            assert_eq!(pb1.used(), pool1_align + 1);
            assert_eq!(pb1.capacity(), pool1_align * 2);
            assert_eq!(pool1.capacity(), pool1_align * 2);
            assert_eq!(entry1.arbitrator_mu.lock().unwrap().quota, pool1_align * 2);
            assert!(!m.notifer.is_awake());
            check_task_exec(&m, PairSuccessFail { succ: 2, fail: 0 }, 0, [0; 4]);
            cleanup_notifer(&m);
            check_entries(&m, &[&entry1]);

            // same quota shard as quota 0
            {
                let st = entry1.arbitrator_mu.lock().unwrap();
                assert_eq!(
                    st.quota_shard,
                    Some((
                        ArbitrationPriority::Medium,
                        get_quota_shard(0, m.entry_map.max_quota_shard_index)
                    ))
                );
            }

            let m2 = Arc::clone(&m);
            let e1 = Arc::clone(&entry1);
            let h = std::thread::spawn(move || {
                m2.notifer.wait();
                assert_eq!(tasks_count(&m2), 1);
                let e = front_task_entry_for_test(&m2).unwrap();
                assert!(Arc::ptr_eq(&e, &e1));
                assert_eq!(m2.run_one_round(), 1);
            });
            pb1.grow(BASE_QUOTA_UNIT).unwrap();
            h.join().unwrap();
            {
                let st = entry1.arbitrator_mu.lock().unwrap();
                assert_eq!(
                    st.quota_shard,
                    Some((
                        ArbitrationPriority::Medium,
                        get_quota_shard(BASE_QUOTA_UNIT * 2 - 1, m.entry_map.max_quota_shard_index)
                    ))
                );
            }
            check_task_exec(&m, PairSuccessFail { succ: 3, fail: 0 }, 0, [0; 4]);
            reset_entries_for_test(&m, &[&entry1]);
        }

        {
            // illegal operation (standard mode variant)
            let e0 = add_entry_for_test(&m, None);
            m.prepare_alloc(&e0, 1);
            assert_eq!(m.root_pool_num(), 2);
            assert_eq!(m.run_one_round(), 1);
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
            m.do_execute_first_task();
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
            assert_eq!(m.run_one_round(), 0);
            assert_eq!(m.wait_alloc(&e0), ArbitrateResult::Fail);
            assert!(m.get_root_pool_entry(e0.pool.uid()).is_none());
            assert_eq!(m.root_pool_num(), 1);
            m.prepare_alloc(&e0, 1);
            assert_eq!(m.run_one_round(), 1);
            assert_eq!(m.wait_alloc(&e0), ArbitrateResult::Fail);

            let e1 = add_entry_for_test(&m, None);
            assert!(m.remove_root_pool_entry(&e1));
            assert!(!m.reset_root_pool_entry(&e1));
            assert_eq!(m.cleanup_fifo.lock().unwrap().size(), 1);
            m.prepare_alloc(&e1, 1);
            m.do_execute_first_task();
            assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
            m.do_execute_cleanup_tasks();
        }

        {
            // async run
            assert!(m.restart_entry_by_context(
                RootPoolWrap {
                    entry: Some(Arc::clone(&entry1))
                },
                entry1.load_ctx()
            ));
            let mut b = pool1.create_budget();
            assert!(m.async_run(Duration::from_secs(3600)));
            assert!(!m.async_run(Duration::from_secs(3600)));
            b.grow(BASE_QUOTA_UNIT).unwrap();
            assert!(m.stop());
            assert!(!m.stop());
            assert!(!m.control_running.load(SeqCst));
            assert_eq!(m.allocated(), b.capacity());
            assert_eq!(m.limit(), new_limit);
            check_entries(&m, &[&entry1]);
            reset_entries_for_test(&m, &[&entry1]);

            reset_exec_metrics_for_test(&m);
            let (cancel_handle, cancel_ch) = cancel_channel();
            assert!(m.restart_entry_by_context(
                RootPoolWrap {
                    entry: Some(Arc::clone(&entry1))
                },
                Some(ArbitrationContext::new(
                    Some(cancel_ch),
                    None,
                    ArbitrationPriority::Medium,
                    NO_WAIT_AVERSE,
                    false
                ))
            ));
            let m2 = Arc::clone(&m);
            let e1 = Arc::clone(&entry1);
            let cancel_slot = Arc::new(Mutex::new(Some(cancel_handle)));
            let cs = Arc::clone(&cancel_slot);
            let h = std::thread::spawn(move || {
                m2.notifer.wait();
                assert_eq!(tasks_count(&m2), 1);
                assert!(Arc::ptr_eq(&front_task_entry_for_test(&m2).unwrap(), &e1));
                let _ = cs.lock().unwrap().take(); // close(cancel)
            });
            // blocking grow with cancel
            let err = b.grow(BASE_QUOTA_UNIT).unwrap_err();
            assert_eq!(err.to_string(), ERR_ARBITRATE_FAIL);
            h.join().unwrap();
            assert_eq!(tasks_count(&m), 0);
            check_task_exec(&m, PairSuccessFail { succ: 0, fail: 1 }, 0, [0; 4]);
            reset_entries_for_test(&m, &[&entry1]);

            let (cancel_handle, cancel_ch) = cancel_channel();
            let _keep = cancel_handle;
            assert!(m.restart_entry_by_context(
                RootPoolWrap {
                    entry: Some(Arc::clone(&entry1))
                },
                Some(ArbitrationContext::new(
                    Some(cancel_ch),
                    None,
                    ArbitrationPriority::Medium,
                    NO_WAIT_AVERSE,
                    false
                ))
            ));
            let m2 = Arc::clone(&m);
            let e1 = Arc::clone(&entry1);
            let h = std::thread::spawn(move || {
                m2.notifer.wait();
                assert_eq!(tasks_count(&m2), 1);
                assert!(Arc::ptr_eq(&front_task_entry_for_test(&m2).unwrap(), &e1));
                assert_eq!(m2.run_one_round(), 0);
            });
            let err = b.grow(new_limit + 1).unwrap_err();
            assert_eq!(err.to_string(), ERR_ARBITRATE_FAIL);
            h.join().unwrap();
            assert_eq!(tasks_count(&m), 0);
            check_task_exec(&m, PairSuccessFail { succ: 0, fail: 2 }, 1, [0; 4]);
        }
        delete_entries_for_test(&m, &[&entry1]);
    }

    // Priority mode
    m.set_work_mode(ArbitratorWorkMode::Priority);
    assert_eq!(m.work_mode(), ArbitratorWorkMode::Priority);
    {
        // error paths (Go: panics in the test helper)
        {
            let pool = ResourcePool::new_default("?", 1);
            pool.start(None, 1);
            assert_eq!(pool.reserved(), 1);
            assert_eq!(
                m.add_root_pool(pool).err().unwrap().to_string(),
                "?: has 1 reserved budget left"
            );
        }
        {
            let pool = ResourcePool::new_default("?", 1);
            pool.force_add_cap_unlocked(1);
            assert_ne!(pool.approx_cap(), 0);
            assert_eq!(
                m.add_root_pool(pool).err().unwrap().to_string(),
                "?: has 1 bytes budget left"
            );
        }
        {
            let p1 = ResourcePool::new_default("p1", 1);
            let p2 = ResourcePool::new(
                p1.uid(),
                "p2",
                0,
                1,
                DEF_MAX_UNUSED_BLOCKS,
                Default::default(),
            );
            let p3 = ResourcePool::new_default("p3", 1);
            let e1 = add_root_pool_for_test(&m, Arc::clone(&p1), None);
            check_entries(&m, &[&e1]);
            assert_eq!(
                m.add_root_pool(Arc::clone(&p2)).err().unwrap().to_string(),
                "p2: already exists"
            );
            p3.start_no_reserved(Some(&p2));
            assert_eq!(
                m.add_root_pool(p3).err().unwrap().to_string(),
                "p3: already started with pool p2"
            );
            check_entries(&m, &[&e1]);
            let e = m.get_root_pool_entry(p1.uid()).unwrap();
            delete_entries_for_test(&m, &[&e]);
        }
        check_entries(&m, &[]);
    }
    {
        // prefer privileged budget under priority mode
        let new_limit = 100i64;
        reset_exec_metrics_for_test(&m);
        set_limit_for_test(&m, new_limit);
        let (e1, _h1) =
            new_pool_with_helper(&m, "e1", ArbitrationPriority::Low, NO_WAIT_AVERSE, true);
        e1.load_ctx().unwrap().set_helper(None);
        assert!(m.privileged_entry.lock().unwrap().is_none());
        assert!(e1.ctx.prefer_privilege.load(SeqCst));

        let req_quota = new_limit + 1;

        m.prepare_alloc(&e1, req_quota);
        assert!(find_task_by_mode(
            &m,
            &e1,
            ArbitrationPriority::Low,
            NO_WAIT_AVERSE
        ));
        assert_eq!(m.run_one_round(), 1);
        assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
        check_task_exec(&m, PairSuccessFail { succ: 1, fail: 0 }, 0, [0; 4]);
        check_entry_quota_by_priority(&m, &e1, ArbitrationPriority::Low, req_quota);
        assert!(Arc::ptr_eq(
            m.privileged_entry.lock().unwrap().as_ref().unwrap(),
            &e1
        ));

        let (e2, _h2) = new_pool_with_helper(&m, "e2", ArbitrationPriority::High, true, true);
        e2.load_ctx().unwrap().set_helper(None);
        assert!(e2.load_ctx().unwrap().prefer_privilege);
        assert!(!e2.ctx.prefer_privilege.load(SeqCst));
        m.prepare_alloc(&e2, req_quota);
        assert!(find_task_by_mode(&m, &e2, ArbitrationPriority::High, true));
        assert_eq!(m.run_one_round(), 0);
        assert_eq!(m.wait_alloc(&e2), ArbitrateResult::Fail);
        check_task_exec(&m, PairSuccessFail { succ: 1, fail: 1 }, 0, [0, 0, 0, 1]);
        check_entries(&m, &[&e1, &e2]);
        assert!(Arc::ptr_eq(
            m.privileged_entry.lock().unwrap().as_ref().unwrap(),
            &e1
        ));

        let (e3, _h3) =
            new_pool_with_helper(&m, "e3", ArbitrationPriority::Low, NO_WAIT_AVERSE, true);
        e3.load_ctx().unwrap().set_helper(None);
        assert!(e3.ctx.prefer_privilege.load(SeqCst));
        m.prepare_alloc(&e3, req_quota);
        assert_eq!(m.run_one_round(), 0);
        assert_eq!(tasks_count(&m), 1);

        m.prepare_alloc(&e1, req_quota);
        assert_eq!(tasks_count(&m), 2);
        assert_eq!(m.run_one_round(), 1);
        assert_eq!(tasks_count(&m), 1);
        assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
        check_task_exec(&m, PairSuccessFail { succ: 2, fail: 1 }, 0, [0, 0, 0, 1]);

        m.reset_root_pool_entry(&e1);
        assert!(m.restart_entry_by_context(
            RootPoolWrap {
                entry: Some(Arc::clone(&e1))
            },
            e1.load_ctx()
        ));
        m.prepare_alloc(&e1, req_quota);

        assert_eq!(tasks_count(&m), 2);
        assert!(Arc::ptr_eq(
            m.privileged_entry.lock().unwrap().as_ref().unwrap(),
            &e1
        ));
        assert_eq!(m.run_one_round(), 1);
        assert_eq!(m.wait_alloc(&e3), ArbitrateResult::Ok);
        check_task_exec(&m, PairSuccessFail { succ: 3, fail: 1 }, 0, [0, 0, 0, 1]);
        assert!(Arc::ptr_eq(
            m.privileged_entry.lock().unwrap().as_ref().unwrap(),
            &e3
        ));
        set_limit_for_test(&m, m.waiting_alloc_size() + m.allocated());
        assert_eq!(m.run_one_round(), 1);
        assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
        check_task_exec(&m, PairSuccessFail { succ: 4, fail: 1 }, 0, [0, 0, 0, 1]);
        assert!(Arc::ptr_eq(
            m.privileged_entry.lock().unwrap().as_ref().unwrap(),
            &e3
        ));
        check_entries(&m, &[&e1, &e2, &e3]);
        delete_entries_for_test(&m, &[&e1, &e2, &e3]);
    }

    {
        // soft-limit & limit
        let x = 1_000_000_000i64;
        assert!(!m.set_limit(u64::MAX));
        assert!(!m.notifer.is_awake());
        assert!(m.set_limit(x as u64));
        assert!(m.notifer.is_awake());
        assert_eq!(m.mu_limit.load(SeqCst), x);
        assert_eq!(
            m.mu_threshold_oom_risk.load(SeqCst),
            (x as f64 * DEF_OOM_RISK_RATIO) as i64
        );
        assert_eq!(
            m.mu_threshold_risk.load(SeqCst),
            (x as f64 * DEF_MEM_RISK_RATIO) as i64
        );
        assert_eq!(
            m.mu_soft_limit_size.load(SeqCst),
            m.mu_threshold_oom_risk.load(SeqCst)
        );
        cleanup_notifer(&m);
        assert!(m.set_limit(DEF_MAX_LIMIT as u64 + 1));
        assert!(m.notifer.is_awake());
        assert_eq!(m.mu_limit.load(SeqCst), DEF_MAX_LIMIT);
        assert_eq!(
            m.mu_threshold_oom_risk.load(SeqCst),
            (DEF_MAX_LIMIT as f64 * DEF_OOM_RISK_RATIO) as i64
        );
        assert_eq!(m.mu_soft_specified_size.load(SeqCst), 0);
        assert_eq!(
            m.mu_soft_limit_size.load(SeqCst),
            m.mu_threshold_oom_risk.load(SeqCst)
        );
        assert_eq!(
            *m.mu_soft_limit_mode.lock().unwrap(),
            SoftLimitMode::Disable
        );

        m.set_soft_limit(1, 0.0, SoftLimitMode::Specified);
        assert!(m.mu_soft_specified_size.load(SeqCst) > 0);
        assert_eq!(m.mu_soft_limit_size.load(SeqCst), 1);
        assert!(m.set_limit(100));
        assert_eq!(m.mu_soft_limit_size.load(SeqCst), 1);

        let rate = 0.8f64;
        m.set_soft_limit(0, rate, SoftLimitMode::Specified);
        assert_eq!(m.mu_soft_specified_size.load(SeqCst), 0);
        assert_eq!(
            m.mu_soft_specified_ratio.load(SeqCst),
            (rate * 1000.0) as i64
        );
        assert_eq!(
            m.mu_soft_limit_size.load(SeqCst),
            (rate * m.mu_limit.load(SeqCst) as f64) as i64
        );
        assert!(m.set_limit(200));
        assert_eq!(m.mu_soft_limit_size.load(SeqCst), (rate * 200.0) as i64);

        m.set_soft_limit(0, 0.0, SoftLimitMode::Disable);
        assert_eq!(
            m.mu_soft_limit_size.load(SeqCst),
            m.mu_threshold_oom_risk.load(SeqCst)
        );
        assert!(m.set_limit(100));
        assert_eq!(
            m.mu_soft_limit_size.load(SeqCst),
            m.mu_threshold_oom_risk.load(SeqCst)
        );

        m.set_soft_limit(0, 0.0, SoftLimitMode::Auto);
        assert_eq!(
            m.mu_soft_limit_size.load(SeqCst),
            m.mu_threshold_oom_risk.load(SeqCst)
        );
        assert!(m.set_limit(200));
        assert_eq!(
            m.mu_soft_limit_size.load(SeqCst),
            m.mu_threshold_oom_risk.load(SeqCst)
        );
    }

    {
        // out-of-control
        m.set_soft_limit(0, 0.0, SoftLimitMode::Disable);
        assert_eq!(m.await_free_pool_cap(), 0);
        assert_eq!(m.allocated(), 0);

        let af = m.await_free_state().unwrap();
        let ele_size = af.pool.alloc_align_size() + 1;
        let budgets_num = af.shards.len() as i64;
        let mut used_heap = ele_size * budgets_num;
        for b in &af.shards {
            b.budget
                .consume_quota(m.approx_unix_time_sec(), ele_size)
                .unwrap();
        }
        assert_eq!(m.await_free_pool_used().tracked_heap, 0);
        for b in &af.shards {
            b.report_heap_inuse(ele_size);
        }

        let mut expect = AwaitFreePoolExecMetrics {
            pair: PairSuccessFail {
                succ: budgets_num,
                fail: 0,
            },
            shrink: 0,
            force_shrink: 0,
        };
        assert_eq!(m.exec_metrics().await_free, expect);

        expect.pair.fail += 1;
        assert!(af.shards[0].budget.reserve(m.limit()).is_err());
        check_await_free(&m);
        assert_eq!(m.exec_metrics().await_free, expect);

        assert_eq!(m.await_free_pool_used().tracked_heap, used_heap);
        assert_eq!(
            m.await_free_pool_cap(),
            af.pool.round_size(ele_size) * budgets_num
        );
        {
            let ori = now_unix_milli() - DEF_TRACK_MEM_STATS_DUR_MILLI;
            m.heap_tracked_last_update_milli.store(ori, SeqCst);
            assert!(m.try_update_tracked_mem_stats(now_unix_milli()));
            assert_ne!(m.heap_tracked_last_update_milli.load(SeqCst), ori);
        }

        assert_eq!(m.heap_tracked.load(SeqCst), used_heap);
        assert_eq!(m.buffer_size.load(SeqCst), 0);
        assert_eq!(m.avoid_size.load(SeqCst), 0);

        let e1_mem = Arc::new(AtomicI64::new(13));
        let (e1, h1) =
            new_pool_with_helper(&m, "e1", ArbitrationPriority::Medium, NO_WAIT_AVERSE, false);
        let e1m = Arc::clone(&e1_mem);
        h1.set_heap_used_cb(move || e1m.load(SeqCst));
        let e2 = add_entry_for_test(&m, None);
        let e3 = add_entry_for_test(&m, Some(new_def_ctx(ArbitrationPriority::Medium)));
        m.update_tracked_heap_stats();
        used_heap += 13;
        assert_eq!(m.heap_tracked.load(SeqCst), used_heap);
        assert_eq!(m.buffer_size.load(SeqCst), 13);
        assert_eq!(m.avoid_size.load(SeqCst), 0);

        let free = 1024i64;
        e1_mem.store(17, SeqCst);
        set_mem_stats_for_test(&m, used_heap + 1, used_heap + 100, used_heap + free, 67);
        assert_eq!(m.hc_heap_inuse.load(SeqCst), used_heap + 100);
        assert_eq!(m.hc_heap_alloc.load(SeqCst), used_heap + 1);
        assert_eq!(m.hc_heap_total_free.load(SeqCst), free - 1);
        assert_eq!(m.hc_mem_inuse.load(SeqCst), used_heap + 100 + 67);
        assert_eq!(m.heap_tracked.load(SeqCst), used_heap);
        assert_eq!(m.buffer_size.load(SeqCst), 13); // no update
        assert_eq!(
            m.avoid_size.load(SeqCst),
            m.hc_heap_alloc.load(SeqCst) + m.hc_mem_off_heap.load(SeqCst)
                - m.heap_tracked.load(SeqCst)
        );
        assert!(
            m.avoid_size.load(SeqCst) > m.mu_limit.load(SeqCst) - m.mu_soft_limit_size.load(SeqCst)
        );

        m.update_tracked_heap_stats();
        assert_eq!(m.buffer_size.load(SeqCst), 17);
        e1_mem.store(3, SeqCst);
        m.update_tracked_heap_stats();
        assert_eq!(m.buffer_size.load(SeqCst), 17); // no update to smaller size

        let now = SystemTime::now();
        let now_unix = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        for (i, b) in af.shards.iter().enumerate() {
            if i % 2 == 0 {
                b.budget.used.store(0, SeqCst);
                if i == 0 {
                    b.budget.set_last_used_time_sec(
                        now_unix - (DEF_AWAIT_FREE_POOL_SHRINK_DUR_MILLI / KILO - 1),
                    );
                } else {
                    b.budget.set_last_used_time_sec(0);
                }
            } else {
                b.budget.used.store(1, SeqCst);
            }
        }
        cleanup_notifer(&m);
        assert!(!m.notifer.is_awake());
        {
            let now_milli = now
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            let ori = now_milli - DEF_AWAIT_FREE_POOL_SHRINK_DUR_MILLI;
            m.await_free_last_shrink_milli.store(ori, SeqCst);
            assert!(m.try_shrink_await_free_pool(0, now_milli));
            assert!(m.able_to_gc());
            assert_ne!(m.await_free_last_shrink_milli.load(SeqCst), ori);
        }
        assert!(m.notifer.is_awake());
        expect.shrink += 1;
        assert_eq!(m.exec_metrics().await_free, expect);
        check_await_free(&m);

        for (i, b) in af.shards.iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(b.budget.used.load(SeqCst), 0);
                if i == 0 {
                    assert_ne!(b.budget.capacity(), 0);
                } else {
                    assert_eq!(b.budget.capacity(), 0);
                }
            } else {
                assert_eq!(b.budget.used.load(SeqCst), 1);
            }
        }
        assert_eq!(m.await_free_pool_cap(), m.allocated());
        m.set_buffer_size(0);
        check_entries(&m, &[&e1, &e2, &e3]);
        delete_entries_for_test(&m, &[&e1, &e2, &e3]);
        check_entries(&m, &[]);
        reset_await_free_for_test(&m);
        set_mem_stats_for_test(&m, 0, 0, 0, 0);
        assert_eq!(m.allocated(), 0);
    }

    {
        // calc buffer & digest cache
        reset_exec_metrics_for_test(&m);
        m.try_to_update_buffer(2, DEF_UPDATE_BUFFER_TIME_ALIGN_SEC);
        assert_eq!(m.buffer_size.load(SeqCst), 2);
        m.try_to_update_buffer(1, DEF_UPDATE_BUFFER_TIME_ALIGN_SEC);
        assert_eq!(m.buffer_size.load(SeqCst), 2);
        m.try_to_update_buffer(4, DEF_UPDATE_BUFFER_TIME_ALIGN_SEC);
        assert_eq!(m.buffer_size.load(SeqCst), 4);
        m.try_to_update_buffer(1, DEF_UPDATE_BUFFER_TIME_ALIGN_SEC);
        assert_eq!(m.buffer_size.load(SeqCst), 4);
        m.try_to_update_buffer(
            1,
            DEF_UPDATE_BUFFER_TIME_ALIGN_SEC * (DEF_REDUNDANCY as i64),
        );
        assert_eq!(m.buffer_size.load(SeqCst), 4);
        m.try_to_update_buffer(
            3,
            DEF_UPDATE_BUFFER_TIME_ALIGN_SEC * (DEF_REDUNDANCY as i64 + 1),
        );
        assert_eq!(m.buffer_size.load(SeqCst), 3);
        m.try_to_update_buffer(
            1,
            DEF_UPDATE_BUFFER_TIME_ALIGN_SEC * (DEF_REDUNDANCY as i64 + 1),
        );
        assert_eq!(m.buffer_size.load(SeqCst), 3);
        m.set_buffer_size(0);

        m.set_limit(10000);
        assert_eq!(
            *m.pool_alloc_stats.read().unwrap(),
            PoolAllocProfile {
                small_pool_limit: 10,
                pool_alloc_unit: 20,
                max_pool_alloc_unit: 100
            }
        );

        let digest_id1 = hash_str("test");
        let digest_id2 = digest_id1 + 1;
        assert!(m.get_digest_profile_cache(digest_id1, 1).is_none());
        assert_eq!(m.digest_num.load(SeqCst), 0);
        m.update_digest_profile_cache(digest_id1, 1009, 0);
        assert_eq!(m.digest_num.load(SeqCst), 1);
        let shard1 = m.digest_shard(digest_id1);
        {
            assert_eq!(m.get_digest_profile_cache(digest_id1, 2), Some(1009));
            assert_eq!(shard1.num.load(SeqCst), 1);
            let pf = shard1
                .map
                .lock()
                .unwrap()
                .get(&digest_id1)
                .cloned()
                .unwrap();
            assert_eq!(pf.last_fetch_utime_sec.load(SeqCst), 2);
        }
        m.update_digest_profile_cache(digest_id2, 7, 0);
        {
            assert_eq!(m.get_digest_profile_cache(digest_id2, 5), Some(7));
            assert_eq!(m.digest_num.load(SeqCst), 2);
            let shard = m.digest_shard(digest_id2);
            let pf = shard.map.lock().unwrap().get(&digest_id2).cloned().unwrap();
            assert_eq!(pf.last_fetch_utime_sec.load(SeqCst), 5);
        }
        m.update_digest_profile_cache(digest_id1, 107, 0);
        {
            assert_eq!(m.get_digest_profile_cache(digest_id1, 3), Some(1009));
            let pf = shard1
                .map
                .lock()
                .unwrap()
                .get(&digest_id1)
                .cloned()
                .unwrap();
            assert_eq!(pf.last_fetch_utime_sec.load(SeqCst), 3);
        }
        m.update_digest_profile_cache(digest_id1, 107, DEF_UPDATE_BUFFER_TIME_ALIGN_SEC);
        assert_eq!(m.get_digest_profile_cache(digest_id1, 4), Some(1009));
        m.update_digest_profile_cache(digest_id1, 107, DEF_UPDATE_BUFFER_TIME_ALIGN_SEC * 2);
        assert_eq!(m.get_digest_profile_cache(digest_id1, 4), Some(107));
        clean_digest_profile_for_test(&m);

        m.set_limit(5000 * 1000);
        assert_eq!(
            *m.pool_alloc_stats.read().unwrap(),
            PoolAllocProfile {
                small_pool_limit: 5000,
                pool_alloc_unit: 10000,
                max_pool_alloc_unit: 50000
            }
        );

        let test_now = DEF_DIGEST_PROFILE_MEM_TIMEOUT_SEC + 1;
        let mut uid = 107u64;
        let data: [(i64, i64, i64); 6] = [
            (5003, 0, 2),
            (7, 0, 2),
            (4099, test_now - DEF_DIGEST_PROFILE_SMALL_MEM_TIMEOUT_SEC, 2),
            (10003, test_now, 2),
            (5, test_now, 2),
            (4111, test_now, 2),
        ];
        for (v, utime, cnt) in data {
            for _ in 0..cnt {
                m.update_digest_profile_cache(uid, v, utime);
                uid += 1;
            }
        }
        assert_eq!(m.digest_num.load(SeqCst), 12);
        assert_eq!(m.shrink_digest_profile(test_now, DEF_MAX, 0), 0);
        assert_eq!(m.shrink_digest_profile(test_now, 11, 9), 4);
        assert_eq!(m.digest_num.load(SeqCst), 8);
        assert_eq!(m.shrink_digest_profile(test_now, 0, 4), 4);
        clean_digest_profile_for_test(&m);
    }

    {
        // gc when quota not available
        let limit = 1000i64;
        let heap = Arc::new(AtomicI64::new(500));
        let buffer = 100i64;
        let alloc = 500i64;

        m.set_limit(limit as u64);
        m.hc_heap_alloc.store(heap.load(SeqCst), SeqCst);
        m.hc_heap_inuse.store(heap.load(SeqCst), SeqCst);
        m.set_buffer_size(buffer);
        m.hc_last_gc_utime.store(0, SeqCst);
        m.hc_last_gc_heap_alloc.store(0, SeqCst);
        assert_eq!(m.exec_metrics().action.gc, 0);
        assert!(m.is_mem_safe());

        let expect_buffer_size = Arc::new(AtomicI64::new(-1));
        let gc_cnt = Arc::new(AtomicI64::new(0));
        let gc_ut = Arc::new(AtomicI64::new(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
        ));
        {
            let m2 = Arc::clone(&m);
            let ebs = Arc::clone(&expect_buffer_size);
            let g = Arc::clone(&gc_cnt);
            let m3 = Arc::clone(&m);
            let heap2 = Arc::clone(&heap);
            let gcu = Arc::clone(&gc_ut);
            set_actions(
                &m,
                |_| {},
                |_| {},
                |_| {},
                Some(Box::new(move || {
                    m3.set_runtime_mem_stats(MemStats {
                        heap_alloc: heap2.load(SeqCst),
                        heap_inuse: heap2.load(SeqCst) + 100,
                        total_free: 0,
                        mem_off_heap: 3,
                        last_gc: gcu.load(SeqCst),
                    });
                })),
                Some(Box::new(move || {
                    assert_eq!(m2.buffer_size.load(SeqCst), ebs.load(SeqCst));
                    g.fetch_add(1, SeqCst);
                })),
            );
        }
        let make_gc_able = |m: &MemArbitrator| {
            m.mu_last_gc.store(
                m.mu_released
                    .load(SeqCst)
                    .wrapping_sub(m.pool_alloc_stats.read().unwrap().small_pool_limit as u64),
                SeqCst,
            );
        };
        make_gc_able(&m);
        let (e1, h1) =
            new_pool_with_helper(&m, "e1", ArbitrationPriority::Medium, NO_WAIT_AVERSE, false);
        let e1_mem_used = Arc::new(AtomicI64::new(0));
        let e1m = Arc::clone(&e1_mem_used);
        h1.set_heap_used_cb(move || e1m.load(SeqCst));
        m.prepare_alloc(&e1, alloc);
        expect_buffer_size.store(100, SeqCst);
        assert_eq!(m.run_one_round(), 0);
        assert_eq!(m.exec_metrics().action.gc, 1);
        assert_eq!(gc_cnt.load(SeqCst), 1);
        assert_eq!(m.hc_heap_alloc.load(SeqCst), heap.load(SeqCst));
        assert_eq!(m.hc_heap_inuse.load(SeqCst), heap.load(SeqCst) + 100);
        assert_eq!(m.hc_last_gc_heap_alloc.load(SeqCst), heap.load(SeqCst));
        assert_eq!(m.hc_last_gc_utime.load(SeqCst), gc_ut.load(SeqCst));

        // unable to trigger runtime GC
        assert_eq!(m.run_one_round(), 0);
        assert_eq!(m.exec_metrics().action.gc, 1);
        assert_eq!(gc_cnt.load(SeqCst), 1);

        heap.store(450, SeqCst); // -50
        let last_alloc = m.hc_last_gc_heap_alloc.load(SeqCst);
        make_gc_able(&m);
        assert_eq!(m.run_one_round(), 0);
        assert_eq!(m.exec_metrics().action.gc, 2);
        assert_eq!(gc_cnt.load(SeqCst), 2);
        assert_eq!(m.hc_heap_alloc.load(SeqCst), 450);
        assert_eq!(m.hc_heap_inuse.load(SeqCst), 550);
        assert_eq!(m.hc_last_gc_heap_alloc.load(SeqCst), last_alloc); // no gc
        assert_eq!(m.hc_last_gc_utime.load(SeqCst), gc_ut.load(SeqCst));
        assert_eq!(m.avoid_size.load(SeqCst), 450 + 3);

        heap.store(390, SeqCst); // -60
        gc_ut.store(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
            SeqCst,
        );
        make_gc_able(&m);
        assert_eq!(m.run_one_round(), 1);
        assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
        assert_eq!(m.exec_metrics().action.gc, 3);
        assert_eq!(gc_cnt.load(SeqCst), 3);
        assert_eq!(m.hc_heap_alloc.load(SeqCst), 390);
        assert_eq!(m.hc_last_gc_heap_alloc.load(SeqCst), 390);
        assert_eq!(m.hc_last_gc_utime.load(SeqCst), gc_ut.load(SeqCst));
        assert_eq!(m.heap_tracked.load(SeqCst), 0);
        assert_eq!(m.avoid_size.load(SeqCst), 390 + 3);

        m.prepare_alloc(&e1, 50);
        assert_eq!(m.run_one_round(), 0);
        assert_eq!(m.exec_metrics().action.gc, 3);
        assert_eq!(gc_cnt.load(SeqCst), 3);
        assert_eq!(m.avoid_size.load(SeqCst), 390 + 3);
        assert_eq!(m.buffer_size.load(SeqCst), 100);

        gc_ut.store(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
            SeqCst,
        );
        e1_mem_used.store(150, SeqCst);
        make_gc_able(&m);
        assert!(e1_mem_used.load(SeqCst) > m.buffer_size.load(SeqCst));
        expect_buffer_size.store(150, SeqCst);
        assert_eq!(m.run_one_round(), 1);
        assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);

        assert_eq!(m.exec_metrics().action.gc, 4);
        assert_eq!(gc_cnt.load(SeqCst), 4);
        assert_eq!(m.hc_heap_alloc.load(SeqCst), 390);
        assert_eq!(m.hc_last_gc_heap_alloc.load(SeqCst), 390);
        assert_eq!(m.hc_last_gc_utime.load(SeqCst), gc_ut.load(SeqCst));
        assert_eq!(m.heap_tracked.load(SeqCst), 150);
        assert_eq!(m.avoid_size.load(SeqCst), 390 + 3 - 150);

        delete_entries_for_test(&m, &[&e1]);
        check_entries(&m, &[]);
        m.update_tracked_heap_stats();
        set_mem_stats_for_test(&m, 0, 0, 0, 0);
    }
    let _ = recorder;

    // From here on the test drives the arbitrator's clock explicitly
    // (Go `mockNow`).
    let clock = MockClock::install(SystemTime::UNIX_EPOCH);

    {
        // mem risk
        let new_limit = 100_000i64;
        const HEAP_INUSE_RATE_MILLI: i64 = (DEF_OOM_RISK_RATIO * KILO as f64) as i64;
        let mock_heap = Arc::new(Mutex::new([
            multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI),
            multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI),
            0,
            0,
        ]));
        let t_update = Arc::new(AtomicI64::new(0));
        let t_gc = Arc::new(AtomicI64::new(0));
        let t_record_fail = Arc::new(AtomicI64::new(0));
        let t_record_succ = Arc::new(AtomicI64::new(0));
        let logs_info = Arc::new(AtomicI64::new(0));
        let logs_warn = Arc::new(AtomicI64::new(0));
        let logs_error = Arc::new(AtomicI64::new(0));
        let install_actions = |m: &Arc<MemArbitrator>| {
            let mu = Arc::clone(&t_update);
            let mg = Arc::clone(&t_gc);
            let li = Arc::clone(&logs_info);
            let lw = Arc::clone(&logs_warn);
            let le = Arc::clone(&logs_error);
            let m3 = Arc::clone(m);
            let mh = Arc::clone(&mock_heap);
            set_actions(
                m,
                move |_| {
                    li.fetch_add(1, SeqCst);
                },
                move |_| {
                    lw.fetch_add(1, SeqCst);
                },
                move |_| {
                    le.fetch_add(1, SeqCst);
                },
                Some(Box::new(move || {
                    mu.fetch_add(1, SeqCst);
                    let h = *mh.lock().unwrap();
                    set_mem_stats_for_test(&m3, h[0], h[1], h[2], h[3]);
                })),
                Some(Box::new(move || {
                    mg.fetch_add(1, SeqCst);
                })),
            );
        };
        install_actions(&m);
        let check_logs = |i: i64, w: i64, e: i64| {
            assert_eq!(
                (
                    logs_info.load(SeqCst),
                    logs_warn.load(SeqCst),
                    logs_error.load(SeqCst)
                ),
                (i, w, e)
            );
        };

        reset_exec_metrics_for_test(&m);
        assert_eq!(m.hc_heap_alloc.load(SeqCst), 0);
        assert_eq!(m.hc_heap_inuse.load(SeqCst), 0);
        assert_eq!(m.hc_heap_total_free.load(SeqCst), 0);
        assert_eq!(m.heap_tracked.load(SeqCst), 0);
        assert_eq!(m.avoid_size.load(SeqCst), m.limit() - m.soft_limit_i());
        assert_eq!(m.mem_magnif(), 0);
        m.set_limit(new_limit as u64);
        m.set_soft_limit(0, 0.0, SoftLimitMode::Disable);
        assert!(m.is_mem_safe());
        let (e1, h1) =
            new_pool_with_helper(&m, "e1", ArbitrationPriority::Low, NO_WAIT_AVERSE, true);
        let (e2, h2) =
            new_pool_with_helper(&m, "e2", ArbitrationPriority::Low, NO_WAIT_AVERSE, true);
        let (e3, h3) =
            new_pool_with_helper(&m, "e3", ArbitrationPriority::High, NO_WAIT_AVERSE, true);
        let (e4, h4) =
            new_pool_with_helper(&m, "e4", ArbitrationPriority::Low, NO_WAIT_AVERSE, true);
        let (e5, h5) =
            new_pool_with_helper(&m, "e5", ArbitrationPriority::Low, NO_WAIT_AVERSE, true);
        {
            m.set_unix_time_sec(0);
            assert!(m.consume_quota_from_await_free_pool(0, 1000));
            {
                m.prepare_alloc(&e1, 1000);
                m.prepare_alloc(&e2, 5000);
                m.prepare_alloc(&e3, 9000);
                assert_eq!(m.run_one_round(), 3);
                assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
                assert_eq!(m.wait_alloc(&e2), ArbitrateResult::Ok);
                assert_eq!(m.wait_alloc(&e3), ArbitrateResult::Ok);
            }
            assert_eq!(m.allocated(), 16000);
            assert_eq!(m.root_pool_num(), 5);
            {
                let s1 = e1.arbitrator_mu.lock().unwrap().quota_shard;
                let s2 = e2.arbitrator_mu.lock().unwrap().quota_shard;
                let s3 = e3.arbitrator_mu.lock().unwrap().quota_shard;
                assert_ne!(s1, s2);
                assert_ne!(s1, s3);
                assert_ne!(s2, s3);
            }
            assert_eq!(
                m.exec_metrics().task.pair,
                PairSuccessFail { succ: 3, fail: 0 }
            );
            h1.set_heap_used_cb(|| 1009);
        }
        let ori_released = m.mu_released.load(SeqCst);
        let ori_allocated = m.allocated();
        m.refresh_runtime_mem_stats();
        assert_eq!(m.mu_released.load(SeqCst), ori_released + 1000);
        assert_eq!(m.allocated(), ori_allocated - 1000);
        assert_eq!(
            m.soft_limit_i(),
            multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI)
        );
        assert_eq!(
            m.avoid_size.load(SeqCst),
            multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI)
        );
        assert!(!m.is_mem_safe());

        *mock_heap.lock().unwrap() = [
            multi_ratio(new_limit, 900),
            multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI),
            multi_ratio(new_limit, 900) + 500,
            multi_ratio(new_limit, 50),
        ];
        m.refresh_runtime_mem_stats();
        assert_eq!(m.hc_heap_total_free.load(SeqCst), 500);
        assert!(!m.is_mem_safe());

        assert_eq!(m.exec_metrics().risk, ExecMetricsRisk::default());
        assert!(!m.at_mem_risk());
        assert_eq!(m.min_heap_free_bps(), DEF_MIN_HEAP_FREE_BPS);
        assert_eq!(m.mem_magnif(), 0);
        reset_exec_metrics_for_test(&m);

        t_update.store(0, SeqCst);
        t_gc.store(0, SeqCst);
        logs_info.store(0, SeqCst);
        logs_warn.store(0, SeqCst);
        logs_error.store(0, SeqCst);

        {
            let rf = Arc::clone(&t_record_fail);
            recorder.set_store(move |_| {
                rf.fetch_add(1, SeqCst);
                Err("test".to_string())
            });
        }
        *m.hc_last_mem_state.lock().unwrap() = Some(RuntimeMemStateV1 {
            version: 1,
            magnif: 1111,
            ..Default::default()
        });
        m.set_soft_limit(0, 0.0, SoftLimitMode::Auto);
        m.set_min_heap_free_bps(2); // 2 B/s
        let start_time = SystemTime::now();
        clock.set(start_time);
        assert_eq!(m.min_heap_free_bps(), 2);
        {
            assert_eq!(m.run_one_round(), -2);
            assert!(m.at_mem_risk());
            {
                let risk = m.hc_mem_risk.lock().unwrap();
                assert_eq!(risk.start_time, start_time);
                assert_eq!(risk.last_stats_start_time, start_time);
                assert_eq!(risk.last_heap_total_free, 500);
            }
            let last = m.last_mem_state().unwrap();
            assert_eq!(
                last,
                RuntimeMemStateV1 {
                    version: 1,
                    last_risk: LastRisk {
                        heap_alloc: multi_ratio(new_limit, 900),
                        quota_alloc: 15000,
                    },
                    magnif: 6100,
                    pool_medium_cap: 0,
                }
            );
            assert_eq!(m.mem_magnif(), last.magnif);
            assert_eq!(m.heap_tracked.load(SeqCst), 0);
            assert_eq!(m.avoid_size.load(SeqCst), 95000);
            assert_eq!(
                (
                    t_gc.load(SeqCst),
                    t_update.load(SeqCst),
                    t_record_fail.load(SeqCst)
                ),
                (1, 1, 1)
            );
            check_logs(0, 1, 1);
            let em = m.exec_metrics();
            assert_eq!(
                em.action,
                ExecMetricsAction {
                    gc: 1,
                    update_runtime_mem_stats: 1,
                    record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                }
            );
            assert_eq!(
                em.risk,
                ExecMetricsRisk {
                    mem: 1,
                    oom: 0,
                    oom_kill: [0; 3]
                }
            );
        }

        {
            // wait to check oom risk
            let expect_action = {
                let mut a = m.exec_metrics().action;
                a.update_runtime_mem_stats += 1;
                a
            };
            let last_risk_state = m.last_mem_state().unwrap();
            {
                let rs = Arc::clone(&t_record_succ);
                recorder.set_store(move |_| {
                    rs.fetch_add(1, SeqCst);
                    Ok(())
                });
            }
            let base = m.hc_mem_risk.lock().unwrap().last_stats_start_time;
            clock.set(base + DEF_HEAP_RECLAIM_CHECK_DURATION - Duration::from_nanos(1));
            assert_eq!(m.run_one_round(), -2);
            assert!(m.at_mem_risk());
            {
                let risk = m.hc_mem_risk.lock().unwrap();
                assert_eq!(risk.start_time, start_time);
                assert_eq!(risk.last_stats_start_time, start_time);
                assert_eq!(risk.last_heap_total_free, 500);
            }
            assert_eq!(m.mem_magnif(), last_risk_state.magnif);
            assert_eq!(m.last_mem_state().unwrap(), last_risk_state);
            assert_eq!(m.exec_metrics().action, expect_action);
            check_logs(0, 1, 1);
        }
        {
            assert_eq!(m.heap_tracked.load(SeqCst), 0);
            let expect_action = {
                let mut a = m.exec_metrics().action;
                a.gc += 1;
                a.update_runtime_mem_stats += 1;
                a
            };
            m.mu_last_gc.store(
                m.mu_released
                    .load(SeqCst)
                    .wrapping_sub(m.pool_alloc_stats.read().unwrap().small_pool_limit as u64),
                SeqCst,
            );
            assert_eq!(m.run_one_round(), -2);
            assert_eq!(m.exec_metrics().action, expect_action);
            check_logs(0, 1, 1);
            assert_eq!(m.heap_tracked.load(SeqCst), 1009);
        }

        {
            // next round of oom check
            let last_risk_state = m.last_mem_state().unwrap();
            let base = m.hc_mem_risk.lock().unwrap().last_stats_start_time;
            clock.set(base + DEF_HEAP_RECLAIM_CHECK_DURATION);
            let now_t = base + DEF_HEAP_RECLAIM_CHECK_DURATION;
            *mock_heap.lock().unwrap() = [
                multi_ratio(new_limit, 900),
                multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI),
                multi_ratio(new_limit, 900) + 500 + 2,
                multi_ratio(new_limit, 50),
            ];
            assert_eq!(m.run_one_round(), -2);
            assert!(m.at_mem_risk());
            {
                let risk = m.hc_mem_risk.lock().unwrap();
                assert_eq!(risk.start_time, start_time);
                assert_eq!(risk.last_stats_start_time, now_t);
                assert_eq!(risk.last_heap_total_free, 500 + 2);
            }
            assert_eq!(m.mem_magnif(), last_risk_state.magnif);
            assert_eq!(m.last_mem_state().unwrap(), last_risk_state);
            assert_eq!(
                (
                    t_gc.load(SeqCst),
                    t_update.load(SeqCst),
                    t_record_fail.load(SeqCst)
                ),
                (3, 4, 1)
            );
            check_logs(0, 2, 1);
            assert_eq!(
                m.exec_metrics().action,
                ExecMetricsAction {
                    gc: 3,
                    update_runtime_mem_stats: 4,
                    record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                }
            );
            assert_eq!(
                m.exec_metrics().risk,
                ExecMetricsRisk {
                    mem: 1,
                    oom: 0,
                    oom_kill: [0; 3]
                }
            );
        }

        {
            let base = m.hc_mem_risk.lock().unwrap().last_stats_start_time;
            clock.set(base + DEF_HEAP_RECLAIM_CHECK_DURATION);
            let now_t = base + DEF_HEAP_RECLAIM_CHECK_DURATION;
            let last_risk_state = m.last_mem_state().unwrap();
            *mock_heap.lock().unwrap() = [
                multi_ratio(new_limit, 900),
                multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI),
                multi_ratio(new_limit, 900) + 500 + 4,
                multi_ratio(new_limit, 50),
            ];
            m.mu_last_gc.store(
                m.mu_released
                    .load(SeqCst)
                    .wrapping_sub(m.pool_alloc_stats.read().unwrap().small_pool_limit as u64),
                SeqCst,
            );
            assert_eq!(m.run_one_round(), -2);
            {
                let risk = m.hc_mem_risk.lock().unwrap();
                assert_eq!(risk.last_stats_start_time, now_t);
                assert_eq!(risk.last_heap_total_free, 500 + 4);
            }
            assert_eq!(m.mem_magnif(), last_risk_state.magnif);
            assert_eq!(m.last_mem_state().unwrap(), last_risk_state);
            check_logs(0, 3, 1);
            assert_eq!(
                m.exec_metrics().action,
                ExecMetricsAction {
                    gc: 4,
                    update_runtime_mem_stats: 5,
                    record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                }
            );
            assert_eq!(
                m.exec_metrics().risk,
                ExecMetricsRisk {
                    mem: 1,
                    oom: 0,
                    oom_kill: [0; 3]
                }
            );
        }
        {
            let kill_event: CbMap = Arc::new(Mutex::new(HashMap::new()));
            let last_risk_state = m.last_mem_state().unwrap();
            {
                let ke = Arc::clone(&kill_event);
                let u2 = e2.pool.uid();
                h2.set_kill_cb(move || {
                    *ke.lock().unwrap().entry(u2).or_insert(0) += 1;
                });
                let ke = Arc::clone(&kill_event);
                let (u1, u2b) = (e1.pool.uid(), e2.pool.uid());
                h1.set_kill_cb(move || {
                    let mut g = ke.lock().unwrap();
                    assert_ne!(*g.get(&u2b).unwrap_or(&0), 0);
                    *g.entry(u1).or_insert(0) += 1;
                });
                let ke = Arc::clone(&kill_event);
                let (u1b, u2c, u3) = (e1.pool.uid(), e2.pool.uid(), e3.pool.uid());
                h3.set_kill_cb(move || {
                    let mut g = ke.lock().unwrap();
                    assert_ne!(*g.get(&u2c).unwrap_or(&0), 0);
                    assert_ne!(*g.get(&u1b).unwrap_or(&0), 0);
                    *g.entry(u3).or_insert(0) += 1;
                });
                let e1c = Arc::clone(&e1);
                h1.set_heap_used_cb(move || e1c.arbitrator_mu.lock().unwrap().quota);
                let e2c = Arc::clone(&e2);
                h2.set_heap_used_cb(move || e2c.arbitrator_mu.lock().unwrap().quota);
                let e3c = Arc::clone(&e3);
                h3.set_heap_used_cb(move || e3c.arbitrator_mu.lock().unwrap().quota);
            }
            m.prepare_alloc(&e2, 1000);
            let base = m.hc_mem_risk.lock().unwrap().last_stats_start_time;
            clock.set(base + DEF_HEAP_RECLAIM_CHECK_DURATION);
            let now_t = base + DEF_HEAP_RECLAIM_CHECK_DURATION;
            *mock_heap.lock().unwrap() = [
                multi_ratio(new_limit, 900),
                multi_ratio(new_limit, HEAP_INUSE_RATE_MILLI) - 233,
                multi_ratio(new_limit, 900) + 500 + 4 + 1,
                233,
            ];
            assert_eq!(m.run_one_round(), -2);
            assert!(!mem_hang_risk(
                m.min_heap_free_bps(),
                m.min_heap_free_bps(),
                SystemTime::UNIX_EPOCH + DEF_HEAP_RECLAIM_CHECK_MAX_DURATION,
                SystemTime::UNIX_EPOCH
            ));
            assert!(mem_hang_risk(
                0,
                0,
                SystemTime::UNIX_EPOCH
                    + DEF_HEAP_RECLAIM_CHECK_MAX_DURATION
                    + Duration::from_nanos(1),
                SystemTime::UNIX_EPOCH
            ));
            {
                let risk = m.hc_mem_risk.lock().unwrap();
                assert_eq!(risk.last_stats_start_time, now_t);
                assert_eq!(risk.last_heap_total_free, 500 + 4 + 1);
            }
            assert_eq!(m.mem_magnif(), last_risk_state.magnif);
            assert_eq!(m.last_mem_state().unwrap(), last_risk_state);
            check_logs(0, 7, 1); // OOM RISK; Start to KILL; make task failed; restart check
            assert_eq!(
                m.exec_metrics().action,
                ExecMetricsAction {
                    gc: 5,
                    update_runtime_mem_stats: 6,
                    record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                }
            );
            assert_eq!(m.wait_alloc(&e2), ArbitrateResult::Fail);
            assert_eq!(
                m.exec_metrics().task.pair,
                PairSuccessFail { succ: 0, fail: 1 }
            );
            assert_eq!(
                m.exec_metrics().risk,
                ExecMetricsRisk {
                    mem: 1,
                    oom: 1,
                    oom_kill: [1, 0, 0]
                }
            );
            assert_eq!(kill_event.lock().unwrap().len(), 1);
            assert_eq!(kill_event.lock().unwrap()[&e2.pool.uid()], 1);
            assert_eq!(m.under_kill.lock().unwrap().num, 1);

            let base = m.hc_mem_risk.lock().unwrap().last_stats_start_time;
            clock.set(base + DEF_HEAP_RECLAIM_CHECK_DURATION);
            assert_eq!(m.run_one_round(), -2);
            check_logs(0, 8, 1); // OOM RISK
            assert_eq!(
                m.exec_metrics().action,
                ExecMetricsAction {
                    gc: 6,
                    update_runtime_mem_stats: 7,
                    record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                }
            );
            assert!(e2.load_ctx().unwrap().stopped.load(SeqCst));
            assert!(e2
                .ctx
                .cancel_ch
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .is_closed());
            assert_eq!(
                m.exec_metrics().risk,
                ExecMetricsRisk {
                    mem: 1,
                    oom: 2,
                    oom_kill: [1, 0, 0]
                }
            );

            m.mu_allocated.fetch_add(100_000, SeqCst);
            m.entry_map.add_quota(&e5, 100_000);
            h5.set_heap_used_cb(|| 100_000);
            {
                let ke = Arc::clone(&kill_event);
                let u5 = e5.pool.uid();
                h5.set_kill_cb(move || {
                    *ke.lock().unwrap().entry(u5).or_insert(0) += 1;
                });
            }

            let base = m.hc_mem_risk.lock().unwrap().last_stats_start_time;
            clock.set(base + DEF_KILL_CANCEL_CHECK_TIMEOUT);
            assert_eq!(m.run_one_round(), -2);
            check_logs(0, 11, 2); // Failed to KILL; Start to KILL
            assert_eq!(
                m.exec_metrics().action,
                ExecMetricsAction {
                    gc: 7,
                    update_runtime_mem_stats: 8,
                    record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                }
            );
            assert_eq!(m.under_kill.lock().unwrap().num, 2);
            assert!(e2.arbitrator_mu.lock().unwrap().under_kill.fail);
            assert!(e5.arbitrator_mu.lock().unwrap().under_kill.start);
            assert!(!e5.arbitrator_mu.lock().unwrap().under_kill.fail);
            assert_eq!(
                m.exec_metrics().risk,
                ExecMetricsRisk {
                    mem: 1,
                    oom: 3,
                    oom_kill: [2, 0, 0]
                }
            );

            // release quota which makes it able to gc
            assert!(m.remove_root_pool_entry(&e2));
            assert!(m.remove_root_pool_entry(&e5));
            let base = m.hc_mem_risk.lock().unwrap().last_stats_start_time;
            clock.set(base + DEF_HEAP_RECLAIM_CHECK_DURATION);
            assert_eq!(m.run_one_round(), -2);
            assert!(!e5.arbitrator_mu.lock().unwrap().under_kill.start);
            assert!(!e2.arbitrator_mu.lock().unwrap().under_kill.start);
            assert_eq!(m.under_kill.lock().unwrap().num, 2);
            check_logs(0, 17, 2); // Finish KILL x2; OOM RISK; Start KILL x2; Restart check
            assert_eq!(
                m.exec_metrics().action,
                ExecMetricsAction {
                    gc: 8,
                    update_runtime_mem_stats: 9,
                    record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                }
            );
            {
                let q1 = e1.arbitrator_mu.lock().unwrap().quota;
                let q3 = e3.arbitrator_mu.lock().unwrap().quota;
                assert_eq!(m.heap_tracked.load(SeqCst), q1 + q3);
            }
            assert_eq!(
                m.exec_metrics().risk,
                ExecMetricsRisk {
                    mem: 1,
                    oom: 4,
                    oom_kill: [3, 0, 1]
                }
            );
            {
                let ke = kill_event.lock().unwrap();
                assert_eq!(ke.len(), 4);
                for e in [&e1, &e2, &e3, &e5] {
                    assert_eq!(ke[&e.pool.uid()], 1);
                }
            }
            assert_eq!(m.buffer_size.load(SeqCst), 9000);

            assert!(m.remove_root_pool_entry(&e1));
            assert!(m.remove_root_pool_entry(&e3));
            *mock_heap.lock().unwrap() = [
                multi_ratio(new_limit, 900) - 1,
                multi_ratio(new_limit, 900) - 1,
                0,
                0,
            ];
            h4.set_heap_used_cb(|| 1013);
            assert_eq!(m.run_one_round(), 0);
            assert!(!e1.arbitrator_mu.lock().unwrap().under_kill.start);
            assert!(!e3.arbitrator_mu.lock().unwrap().under_kill.start);
            assert_eq!(m.under_kill.lock().unwrap().num, 0);
            assert_eq!(m.heap_tracked.load(SeqCst), 1013);
            check_logs(1, 19, 2); // Finish KILL x2; mem is safe
            assert_eq!(
                m.exec_metrics().action,
                ExecMetricsAction {
                    gc: 9,
                    update_runtime_mem_stats: 10,
                    record_mem_state: PairSuccessFail { succ: 0, fail: 1 }
                }
            );
            assert!(!m.at_mem_risk());
        }
        assert!(m.consume_quota_from_await_free_pool(0, -1000));
        assert_eq!(m.await_free_pool_used(), MemPoolQuotaUsage::default());
        assert_eq!(m.await_free_pool_cap(), 0);
        m.shrink_await_free_pool(0, DEF_AWAIT_FREE_POOL_SHRINK_DUR_MILLI);
        assert_eq!(m.mem_magnif(), 6100);
        assert_eq!(
            m.avoid_size.load(SeqCst),
            mock_heap.lock().unwrap()[0] - 1013
        );
        delete_entries_for_test(&m, &[&e4]);
        check_entries(&m, &[]);
    }
    drop(clock);

    let clock = MockClock::install(SystemTime::UNIX_EPOCH);
    {
        // tick task: reduce mem magnification
        reset_exec_metrics_for_test(&m);
        assert_eq!(m.root_pool_num(), 0);
        assert_eq!(m.allocated(), 0);
        let epoch = SystemTime::UNIX_EPOCH;
        clock.set(epoch + Duration::from_secs(DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN as u64));
        m.set_unix_time_sec(DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN);

        m.try_to_update_buffer(23, m.approx_unix_time_sec());
        assert_eq!(m.buffer_size.load(SeqCst), 23);
        let (e1ctx, e1h) = new_ctx_with_helper(
            ArbitrationPriority::Medium,
            NO_WAIT_AVERSE,
            REQUIRE_PRIVILEGE,
        );
        e1h.set_heap_used_cb(|| 31);
        let e1 = add_entry_for_test(&m, Some(e1ctx));
        m.update_tracked_heap_stats();
        assert_eq!(m.buffer_size.load(SeqCst), 31);

        m.reset_root_pool_by_id(e1.pool.uid(), 19, true);
        assert_eq!(m.buffer_size.load(SeqCst), 31);

        m.reset_root_pool_by_id(e1.pool.uid(), 389, true);
        assert_eq!(m.buffer_size.load(SeqCst), 389);

        let logs_info = Arc::new(AtomicI64::new(0));
        {
            let li = Arc::clone(&logs_info);
            set_actions(
                &m,
                move |_| {
                    li.fetch_add(1, SeqCst);
                },
                |_| {},
                |_| {},
                None,
                None,
            );
        }

        {
            // mock oom-check running blocks the tick
            m.hc_mem_risk_start_unix_milli.store(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
                SeqCst,
            );
            assert!(!m.execute_tick(DEF_MAX));
            m.hc_mem_risk_start_unix_milli.store(0, SeqCst);
        }

        let mut tl_pre = 0i64;
        let mut tl_now = DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;

        let check_prof_impl = |ms: i64, heap: i64, quota: i64, ratio: i64| {
            let sec = ms / KILO;
            let align = sec / DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN;
            let profs = m.hc_timed_mem_profile.lock().unwrap();
            let p = &profs[(align % 2) as usize];
            assert_eq!(
                (p.start_utime_milli, p.ts_align, p.heap, p.quota, p.ratio),
                (ms, align, heap, quota, ratio)
            );
        };
        let check_prof = |ms: i64, heap: i64, quota: i64| check_prof_impl(ms, heap, quota, 0);

        {
            // new suggest pool cap: last mem state is nil
            let ori = m.last_mem_state();
            assert_eq!(
                ori.unwrap(),
                RuntimeMemStateV1 {
                    version: 1,
                    last_risk: LastRisk {
                        heap_alloc: 90000,
                        quota_alloc: 15000
                    },
                    magnif: 6100,
                    pool_medium_cap: 0,
                }
            );
            assert_eq!(m.mem_magnif(), 6100);
            assert_eq!(m.pool_medium_quota(), 0);
            assert_eq!(m.pool_alloc_last_update_milli.load(SeqCst), 0);
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 0);

            *m.hc_last_mem_state.lock().unwrap() = None;
            assert!(m.execute_tick(tl_now));
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 1);
            assert_eq!(logs_info.load(SeqCst), 1);
            assert_eq!(m.pool_medium_quota(), 400);
            assert_eq!(m.pool_alloc_last_update_milli.load(SeqCst), tl_now);
            let mut ori = ori.unwrap();
            ori.pool_medium_cap = m.pool_medium_quota();
            *m.hc_last_mem_state.lock().unwrap() = Some(ori);

            // same value
            assert!(!m.try_store_pool_medium_capacity(tl_now + DEF_TICK_DUR_MILLI * 10 + 1, 400));
            // time not satisfied
            assert!(!m.try_store_pool_medium_capacity(tl_now + DEF_TICK_DUR_MILLI * 10 - 1, 399));
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 1);
            assert_eq!(logs_info.load(SeqCst), 1);
            // last mem state not nil & value differs
            assert!(m.try_store_pool_medium_capacity(tl_now + DEF_TICK_DUR_MILLI * 10, 401));
            assert_eq!(m.exec_metrics().action.record_mem_state.succ, 2);
            assert_eq!(logs_info.load(SeqCst), 2);
            assert_eq!(m.last_mem_state().unwrap().pool_medium_cap, 401);
        }

        {
            m.set_soft_limit(0, 0.0, SoftLimitMode::Disable);
            set_mem_stats_for_test(&m, 0, 0, 0, 0);
            assert_eq!(m.avoid_size.load(SeqCst), 5000);
            m.set_soft_limit(0, 0.0, SoftLimitMode::Auto);
            set_mem_stats_for_test(&m, 0, 0, 0, 0);
            assert_eq!(m.avoid_size.load(SeqCst), 83607);
        }

        {
            // init last gc state
            m.hc_last_gc_heap_alloc.store(DEF_MAX, SeqCst);
            m.hc_last_gc_utime.store(0, SeqCst);
        }

        m.set_limit(15000);
        assert_eq!(m.limit(), 15000);
        m.mu_allocated.store(20000, SeqCst);
        assert_eq!(m.allocated(), 20000);

        tl_pre = tl_now;
        tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
        m.prepare_alloc(&e1, DEF_MAX);
        assert_eq!(
            m.exec_mu.lock().unwrap().blocked_state,
            BlockedState::default()
        );
        m.set_unix_time_sec(tl_now / KILO + DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN - 1);
        m.hc_last_gc_heap_alloc.store(m.limit(), SeqCst);
        m.hc_last_gc_utime
            .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
        assert!(!m.do_execute_first_task());
        assert_eq!(
            m.exec_mu.lock().unwrap().blocked_state,
            BlockedState {
                allocated: 20000,
                utime_sec: m.approx_unix_time_sec()
            }
        );
        // calculate ratio of the previous
        assert!(m.execute_tick(tl_now));
        assert_eq!(m.exec_metrics().action.record_mem_state.succ, 3);
        assert_eq!(logs_info.load(SeqCst), 3);
        assert_eq!(m.last_mem_state().unwrap().pool_medium_cap, 400);
        check_prof(tl_pre, 0, 0);
        check_prof(tl_now, 15000, 20000);

        tl_pre = tl_now;
        tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
        m.set_unix_time_sec(tl_now / KILO + DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN - 1);
        m.mu_allocated.store(40000, SeqCst);
        m.hc_last_gc_heap_alloc.store(35000, SeqCst);
        m.hc_last_gc_utime
            .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
        assert!(!m.do_execute_first_task());
        assert_eq!(
            m.exec_mu.lock().unwrap().blocked_state,
            BlockedState {
                allocated: 40000,
                utime_sec: m.approx_unix_time_sec()
            }
        );
        assert!(m.execute_tick(tl_now));
        assert_eq!(m.exec_metrics().action.record_mem_state.succ, 3);
        assert_eq!(logs_info.load(SeqCst), 4); // mem profile timeline
        check_prof_impl(tl_pre, 15000, 20000, 750);
        check_prof(tl_now, 35000, 40000);

        tl_pre = tl_now;
        tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
        m.set_unix_time_sec(tl_now / KILO + DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN - 1);
        m.mu_allocated.store(50000, SeqCst);
        m.hc_last_gc_heap_alloc.store(40000, SeqCst);
        m.hc_last_gc_utime
            .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
        assert!(!m.do_execute_first_task());
        assert_eq!(
            m.exec_mu.lock().unwrap().blocked_state,
            BlockedState {
                allocated: 50000,
                utime_sec: m.approx_unix_time_sec()
            }
        );
        assert!(m.execute_tick(tl_now));
        // no update because the heap stats are NOT safe
        assert_eq!(m.mem_magnif(), 6100);
        assert_eq!(logs_info.load(SeqCst), 5);
        assert_eq!(m.exec_metrics().action.record_mem_state.succ, 3);
        check_prof_impl(tl_pre, 35000, 40000, 875);
        check_prof(tl_now, 40000, 50000);

        // restore limit to 100000
        m.set_limit(100_000);
        tl_pre = tl_now;
        tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
        m.set_unix_time_sec(tl_now / KILO + 1);
        {
            let mut exec = m.exec_mu.lock().unwrap();
            exec.blocked_state = BlockedState {
                allocated: 50000,
                utime_sec: m.unix_time_sec.load(SeqCst),
            };
        }
        m.hc_last_gc_heap_alloc.store(40000, SeqCst);
        m.hc_last_gc_utime
            .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
        assert!(m.execute_tick(tl_now));
        assert_eq!(m.mem_magnif(), (875 + 6100) / 2); // choose 875 over 800
        assert_eq!(logs_info.load(SeqCst), 8);
        assert_eq!(m.exec_metrics().action.record_mem_state.succ, 4);
        check_prof_impl(tl_pre, 40000, 50000, 800);
        check_prof(tl_now, 40000, 50000);

        tl_pre = tl_now;
        tl_now += KILO;
        m.set_unix_time_sec(m.approx_unix_time_sec() + 1);
        m.mu_allocated.store(40000, SeqCst);
        m.hc_last_gc_heap_alloc.store(41000, SeqCst);
        m.hc_last_gc_utime
            .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
        assert!(!m.do_execute_first_task());
        assert_eq!(
            m.exec_mu.lock().unwrap().blocked_state,
            BlockedState {
                allocated: 40000,
                utime_sec: m.approx_unix_time_sec()
            }
        );
        assert!(m.execute_tick(tl_now));
        assert_eq!(m.mem_magnif(), (875 + 6100) / 2); // no update
        assert_eq!(logs_info.load(SeqCst), 8);
        assert_eq!(m.exec_metrics().action.record_mem_state.succ, 4);
        check_prof(tl_now - KILO, 41000, 50000);
        tl_now = tl_pre + DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
        assert!(m.execute_tick(tl_now));
        check_prof_impl(tl_pre, 41000, 50000, 820);
        check_prof(tl_now, 0, 0);
        assert_eq!(m.mem_magnif(), (3487 + 820) / 2);
        assert_eq!(logs_info.load(SeqCst), 11);
        assert_eq!(m.exec_metrics().action.record_mem_state.succ, 5);

        tl_pre = tl_now;
        tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
        // no valid pre profile
        assert!(m.execute_tick(tl_now));
        assert_eq!(logs_info.load(SeqCst), 13);
        assert_eq!(m.exec_metrics().action.record_mem_state.succ, 6);
        assert_eq!(m.mem_magnif(), (2153 + 820) / 2);
        check_prof(tl_now, 0, 0);
        check_prof(tl_pre, 0, 0);
        assert_eq!(logs_info.load(SeqCst), 13);
        assert_eq!(m.exec_metrics().action.record_mem_state.succ, 6);

        tl_pre = tl_now;
        tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
        assert!(m.execute_tick(tl_now));
        assert_eq!(m.mem_magnif(), (2153 + 820) / 2); // no update
        tl_pre = tl_now;
        tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;

        // new start
        tl_pre = tl_now;
        tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
        m.set_unix_time_sec(tl_now / KILO);
        {
            let mut exec = m.exec_mu.lock().unwrap();
            exec.blocked_state = BlockedState {
                allocated: 10000,
                utime_sec: m.unix_time_sec.load(SeqCst),
            };
        }
        m.hc_last_gc_heap_alloc.store(10000, SeqCst);
        m.hc_last_gc_utime
            .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
        assert!(m.execute_tick(tl_now));
        assert_eq!(m.mem_magnif(), (2153 + 820) / 2); // no update
        assert_eq!(logs_info.load(SeqCst), 13);
        assert_eq!(m.exec_metrics().action.record_mem_state.succ, 6);

        tl_pre = tl_now;
        tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
        m.set_unix_time_sec(tl_now / KILO);
        {
            let mut exec = m.exec_mu.lock().unwrap();
            exec.blocked_state = BlockedState {
                allocated: 11111,
                utime_sec: m.unix_time_sec.load(SeqCst),
            };
        }
        m.hc_last_gc_heap_alloc.store(2222, SeqCst);
        m.hc_last_gc_utime
            .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
        assert!(m.execute_tick(tl_now));
        assert_eq!(m.mem_magnif(), (2153 + 820) / 2); // no update
        assert_eq!(logs_info.load(SeqCst), 14); // timed mem profile
        assert_eq!(m.exec_metrics().action.record_mem_state.succ, 6);
        check_prof_impl(tl_pre, 10000, 10000, 1000);
        check_prof(tl_now, 2222, 11111);

        tl_pre = tl_now;
        tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
        m.set_unix_time_sec(tl_now / KILO);
        {
            let mut exec = m.exec_mu.lock().unwrap();
            exec.blocked_state = BlockedState {
                allocated: 10000,
                utime_sec: m.unix_time_sec.load(SeqCst),
            };
        }
        m.hc_last_gc_heap_alloc.store(10000, SeqCst);
        m.hc_last_gc_utime
            .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
        assert!(m.execute_tick(tl_now));
        assert_eq!(m.mem_magnif(), (1486 + 1000) / 2);
        assert_eq!(logs_info.load(SeqCst), 17);
        assert_eq!(m.exec_metrics().action.record_mem_state.succ, 7);
        check_prof_impl(tl_pre, 2222, 11111, 199);
        check_prof(tl_now, 10000, 10000);

        tl_pre = tl_now;
        tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
        m.set_unix_time_sec(tl_now / KILO);
        {
            let mut exec = m.exec_mu.lock().unwrap();
            exec.blocked_state = BlockedState {
                allocated: 10000,
                utime_sec: m.unix_time_sec.load(SeqCst),
            };
        }
        m.hc_last_gc_heap_alloc.store(5000, SeqCst);
        m.hc_last_gc_utime
            .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
        assert!(m.execute_tick(tl_now));
        check_prof_impl(tl_pre, 10000, 10000, 1000);
        check_prof(tl_now, 5000, 10000);
        // choose 1000 over 199
        assert_eq!(m.mem_magnif(), (1243 + 1000) / 2);
        assert_eq!(logs_info.load(SeqCst), 20);
        assert_eq!(m.exec_metrics().action.record_mem_state.succ, 8);

        tl_pre = tl_now;
        tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
        m.set_unix_time_sec(tl_now / KILO);
        {
            let mut exec = m.exec_mu.lock().unwrap();
            exec.blocked_state = BlockedState {
                allocated: 10000,
                utime_sec: m.unix_time_sec.load(SeqCst),
            };
        }
        m.hc_last_gc_heap_alloc.store(5000, SeqCst);
        m.hc_last_gc_utime
            .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
        assert!(m.execute_tick(tl_now));
        assert_eq!(m.mem_magnif(), (1121 + 1000) / 2);
        assert_eq!(logs_info.load(SeqCst), 23);
        assert_eq!(m.exec_metrics().action.record_mem_state.succ, 9);

        tl_pre = tl_now;
        tl_now += DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN * KILO;
        m.set_unix_time_sec(tl_now / KILO);
        {
            let mut exec = m.exec_mu.lock().unwrap();
            exec.blocked_state = BlockedState {
                allocated: 10000,
                utime_sec: m.unix_time_sec.load(SeqCst),
            };
        }
        m.hc_last_gc_heap_alloc.store(20000, SeqCst);
        m.hc_last_gc_utime
            .store(m.approx_unix_time_sec() * 1_000_000_000, SeqCst);
        assert!(m.execute_tick(tl_now));
        assert_eq!(m.mem_magnif(), 0); // smaller than 1000
        assert_eq!(logs_info.load(SeqCst), 26);
        assert_eq!(m.exec_metrics().action.record_mem_state.succ, 10);
        e1h.cancel_self();
        m.wait_alloc(&e1);
        delete_entries_for_test(&m, &[&e1]);
        m.mu_allocated.store(0, SeqCst);
        let _ = tl_pre;
    }
    {
        // no more root pool can be killed
        reset_exec_metrics_for_test(&m);
        assert!(m.is_mem_safe());
        check_entries(&m, &[]);
        m.set_limit(100_000);
        const HEAP_INUSE_RATE_MILLI: i64 = 950;
        let mock_heap = Arc::new(Mutex::new([0i64; 4]));
        {
            let m3 = Arc::clone(&m);
            let mh = Arc::clone(&mock_heap);
            set_actions(
                &m,
                |_| {},
                |_| {},
                |_| {},
                Some(Box::new(move || {
                    let h = *mh.lock().unwrap();
                    set_mem_stats_for_test(&m3, h[0], h[1], h[2], h[3]);
                })),
                Some(Box::new(|| {})),
            );
        }
        let (c1, h1) = new_ctx_with_helper(
            ArbitrationPriority::Medium,
            NO_WAIT_AVERSE,
            REQUIRE_PRIVILEGE,
        );
        let e1 = add_entry_for_test(&m, Some(c1));
        h1.set_heap_used_cb(|| 10000);
        let kill1 = Arc::new(AtomicI64::new(0));
        {
            let k = Arc::clone(&kill1);
            h1.set_kill_cb(move || {
                k.fetch_add(1, SeqCst);
            });
        }
        m.prepare_alloc(&e1, 10000);
        assert_eq!(m.run_one_round(), 1);
        assert_eq!(m.wait_alloc(&e1), ArbitrateResult::Ok);
        *mock_heap.lock().unwrap() = [0, multi_ratio(100_000, HEAP_INUSE_RATE_MILLI + 1), 0, 0];
        let (c2, h2) = new_ctx_with_helper(
            ArbitrationPriority::Medium,
            NO_WAIT_AVERSE,
            REQUIRE_PRIVILEGE,
        );
        let e2 = add_entry_for_test(&m, Some(c2));
        let kill2 = Arc::new(AtomicI64::new(0));
        {
            let k = Arc::clone(&kill2);
            h2.set_kill_cb(move || {
                k.fetch_add(1, SeqCst);
            });
        }
        m.refresh_runtime_mem_stats();
        let debug_now = SystemTime::now();
        clock.set(debug_now);
        assert_eq!(m.run_one_round(), -2);
        clock.set(debug_now + Duration::from_secs(1));
        assert_eq!(m.run_one_round(), -2);
        assert_eq!(m.under_kill.lock().unwrap().num, 1);
        assert!(e1.arbitrator_mu.lock().unwrap().under_kill.start);
        assert!(!e1.arbitrator_mu.lock().unwrap().under_kill.fail);
        clock.set(debug_now + Duration::from_secs(1) + DEF_KILL_CANCEL_CHECK_TIMEOUT);
        m.prepare_alloc(&e2, 10000);
        assert_eq!(m.run_one_round(), -2);
        assert!(e1.arbitrator_mu.lock().unwrap().under_kill.fail);
        assert!(e1.arbitrator_mu.lock().unwrap().under_kill.start);
        assert!(!e2.load_ctx().unwrap().available());
        assert_eq!(m.wait_alloc(&e2), ArbitrateResult::Fail);
        delete_entries_for_test(&m, &[&e1, &e2]);
        check_entries(&m, &[]);
    }
    drop(clock);
}
