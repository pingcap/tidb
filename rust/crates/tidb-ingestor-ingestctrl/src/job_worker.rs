// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/job_worker.go`.
use crate::{
    Error, Result,
    region_job::{JobStage, RegionJob, TiKVWriteResult, get_next_stage_on_ingest_error},
};
use std::time::Duration;
use tidb_gocontext::Context;
use tidb_proto::metapb;

/// Go `regionJobBaseWorker`, with the same write, ingest and regeneration hooks.
pub struct RegionJobBaseWorker<'a, 'data> {
    /// Go `ctx`.
    pub ctx: Context,
    /// Go `writeFn`.
    pub write_fn: &'a (dyn Fn(&Context, &mut RegionJob<'data>) -> Result<TiKVWriteResult> + Sync),
    /// Go `ingestFn`.
    pub ingest_fn: &'a (dyn Fn(&Context, &mut RegionJob<'data>) -> Result<()> + Sync),
    /// Go `preRunJobFn`.
    pub pre_run_job_fn: &'a (dyn Fn(&Context, &mut RegionJob<'data>) -> Result<()> + Sync),
    /// Go `afterRunJobFn`.
    pub after_run_job_fn: Option<&'a (dyn Fn(&[metapb::Peer]) + Sync)>,
    /// Go `regenerateJobsFn`.
    pub regenerate_jobs_fn:
        &'a (dyn Fn(&Context, &RegionJob<'data>) -> Result<Vec<RegionJob<'data>>> + Sync),
}
impl<'data> RegionJobBaseWorker<'_, 'data> {
    /// Go `regionJobBaseWorker.HandleTask`; owned jobs release references even
    /// on cancellation, regenerated-job errors, or panic.
    pub fn handle_task(
        &self,
        mut job: RegionJob<'data>,
        send: &mut dyn FnMut(RegionJob<'data>),
    ) -> Result<()> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tidb_failpoint::inject(
                "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/injectPanicForRegionJob",
                |_| {},
            );
            let peers = job
                .region
                .as_ref()
                .map_or_else(Vec::new, |r| r.region.peers.clone());
            // Go `regionJobBaseWorker.process`.
            tidb_failpoint::inject_call(
                "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/beforeExecuteRegionJob",
                &mut self.ctx.clone(),
            );
            let result = self.run_job(&mut job);
            if let Some(after) = self.after_run_job_fn {
                after(&peers);
            }
            if job.stage == JobStage::NeedRescan {
                let mut jobs = (self.regenerate_jobs_fn)(&self.ctx, &job)?;
                if let Some(completion) = &job.completion {
                    for new_job in &mut jobs {
                        new_job.completion = Some(completion.acquire());
                    }
                }
                for mut new_job in jobs {
                    new_job.last_retryable_error = job.last_retryable_error.clone();
                    if self.ctx.err().is_none() {
                        send(new_job);
                    }
                }
            } else if self.ctx.err().is_none() {
                send(job);
            }
            result
        }))
        .unwrap_or_else(|_| Err(Error::Other("region job worker panic".into())))
    }
    /// Go `regionJobBaseWorker.runJob`.
    pub fn run_job(&self, job: &mut RegionJob<'data>) -> Result<()> {
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/mockRunJobSucceed")
            .is_some()
        {
            job.convert_stage_to(JobStage::RegionScanned);
            return Ok(());
        }
        (self.pre_run_job_fn)(&self.ctx, job)?;
        loop {
            if job.stage == JobStage::RegionScanned {
                let leader = job
                    .region
                    .as_ref()
                    .and_then(|region| region.leader)
                    .map_or(0, |peer| peer.store_id);
                if leader == 0 {
                    let id = job.region.as_ref().map_or(0, |r| r.region.id);
                    job.last_retryable_error = Some(Error::Ingest(
                        tidb_ingestor_errdef::ERR_NO_LEADER
                            .gen_with_stack_by_args(&[(id as i64).into()]),
                    ));
                    job.convert_stage_to(JobStage::NeedRescan);
                    return Ok(());
                }
                let result = self.write_with_timeout(job);
                match result {
                    Err(error) => {
                        if !is_retryable_import_tikv_error(&error) {
                            return Err(error);
                        }
                        job.convert_stage_to(if error.to_string().contains("RequestTooNew") {
                            JobStage::RegionScanned
                        } else {
                            JobStage::NeedRescan
                        });
                        job.last_retryable_error = Some(error);
                        return Ok(());
                    }
                    Ok(result) if result.empty_job => job.convert_stage_to(JobStage::Ingested),
                    Ok(result) => {
                        job.write_result = Some(result);
                        job.convert_stage_to(JobStage::Wrote);
                    }
                }
            }
            if job.stage == JobStage::Wrote {
                if let Err(error) = (self.ingest_fn)(&self.ctx, job) {
                    if !is_retryable_import_tikv_error(&error) {
                        return Err(error);
                    }
                    job.convert_stage_to(get_next_stage_on_ingest_error(&error));
                    if let Error::IngestRegion { region, .. } = &error {
                        job.region = Some(region.clone());
                    }
                    job.last_retryable_error = Some(error);
                    return Ok(());
                }
                job.convert_stage_to(JobStage::Ingested);
            }
            if job.stage != JobStage::Ingested {
                return Ok(());
            }
            let remaining = job
                .write_result
                .as_ref()
                .and_then(|r| r.remaining_start_key.clone());
            let Some(remaining) = remaining else {
                return Ok(());
            };
            job.key_range.0 = remaining;
            job.convert_stage_to(JobStage::RegionScanned);
        }
    }
    /// Go `regionJobBaseWorker.writeWithTimeout`.
    fn write_with_timeout(&self, job: &mut RegionJob<'data>) -> Result<TiKVWriteResult> {
        let timeout = match tidb_failpoint::eval(
            "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/shortWaitNTimeout",
        ) {
            Some(value) => Duration::from_millis(value.int().max(0) as u64),
            _ => Duration::from_secs(15 * 60),
        };
        let (ctx, cancel) = Context::with_timeout(&self.ctx, timeout);
        let result = (self.write_fn)(&ctx, job);
        let context_error = ctx.err();
        cancel.cancel();
        if result.is_err() && context_error == Some(tidb_gocontext::CtxError::DeadlineExceeded) {
            return Err(Error::Source(std::sync::Arc::new(
                tidb_lightning_common::WriteTooSlow,
            )));
        }
        result
    }
    /// Go `regionJobBaseWorker.Close`.
    pub fn close(&self) -> Result<()> {
        Ok(())
    }
}
/// Go `regionJobBaseWorker.isRetryableImportTiKVError`, including EOF on restart.
pub fn is_retryable_import_tikv_error(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut cause = error;
    while let Some(source) = cause.source() {
        cause = source;
    }
    if cause
        .downcast_ref::<std::io::Error>()
        .is_some_and(|e| e.kind() == std::io::ErrorKind::UnexpectedEof)
    {
        return true;
    }
    tidb_lightning_common::is_retryable_error(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::region_job::RegionInfo;
    use tidb_ingestor_errdef::*;
    /// Go `TestRegionJobBaseWorker region fixture`.
    fn region() -> RegionInfo {
        RegionInfo {
            region: metapb::Region {
                id: 1,
                peers: vec![metapb::Peer {
                    store_id: 1,
                    ..Default::default()
                }],
                ..Default::default()
            },
            leader: Some(metapb::Peer {
                store_id: 1,
                ..Default::default()
            }),
        }
    }
    /// Go `TestRegionJobBaseWorker`: success, no leader, empty job,
    /// nonretryable error, rewrite, replacement region, rescan and preflight failure.
    #[test]
    fn test_region_job_base_worker() {
        for case in 0..8 {
            let write = |_: &Context, _: &mut RegionJob<'_>| {
                Ok(TiKVWriteResult {
                    empty_job: case == 2,
                    ..Default::default()
                })
            };
            let ingest = |_: &Context, _: &mut RegionJob<'_>| match case {
                3 => Err(Error::Ingest(ERR_KV_DISK_FULL.clone())),
                4 => Err(Error::Ingest(ERR_KV_INGEST_FAILED.clone())),
                5 => Err(Error::IngestRegion {
                    error: ERR_KV_EPOCH_NOT_MATCH.clone(),
                    region: RegionInfo {
                        region: metapb::Region {
                            id: 123,
                            ..Default::default()
                        },
                        leader: None,
                    },
                }),
                6 => Err(Error::Ingest(ERR_KV_NOT_LEADER.clone())),
                _ => Ok(()),
            };
            let pre = |_: &Context, _: &mut RegionJob<'_>| {
                if case == 7 {
                    Err(Error::Other(
                        "the remaining storage capacity of TiKV is less than 10%".into(),
                    ))
                } else {
                    Ok(())
                }
            };
            let regen = |_: &Context, job: &RegionJob<'_>| {
                Ok((0..3)
                    .map(|_| {
                        RegionJob::new(
                            region(),
                            None,
                            job.key_range.0.clone(),
                            job.key_range.1.clone(),
                            0,
                            0,
                        )
                    })
                    .collect())
            };
            let worker = RegionJobBaseWorker {
                ctx: Context::background(),
                write_fn: &write,
                ingest_fn: &ingest,
                pre_run_job_fn: &pre,
                after_run_job_fn: None,
                regenerate_jobs_fn: &regen,
            };
            let mut job = RegionJob::new(region(), None, b"a".to_vec(), b"z".to_vec(), 0, 0);
            if case == 1 {
                job.region.as_mut().unwrap().leader = None;
            }
            let mut output = Vec::new();
            let result = worker.handle_task(job, &mut |job| output.push(job));
            assert_eq!(result.is_err(), case == 3 || case == 7);
            assert_eq!(output.len(), if case == 1 || case == 6 { 3 } else { 1 });
            match case {
                0 | 2 => assert_eq!(output[0].stage, JobStage::Ingested),
                1 => assert!(
                    ERR_NO_LEADER.equal(
                        output[0]
                            .last_retryable_error
                            .as_ref()
                            .map(|e| e as &dyn std::error::Error)
                    )
                ),
                3 => {
                    assert_eq!(output[0].stage, JobStage::Wrote);
                    assert!(output[0].last_retryable_error.is_none());
                }
                4 => assert_eq!(output[0].stage, JobStage::RegionScanned),
                5 => {
                    assert_eq!(output[0].stage, JobStage::RegionScanned);
                    assert_eq!(output[0].region.as_ref().unwrap().region.id, 123);
                }
                6 => assert!(
                    ERR_KV_NOT_LEADER.equal(
                        output[0]
                            .last_retryable_error
                            .as_ref()
                            .map(|e| e as &dyn std::error::Error)
                    )
                ),
                _ => {}
            }
        }
    }
    /// Go `TestIsRetryableTiKVWriteError`.
    #[test]
    fn test_is_retryable_tikv_write_error() {
        let eof = std::io::Error::from(std::io::ErrorKind::UnexpectedEof);
        assert!(is_retryable_import_tikv_error(&eof));
        assert!(is_retryable_import_tikv_error(&Error::Source(
            std::sync::Arc::new(eof)
        )));
        assert!(!is_retryable_import_tikv_error(&Error::Ingest(
            ERR_KV_DISK_FULL.clone()
        )));
    }
    /// Go `TestRegionJobResetRetryCounter`: a rescan creates fresh retry counters,
    /// while an already-exhausted old job may still succeed without another retry.
    #[test]
    fn test_region_job_reset_retry_counter() {
        let write = |_: &Context, _: &mut RegionJob<'_>| Ok(TiKVWriteResult::default());
        let ingest = |_: &Context, job: &mut RegionJob<'_>| {
            if job.key_range.0 == b"c" && job.retry_count == 30 {
                Err(Error::Ingest(ERR_KV_NOT_LEADER.clone()))
            } else {
                Ok(())
            }
        };
        let pre = |_: &Context, _: &mut RegionJob<'_>| Ok(());
        let regen = |_: &Context, job: &RegionJob<'_>| {
            Ok(vec![RegionJob::new(
                region(),
                None,
                job.key_range.0.clone(),
                job.key_range.1.clone(),
                0,
                0,
            )])
        };
        let worker = RegionJobBaseWorker {
            ctx: Context::background(),
            write_fn: &write,
            ingest_fn: &ingest,
            pre_run_job_fn: &pre,
            after_run_job_fn: None,
            regenerate_jobs_fn: &regen,
        };
        for (start, end, rescan) in [
            (b"c".as_slice(), b"c2".as_slice(), true),
            (b"c2".as_slice(), b"d".as_slice(), false),
        ] {
            let mut job = RegionJob::new(region(), None, start.to_vec(), end.to_vec(), 0, 0);
            job.retry_count = 30;
            let mut jobs = Vec::new();
            worker.handle_task(job, &mut |job| jobs.push(job)).unwrap();
            assert_eq!(jobs.len(), 1);
            let job = jobs.pop().unwrap();
            if rescan {
                assert_eq!(job.retry_count, 0);
                assert!(
                    ERR_KV_NOT_LEADER.equal(
                        job.last_retryable_error
                            .as_ref()
                            .map(|e| e as &dyn std::error::Error)
                    )
                );
                worker.handle_task(job, &mut |job| jobs.push(job)).unwrap();
                assert_eq!(jobs[0].stage, JobStage::Ingested);
            } else {
                assert_eq!(job.stage, JobStage::Ingested);
            }
        }
    }
    /// Go `TestWorkerPoolWithErrors`: normal completion, drainer/generator errors,
    /// and recovered worker panic all release every job reference.
    #[test]
    fn test_worker_pool_with_errors() {
        use crate::region_job::JobTracker;
        use std::sync::Mutex;
        for case in 0..4 {
            let path = if case == 3 {
                "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/injectPanicForRegionJob"
            } else {
                "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/mockRunJobSucceed"
            };
            let _failpoint =
                tidb_failpoint::enable_for_test(path, if case == 3 { "panic" } else { "return" });
            let (ctx, cancel) = Context::with_cancel(&Context::background());
            let error = Mutex::new(None);
            let fail = |e: Error| {
                error.lock().unwrap().get_or_insert(e);
                cancel.cancel();
            };
            let tracker = JobTracker::default();
            let generating = tracker.acquire();
            let (sender, jobs) = crossbeam_channel::bounded(0);
            let (output, completed) = crossbeam_channel::bounded(0);
            let unused = |_: &Context, _: &mut RegionJob<'_>| -> Result<TiKVWriteResult> {
                panic!("failpoint must bypass writes")
            };
            let noop = |_: &Context, _: &mut RegionJob<'_>| Ok(());
            let regen = |_: &Context, _: &RegionJob<'_>| Ok(vec![]);
            let worker = RegionJobBaseWorker {
                ctx: ctx.clone(),
                write_fn: &unused,
                ingest_fn: &noop,
                pre_run_job_fn: &noop,
                after_run_job_fn: None,
                regenerate_jobs_fn: &regen,
            };
            std::thread::scope(|scope| {
                for _ in 0..4 {
                    let jobs = &jobs;
                    let output = &output;
                    let ctx = &ctx;
                    let worker = &worker;
                    let fail = &fail;
                    scope.spawn(move || loop {
                        let job = crossbeam_channel::select! { recv(ctx.done()) -> _ => return, recv(jobs) -> result => match result { Ok(job) => job,Err(_) => return } };
                        if let Err(error) = worker.handle_task(job,&mut |job| { crossbeam_channel::select! { recv(ctx.done()) -> _ => {},send(output,job) -> _ => {} } }) { fail(error);return; }
                    });
                }
                scope.spawn(|| { for index in 0..4 {
                    let mut job = RegionJob::new(region(),None,vec![],vec![],0,0);job.completion = Some(tracker.acquire());
                    crossbeam_channel::select! { recv(ctx.done()) -> _ => break,send(sender,job) -> _ => {} }
                    if case == 2 && index == 2 { fail(Error::Other("generator error".into()));break; }
                } drop(generating); });
                scope.spawn(|| { for index in 0..4 {
                    crossbeam_channel::select! { recv(ctx.done()) -> _ => return,recv(completed) -> result => drop(result.unwrap()) }
                    if case == 1 && index == 2 { fail(Error::Other("drainer error".into()));return; }
                } });
                tracker.wait(&ctx);
                cancel.cancel();
            });
            drop(jobs);
            drop(completed);
            tracker.wait(&Context::background());
            let expected = [
                None,
                Some("drainer error"),
                Some("generator error"),
                Some("region job worker panic"),
            ][case];
            assert_eq!(
                error
                    .into_inner()
                    .unwrap()
                    .map(|e| e.to_string())
                    .as_deref(),
                expected
            );
        }
    }
}
