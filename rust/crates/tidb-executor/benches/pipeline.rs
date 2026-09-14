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

//! In-process benchmarks for the two execution paths the TPC-H parity work
//! measures most often, so a change can be judged without a cluster.
//!
//! A run against the real node costs a release build of the whole server and
//! a warm A/B over TiKV, and returns a wall-clock difference that TiKV noise
//! and coprocessor-cache history can swamp: several rounds of this campaign
//! landed inside their own spread. These benchmarks isolate what the profiles
//! actually name as the cost -- the row materialisation a join does into its
//! output chunk, and the hash aggregate's fold -- and answer in seconds.
//!
//! Run it with address-space randomisation off:
//!
//! ```text
//! cargo bench -p tidb-executor --bench pipeline --no-run
//! setarch -R <the printed binary path>
//! ```
//!
//! Layout is worth a fifth of these numbers -- with randomisation left on the
//! same binary spread 40% where it spreads 22% without -- and it is fixed for
//! the life of a binary, so two builds compared under it differ by their code
//! and not by where the loader happened to put it.
//!
//! Output is one `name metric value` triple per line, so two runs can be
//! differenced mechanically. Read the `cal_` metrics, not the nanoseconds:
//! see [`best_of_blocks`]. `calibration cal_per_op` is the run's own noise
//! floor -- it is the reference divided by itself, so it reads 1.0 and its
//! drift bounds how finely anything below it can be read.
//!
//! The `bench` profile builds without LTO and with sixteen codegen units,
//! which is what makes the rebuild a minute instead of four. That is a
//! different code generation from the server the cluster harness runs, so the
//! absolute nanoseconds here are not the node's. What carries is the
//! comparison: two builds measured under the same profile differ by their
//! source. Confirm anything surprising, or anything that turns on cross-crate
//! inlining, with `cargo bench --profile release`.

use std::hint::black_box;
use std::time::{Duration, Instant};

use tidb_chunk::chunk::Chunk;
use tidb_datatype::Datum;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_executor::executor::{ExecError, Executor, ExecutorMeta};
use tidb_executor::hash_agg::{AggFunc, AggKind, HashAggContext, HashAggExec};
use tidb_executor::mem_quota::{OomAction, StatementMemory};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

/// Rows the aggregate folds per run.
///
/// Small enough that one run stays in the tens of milliseconds rather than a
/// quarter of a second, so it still interleaves with the reference several
/// times a block, and large enough that the wide group count below builds a
/// map of a realistic size.
const ROWS: usize = 1 << 18;

/// Go's `max_chunk_size`.
const CHUNK: usize = 1024;

/// Timed blocks per metric. The minimum over the blocks is the block the
/// neighbours interrupted least, which is the figure a comparison wants.
const PASSES: usize = 5;

/// Operations in one calibration unit.
const CAL_OPS: usize = 1 << 20;

/// How long one slice of work runs before the reference gets its turn.
///
/// The reference only cancels the host's interference if it meets the same
/// interference, and on this box that arrives in bursts far shorter than a
/// block: with work and reference run as two halves of a 150ms block, the
/// reference divided by itself still moved 12% run to run. Sliced this fine
/// they interleave about a hundred times a block, and a burst lands on both.
const SLICE: Duration = Duration::from_millis(2);

/// How long one timed block runs, as slices of work and reference in turn.
const BLOCK: Duration = Duration::from_millis(250);

/// A fixed unit of arithmetic over a cache-resident array.
///
/// It touches one flat array and no allocator, so nothing in this file can
/// change what it costs. That is what makes it the reference every other
/// measurement is divided by.
fn calibration_unit() {
    let data: Vec<u64> = (0..1024_u64).collect();
    let mut acc = 0_u64;
    for _ in 0..(CAL_OPS / data.len()) {
        for value in &data {
            acc = acc.wrapping_add(*value ^ acc);
        }
    }
    black_box(acc);
}

/// How many times `work` must run to fill one `SLICE`.
fn reps_for(work: &mut dyn FnMut()) -> usize {
    const MAX_REPS: usize = 1 << 22;
    let mut reps = 1;
    loop {
        let started = Instant::now();
        for _ in 0..reps {
            work();
        }
        let took = started.elapsed();
        if took >= SLICE || reps >= MAX_REPS {
            return reps;
        }
        // Scaling straight to SLICE from the reading is what a sizing loop
        // wants to do, but a reading of a few microseconds of work is mostly
        // clock, and the jump then lands on whatever ceiling the loop carries.
        // A bounded factor reaches a usable reading in a few steps instead,
        // and cannot overshoot by more than itself.
        let factor = (SLICE.as_nanos() / took.as_nanos().max(1) + 1).clamp(2, 64) as usize;
        reps = reps.saturating_mul(factor).min(MAX_REPS);
    }
}

/// One interleaved round: a slice of each entry of `work`, then a slice of
/// the reference, each added to its own accumulator.
fn round(
    work: &mut [(&str, &mut dyn FnMut())],
    reps: &[usize],
    cal_reps: usize,
    acc: &mut [Duration],
) {
    for (index, ((_, run), reps)) in work.iter_mut().zip(reps).enumerate() {
        let started = Instant::now();
        for _ in 0..*reps {
            run();
        }
        acc[index] += started.elapsed();
    }
    let started = Instant::now();
    for _ in 0..cal_reps {
        calibration_unit();
    }
    let last = acc.len() - 1;
    acc[last] += started.elapsed();
}

/// Runs the entries of `work` interleaved with the calibration reference for
/// `PASSES` blocks, and returns for each entry the least one run of it cost:
/// once in nanoseconds, and once as a multiple of one calibration unit
/// measured in the same block.
///
/// **The second figure is the one an A/B should read.** This host is a virtual
/// machine, and the time its vCPU spends descheduled by the hypervisor is
/// charged to whatever thread the guest believes is running. That is invisible
/// to the guest's own CPU accounting exactly as it is to the wall clock, so no
/// accounting removes it and no number of blocks averages it out at the
/// timescale it arrives on: the calibration loop, which cannot vary, swings
/// 25% run to run in absolute nanoseconds. What the hypervisor cannot do is
/// take one 2ms slice and not the next, so dividing by a reference sliced in
/// among the work cancels it.
///
/// The clock is the wall clock, not the process's CPU time. `schedstat`
/// advances only at context switches and scheduler ticks, so a short slice
/// reads stale, and taking a *minimum* of a stale-quantised counter does
/// something worse than add noise -- it selects the stalest reading, so the
/// answer is biased low and gets worse the more blocks are run. The
/// calibration loop came to spread 51% over 25 blocks of 4ms that way, against
/// 0.9% over 7. `Instant` has neither problem, and what it does have -- time
/// on the floor while something else runs -- is what the ratio removes.
///
/// The first block is discarded: it warms the caches and the allocator.
fn best_of_blocks(work: &mut [(&str, &mut dyn FnMut())]) -> Vec<(Duration, f64)> {
    let entries = work.len();
    let reps: Vec<usize> = work.iter_mut().map(|(_, w)| reps_for(*w)).collect();
    let cal_reps = reps_for(&mut calibration_unit);

    // Rounds per block, from what a round actually costs: an entry whose
    // single run already exceeds SLICE -- the aggregate's does -- makes rounds
    // longer than the slice count alone would suggest.
    let mut probe = vec![Duration::ZERO; entries + 1];
    round(work, &reps, cal_reps, &mut probe);
    let per_round: Duration = probe.iter().sum();
    let rounds = ((BLOCK.as_nanos() / per_round.as_nanos().max(1)) as usize).clamp(2, 4096);

    let mut blocks = Vec::with_capacity(PASSES);
    for block in 0..=PASSES {
        let mut acc = vec![Duration::ZERO; entries + 1];
        for _ in 0..rounds {
            round(work, &reps, cal_reps, &mut acc);
        }
        if block > 0 {
            blocks.push(acc);
        }
    }

    let cal_ops = (cal_reps * rounds * CAL_OPS) as f64;
    (0..entries)
        .map(|index| {
            let runs = (reps[index] * rounds) as f64;
            let best_secs = blocks
                .iter()
                .map(|acc| acc[index].as_secs_f64() / runs)
                .fold(f64::INFINITY, f64::min);
            let best_ratio = blocks
                .iter()
                // Per block, so the hypervisor's share of both is the same.
                .map(|acc| {
                    (acc[index].as_secs_f64() / runs) / (acc[entries].as_secs_f64() / cal_ops)
                })
                .fold(f64::INFINITY, f64::min);
            (Duration::from_secs_f64(best_secs), best_ratio)
        })
        .collect()
}

/// TPC-H Q10's output row: two keys and the wide customer text.
fn wide_types() -> Vec<FieldType> {
    vec![
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::Varchar),
        FieldType::new(FieldTypeCode::Varchar),
        FieldType::new(FieldTypeCode::Varchar),
    ]
}

fn schema_of(types: &[FieldType]) -> Schema {
    Schema::new(
        types
            .iter()
            .enumerate()
            .map(|(index, field_type)| {
                let mut column = Column::new(index as i64 + 1, field_type.clone());
                column.index = index as i64;
                column
            })
            .collect(),
    )
}

fn column(index: usize, field_type: &FieldType) -> Expression {
    let mut column = Column::new(index as i64 + 1, field_type.clone());
    column.index = index as i64;
    Expression::Column(column)
}

/// One chunk of the wide shape, with the text widths of `c_name`,
/// `c_address` and `c_comment`.
fn wide_chunk(rows: usize, groups: i64) -> Chunk {
    let types = wide_types();
    let mut chunk = Chunk::new_with_capacity(&types, rows);
    for row in 0..rows {
        chunk.append_int64(0, (row as i64) % groups);
        chunk.append_int64(1, row as i64);
        chunk.append_bytes(2, &[b'n'; 25]);
        chunk.append_bytes(3, &[b'a'; 40]);
        chunk.append_bytes(4, &[b'c'; 117]);
    }
    chunk
}

/// Bytes one row of `col_idxs` carries, for a throughput figure.
fn row_bytes(col_idxs: &[usize]) -> usize {
    col_idxs
        .iter()
        .map(|index| match index {
            0 | 1 => 8,
            2 => 25,
            3 => 40,
            _ => 117,
        })
        .sum()
}

/// Go `chunk.AppendPartialRow` in the shape the joins use: copy the selected
/// columns of a probe/build row into the output chunk.
fn bench_row_copy() {
    let types = wide_types();
    let source = wide_chunk(CHUNK, 1 << 20);
    let shapes: [(&str, Vec<usize>); 2] = [
        ("row_copy_keys_only", vec![0, 1]),
        ("row_copy_wide", vec![0, 1, 2, 3, 4]),
    ];

    // One run of the closure copies the whole source chunk.
    let rows = source.num_rows() as f64;
    for (label, col_idxs) in shapes {
        let out_types: Vec<FieldType> = col_idxs.iter().map(|i| types[*i].clone()).collect();
        let mut out = Chunk::new_with_capacity(&out_types, CHUNK);
        let mut pass = || {
            for index in 0..source.num_rows() {
                if out.num_rows() == CHUNK {
                    out.reset();
                }
                black_box(out.append_partial_row_by_col_idxs(
                    0,
                    source.get_row(index),
                    Some(&col_idxs),
                ));
            }
        };
        let (elapsed, ratio) = best_of_blocks(&mut [(label, &mut pass)])[0];

        let per_row = elapsed.as_secs_f64() * 1e9 / rows;
        let mib = (rows * row_bytes(&col_idxs) as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
        println!("{label} ns_per_row {per_row:.1}");
        println!("{label} mib_per_s {mib:.0}");
        println!("{label} cal_per_row {:.4}", ratio / rows);
    }
}

/// Replays one prepared chunk until `remaining` rows have been produced: the
/// cheapest child that still fills the parent's chunk the way a reader does.
struct Replay {
    meta: ExecutorMeta,
    template: Chunk,
    remaining: usize,
}

impl Executor for Replay {
    fn open(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let take = self.remaining.min(self.meta.max_chunk_size());
        for index in 0..take {
            req.append_partial_row(0, self.template.get_row(index % self.template.num_rows()));
        }
        self.remaining -= take;
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn new_chunk(&self) -> Chunk {
        Chunk::new_with_capacity(self.meta.ret_field_types(), self.meta.init_cap())
    }
}

fn replay(types: &[FieldType], rows: usize, groups: i64) -> Replay {
    Replay {
        meta: ExecutorMeta::new(schema_of(types), 0, CHUNK, CHUNK),
        template: wide_chunk(CHUNK, groups),
        remaining: rows,
    }
}

fn drain(exec: &mut dyn Executor) -> usize {
    exec.open().expect("open");
    let mut chunk = exec.new_chunk();
    let mut rows = 0;
    loop {
        exec.next(&mut chunk).expect("next");
        if chunk.num_rows() == 0 {
            break;
        }
        rows += chunk.num_rows();
    }
    exec.close().expect("close");
    rows
}

/// An evaluation context that pins the aggregate to one lane.
///
/// With the production context the aggregate resolves its worker counts from
/// `tidb_hashagg_partial_concurrency` / `..._final_concurrency` and runs the
/// parallel pipeline. On a four-core box that is also running TiKV, PD and a
/// second SQL node, what those lanes cost depends on how much of the machine
/// they actually got, and that is not measurement noise -- it is a real
/// quantity that moves 20% run to run, and no in-block reference cancels it,
/// because the reference is one thread.
///
/// The fold and the group map are what a change to this executor touches, and
/// one lane measures them exactly. How well the lanes overlap is a separate
/// question, and a contended four-core box is the wrong instrument for it:
/// that one belongs to the cluster harness.
struct SerialAgg;

impl Columns for SerialAgg {
    fn get(&self, _path: &[String]) -> Option<Datum> {
        None
    }
}

impl HashAggContext for SerialAgg {
    // Never consulted: `hashagg_concurrency` keeps this aggregate serial, and
    // the default `run_parallel_pipeline_bridge` refuses the pipeline anyway.
    const PARALLEL_WORKERS_MAY_EVAL: bool = false;

    fn hashagg_concurrency(&self) -> Option<(usize, usize)> {
        Some((1, 1))
    }
}

/// The aggregate's own cost: the same source drained bare, then under a
/// `GROUP BY` with a sum, so the difference is the fold and the map.
///
/// Both sides are entries of one block, so they are sliced in among each other
/// and the subtraction is a paired difference. Differencing two independently
/// minimised numbers instead adds their errors -- each already the tail of its
/// own distribution -- which is how the fold line came to spread 21-47% while
/// the total beside it held 5%.
fn bench_aggregate() {
    let types = wide_types();
    let out_types = vec![types[0].clone(), types[1].clone()];
    for (label, groups) in [("agg_groups_1k", 1_000_i64), ("agg_groups_60k", 60_000)] {
        let mut source_pass = || {
            let mut source = replay(&types, ROWS, groups);
            black_box(drain(&mut source));
        };
        let mut total_pass = || {
            let mut agg = HashAggExec::new(
                ExecutorMeta::new(schema_of(&out_types), 1, CHUNK, CHUNK),
                vec![column(0, &types[0])],
                vec![
                    AggFunc::new(AggKind::FirstRow, Some(column(0, &types[0]))),
                    AggFunc::new(AggKind::Sum, Some(column(1, &types[1]))),
                ],
                Box::new(replay(&types, ROWS, groups)),
                SerialAgg,
                StatementMemory::new(1 << 30, OomAction::Cancel, 1).with_tmp_storage_on_oom(false),
            );
            black_box(drain(&mut agg));
        };
        let results =
            best_of_blocks(&mut [("source", &mut source_pass), ("total", &mut total_pass)]);

        let rows = ROWS as f64;
        let (source_time, source_ratio) = results[0];
        let (total_time, total_ratio) = results[1];
        println!(
            "{label} total_ns_per_row {:.1}",
            total_time.as_secs_f64() * 1e9 / rows
        );
        println!(
            "{label} fold_ns_per_row {:.1}",
            (total_time.as_secs_f64() - source_time.as_secs_f64()) * 1e9 / rows
        );
        println!("{label} total_cal_per_row {:.4}", total_ratio / rows);
        println!(
            "{label} fold_cal_per_row {:.4}",
            (total_ratio - source_ratio) / rows
        );
    }
}

fn main() {
    let (elapsed, ratio) = best_of_blocks(&mut [("calibration", &mut calibration_unit)])[0];
    // A canary, and a check on the method. The ratio is the reference over a
    // *different* slice of the same reference, so it reads a little under
    // 1.0000, and how far under is this method's own noise floor: no metric
    // below can be read more finely than that. The nanoseconds beside it are
    // free to drift, and do.
    println!(
        "calibration ns_per_op {:.4}",
        elapsed.as_secs_f64() * 1e9 / CAL_OPS as f64
    );
    println!("calibration cal_per_op {:.4}", ratio / CAL_OPS as f64);
    bench_row_copy();
    bench_aggregate();
}
