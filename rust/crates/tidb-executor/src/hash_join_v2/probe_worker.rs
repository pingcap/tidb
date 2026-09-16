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

//! Go `ProbeWorkerV2`: carry result chunks across inputs, recycle their storage,
//! and scan preserved build rows only after all probe workers have finished.
//! The executor owns worker scheduling and the two broadcast-channel senders:
//! dropping `close` interrupts every wait; dropping `scan_ready` releases the
//! post-probe barrier. Neither the worker nor its queues collect whole results.

use crossbeam_channel::{bounded, select_biased, Receiver, Sender, TryRecvError};
use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_in_disk::DataInDiskByChunks;

use super::ProbeV2;
use crate::base_join_probe::ProbeError;
use crate::{ExecError, StatementMemory};

/// One worker's fetcher and result-chunk resource queues, each of capacity one.
pub struct ProbeWorkerPorts {
    /// Drop after the final input chunk, as Go closes `probeResultCh` at EOF.
    pub input: Sender<Chunk>,
    /// Return a result buffer after swapping its columns into the caller's chunk.
    pub recycle: Sender<Chunk>,
}

/// Messages on the executor's bounded, shared result/resource channel.
pub enum ProbeWorkerEvent {
    /// Fully consumed input allocation, reset and ready for the fetcher.
    Input { worker_id: usize, chunk: Chunk },
    /// Full output, or the final partial output at the end of a stage.
    Output { worker_id: usize, chunk: Chunk },
    /// No more probe input remains; used flags are final only after every worker
    /// reports this event. The coordinator then releases `scan_ready`.
    ProbeDone {
        worker_id: usize,
        scan_required: bool,
    },
    /// Both probe and any post-barrier scan have finished.
    Done { worker_id: usize, collisions: u64 },
    /// Terminal error, retaining SQL/expression identity across the queue.
    Error { worker_id: usize, error: ExecError },
    /// Terminal error raised by the Go-equivalent probe-side fetcher.
    FetcherError { error: ExecError },
    /// The probe source returned an empty chunk and will send no more input.
    FetcherDone,
}

pub struct ProbeWorkerV2 {
    worker_id: usize,
    input: Receiver<Chunk>,
    resources: Receiver<Chunk>,
    events: Sender<ProbeWorkerEvent>,
    close: Receiver<()>,
    scan_ready: Receiver<()>,
}

enum Stop {
    Closed,
    Error(ExecError),
}

impl From<ExecError> for Stop {
    fn from(error: ExecError) -> Self {
        Self::Error(error)
    }
}

impl From<ProbeError> for Stop {
    fn from(error: ProbeError) -> Self {
        Self::Error(match error {
            ProbeError::Killed(error) => ExecError::Killed(error),
            ProbeError::Expression(error) => ExecError::Eval(error),
            ProbeError::Spill(error) => ExecError::SpillFailed(error),
            other => ExecError::internal(other.to_string()),
        })
    }
}

impl ProbeWorkerV2 {
    fn checkpoint(&self, memory: &StatementMemory) -> Result<(), Stop> {
        if !matches!(self.close.try_recv(), Err(TryRecvError::Empty)) {
            return Err(Stop::Closed);
        }
        memory.check().map_err(Stop::from)
    }

    /// Go `initializeForProbe`: allocate one output buffer and bounded queues.
    /// `events` must be shared across workers; the coordinator drains it while
    /// dispatching inputs and returns each output via that worker's `recycle`.
    pub fn new(
        worker_id: usize,
        output: Chunk,
        events: Sender<ProbeWorkerEvent>,
        close: Receiver<()>,
        scan_ready: Receiver<()>,
    ) -> (Self, ProbeWorkerPorts) {
        let (input_tx, input) = bounded(1);
        let (recycle, resources) = bounded(1);
        recycle
            .send(output)
            .expect("new resource queue has one slot");
        (
            Self {
                worker_id,
                input,
                resources,
                events,
                close,
                scan_ready,
            },
            ProbeWorkerPorts {
                input: input_tx,
                recycle,
            },
        )
    }

    /// Runs inside the executor's worker task. The probe borrows the immutable
    /// built table for the task's lifetime; no self-reference or leaked Arc is
    /// needed. The coordinator retains that table until all workers exit.
    pub fn run(self, probe: &mut dyn ProbeV2, memory: &StatementMemory) {
        self.run_mode(probe, memory, None);
    }

    pub fn run_restored(
        self,
        probe: &mut dyn ProbeV2,
        memory: &StatementMemory,
        disk: &mut DataInDiskByChunks,
        chunk: Chunk,
    ) {
        self.run_mode(probe, memory, Some((disk, chunk)));
    }

    fn run_mode(
        self,
        probe: &mut dyn ProbeV2,
        memory: &StatementMemory,
        restored: Option<(&mut DataInDiskByChunks, Chunk)>,
    ) {
        let killed = memory.sql_killer().get_kill_event_chan();
        let result = crate::sort_util::recover_worker_panic(|| {
            match self.run_inner(probe, memory, &killed, restored) {
                Ok(()) | Err(Stop::Closed) => Ok(()),
                Err(Stop::Error(error)) => Err(error),
            }
        });
        if let Err(error) = result {
            // A kill must still be delivered as an error. Only executor Close
            // or a dropped consumer can suppress this terminal publication.
            select_biased! {
                recv(self.close) -> _ => {},
                send(self.events, ProbeWorkerEvent::Error {
                    worker_id: self.worker_id, error,
                }) -> _ => {},
            }
        }
    }

    fn receive<T>(
        &self,
        channel: &Receiver<T>,
        memory: &StatementMemory,
        killed: &Receiver<()>,
    ) -> Result<Option<T>, Stop> {
        select_biased! {
            recv(self.close) -> _ => Err(Stop::Closed),
            recv(killed) -> _ => {
                memory.check()?;
                Err(Stop::Closed)
            },
            recv(channel) -> value => Ok(value.ok()),
        }
    }

    fn send(
        &self,
        event: ProbeWorkerEvent,
        memory: &StatementMemory,
        killed: &Receiver<()>,
    ) -> Result<(), Stop> {
        select_biased! {
            recv(self.close) -> _ => Err(Stop::Closed),
            recv(killed) -> _ => {
                memory.check()?;
                Err(Stop::Closed)
            },
            send(self.events, event) -> result => result.map_err(|_| Stop::Closed),
        }
    }

    fn new_result(&self, memory: &StatementMemory, killed: &Receiver<()>) -> Result<Chunk, Stop> {
        let mut output = self
            .receive(&self.resources, memory, killed)?
            .ok_or(Stop::Closed)?;
        output.reset();
        Ok(output)
    }

    fn publish(
        &self,
        output: Chunk,
        memory: &StatementMemory,
        killed: &Receiver<()>,
    ) -> Result<(), Stop> {
        self.send(
            ProbeWorkerEvent::Output {
                worker_id: self.worker_id,
                chunk: output,
            },
            memory,
            killed,
        )
    }

    fn run_inner(
        &self,
        probe: &mut dyn ProbeV2,
        memory: &StatementMemory,
        killed: &Receiver<()>,
        restored: Option<(&mut DataInDiskByChunks, Chunk)>,
    ) -> Result<(), Stop> {
        self.checkpoint(memory)?;
        let mut output = self.new_result(memory, killed)?;
        let result = (|| -> Result<Chunk, Stop> {
            let (mut disk, mut restored_chunk) = match restored {
                Some((disk, chunk)) => (Some(disk), Some(chunk)),
                None => (None, None),
            };
            let mut index = 0;
            loop {
                self.checkpoint(memory)?;
                let chunk = if let Some(disk) = disk.as_mut() {
                    if index == disk.num_chunks() {
                        break;
                    }
                    let mut chunk = restored_chunk.take().expect("restored input buffer");
                    disk.fill_chunk(index, &mut chunk)
                        .map_err(|error| ExecError::SpillFailed(error.to_string()))?;
                    index += 1;
                    chunk
                } else {
                    let Some(chunk) = self.receive(&self.input, memory, killed)? else {
                        break;
                    };
                    chunk
                };
                if disk.is_some() {
                    probe.set_restored_chunk_for_probe(chunk)?;
                } else {
                    probe.set_chunk_for_probe(chunk)?;
                }
                while !probe.is_current_chunk_probe_done() {
                    self.checkpoint(memory)?;
                    probe.probe(&mut output, memory.sql_killer())?;
                    if output.is_full() {
                        self.publish(output, memory, killed)?;
                        output = self.new_result(memory, killed)?;
                    }
                }
                let mut chunk = probe
                    .take_probe_chunk()
                    .expect("successful probe retains input");
                chunk.reset();
                if disk.is_some() {
                    restored_chunk = Some(chunk);
                    continue;
                }
                self.send(
                    ProbeWorkerEvent::Input {
                        worker_id: self.worker_id,
                        chunk,
                    },
                    memory,
                    killed,
                )?;
            }
            Ok(output)
        })();
        if matches!(result, Err(Stop::Closed)) {
            return Err(Stop::Closed);
        }
        // Go flushes even after a probe error; a spill failure replaces it.
        probe.spill_remaining_probe_chunks()?;
        let output = result?;
        // Go publishes the final partial result before reporting probe completion.
        // If empty, retain this buffer for the scan instead of a needless exchange.
        let mut scan_output = if output.num_rows() > 0 {
            self.publish(output, memory, killed)?;
            None
        } else {
            Some(output)
        };
        let scan_required = probe.need_scan_row_table();
        self.send(
            ProbeWorkerEvent::ProbeDone {
                worker_id: self.worker_id,
                scan_required,
            },
            memory,
            killed,
        )?;
        if scan_required {
            self.receive(&self.scan_ready, memory, killed)?;
            self.checkpoint(memory)?;
            probe.init_for_scan_row_table();
            let mut output = match scan_output.take() {
                Some(output) => output,
                None => self.new_result(memory, killed)?,
            };
            while !probe.is_scan_row_table_done() {
                self.checkpoint(memory)?;
                probe.scan_row_table(&mut output, memory.sql_killer())?;
                if output.is_full() {
                    self.publish(output, memory, killed)?;
                    output = self.new_result(memory, killed)?;
                }
            }
            if output.num_rows() > 0 {
                self.publish(output, memory, killed)?;
            }
        }
        self.send(
            ProbeWorkerEvent::Done {
                worker_id: self.worker_id,
                collisions: probe.get_probe_collision(),
            },
            memory,
            killed,
        )
    }
}
