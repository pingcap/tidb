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

//! Go `pkg/util/replayer`.

use std::io;
use std::path::Path;
use std::sync::{Once, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE;
use base64::Engine as _;

const PLAN_REPLAYER_DIR_NAME: &str = "replayer";

/// Go `PlanReplayerTaskKey`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct PlanReplayerTaskKey {
    /// Go `SQLDigest`.
    pub sql_digest: String,
    /// Go `PlanDigest`.
    pub plan_digest: String,
}

/// Native boundary for Go `objectio.Writer` with its captured context.
pub trait ObjectWriter {
    /// Go `Writer.Write(ctx, p)`.
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize>;

    /// Go `Writer.Close(ctx)`.
    fn close(&mut self) -> io::Result<()>;
}

/// Native boundary for the `storeapi.Storage.Create` operation this package uses.
pub trait Storage {
    /// Writer returned by `Create`.
    type Writer: ObjectWriter;

    /// Creates one storage-relative file.
    fn create(&self, path: &Path) -> io::Result<Self::Writer>;
}

/// Native equivalent of Go `io.WriteCloser`.
pub trait WriteCloser {
    /// Writes bytes.
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize>;

    /// Closes the writer.
    fn close(&mut self) -> io::Result<()>;
}

struct FileWriter<W> {
    writer: W,
}

impl<W: ObjectWriter> WriteCloser for FileWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.writer.write(bytes)
    }

    fn close(&mut self) -> io::Result<()> {
        self.writer.close()
    }
}

/// Go `NewFileWriter`.
pub fn new_file_writer<W: ObjectWriter>(writer: W) -> impl WriteCloser {
    FileWriter { writer }
}

/// Go `GeneratePlanReplayerFile`.
pub fn generate_plan_replayer_file<S: Storage>(
    storage: &S,
    is_capture: bool,
    is_continues_capture: bool,
    enable_historical_stats_for_capture: bool,
) -> io::Result<(impl WriteCloser, String)> {
    let file_name = generate_plan_replayer_file_name(
        is_capture,
        is_continues_capture,
        enable_historical_stats_for_capture,
    )?;
    let path = Path::new(get_plan_replayer_dir_name()).join(&file_name);
    let writer = storage.create(&path)?;
    Ok((new_file_writer(writer), file_name))
}

/// Go `GeneratePlanReplayerFileName`.
pub fn generate_plan_replayer_file_name(
    is_capture: bool,
    is_continues_capture: bool,
    enable_historical_stats_for_capture: bool,
) -> io::Result<String> {
    let time = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos() as i64,
        Err(error) => -(error.duration().as_nanos() as i64),
    };
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| io::Error::other(error.to_string()))?;
    let key = URL_SAFE.encode(bytes);
    if is_continues_capture || is_capture && enable_historical_stats_for_capture {
        return Ok(format!("capture_replayer_{key}_{time}.zip"));
    }
    if is_capture && !enable_historical_stats_for_capture {
        return Ok(format!("capture_normal_replayer_{key}_{time}.zip"));
    }
    Ok(format!("replayer_{key}_{time}.zip"))
}

/// Go `PlanReplayerPath`, protected for safe Rust mutation.
pub static PLAN_REPLAYER_PATH: RwLock<String> = RwLock::new(String::new());

/// Go `PlanReplayerPathOnce`.
pub static PLAN_REPLAYER_PATH_ONCE: Once = Once::new();

/// Go `GetPlanReplayerDirName`.
#[must_use]
pub const fn get_plan_replayer_dir_name() -> &'static str {
    PLAN_REPLAYER_DIR_NAME
}
