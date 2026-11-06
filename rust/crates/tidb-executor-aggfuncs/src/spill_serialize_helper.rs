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

//! Go `pkg/executor/aggfuncs/spill_serialize_helper.go`: the bytes a
//! spilled partial result is stored as.

use tidb_serialization as util;

/// Go `SerializeHelper`: one reused buffer the encodings are written into.
pub struct SerializeHelper {
    buf: Vec<u8>,
}

impl Default for SerializeHelper {
    fn default() -> Self {
        Self::new()
    }
}

impl SerializeHelper {
    /// Go `NewSerializeHelper`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(64),
        }
    }

    /// Go `serializePartialResult4Count`.
    pub(crate) fn serialize_partial_result_4_count(&mut self, value: i64) -> &[u8] {
        self.buf.clear();
        util::serialize_i64(value, &mut self.buf);
        &self.buf
    }
}
