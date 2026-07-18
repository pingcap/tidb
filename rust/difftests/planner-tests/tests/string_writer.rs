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

//! Dependency-closed vectors for
//! `pkg/planner/cascades/util/string_writer.go`.
//!
//! The source helper is exercised by the writer assertions in `TestBinderFail`
//! at `pkg/planner/cascades/rule/binder_test.go:69`.

use tidb_planner::string_writer::{new_memory_buffer, new_str_buffer, StrBufferWriter};

#[test]
fn source_string_writer_hides_lengths_and_flushes() {
    let mut writer = new_memory_buffer();
    writer.write_string("GE:DataSource_1{}\n");
    writer.write_string("GE:Limit_4{GID:1}\n");
    writer.flush();
    assert_eq!(
        writer.into_inner(),
        b"GE:DataSource_1{}\nGE:Limit_4{GID:1}\n"
    );
}

#[test]
fn source_constructor_accepts_a_standard_writer() {
    let mut writer = new_str_buffer(Vec::<u8>::new());
    writer.write_string("source");
    writer.flush();
    assert_eq!(writer.into_inner(), b"source");
}
