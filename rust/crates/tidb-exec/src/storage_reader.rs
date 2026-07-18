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

//! Executor-owned TableReader and IndexReader runtime.
//!
//! Physical-plan lowering stops before this module: callers supply immutable,
//! already-built DistSQL requests. The reader owns their response lifecycles,
//! ordered consumption, caller-sized batches, and dummy-reader no-send path.
//! Concrete TiKV transport remains the responsibility of the injected
//! `QueryTransport` implementation.

mod table_index_reader;

pub use table_index_reader::{
    ReaderKind, ReaderPlan, ReaderState, StorageReaderError, TableIndexReader,
};
