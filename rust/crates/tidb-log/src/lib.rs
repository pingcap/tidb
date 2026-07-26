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

//! Transcreation of the pinned `github.com/pingcap/log` module
//! (v1.1.1-0.20250917021125): the TiKV unified log format
//! (RFC 2018-12-19-unified-log-format) text encoder and its config types.
//!
//! The zap plumbing (cores, syncers, global logger swap) is Go-ecosystem
//! machinery; the Rust wiring rides on `tracing` in `pkg/util/logutil`'s
//! port. What this crate owns is the byte-level behavioral contract: the
//! header/field encoding, escaping and quoting rules that log-parsing
//! tooling relies on.

#![warn(missing_docs)]

pub mod config;
pub mod text_encoder;

pub use config::{Config, FileLogConfig, DEFAULT_LOG_MAX_SIZE};
pub use text_encoder::{Entry, Field, Level, TextEncoder, Value};
