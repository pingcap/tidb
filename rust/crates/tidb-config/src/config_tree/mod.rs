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

//! Transcreation of Go `pkg/config/config.go`'s top-level `Config` tree.
//!
//! PACKAGE IN PROGRESS: this is the seed of the `pkg/config` `Config`
//! struct. It currently covers the self-contained marshaling helpers
//! (`nullableBool`, `AtomicBool`), the error-message-extension
//! preparation, the `CSE`/`TrxSummary` sub-sections,
//! `max_allowed_packet` validity, and `FlattenConfigItems`. Still to land within the same package
//! claim: the sub-section structs (Log/Instance/Security/Performance/...),
//! the `Config` struct + `DefaultConfig`, `Valid`, `Load` (TOML), and the
//! `config_util` helpers. The tikvcfg-embedded fields use
//! [`crate::tikvcfg`]; the actual TiKV client is `tikv/client-rust`.

pub mod big_sections;
pub mod helpers;
pub mod log_instance;
pub mod marshal;
pub mod sections;

pub use big_sections::{Performance, Security, Status};
pub use helpers::{
    flatten_config_items, prepare_error_message_extensions, valid_max_allowed_packet, Cse,
    ErrorMessageExtension, TrxSummary, DEF_MAX_ALLOWED_PACKET,
};
pub use log_instance::{FileLogConfig, Instance, Log};
pub use marshal::{AtomicBool, NullableBool, NB_FALSE, NB_TRUE, NB_UNSET};
pub use sections::{
    Experimental, IsolationRead, OpenTracing, OpenTracingReporter, OpenTracingSampler,
    PessimisticTxn, PlanCache, Plugin, PreparedPlanCache, ProxyProtocol, RuV2Config, Standby,
    StarterParams, TopSql,
};
