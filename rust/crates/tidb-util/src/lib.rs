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

//! Complete transcreations of TiDB's dependency-leaf utility packages.
//!
//! Each public module corresponds to one complete Go package. Modules are
//! added only when that package's production files, tests, and support
//! obligations move together.

pub mod arena;
pub mod backoff;
pub mod bitmap;
pub mod checksum;
pub mod column_mapping;
pub mod context;
pub mod dbterror;
pub mod disjointset;
pub mod encrypt;
pub mod fastrand;
pub mod generic;
pub mod globalconn;
pub mod intest;
pub mod intset;
pub mod israce;
pub mod layered_io;
pub mod logutil;
pub mod mathutil;
pub mod memory;
pub mod mvmap;
pub mod naming;
pub mod nocopy;
pub mod paging;
pub mod partialjson;
pub mod ppcpuusage;
pub mod prefetch;
pub mod queue;
pub mod redact;
pub mod selection;
pub mod sem;
pub mod size;
pub mod slice;
pub mod sqlescape;
pub mod sqlkiller;
pub mod table_filter;
pub mod table_rule_selector;
pub mod texttree;
pub mod tikvutil;
pub mod timeutil;
pub mod versioninfo;
pub mod vitess;
pub mod watcher;
pub mod zeropool;
