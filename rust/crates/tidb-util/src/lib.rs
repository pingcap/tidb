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

pub mod checksum;
pub mod disjointset;
pub mod encrypt;
pub mod layered_io;
pub mod zeropool;
