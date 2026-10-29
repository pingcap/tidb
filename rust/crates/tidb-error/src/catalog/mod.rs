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

//! Go error prototypes (`ErrFoo = ClassBar.NewStd(...)`) of every package
//! outside the three that already own a catalog
//! (`pkg/util/dbterror`, `.../plannererrors`, `.../exeerrors`).
//!
//! Go identity travels with the error value: a `*terror.Error` knows its class
//! and code, and `Error()` prints `[<class>:<code>]<message>`. Codes are not
//! unique across classes (`1146` is `infoschema.ErrTableNotExists` in class
//! `schema` and `plannererrors.ErrNoSuchTable` in class `planner`), so the class
//! can only come from the Go definition a raise site names -- these statics are
//! those definitions. Module = Go package, constant = the Go name in
//! SCREAMING_SNAKE.

use tidb_terror::catalog_support::{std_err, Class};

pub mod infoschema;
pub mod infosync;
pub mod table;
