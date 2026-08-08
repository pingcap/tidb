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

// Package export implements the DXF task type for distributed export, modeled
// on pkg/dxf/importinto.
//
// The task is table-set-native: TaskMeta carries a set of tables, and both
// EXPORT TABLE db.t (one table) and EXPORT SCHEMA db (all base tables) run on
// the same backend — single-table is just len(Tables) == 1. The Dump step
// region-splits each table into key-ordered spans and emits subtasks whose meta
// is a list of per-table units; downstream (encode, naming, retry) is uniform
// over that list and never special-cases one table versus many.
//
// This package currently provides the scheduler side (task-type registration,
// step sequencing, and the Dump-step span split). The subtask executor
// (read -> encode -> upload pipeline) and the GC safepoint keeper are added by a
// later milestone. Until the parser grammar lands, no Export task is ever
// submitted, so this code is unreachable from SQL.
package export
