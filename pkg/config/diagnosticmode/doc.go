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

// Package diagnosticmode stores whether the TiDB process runs in diagnostic
// mode.
//
// Diagnostic mode is initialized from a command-line flag during startup and
// cannot be changed afterward. Code that needs to select diagnostic behavior
// should call Enabled.
//
// In this mode, the Domain continues loading existing schema metadata with the
// ordinary schema syncer, but skips MDL checks and server/topology registration.
// Diagnostic startup requires an already bootstrapped keyspace and reads startup
// settings without writing them back. DDL.Start does not start DDL execution
// resources for normal startup; bootstrap, upgrade, and BR startup modes return
// ErrDDLNotAllowed. EnableDDL and SwitchMDL also return ErrDDLNotAllowed, while
// DisableDDL is a no-op. The independent server ID lease, timestamp acquisition,
// and regular min-start-ts reporting remain enabled.
//
// Diagnostic mode is not a general read-only mode: this package does not by
// itself reject every SQL submission or disable every background service.
package diagnosticmode
