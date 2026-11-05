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
// Initialize fixes the mode once during process startup. Reinitializing with the
// same value is allowed; changing an initialized value is rejected. Enabled
// returns false before initialization. SetForTest temporarily overrides the
// process state in tests built with the intest tag; such tests must not run in
// parallel with users of this process-wide state.
//
// This package only stores the mode; callers implement diagnostic behavior.
package diagnosticmode
