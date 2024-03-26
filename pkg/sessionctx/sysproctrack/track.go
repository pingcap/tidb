// Copyright 2024 PingCAP, Inc.
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

package sysproctrack

import (
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
)

// TrackProc is an interface for a process to track
type TrackProc interface {
	// GetSessionVars gets the session variables.
	GetSessionVars() *variable.SessionVars
	// ShowProcess returns ProcessInfo running in current Context
	ShowProcess() *util.ProcessInfo
}

// Tracker is an interface to track system processes.
type Tracker interface {
	Track(id uint64, proc TrackProc) error
	UnTrack(id uint64)
	GetSysProcessList() map[uint64]*util.ProcessInfo
	KillSysProcess(id uint64)
}
