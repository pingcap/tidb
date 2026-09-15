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

package diagnosticmode

import (
	"fmt"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// ErrDDLNotAllowed is returned when a diagnostic instance is asked to run DDL.
var ErrDDLNotAllowed = errors.New("DDL operations are not allowed in diagnostic mode")

type modeState uint32

const (
	modeUninitialized modeState = iota
	modeDisabled
	modeEnabled
)

var currentMode atomic.Uint32

// Initialize sets the process-wide diagnostic mode during TiDB startup.
// Repeated initialization with the same value is allowed, but changing the
// value after initialization is rejected.
func Initialize(enabled bool) error {
	desired := stateFor(enabled)
	current := modeState(currentMode.Load())
	if current == desired {
		return nil
	}
	if current != modeUninitialized {
		return fmt.Errorf(
			"diagnostic mode is already initialized as %s and cannot be changed to %s",
			current,
			desired,
		)
	}
	currentMode.Store(uint32(desired))
	return nil
}

// Enabled returns whether the current TiDB process runs in diagnostic mode.
// It returns false before Initialize is called.
func Enabled() bool {
	return modeState(currentMode.Load()) == modeEnabled
}

// SetForTest overrides diagnostic mode and returns a function that restores the
// previous state. Tests using it must not run in parallel with tests that read
// or override diagnostic mode.
func SetForTest(enabled bool) (restore func()) {
	if !intest.InTest {
		panic("diagnosticmode.SetForTest can only be called in tests built with the intest tag")
	}

	previous := currentMode.Swap(uint32(stateFor(enabled)))
	return func() {
		currentMode.Store(previous)
	}
}

func stateFor(enabled bool) modeState {
	if enabled {
		return modeEnabled
	}
	return modeDisabled
}

func (s modeState) String() string {
	switch s {
	case modeUninitialized:
		return "uninitialized"
	case modeDisabled:
		return "disabled"
	case modeEnabled:
		return "enabled"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}
