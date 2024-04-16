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

package contextstatic

import (
	"math"
	"sync"

	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
)

type warnings struct {
	sync.Mutex
	warnings []stmtctx.SQLWarn
}

// AppendWarning appends a warning
func (w *warnings) AppendWarning(warn error) {
	w.Lock()
	defer w.Unlock()
	if len(w.warnings) < math.MaxUint16 {
		w.warnings = append(w.warnings, stmtctx.SQLWarn{Level: stmtctx.WarnLevelWarning, Err: warn})
	}
}

func (w *warnings) WarningCount() int {
	w.Lock()
	defer w.Unlock()
	return len(w.warnings)
}

func (w *warnings) TruncateWarnings(start int) []stmtctx.SQLWarn {
	w.Lock()
	defer w.Unlock()
	sz := len(w.warnings) - start
	if sz <= 0 {
		return nil
	}
	ret := make([]stmtctx.SQLWarn, sz)
	copy(ret, w.warnings[start:])
	w.warnings = w.warnings[:start]
	return ret
}

func (w *warnings) SetWarnings(warns []stmtctx.SQLWarn) {
	w.Lock()
	defer w.Unlock()

	l := len(warns)
	if w.warnings == nil || cap(w.warnings) < l {
		w.warnings = make([]stmtctx.SQLWarn, l)
	} else {
		w.warnings = w.warnings[:l]
	}

	if l > 0 {
		copy(w.warnings, warns)
	}
}
