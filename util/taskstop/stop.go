// Copyright 2022 PingCAP, Inc.
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

package taskstop

import (
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/stringutil"
)

type sessionStopInjection struct {
	fn          func(string)
	breakPoints []string
}

// EnableSessionStopPoint enables the stop points for a session
func EnableSessionStopPoint(sctx sessionctx.Context, fn func(string), breakPoints ...string) {
	sctx.SetValue(stringutil.StringerStr("sessionStopInjection"), &sessionStopInjection{
		fn:          fn,
		breakPoints: breakPoints,
	})
}

// DisableSessionStopPoint disables the stop points for a session
func DisableSessionStopPoint(sctx sessionctx.Context) {
	sctx.SetValue(stringutil.StringerStr("sessionStopInjection"), nil)
}

// InjectSessionStopPoint injects a stop point
func InjectSessionStopPoint(sctx sessionctx.Context, breakPoint string) {
	failpoint.Inject("sessionStop", func() {
		if inject, ok := sctx.Value(stringutil.StringerStr("sessionStopInjection")).(*sessionStopInjection); ok {
			for _, p := range inject.breakPoints {
				if p == breakPoint {
					inject.fn(breakPoint)
					break
				}
			}
		}
	})
}

// EnableGlobalSessionStopFailPoint enables the global session stop fail point
func EnableGlobalSessionStopFailPoint() error {
	return failpoint.Enable("github.com/pingcap/tidb/util/taskstop/sessionStop", "return")
}

// DisableGlobalSessionStopFailPoint disables the global session stop fail point
func DisableGlobalSessionStopFailPoint() error {
	return failpoint.Disable("github.com/pingcap/tidb/util/taskstop/sessionStop")
}
