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

package breakpoint

import (
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

// NotifyBreakPointFuncKey is the key where break point notify function located
const NotifyBreakPointFuncKey = stringutil.StringerStr("breakPointNotifyFunc")

// Inject injects a break point to a session
func Inject(sctx sessionctx.Context, name string) {
	failpoint.Inject(name, func(_ failpoint.Value) {
		val := sctx.Value(NotifyBreakPointFuncKey)
		if breakPointNotifyAndWaitContinue, ok := val.(func(string)); ok {
			breakPointNotifyAndWaitContinue(name)
		}
	})
}
