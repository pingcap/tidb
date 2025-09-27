// Copyright 2025 PingCAP, Inc.
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

package rewriter

import (
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestVariableRewritter(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		// scope validation
		tk.MustGetErrCode("select @@session.ddl_slow_threshold", errno.ErrIncorrectGlobalLocalVar)
		tk.MustGetErrCode("select @@global.warning_count", errno.ErrIncorrectGlobalLocalVar)
		tk.MustGetErrCode("select @@instance.tidb_redact_log", errno.ErrIncorrectGlobalLocalVar)

		// hidden internal system variable
		tk.MustGetErrCode("select @@session.tidb_redact_log", errno.ErrUnknownSystemVariable)
		tk.MustExec("select @@tidb_redact_log")
	})
}
