// Copyright 2023 PingCAP, Inc.
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

package testkit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMultiStatementInTk tests whether statement context will leak with multi-statements in testkit. See #47365
func TestMultiStatementInTk(t *testing.T) {
	store := CreateMockStore(t)
	tk := NewTestKit(t, store)
	tk.MustExec("use test")
	require.Len(t, tk.Session().GetSessionVars().MemTracker.GetChildrenForTest(), 0)
	for i := 0; i < 100; i++ {
		// should return the first result set
		tk.MustQuery("select 1;select 2;").Check(Rows("1"))
		require.Len(t, tk.Session().GetSessionVars().MemTracker.GetChildrenForTest(), 0)
	}
}
