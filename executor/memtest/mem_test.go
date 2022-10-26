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

package memtest

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestInsertUpdateTrackerOnCleanUp(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")

	originConsume := tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed()
	// assert insert
	tk.MustExec("insert t (id) values (1)")
	tk.MustExec("insert t (id) values (2)")
	tk.MustExec("insert t (id) values (3)")
	afterConsume := tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed()
	require.Equal(t, afterConsume, originConsume)

	originConsume = tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed()
	// assert update
	tk.MustExec("update t set id = 4 where id = 1")
	tk.MustExec("update t set id = 5 where id = 2")
	tk.MustExec("update t set id = 6 where id = 3")
	afterConsume = tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed()
	require.Equal(t, afterConsume, originConsume)
}
