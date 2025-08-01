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

package staticrecordset_test

import (
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestDDLInsideTXNNotBlockMinStartTS(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(id int)")
	tk.MustExec("insert into t values (1), (2), (3)")

	ch := make(chan struct{})
	ddl.MockDMLExecution = func() {
		<-ch
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockDMLExecution", "1*return(true)->return(false)"))

	tk2.MustExec("begin")
	ddlTs := tk2.Session().GetSessionVars().TxnCtx.StartTS
	go func() {
		tk2.Exec("alter table test.t add index idx(id)")
	}()

	tk.Exec("begin")
	tk.MustExec("insert into t values (1), (2), (3)")
	tkTs := tk.Session().GetSessionVars().TxnCtx.StartTS
	require.Greater(t, tkTs, ddlTs)

	infoSyncer := dom.InfoSyncer()
	require.Eventually(t, func() bool {
		infoSyncer.ReportMinStartTS(store)
		return infoSyncer.GetMinStartTS() == tkTs
	}, time.Second*5, time.Millisecond*100)
	close(ch)
}
