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

package ddl_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestGenIDAndInsertJobsWithRetry(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	// disable DDL to avoid it interfere the test
	tk := testkit.NewTestKit(t, store)
	dom := domain.GetDomain(tk.Session())
	dom.DDL().OwnerManager().CampaignCancel()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)

	jobs := []*model.Job{
		{
			Type:       model.ActionCreateTable,
			SchemaName: "test",
			TableName:  "t1",
		},
	}
	barrier := make(chan struct{})
	var wg util.WaitGroupWrapper
	for i := 0; i < 10; i++ {
		wg.Run(func() {
			<-barrier
			kit := testkit.NewTestKit(t, store)
			for i := 0; i < 1000; i++ {
				require.NoError(t, ddl.GenIDAndInsertJobsWithRetry(ctx, kit.Session(), jobs))
			}
		})
	}
	close(barrier)
	wg.Wait()

	jobs, err := ddl.GetAllDDLJobs(tk.Session())
	require.NoError(t, err)
	require.Len(t, jobs, 10000)
}
