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

package sessiontest

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestNextGenTiKVRegionStatus(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, key idx(a));")
	tk.MustExec("split table t between (0) and (10000) regions 4")
	tk.MustExec("split table t index idx between (0) and (10000) regions 4")

	tableID := tk.MustQuery(`select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 't'`).Rows()[0][0]
	showRegions := uniqueSortedRegionIDs(tk.MustQuery("show table t regions").Rows())
	showIndexRegions := uniqueSortedRegionIDs(tk.MustQuery("show table t index idx regions").Rows())
	tikvRegions := tk.MustQuery(fmt.Sprintf(
		"select region_id from information_schema.tikv_region_status where table_id = %v", tableID)).Rows()
	tikvIndexRegions := tk.MustQuery(fmt.Sprintf(
		"select region_id from information_schema.tikv_region_status where table_id = %v and is_index = 1", tableID)).Rows()
	require.Equal(t, showRegions, uniqueSortedRegionIDs(tikvRegions))
	require.Equal(t, showIndexRegions, uniqueSortedRegionIDs(tikvIndexRegions))
}

func TestNextGenTiKVRegionStatusDoesNotMixOtherKeyspaces(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only runs in nextgen kernel")
	}

	var closeStores []func() error
	// Stores are cached, so close them after domains to avoid blocking routines.
	t.Cleanup(func() {
		for _, closeStore := range closeStores {
			require.NoError(t, closeStore())
		}
	})

	sysStore, sysDom := realtikvtest.CreateMockStoreAndDomainAndSetup(t,
		realtikvtest.WithKeyspaceName(keyspace.System),
		realtikvtest.WithKeepSelfStore(true),
		realtikvtest.WithAllocPort(true),
	)
	closeStores = append(closeStores, sysStore.Close)
	// This test only needs SYSTEM metadata as query input. Stop the unrelated
	// SYSTEM TTL manager before bootstrapping the user keyspace so its internal
	// SQL worker cannot race with bootstrap's global stats hook registration.
	ttlJobManager := sysDom.TTLJobManager()
	require.NotNil(t, ttlJobManager)
	ttlJobManager.Stop()
	require.NoError(t, ttlJobManager.WaitStopped(context.Background(), 10*time.Second))

	userStore := realtikvtest.CreateMockStoreAndSetup(t,
		realtikvtest.WithKeyspaceName("keyspace1"),
		realtikvtest.WithKeepSystemStore(true),
		realtikvtest.WithKeepSelfStore(true),
		realtikvtest.WithAllocPort(true),
	)
	closeStores = append(closeStores, userStore.Close)

	sysTK := testkit.NewTestKit(t, sysStore)
	sysTK.MustExec("create database if not exists sys_region_status")
	sysTK.MustExec("use sys_region_status")
	sysTK.MustExec("drop table if exists t")
	sysTK.MustExec("create table t (a int, key idx(a))")
	sysTK.MustExec("split table t between (0) and (10000) regions 4")
	sysTK.MustExec("split table t index idx between (0) and (10000) regions 4")

	systemRegionIDs := uniqueSortedRegionIDs(sysTK.MustQuery("show table t regions").Rows())
	require.NotEmpty(t, systemRegionIDs)

	userTK := testkit.NewTestKit(t, userStore)
	// Physical regions can overlap keyspace boundaries; reject SYSTEM table
	// metadata leakage instead of treating region IDs as keyspace-owned.
	userTK.MustQuery(fmt.Sprintf(
		"select count(*) from information_schema.tikv_region_status where region_id in (%s) and db_name = 'sys_region_status' and table_name = 't'",
		strings.Join(systemRegionIDs, ","))).Check(testkit.Rows("0"))
}

func TestTableReaderWithSnapshot(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MockGCSavePoint()

	tk.MustExec("use test")
	tk.MustExec("create table t(id int);")
	tk.MustExec("begin")
	tk.MustExec("set @ts := @@tidb_current_ts;")
	tk.MustExec("rollback")
	tk.MustQuery("select sleep(2);")
	tk.MustExec("drop table t;")
	tk.MustExec("begin")
	tk.MustExec("set @@tidb_snapshot=@ts;")
	tk.MustQuery("SELECT TABLE_NAME,TABLE_TYPE,AVG_ROW_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='test' AND (TABLE_TYPE='BASE TABLE')").Check(testkit.Rows("t BASE TABLE 0"))
}

func uniqueSortedRegionIDs(rows [][]any) []string {
	seen := make(map[string]struct{}, len(rows))
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		id := row[0].(string)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
