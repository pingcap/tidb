// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

<<<<<<< HEAD
func TestDDLStatsInfo(t *testing.T) {
	store := createMockStore(t)
	defer func() {
		require.NoError(t, store.Close())
	}()
=======
func getDDLSchemaVer(t *testing.T, d *ddl) int64 {
	m, err := d.Stats(nil)
	require.NoError(t, err)
	v := m[ddlSchemaVersion]
	return v.(int64)
}

func TestDDLStatsInfo(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
>>>>>>> 44bb7409b (ddl: improve test)

	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(t, err)
	defer func() {
<<<<<<< HEAD
		require.NoError(t, d.Stop())
=======
		err := d.Stop()
		require.NoError(t, err)
>>>>>>> 44bb7409b (ddl: improve test)
	}()

	dbInfo, err := testSchemaInfo(d, "test_stat")
	require.NoError(t, err)
	testCreateSchema(t, testNewContext(d), d, dbInfo)
	tblInfo, err := testTableInfo(d, "t", 2)
	require.NoError(t, err)
	ctx := testNewContext(d)
	testCreateTable(t, ctx, d, dbInfo, tblInfo)

<<<<<<< HEAD
	m := testGetTable(t, d, dbInfo.ID, tblInfo.ID)
	// insert t values (1, 1), (2, 2), (3, 3)
	_, err = m.AddRecord(ctx, types.MakeDatums(1, 1))
	require.NoError(t, err)
	_, err = m.AddRecord(ctx, types.MakeDatums(2, 2))
	require.NoError(t, err)
	_, err = m.AddRecord(ctx, types.MakeDatums(3, 3))
=======
	tt := testGetTable(t, d, dbInfo.ID, tblInfo.ID)
	// insert t values (1, 1), (2, 2), (3, 3)
	_, err = tt.AddRecord(ctx, types.MakeDatums(1, 1))
	require.NoError(t, err)
	_, err = tt.AddRecord(ctx, types.MakeDatums(2, 2))
	require.NoError(t, err)
	_, err = tt.AddRecord(ctx, types.MakeDatums(3, 3))
>>>>>>> 44bb7409b (ddl: improve test)
	require.NoError(t, err)
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	job := buildCreateIdxJob(dbInfo, tblInfo, true, "idx", "c1")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum"))
	}()

	done := make(chan error, 1)
	go func() {
		done <- d.doDDLJob(ctx, job)
	}()

	exit := false
	for !exit {
		select {
		case err := <-done:
			require.NoError(t, err)
			exit = true
		case wg := <-TestCheckWorkerNumCh:
			varMap, err := d.Stats(nil)
			wg.Done()
			require.NoError(t, err)
			require.Equal(t, varMap[ddlJobReorgHandle], "1")
		}
	}
}
