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
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type testStatSuiteToVerify struct {
	suite.Suite
}

func TestStatSuite(t *testing.T) {
	suite.Run(t, new(testStatSuiteToVerify))
}

func (s *testStatSuiteToVerify) SetupSuite() {
}

func (s *testStatSuiteToVerify) TearDownSuite() {
}

type testSerialStatSuiteToVerify struct {
	suite.Suite
}

func ExportTestSerialStatSuite(t *testing.T) {
	suite.Run(t, new(testSerialStatSuiteToVerify))
}

func (s *testStatSuiteToVerify) getDDLSchemaVer(d *ddl) int64 {
	m, err := d.Stats(nil)
	require.NoError(s.T(), err)
	v := m[ddlSchemaVersion]
	return v.(int64)
}

func (s *testSerialStatSuiteToVerify) TestDDLStatsInfo() {
	store := createMockStore(s.T())
	defer func() {
		err := store.Close()
		require.NoError(s.T(), err)
	}()

	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	defer func() {
		err := d.Stop()
		require.NoError(s.T(), err)
	}()

	dbInfo, err := testSchemaInfo(d, "test_stat")
	require.NoError(s.T(), err)
	testCreateSchema(s.T(), testNewContext(d), d, dbInfo)
	tblInfo, err := testTableInfo(d, "t", 2)
	require.NoError(s.T(), err)
	ctx := testNewContext(d)
	testCreateTable(s.T(), ctx, d, dbInfo, tblInfo)

	t := testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	// insert t values (1, 1), (2, 2), (3, 3)
	_, err = t.AddRecord(ctx, types.MakeDatums(1, 1))
	require.NoError(s.T(), err)
	_, err = t.AddRecord(ctx, types.MakeDatums(2, 2))
	require.NoError(s.T(), err)
	_, err = t.AddRecord(ctx, types.MakeDatums(3, 3))
	require.NoError(s.T(), err)
	txn, err := ctx.Txn(true)
	require.NoError(s.T(), err)
	err = txn.Commit(context.Background())
	require.NoError(s.T(), err)

	job := buildCreateIdxJob(dbInfo, tblInfo, true, "idx", "c1")

	require.Nil(s.T(), failpoint.Enable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum", `return(true)`))
	defer func() {
		require.Nil(s.T(), failpoint.Disable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum"))
	}()

	done := make(chan error, 1)
	go func() {
		done <- d.doDDLJob(ctx, job)
	}()

	exit := false
	for !exit {
		select {
		case err := <-done:
			require.NoError(s.T(), err)
			exit = true
		case <-TestCheckWorkerNumCh:
			varMap, err := d.Stats(nil)
			require.NoError(s.T(), err)
			require.Equal(s.T(), varMap[ddlJobReorgHandle], "1")
		}
	}
}
