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

package importer_test

import (
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
)

func TestVerifyChecksum(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()

	plan := &importer.Plan{
		DBName: "db",
		TableInfo: &model.TableInfo{
			Name: model.NewCIStr("tb"),
		},
		Checksum: config.OpLevelRequired,
	}
	tk.MustExec("create database db")
	tk.MustExec("create table db.tb(id int)")
	tk.MustExec("insert into db.tb values(1)")

	// admin checksum table always return 1, 1, 1 for memory store
	// Checksum = required
	localChecksum := verify.MakeKVChecksum(1, 1, 1)
	err := importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.NoError(t, err)
	localChecksum = verify.MakeKVChecksum(1, 2, 1)
	err = importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.ErrorIs(t, err, common.ErrChecksumMismatch)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum", `3*return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum"))
	}()
	err = importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.ErrorContains(t, err, "occur an error when checksum")
	// remote checksum success after retry
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum", `1*return(true)`))
	localChecksum = verify.MakeKVChecksum(1, 1, 1)
	err = importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.NoError(t, err)

	// checksum = optional
	plan.Checksum = config.OpLevelOptional
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum"))
	localChecksum = verify.MakeKVChecksum(1, 1, 1)
	err = importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.NoError(t, err)
	localChecksum = verify.MakeKVChecksum(1, 2, 1)
	err = importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.NoError(t, err)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum", `3*return(true)`))
	err = importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.NoError(t, err)

	// checksum = off
	plan.Checksum = config.OpLevelOff
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum"))
	localChecksum = verify.MakeKVChecksum(1, 2, 1)
	err = importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.NoError(t, err)
}

func TestGetTargetNodeCpuCnt(t *testing.T) {
	_, tm, ctx := testutil.InitTableTest(t)
	require.False(t, variable.EnableDistTask.Load())

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(16)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu"))
		variable.EnableDistTask.Store(false)
	})
	require.NoError(t, tm.InitMeta(ctx, "tidb1", ""))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(8)"))
	targetNodeCPUCnt, err := importer.GetTargetNodeCPUCnt(ctx, importer.DataSourceTypeQuery, "")
	require.NoError(t, err)
	require.Equal(t, 8, targetNodeCPUCnt)

	// invalid path
	_, err = importer.GetTargetNodeCPUCnt(ctx, importer.DataSourceTypeFile, ":xx")
	require.ErrorIs(t, err, exeerrors.ErrLoadDataInvalidURI)
	// server disk import
	targetNodeCPUCnt, err = importer.GetTargetNodeCPUCnt(ctx, importer.DataSourceTypeFile, "/path/to/xxx.csv")
	require.NoError(t, err)
	require.Equal(t, 8, targetNodeCPUCnt)
	// disttask disabled
	targetNodeCPUCnt, err = importer.GetTargetNodeCPUCnt(ctx, importer.DataSourceTypeFile, "s3://path/to/xxx.csv")
	require.NoError(t, err)
	require.Equal(t, 8, targetNodeCPUCnt)
	// disttask enabled
	variable.EnableDistTask.Store(true)

	targetNodeCPUCnt, err = importer.GetTargetNodeCPUCnt(ctx, importer.DataSourceTypeFile, "s3://path/to/xxx.csv")
	require.NoError(t, err)
	require.Equal(t, 16, targetNodeCPUCnt)
}
