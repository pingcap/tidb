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

package importinto_test

import (
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/failpoint"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestChecksumTable(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	mgr := storage.NewTaskManager(util.WithInternalSourceType(ctx, "taskManager"), pool)

	taskMeta := &importinto.TaskMeta{
		Plan: importer.Plan{
			DBName: "db",
			TableInfo: &model.TableInfo{
				Name: model.NewCIStr("tb"),
			},
		},
	}
	// fake result
	localChecksum := verify.MakeKVChecksum(1, 1, 1)
	gtk.MustExec("create database db")
	gtk.MustExec("create table db.tb(id int)")
	gtk.MustExec("insert into db.tb values(1)")
	remoteChecksum, err := importinto.TestChecksumTable(ctx, mgr, taskMeta, logutil.BgLogger())
	require.NoError(t, err)
	require.True(t, remoteChecksum.IsEqual(&localChecksum))
	// again
	remoteChecksum, err = importinto.TestChecksumTable(ctx, mgr, taskMeta, logutil.BgLogger())
	require.NoError(t, err)
	require.True(t, remoteChecksum.IsEqual(&localChecksum))

	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/importinto/errWhenChecksum", `return(true)`)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/importinto/errWhenChecksum")
	}()
	remoteChecksum, err = importinto.TestChecksumTable(ctx, mgr, taskMeta, logutil.BgLogger())
	require.NoError(t, err)
	require.True(t, remoteChecksum.IsEqual(&localChecksum))
}
