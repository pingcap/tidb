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
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
)

func TestChecksumTable(t *testing.T) {
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
	}
	// fake result
	localChecksum := verify.MakeKVChecksum(1, 1, 1)
	tk.MustExec("create database db")
	tk.MustExec("create table db.tb(id int)")
	tk.MustExec("insert into db.tb values(1)")
	remoteChecksum, err := importer.ChecksumTable(ctx, tk.Session(), plan, logutil.BgLogger())
	require.NoError(t, err)
	require.True(t, remoteChecksum.IsEqual(&localChecksum))
	// again
	remoteChecksum, err = importer.ChecksumTable(ctx, tk.Session(), plan, logutil.BgLogger())
	require.NoError(t, err)
	require.True(t, remoteChecksum.IsEqual(&localChecksum))

	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum", `return(true)`)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum")
	}()
	remoteChecksum, err = importer.ChecksumTable(ctx, tk.Session(), plan, logutil.BgLogger())
	require.NoError(t, err)
	require.True(t, remoteChecksum.IsEqual(&localChecksum))
}
