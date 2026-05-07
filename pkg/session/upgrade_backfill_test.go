// Copyright 2026 PingCAP, Inc.
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

package session

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func TestUpgradeToVer259BackfillsIgnoreInlistPlanDigest(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	ver258 := version258
	seV258 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(ver258))
	require.NoError(t, err)
	RevertVersionAndVariables(t, seV258, ver258)

	// Simulate a cluster upgraded through the old path where the variable existed in code
	// but its row was never backfilled into mysql.global_variables.
	MustExec(t, seV258, fmt.Sprintf(
		"delete from mysql.GLOBAL_VARIABLES where variable_name='%s'",
		vardef.TiDBIgnoreInlistPlanDigest,
	))
	err = txn.Commit(ctx)
	require.NoError(t, err)
	store.SetOption(StoreBootstrappedKey, nil)

	res := MustExecToRecodeSet(t, seV258, fmt.Sprintf(
		"select * from mysql.GLOBAL_VARIABLES where variable_name='%s'",
		vardef.TiDBIgnoreInlistPlanDigest,
	))
	chk := res.NewChunk(nil)
	require.NoError(t, res.Next(ctx, chk))
	require.Equal(t, 0, chk.NumRows())
	require.NoError(t, res.Close())

	dom.Close()
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()

	seCurVer := CreateSessionAndSetID(t, store)
	ver, err := GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	res = MustExecToRecodeSet(t, seCurVer, fmt.Sprintf(
		"select * from mysql.GLOBAL_VARIABLES where variable_name='%s'",
		vardef.TiDBIgnoreInlistPlanDigest,
	))
	chk = res.NewChunk(nil)
	require.NoError(t, res.Next(ctx, chk))
	require.Equal(t, 1, chk.NumRows())
	require.Equal(t, "OFF", chk.GetRow(0).GetString(1))
	require.NoError(t, res.Close())

	res = MustExecToRecodeSet(t, seCurVer, "select @@global.tidb_ignore_inlist_plan_digest")
	chk = res.NewChunk(nil)
	require.NoError(t, res.Next(ctx, chk))
	require.Equal(t, 1, chk.NumRows())
	require.Equal(t, int64(0), chk.GetRow(0).GetInt64(0))
	require.NoError(t, res.Close())
}
