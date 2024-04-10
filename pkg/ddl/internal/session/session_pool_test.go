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

package session_test

import (
	"context"
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestSessionPool(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	resourcePool := pools.NewResourcePool(func() (pools.Resource, error) {
		newTk := testkit.NewTestKit(t, store)
		return newTk.Session(), nil
	}, 4, 4, 0)
	pool := session.NewSessionPool(resourcePool, store)
	sessCtx, err := pool.Get()
	require.NoError(t, err)
	se := session.NewSession(sessCtx)
	err = se.Begin()
	startTS := se.GetSessionVars().TxnCtx.StartTS
	require.NoError(t, err)
	rows, err := se.Execute(context.Background(), "select 2;", "test")
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	require.Equal(t, int64(2), rows[0].GetInt64(0))
	mgr := tk.Session().GetSessionManager()
	tsList := mgr.GetInternalSessionStartTSList()
	var targetTS uint64
	for _, ts := range tsList {
		if ts == startTS {
			targetTS = ts
			break
		}
	}
	require.NotEqual(t, uint64(0), targetTS)
	err = se.Commit()
	pool.Put(sessCtx)
	require.NoError(t, err)
	tsList = mgr.GetInternalSessionStartTSList()
	targetTS = 0
	for _, ts := range tsList {
		if ts == startTS {
			targetTS = ts
			break
		}
	}
	require.Equal(t, uint64(0), targetTS)
}
