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

package restore_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/stretchr/testify/require"
)

func TestTransferBoolToValue(t *testing.T) {
	require.Equal(t, "ON", restore.TransferBoolToValue(true))
	require.Equal(t, "OFF", restore.TransferBoolToValue(false))
}

func TestGetTableSchema(t *testing.T) {
	m, err := mock.NewCluster()
	require.Nil(t, err)
	defer m.Stop()
	dom := m.Domain

	_, err = restore.GetTableSchema(dom, ast.NewCIStr("test"), ast.NewCIStr("tidb"))
	require.Error(t, err)
	tableInfo, err := restore.GetTableSchema(dom, ast.NewCIStr("mysql"), ast.NewCIStr("tidb"))
	require.NoError(t, err)
	require.Equal(t, ast.NewCIStr("tidb"), tableInfo.Name)
}

func TestAssertUserDBsEmpty(t *testing.T) {
	m, err := mock.NewCluster()
	require.Nil(t, err)
	defer m.Stop()
	dom := m.Domain

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBR)
	se, err := session.CreateSession(dom.Store())
	require.Nil(t, err)

	err = restore.AssertUserDBsEmpty(dom)
	require.Nil(t, err)

	_, err = se.ExecuteInternal(ctx, "CREATE DATABASE d1;")
	require.Nil(t, err)
	err = restore.AssertUserDBsEmpty(dom)
	require.Error(t, err)
	require.Contains(t, err.Error(), "d1.")

	_, err = se.ExecuteInternal(ctx, "CREATE TABLE d1.test(id int);")
	require.Nil(t, err)
	err = restore.AssertUserDBsEmpty(dom)
	require.Error(t, err)
	require.Contains(t, err.Error(), "d1.test")

	_, err = se.ExecuteInternal(ctx, "DROP DATABASE d1;")
	require.Nil(t, err)
	for i := 0; i < 15; i += 1 {
		_, err = se.ExecuteInternal(ctx, fmt.Sprintf("CREATE DATABASE d%d;", i))
		require.Nil(t, err)
	}
	err = restore.AssertUserDBsEmpty(dom)
	require.Error(t, err)
	containCount := 0
	for i := 0; i < 15; i += 1 {
		if strings.Contains(err.Error(), fmt.Sprintf("d%d.", i)) {
			containCount += 1
		}
	}
	require.Equal(t, 10, containCount)

	for i := 0; i < 15; i += 1 {
		_, err = se.ExecuteInternal(ctx, fmt.Sprintf("CREATE TABLE d%d.t1(id int);", i))
		require.Nil(t, err)
	}
	err = restore.AssertUserDBsEmpty(dom)
	require.Error(t, err)
	containCount = 0
	for i := 0; i < 15; i += 1 {
		if strings.Contains(err.Error(), fmt.Sprintf("d%d.t1", i)) {
			containCount += 1
		}
	}
	require.Equal(t, 10, containCount)
}

func TestGetTSWithRetry(t *testing.T) {
	t.Run("PD leader is healthy:", func(t *testing.T) {
		retryTimes := -1000
		pDClient := split.NewFakePDClient(nil, false, &retryTimes)
		_, err := restore.GetTSWithRetry(context.Background(), pDClient)
		require.NoError(t, err)
	})

	t.Run("PD leader failure:", func(t *testing.T) {
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/utils/set-attempt-to-one", "1*return(true)"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/utils/set-attempt-to-one"))
		}()
		retryTimes := -1000
		pDClient := split.NewFakePDClient(nil, true, &retryTimes)
		_, err := restore.GetTSWithRetry(context.Background(), pDClient)
		require.Error(t, err)
	})

	t.Run("PD leader switch successfully", func(t *testing.T) {
		retryTimes := 0
		pDClient := split.NewFakePDClient(nil, true, &retryTimes)
		_, err := restore.GetTSWithRetry(context.Background(), pDClient)
		require.NoError(t, err)
	})
}
