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

package session

import (
	"context"
	"runtime"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
	atomicutil "go.uber.org/atomic"
)

var (
	// GetBootstrapVersion is used in test
	GetBootstrapVersion = getBootstrapVersion
	// CurrentBootstrapVersion is used in test
	CurrentBootstrapVersion = currentBootstrapVersion
	// UnsetStoreBootstrapped is used in test
	UnsetStoreBootstrapped = unsetStoreBootstrapped
)

// CreateStoreAndBootstrap creates a mock store and bootstrap it.
func CreateStoreAndBootstrap(t *testing.T) (kv.Storage, *domain.Domain) {
	runtime.GOMAXPROCS(mathutil.Min(8, runtime.GOMAXPROCS(0)))
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	return store, dom
}

var sessionKitIDGenerator atomicutil.Uint64

// CreateSessionAndSetID creates a session and set connection ID.
func CreateSessionAndSetID(t *testing.T, store kv.Storage) Session {
	se, err := CreateSession4Test(store)
	se.SetConnectionID(sessionKitIDGenerator.Inc())
	require.NoError(t, err)
	return se
}

// MustExec executes a sql statement and asserts no error occurs.
func MustExec(t *testing.T, se Session, sql string, args ...interface{}) {
	rs, err := exec(se, sql, args...)
	require.NoError(t, err)
	if rs != nil {
		require.NoError(t, rs.Close())
	}
}

// MustExecToRecodeSet executes a sql statement and asserts no error occurs.
func MustExecToRecodeSet(t *testing.T, se Session, sql string, args ...interface{}) sqlexec.RecordSet {
	rs, err := exec(se, sql, args...)
	require.NoError(t, err)
	return rs
}

func exec(se Session, sql string, args ...interface{}) (sqlexec.RecordSet, error) {
	ctx := context.Background()
	if len(args) == 0 {
		rs, err := se.Execute(ctx, sql)
		if err == nil && len(rs) > 0 {
			return rs[0], nil
		}
		return nil, err
	}
	stmtID, _, _, err := se.PrepareStmt(sql)
	if err != nil {
		return nil, err
	}
	params := expression.Args2Expressions4Test(args...)
	rs, err := se.ExecutePreparedStmt(ctx, stmtID, params)
	if err != nil {
		return nil, err
	}
	return rs, nil
}
