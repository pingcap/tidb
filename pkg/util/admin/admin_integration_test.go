// Copyright 2019 PingCAP, Inc.
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

package admin_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestAdminCheckTableCorrupted(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int, v int, UNIQUE KEY i1(id, v))")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1, 1)")

	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	memBuffer := txn.GetMemBuffer()
	it, err := memBuffer.Iter(nil, nil)
	require.NoError(t, err)
	for it.Valid() {
		if tablecodec.IsRecordKey(it.Key()) && len(it.Value()) > 0 {
			value := make([]byte, len(it.Value()))
			key := make([]byte, len(it.Key()))
			copy(key, it.Key())
			copy(value, it.Value())
			key[len(key)-1] += 1
			memBuffer.Set(key, value)
		}
		err = it.Next()
		require.NoError(t, err)
	}

	tk.MustExec("commit")
	err = tk.ExecToErr("admin check table t")
	require.Error(t, err)
}
