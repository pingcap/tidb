// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package extension_test

import (
	"testing"

	"github.com/pingcap/tidb/extension"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestBootstrap(t *testing.T) {
	defer func() {
		extension.Reset()
	}()

	extension.Reset()
	require.NoError(t, extension.Register("test1", extension.WithBootstrapSQL("create table test.t1 (a int)")))
	require.NoError(t, extension.Register("test2", extension.WithBootstrap(func(ctx extension.BootstrapContext) error {
		_, err := ctx.ExecuteSQL(ctx, "insert into test.t1 values(1)")
		require.NoError(t, err)

		rows, err := ctx.ExecuteSQL(ctx, "select * from test.t1 where a=1")
		require.NoError(t, err)

		require.Equal(t, 1, len(rows))
		require.Equal(t, int64(1), rows[0].GetInt64(0))
		return nil
	})))
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select * from test.t1").Check(testkit.Rows("1"))
}
