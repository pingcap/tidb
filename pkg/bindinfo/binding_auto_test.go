// Copyright 2025 PingCAP, Inc.
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

package bindinfo_test

import (
	"fmt"
	"github.com/pingcap/tidb/pkg/bindinfo"

	//"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestShowPlanForSQLBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t (a int, b int, c varchar(10), key(a))`)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where b=1"`).Rows()) == 0)
	tk.MustExec(`create global binding using select a from t where b=1`)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where b=1"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "SELECT a FROM t WHERE b=1"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "SELECT a FROM t WHERE b= 1"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "     SELECT  a FROM test.t WHERE b= 1"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "23109784b802bcef5398dd81d3b1c5b79200c257c101a5b9f90758206f3d09ed"`).Rows()) == 1)

	require.True(t, len(tk.MustQuery(`show plan for "select a from t where b in (1, 2, 3)"`).Rows()) == 0)
	tk.MustExec(`create global binding using select a from t where b in (1, 2, 3)`)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where b in (1, 2, 3)"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where b in (1, 2)"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where b in (1)"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "SELECT a from t WHere b in (1)"`).Rows()) == 1)

	require.True(t, len(tk.MustQuery(`show plan for "select a from t where c = ''"`).Rows()) == 0)
	tk.MustExec(`create global binding using select a from t where c = ''`)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where c = ''"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where c = '123'"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where c = '\"'"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where c = '              '"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for 'select a from t where c = ""'`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for 'select a from t where c = "\'"'`).Rows()) == 1)

	tk.MustExecToErr("show plan for 'xxx'", "")
	tk.MustExecToErr("show plan for 'SELECT A FROM'", "")
}

func TestLLMBasic(t *testing.T) {
	key := os.Getenv("OPENAI_API_KEY")
	resp, err := bindinfo.CallLLM(key, "hello")
	fmt.Println(">>>>>>> ", resp, err)
}
