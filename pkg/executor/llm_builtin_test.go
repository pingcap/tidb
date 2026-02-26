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

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/llm"
	"github.com/stretchr/testify/require"
)

func TestLLMBuiltinsUseDefaultModel(t *testing.T) {
	store := testkit.CreateMockStore(t)
	rootTK := testkit.NewTestKit(t, store)
	rootTK.MustExec("use test")
	rootTK.MustExec("set global tidb_enable_llm_inference = on")
	rootTK.MustExec("set global tidb_llm_default_model = 'm1'")

	rootTK.MustExec("create user u1")
	rootTK.MustExec("grant select on test.* to u1")
	rootTK.MustExec("grant LLM_EXECUTE on *.* to u1")

	var gotModel string
	restore := expression.SetLLMCompleteHookForTest(func(model, prompt string, _ llm.CompleteOptions) (string, error) {
		gotModel = model
		return "ok", nil
	})
	t.Cleanup(restore)

	userTK := testkit.NewTestKit(t, store)
	require.NoError(t, userTK.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))
	userTK.MustExec("use test")

	userTK.MustQuery("select llm_complete('hi')").Check(testkit.Rows("ok"))
	require.Equal(t, "m1", gotModel)
}
