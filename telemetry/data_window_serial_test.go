// Copyright 2021 PingCAP, Inc.
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

package telemetry_test

import (
	"testing"

	"github.com/pingcap/tidb/telemetry"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestBuiltinFunctionsUsage(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Clear builtin functions usage
	telemetry.GlobalBuiltinFunctionsUsage.Dump()
	usage := telemetry.GlobalBuiltinFunctionsUsage.Dump()
	require.Equal(t, map[string]uint32{}, usage)

	tk.MustExec("create table t (id int)")
	tk.MustQuery("select id + 1 - 2 from t")
	// Should manually invoke `Session.Close()` to report the usage information
	tk.Session().Close()
	usage = telemetry.GlobalBuiltinFunctionsUsage.Dump()
	require.Equal(t, map[string]uint32{"PlusInt": 1, "MinusInt": 1}, usage)
}
