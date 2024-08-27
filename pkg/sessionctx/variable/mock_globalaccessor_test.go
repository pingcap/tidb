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

package variable

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
)

func TestMockAPI(t *testing.T) {
	defer view.Stop()
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	str, err := mock.GetGlobalSysVar("illegalopt")
	require.Equal(t, "", str)
	require.Error(t, err)

	// invalid option name
	err = mock.SetGlobalSysVar(context.Background(), "illegalopt", "val")
	require.Error(t, err)
	err = mock.SetGlobalSysVarOnly(context.Background(), "illegalopt", "val", true)
	require.Error(t, err)

	// valid option, invalid value
	err = mock.SetGlobalSysVar(context.Background(), DefaultAuthPlugin, "invalidvalue")
	require.Error(t, err)

	// valid option, valid value
	err = mock.SetGlobalSysVar(context.Background(), DefaultAuthPlugin, "mysql_native_password")
	require.NoError(t, err)
	err = mock.SetGlobalSysVarOnly(context.Background(), DefaultAuthPlugin, "mysql_native_password", true)
	require.NoError(t, err)

	// Test GetTiDBTableValue
	str, err = mock.GetTiDBTableValue("tikv_gc_life_time")
	require.NoError(t, err)
	require.Equal(t, "10m0s", str)
}
