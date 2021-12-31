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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMockAPI(t *testing.T) {
	vars := NewSessionVars()
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	str, err := mock.GetGlobalSysVar("illegalopt")
	require.Equal(t, "", str)
	require.Error(t, err)

	// invalid option name
	err = mock.SetGlobalSysVar("illegalopt", "val")
	require.Error(t, err)
	err = mock.SetGlobalSysVarOnly("illegalopt", "val")
	require.Error(t, err)

	// valid option, invalid value
	err = mock.SetGlobalSysVar(DefaultAuthPlugin, "invalidvalue")
	require.Error(t, err)

	// valid option, valid value
	err = mock.SetGlobalSysVar(DefaultAuthPlugin, "mysql_native_password")
	require.NoError(t, err)
	err = mock.SetGlobalSysVarOnly(DefaultAuthPlugin, "mysql_native_password")
	require.NoError(t, err)

}
