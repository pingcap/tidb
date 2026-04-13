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

package variable

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetTiDBEnableDropTableForceMerge(t *testing.T) {
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	oldValue := EnableDropTableForceMerge.Load()
	EnableDropTableForceMerge.Store(DefTiDBEnableDropTableForceMerge)
	defer EnableDropTableForceMerge.Store(oldValue)

	sysVar := GetSysVar(TiDBEnableDropTableForceMerge)
	require.Equal(t, Off, sysVar.Value)

	require.NoError(t, mock.SetGlobalSysVar(context.Background(), TiDBEnableDropTableForceMerge, On))
	require.True(t, EnableDropTableForceMerge.Load())

	val, err := mock.GetGlobalSysVar(TiDBEnableDropTableForceMerge)
	require.NoError(t, err)
	require.Equal(t, On, val)

	val, err = sysVar.GetGlobalFromHook(context.Background(), vars)
	require.NoError(t, err)
	require.Equal(t, On, val)

	require.NoError(t, mock.SetGlobalSysVar(context.Background(), TiDBEnableDropTableForceMerge, Off))
	require.False(t, EnableDropTableForceMerge.Load())

	val, err = mock.GetGlobalSysVar(TiDBEnableDropTableForceMerge)
	require.NoError(t, err)
	require.Equal(t, Off, val)

	val, err = sysVar.GetGlobalFromHook(context.Background(), vars)
	require.NoError(t, err)
	require.Equal(t, Off, val)
}
