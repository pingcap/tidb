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

package variable_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func TestVardefCompatibilityAliases(t *testing.T) {
	var scope variable.ScopeFlag = variable.ScopeGlobal | variable.ScopeSession
	require.Equal(t, vardef.ScopeGlobal|vardef.ScopeSession, scope)
	require.Equal(t, vardef.ScopeNone, variable.ScopeNone)
	require.Equal(t, vardef.ScopeInstance, variable.ScopeInstance)

	var typ variable.TypeFlag = variable.TypeBool
	require.Equal(t, vardef.TypeBool, typ)
	require.Equal(t, vardef.TypeStr, variable.TypeStr)
	require.Equal(t, vardef.TypeInt, variable.TypeInt)
	require.Equal(t, vardef.TypeEnum, variable.TypeEnum)
	require.Equal(t, vardef.TypeFloat, variable.TypeFloat)
	require.Equal(t, vardef.TypeUnsigned, variable.TypeUnsigned)
	require.Equal(t, vardef.TypeTime, variable.TypeTime)
	require.Equal(t, vardef.TypeDuration, variable.TypeDuration)
}
