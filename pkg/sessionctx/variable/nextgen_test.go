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

//go:build nextgen

package variable

import (
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func TestTiDBPessimisticTransactionFairLocking(t *testing.T) {
	sv := GetSysVar(vardef.TiDBPessimisticTransactionFairLocking)
	vars := NewSessionVars(nil)

	val, err := sv.Validate(vars, "off", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, vardef.Off, val)
	require.Nil(t, sv.SetSessionFromHook(vars, val))
	require.False(t, vars.PessimisticTransactionFairLocking)

	val, err = sv.Validate(vars, "on", vardef.ScopeSession)
	require.Error(t, err)
	require.True(t, ErrNotSupportedInNextGen.Equal(err))
	require.Equal(t, vardef.Off, val)
	require.Nil(t, sv.SetSessionFromHook(vars, val))
	require.False(t, vars.PessimisticTransactionFairLocking)

	val = GlobalSystemVariableInitialValue(vardef.TiDBPessimisticTransactionFairLocking, BoolToOnOff(vardef.DefTiDBPessimisticTransactionFairLocking))
	require.Equal(t, vardef.Off, val)
}

func TestTiDBDMLTypeInNextGen(t *testing.T) {
	sv := GetSysVar(vardef.TiDBDMLType)
	vars := NewSessionVars(nil)

	val, err := sv.Validate(vars, "standard", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "standard", val)
	require.Nil(t, sv.SetSessionFromHook(vars, val))
	require.False(t, vars.BulkDMLEnabled)

	val, err = sv.Validate(vars, "bulk", vardef.ScopeSession)
	require.Error(t, err)
	require.True(t, ErrNotSupportedInNextGen.Equal(err))
	require.Equal(t, vardef.DefTiDBDMLType, val)
	require.Nil(t, sv.SetSessionFromHook(vars, val))
	require.False(t, vars.BulkDMLEnabled)
}

func TestTiDBReplicaReadInNextGen(t *testing.T) {
	sv := GetSysVar(vardef.TiDBReplicaRead)
	vars := NewSessionVars(nil)

	val, err := sv.Validate(vars, "leader", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "leader", val)
	require.Nil(t, sv.SetSessionFromHook(vars, val))
	require.Equal(t, kv.ReplicaReadLeader, vars.GetReplicaRead())

	for _, replicaRead := range []string{"follower", "prefer-leader", "leader-and-follower", "closest-replicas", "closest-adaptive", "learner"} {
		val, err = sv.Validate(vars, replicaRead, vardef.ScopeSession)
		require.Error(t, err, replicaRead)
		require.True(t, ErrNotSupportedInNextGen.Equal(err), replicaRead)
		require.Equal(t, "leader", val, replicaRead)
		require.Nil(t, sv.SetSessionFromHook(vars, val))
		require.Equal(t, kv.ReplicaReadLeader, vars.GetReplicaRead(), replicaRead)
	}
}
