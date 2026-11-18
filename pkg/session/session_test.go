// Copyright 2024 PingCAP, Inc.
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

package session

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestGetStartMode(t *testing.T) {
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion))
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion+1))
	require.Equal(t, ddl.Upgrade, getStartMode(currentBootstrapVersion-1))
	require.Equal(t, ddl.Bootstrap, getStartMode(0))
}

func TestMDLInitializationPolicies(t *testing.T) {
	original := variable.EnableMDL.Load()
	t.Cleanup(func() { variable.EnableMDL.Store(original) })
	for _, initializer := range []struct {
		name   string
		policy mdlInitPolicy
	}{
		{"normal", mdlInitEnableOnNull},
		{"bootstrap", mdlInitForceEnable},
		{"upgrade", mdlInitKeepDisabledOnNull},
		{"BR", mdlInitKeepDisabledOnNull},
	} {
		for _, state := range []string{"missing", "enabled", "disabled"} {
			t.Run(initializer.name+"/"+state, func(t *testing.T) {
				store, err := mockstore.NewMockStore()
				require.NoError(t, err)
				defer func() { require.NoError(t, store.Close()) }()
				if state != "missing" {
					txn, err := store.Begin()
					require.NoError(t, err)
					require.NoError(t, meta.NewMutator(txn).SetMetadataLock(state == "enabled"))
					require.NoError(t, txn.Commit(context.Background()))
				}
				expectedEnabled := state == "enabled" || initializer.name == "bootstrap" ||
					(state == "missing" && initializer.name == "normal")
				variable.EnableMDL.Store(!expectedEnabled)
				_, err = initMDLVariable(store, initializer.policy)
				require.NoError(t, err)
				require.Equal(t, expectedEnabled, variable.EnableMDL.Load())
				txn, err := store.Begin()
				require.NoError(t, err)
				enabled, missing, err := meta.NewMutator(txn).GetMetadataLock()
				require.NoError(t, err)
				require.NoError(t, txn.Rollback())
				require.Equal(t, expectedEnabled, enabled)
				require.Equal(t, state == "missing" && !expectedEnabled, missing)
			})
		}
	}
}
