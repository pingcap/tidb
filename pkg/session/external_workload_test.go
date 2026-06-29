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

package session

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

type gcv2AbortStore struct {
	kv.Storage
	codec tikv.Codec
}

func (s *gcv2AbortStore) GetCodec() tikv.Codec {
	return s.codec
}

func TestAbortGCV2Gate(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("Starter deploy mode is only available in nextgen kernel")
	}

	originDeployMode := deploymode.Get()
	restoreConfig := config.RestoreFunc()
	t.Cleanup(func() {
		restoreConfig()
		require.NoError(t, deploymode.Set(originDeployMode))
	})

	keyspaceLevelStore := newGCV2AbortStore(t, &keyspacepb.KeyspaceMeta{
		Id:   1,
		Name: "ks",
		Config: map[string]string{
			pd.KeyspaceConfigGCManagementType: pd.KeyspaceConfigGCManagementTypeKeyspaceLevel,
		},
	})
	unifiedGCStore := newGCV2AbortStore(t, &keyspacepb.KeyspaceMeta{
		Id:   2,
		Name: "ks2",
		Config: map[string]string{
			pd.KeyspaceConfigGCManagementType: pd.KeyspaceConfigGCManagementTypeUnified,
		},
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.ExternalWorkload.Enable = true
		conf.ExternalWorkload.Role = config.RoleGCV2Worker
	})

	require.NoError(t, deploymode.Set(deploymode.Premium))
	abortGCV2(keyspaceLevelStore)

	require.NoError(t, deploymode.Set(deploymode.Starter))
	config.UpdateGlobal(func(conf *config.Config) {
		conf.ExternalWorkload.Role = config.RoleMaster
	})
	abortGCV2(keyspaceLevelStore)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.ExternalWorkload.Role = config.RoleGCV2Worker
	})
	abortGCV2(unifiedGCStore)
}

func newGCV2AbortStore(t *testing.T, meta *keyspacepb.KeyspaceMeta) *gcv2AbortStore {
	codec, err := tikv.NewCodecV2(tikv.ModeTxn, meta)
	require.NoError(t, err)
	return &gcv2AbortStore{codec: codec}
}
