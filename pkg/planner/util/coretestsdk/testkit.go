// Copyright 2023 PingCAP, Inc.
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

package coretestsdk

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

// SetTiFlashReplica is to set TiFlash replica
func SetTiFlashReplica(t *testing.T, dom *domain.Domain, dbName, tableName string) {
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr(dbName))
	require.True(t, exists)
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == tableName {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
}

// WithMockTiFlash sets the mockStore to have N TiFlash stores (naming as tiflash0, tiflash1, ...).
func WithMockTiFlash(nodes int) mockstore.MockTiKVStoreOption {
	return mockstore.WithMultipleOptions(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockCluster := c.(*unistore.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			tiflashIdx := 0
			for tiflashIdx < nodes {
				store2 := c.AllocID()
				peer2 := c.AllocID()
				addr2 := fmt.Sprintf("tiflash%d", tiflashIdx)
				mockCluster.AddStore(store2, addr2, &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
				mockCluster.AddPeer(region1, store2, peer2)
				tiflashIdx++
			}
		}),
		mockstore.WithStoreType(mockstore.EmbedUnistore),
	)
}

// GetFieldValue is to get field value.
func GetFieldValue(prefix, row string) string {
	if idx := strings.Index(row, prefix); idx > 0 {
		start := idx + len(prefix)
		end := strings.Index(row[start:], " ")
		if end > 0 {
			value := row[start : start+end]
			value = strings.Trim(value, ",")
			return value
		}
	}
	return ""
}
