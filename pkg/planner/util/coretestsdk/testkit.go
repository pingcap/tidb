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
	"context"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

// SetTiFlashReplica is to set TiFlash replica
func SetTiFlashReplica(t *testing.T, dom *domain.Domain, dbName, tableName string) {
	is := dom.InfoSchema()
	tblInfo, err := is.TableByName(context.Background(), pmodel.NewCIStr(dbName), pmodel.NewCIStr(tableName))
	require.NoError(t, err)
	tblInfo.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
		Count:     1,
		Available: true,
	}
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
