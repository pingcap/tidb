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

package backup

import (
	"testing"

	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestIsMaskingPolicySystemTableSkipsOnlySystemCopy(t *testing.T) {
	policy := &model.TableInfo{Name: pmodel.NewCIStr(sysMaskingPolicyTableName)}
	staged := &schemaInfo{
		dbInfo:    &model.DBInfo{Name: utils.TemporaryDBName("mysql")},
		tableInfo: policy,
	}
	require.True(t, isMaskingPolicySystemTable(staged))

	user := &schemaInfo{
		dbInfo:    &model.DBInfo{Name: pmodel.NewCIStr("br_stats1")},
		tableInfo: policy,
	}
	require.False(t, isMaskingPolicySystemTable(user))

	userTable := &schemaInfo{
		dbInfo:    staged.dbInfo,
		tableInfo: &model.TableInfo{Name: pmodel.NewCIStr("usertable")},
	}
	require.False(t, isMaskingPolicySystemTable(userTable))
	require.False(t, isMaskingPolicySystemTable(nil))
}
