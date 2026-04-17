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

package snapclient

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
)

func TestSkipRestoreMaskingPoliciesWhenTargetTableNotExist(t *testing.T) {
	rc := &SnapClient{}
	db := &database{
		ExistingTables: map[string]*model.TableInfo{},
		Name:           pmodel.NewCIStr(mysql.SystemDB),
		TemporaryName:  utils.TemporaryDBName(mysql.SystemDB),
	}
	ti := &model.TableInfo{Name: pmodel.NewCIStr(sysMaskingPolicyTableName)}
	tableFilter, err := filter.Parse([]string{"*.*"})
	require.NoError(t, err)

	err = rc.replaceTemporaryTableToSystable(context.Background(), ti, db, tableFilter)
	require.NoError(t, err)
}
