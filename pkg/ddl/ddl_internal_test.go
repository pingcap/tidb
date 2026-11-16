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

package ddl

import (
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
)

func TestCheckPlacementPolicyNotInUseUsesInfoSchema(t *testing.T) {
	policy := &model.PolicyInfo{ID: 1, Name: ast.NewCIStr("p1")}
	table := &model.TableInfo{
		ID:                 1,
		Name:               ast.NewCIStr("t1"),
		State:              model.StatePublic,
		PlacementPolicyRef: &model.PolicyRefInfo{ID: policy.ID, Name: policy.Name},
	}
	infoCache := infoschema.NewCache(nil, 1)
	infoCache.Insert(infoschema.MockInfoSchemaWithSchemaVer([]*model.TableInfo{table}, 1), 0)

	err := checkPlacementPolicyNotInUse(infoCache, policy)
	require.True(t, dbterror.ErrPlacementPolicyInUse.Equal(err), "unexpected error: %v", err)
}
