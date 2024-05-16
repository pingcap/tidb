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

package infoschema

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestInfoSchemaAddDel(t *testing.T) {
	is := newInfoSchema()
	is.addSchema(&schemaTables{
		dbInfo: &model.DBInfo{ID: 1, Name: model.NewCIStr("test")},
	})
	require.Contains(t, is.schemaMap, "test")
	require.Contains(t, is.schemaID2Name, int64(1))
	is.delSchema(&model.DBInfo{ID: 1, Name: model.NewCIStr("test")})
	require.Empty(t, is.schemaMap)
	require.Empty(t, is.schemaID2Name)
}
