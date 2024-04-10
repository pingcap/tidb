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

package multivaluedindex

import (
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCreateMultiValuedIndexHasBinaryCollation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create table test.t (pk varchar(4) primary key clustered, j json, str varchar(255), value int, key idx((cast(j as char(100) array)), str));")
	is := tk.Session().GetDomainInfoSchema().(infoschema.InfoSchema)
	require.NotNil(t, is)

	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	foundIndex := false
	for _, c := range tbl.Cols() {
		if c.Hidden {
			foundIndex = true
			require.True(t, c.FieldType.IsArray())
			require.Equal(t, c.FieldType.GetCharset(), "binary")
			require.Equal(t, c.FieldType.GetCollate(), "binary")
		}
	}
	require.True(t, foundIndex)
}
