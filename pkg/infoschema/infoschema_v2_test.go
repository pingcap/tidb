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

	"github.com/pingcap/tidb/pkg/infoschema/internal"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

func TestV2Basic(t *testing.T) {
	r := internal.CreateAutoIDRequirement(t)
	defer func() {
		r.Store().Close()
	}()
	is := NewInfoSchemaV2(r, NewData())

	dbInfo := internal.MockDBInfo(t, r.Store(), "testDB")
	is.Data.addDB(1, dbInfo)
	internal.AddDB(t, r.Store(), dbInfo)
	tblInfo := internal.MockTableInfo(t, r.Store(), "test")
	is.Data.add(tableItem{"testDB", dbInfo.ID, "test", tblInfo.ID, 2}, internal.MockTable(t, r.Store(), tblInfo))
	internal.AddTable(t, r.Store(), dbInfo, tblInfo)
	require.Equal(t, 1, len(is.AllSchemas()))
	require.Equal(t, 0, len(is.SchemaTables(is.AllSchemas()[0].Name)))
	ver, err := r.Store().CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	is.schemaVersion = 2
	is.ts = ver.Ver
	require.Equal(t, 1, len(is.SchemaTables(is.AllSchemas()[0].Name)))
}
