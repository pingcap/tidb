// Copyright 2022 PingCAP, Inc.
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

package domain

import (
	"testing"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
)

// This file contains utilities for easier testing.

// MockInfoCacheAndLoadInfoSchema only used in unit tests.
func (do *Domain) MockInfoCacheAndLoadInfoSchema(is infoschema.InfoSchema) {
	do.infoCache = infoschema.NewCache(16)
	do.infoCache.Insert(is, 0)
}

// MustGetTableInfo returns the table info. Only used in unit tests.
func (do *Domain) MustGetTableInfo(t *testing.T, dbName, tableName string) *model.TableInfo {
	tbl, err := do.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	require.Nil(t, err)
	return tbl.Meta()
}

// MustGetTableID returns the table ID. Only used in unit tests.
func (do *Domain) MustGetTableID(t *testing.T, dbName, tableName string) int64 {
	ti := do.MustGetTableInfo(t, dbName, tableName)
	return ti.ID
}

// MustGetPartitionAt returns the partition ID. Only used in unit tests.
func (do *Domain) MustGetPartitionAt(t *testing.T, dbName, tableName string, idx int) int64 {
	ti := do.MustGetTableInfo(t, dbName, tableName)
	return ti.Partition.Definitions[idx].ID
}
