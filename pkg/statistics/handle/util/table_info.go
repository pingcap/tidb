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

package util

import (
	"context"
	"sync"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// TableInfoGetter is used to get table meta info.
type TableInfoGetter interface {
	// TableInfoByID returns the table info specified by the physicalID.
	// If the physicalID is corresponding to a partition, return its parent table.
	TableInfoByID(is infoschema.InfoSchema, physicalID int64) (table.Table, bool)
}

// tableInfoGetterImpl is used to get table meta info.
type tableInfoGetterImpl struct {
	// pid2tid is the map from partition ID to table ID.
	pid2tid map[int64]int64
	// schemaVersion is the version of information schema when `pid2tid` is built.
	schemaVersion int64
	mu            sync.RWMutex
}

// NewTableInfoGetter creates a TableInfoGetter.
func NewTableInfoGetter() TableInfoGetter {
	return &tableInfoGetterImpl{pid2tid: map[int64]int64{}}
}

// TableInfoByID returns the table info specified by the physicalID.
// If the physicalID is corresponding to a partition, return its parent table.
func (c *tableInfoGetterImpl) TableInfoByID(is infoschema.InfoSchema, physicalID int64) (table.Table, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if is.SchemaMetaVersion() != c.schemaVersion {
		c.schemaVersion = is.SchemaMetaVersion()
		c.pid2tid = buildPartitionID2TableID(is)
	}
	if id, ok := c.pid2tid[physicalID]; ok {
		return is.TableByID(context.Background(), id)
	}
	return is.TableByID(context.Background(), physicalID)
}

func buildPartitionID2TableID(is infoschema.InfoSchema) map[int64]int64 {
	mapper := make(map[int64]int64)
	rs := is.ListTablesWithSpecialAttribute(infoschema.PartitionAttribute)
	for _, db := range rs {
		for _, tbl := range db.TableInfos {
			pi := tbl.GetPartitionInfo()
			intest.AssertNotNil(pi)
			for _, def := range pi.Definitions {
				mapper[def.ID] = tbl.ID
			}
		}
	}
	return mapper
}
