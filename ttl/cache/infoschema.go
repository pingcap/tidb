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

package cache

import (
	"time"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// InfoSchemaCache is the cache for InfoSchema, it builds a map from physical table id to physical table information
type InfoSchemaCache struct {
	baseCache

	schemaVer int64
	Tables    map[int64]*PhysicalTable
}

// NewInfoSchemaCache creates the cache for info schema
func NewInfoSchemaCache(updateInterval time.Duration) *InfoSchemaCache {
	return &InfoSchemaCache{
		baseCache: newBaseCache(updateInterval),
	}
}

// Update updates the info schema cache
func (isc *InfoSchemaCache) Update(se session.Session) error {
	is := se.SessionInfoSchema()

	if isc.schemaVer == is.SchemaMetaVersion() {
		return nil
	}

	newTables := make(map[int64]*PhysicalTable, len(isc.Tables))
	for _, db := range is.AllSchemas() {
		for _, tbl := range is.SchemaTables(db.Name) {
			tblInfo := tbl.Meta()
			if tblInfo.TTLInfo == nil || !tblInfo.TTLInfo.Enable || tblInfo.State != model.StatePublic {
				continue
			}

			logger := logutil.BgLogger().With(zap.String("schema", db.Name.L), zap.Int64("tableID", tblInfo.ID), zap.String("tableName", tblInfo.Name.L))

			if tblInfo.Partition == nil {
				ttlTable, err := isc.newTable(db.Name, tblInfo, nil)
				if err != nil {
					logger.Warn("fail to build info schema cache", zap.Error(err))
					continue
				}
				newTables[tblInfo.ID] = ttlTable
				continue
			}

			for _, par := range tblInfo.Partition.Definitions {
				par := par
				ttlTable, err := isc.newTable(db.Name, tblInfo, &par)
				if err != nil {
					logger.Warn("fail to build info schema cache", zap.Int64("partitionID", par.ID), zap.String("partition", par.Name.L), zap.Error(err))
					continue
				}
				newTables[par.ID] = ttlTable
			}
		}
	}

	isc.schemaVer = is.SchemaMetaVersion()
	isc.Tables = newTables
	isc.updateTime = time.Now()
	return nil
}

func (isc *InfoSchemaCache) newTable(schema model.CIStr, tblInfo *model.TableInfo, par *model.PartitionDefinition) (*PhysicalTable, error) {
	id := tblInfo.ID
	if par != nil {
		id = par.ID
	}

	if isc.Tables != nil {
		ttlTable, ok := isc.Tables[id]
		if ok && ttlTable.TableInfo == tblInfo {
			return ttlTable, nil
		}
	}

	partitionName := model.NewCIStr("")
	if par != nil {
		partitionName = par.Name
	}
	return NewPhysicalTable(schema, tblInfo, partitionName)
}
