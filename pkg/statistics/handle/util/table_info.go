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

	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// TableInfoGetter is used to get table meta info.
type TableInfoGetter interface {
	// TableInfoByID returns the table info specified by the physicalID.
	// If the physicalID is corresponding to a partition, return its parent table.
	TableInfoByID(is infoschema.InfoSchema, physicalID int64) (table.Table, bool)
<<<<<<< HEAD
	// TableInfoByIDForInitStats returns the table info specified by the physicalID for initializing stats.
	// If the physicalID corresponds to a partition, it returns its parent table.
	// This method is optimized for InfoSchema V1 by caching partition-to-table mappings.
	TableInfoByIDForInitStats(is infoschema.InfoSchema, physicalID int64) (table.Table, bool)
	// TableItemByID returns the schema name and table name specified by the physicalID.
	// This is pure memory operation.
	TableItemByID(is infoschema.InfoSchema, id int64) (infoschema.TableItem, bool)
=======
>>>>>>> 249288e17a4 (statistics: fix stats cache missing slow stats delta dumps (#72812))
}

// tableInfoGetterImpl is used to get table meta info.
type tableInfoGetterImpl struct{}

// NewTableInfoGetter creates a TableInfoGetter.
func NewTableInfoGetter() TableInfoGetter {
	return &tableInfoGetterImpl{}
}

// TableInfoByID returns the table info specified by the physicalID.
// If the physicalID is corresponding to a partition, return its parent table.
func (*tableInfoGetterImpl) TableInfoByID(is infoschema.InfoSchema, physicalID int64) (table.Table, bool) {
	tbl, ok := is.TableByID(context.Background(), physicalID)
	if ok {
		return tbl, true
	}
	tbl, _, _ = is.FindTableByPartitionID(physicalID)
	return tbl, tbl != nil
}

<<<<<<< HEAD
// TableInfoByIDForInitStats returns the table info specified by the physicalID for initializing stats.
func (c *tableInfoGetterImpl) TableInfoByIDForInitStats(is infoschema.InfoSchema, physicalID int64) (table.Table, bool) {
	// NOTE: For InfoSchema V2, partition lookups are efficient,
	// We can directly search for partitioned tables without maintaining a separate cache, making TableByID sufficient for both
	// regular tables and partitions.
	isV2, _ := infoschema.IsV2(is)
	if isV2 {
		return c.TableInfoByID(is, physicalID)
	}

	// For InfoSchema V1, partition lookups require a full scan of all tables, which is
	// expensive. We maintain a cached pid2tid map to optimize these lookups during stats
	// initialization. Locking is needed because this method may be called in parallel.
	// See FindTableByPartitionID implementation for more details on V1's search cost.
	c.forInitStatsAndInfoSchemaV1Only.mu.Lock()
	defer c.forInitStatsAndInfoSchemaV1Only.mu.Unlock()
	if is.SchemaMetaVersion() != c.forInitStatsAndInfoSchemaV1Only.schemaVersion {
		c.forInitStatsAndInfoSchemaV1Only.schemaVersion = is.SchemaMetaVersion()
		c.forInitStatsAndInfoSchemaV1Only.pid2tid = buildPartitionID2TableID(is)
	}
	if id, ok := c.forInitStatsAndInfoSchemaV1Only.pid2tid[physicalID]; ok {
		return is.TableByID(context.Background(), id)
	}
	return is.TableByID(context.Background(), physicalID)
}

=======
>>>>>>> 249288e17a4 (statistics: fix stats cache missing slow stats delta dumps (#72812))
// buildPartitionID2TableID builds the map from partition ID to table ID.
func buildPartitionID2TableID(is infoschema.InfoSchema) map[int64]int64 {
	mapper := make(map[int64]int64)
	rs := is.ListTablesWithSpecialAttribute(infoschemacontext.PartitionAttribute)
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

// SnapshotTableInfoGetter gets the table of a physical ID in one InfoSchema snapshot.
// On InfoSchema V1, each partition lookup scans all tables, so it builds a partition map
// when it is created. It is safe for concurrent use. Create one for each snapshot instead of
// sharing a cache, because callers on different schema versions would rebuild a shared map.
type SnapshotTableInfoGetter struct {
	is      infoschema.InfoSchema
	pid2tid map[int64]int64
	isV2    bool
}

// NewSnapshotTableInfoGetter creates a SnapshotTableInfoGetter for the InfoSchema snapshot.
func NewSnapshotTableInfoGetter(is infoschema.InfoSchema) *SnapshotTableInfoGetter {
	// Session contexts wrap the InfoSchema, and IsV2 does not see through the wrapper.
	// Without the unwrap, V2 would build the partition map instead of using its own index.
	raw := is
	if ext, ok := is.(*infoschema.SessionExtendedInfoSchema); ok {
		raw = ext.InfoSchema
	}
	isV2, _ := infoschema.IsV2(raw)
	g := &SnapshotTableInfoGetter{is: is, isV2: isV2}
	if !isV2 {
		g.pid2tid = buildPartitionID2TableID(is)
	}
	return g
}

// ParentTableID returns the parent table ID if the physicalID is a partition.
func (g *SnapshotTableInfoGetter) ParentTableID(physicalID int64) (tableID int64, isPartition bool) {
	if g.isV2 {
		return g.is.TableIDByPartitionID(physicalID)
	}
	tableID, isPartition = g.pid2tid[physicalID]
	return tableID, isPartition
}

// TableInfoByID returns the table of the physicalID.
// If the physicalID is a partition, it returns the parent table.
func (g *SnapshotTableInfoGetter) TableInfoByID(physicalID int64) (table.Table, bool) {
	if tbl, ok := g.is.TableByID(context.Background(), physicalID); ok {
		return tbl, true
	}
	if tableID, ok := g.ParentTableID(physicalID); ok {
		return g.is.TableByID(context.Background(), tableID)
	}
	return nil, false
}

// TableItemByID returns the table item of the physicalID.
// If the physicalID is a partition, it returns the item of the parent table.
func (g *SnapshotTableInfoGetter) TableItemByID(physicalID int64) (infoschema.TableItem, bool) {
	if item, ok := g.is.TableItemByID(physicalID); ok {
		return item, true
	}
	if tableID, ok := g.ParentTableID(physicalID); ok {
		return g.is.TableItemByID(tableID)
	}
	return infoschema.TableItem{}, false
}
