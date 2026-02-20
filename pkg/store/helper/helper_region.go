// Copyright 2019 PingCAP, Inc.
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

package helper

import (
	"bufio"
	"cmp"
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

// TableInfoWithKeyRange stores table or index information with its key range.
type TableInfoWithKeyRange struct {
	*TableInfo
	StartKey string
	EndKey   string
}

// GetStartKey implements `withKeyRange` interface.
func (t TableInfoWithKeyRange) GetStartKey() string { return t.StartKey }

// GetEndKey implements `withKeyRange` interface.
func (t TableInfoWithKeyRange) GetEndKey() string { return t.EndKey }

// NewTableWithKeyRange constructs TableInfoWithKeyRange for given table, it is exported only for test.
func NewTableWithKeyRange(db *model.DBInfo, table *model.TableInfo) TableInfoWithKeyRange {
	return newTableInfoWithKeyRange(db, table, nil, nil)
}

// NewIndexWithKeyRange constructs TableInfoWithKeyRange for given index, it is exported only for test.
func NewIndexWithKeyRange(db *model.DBInfo, table *model.TableInfo, index *model.IndexInfo) TableInfoWithKeyRange {
	return newTableInfoWithKeyRange(db, table, nil, index)
}

// FilterMemDBs filters memory databases in the input schemas.
func (*Helper) FilterMemDBs(oldSchemas []*model.DBInfo) (schemas []*model.DBInfo) {
	for _, dbInfo := range oldSchemas {
		if metadef.IsMemDB(dbInfo.Name.L) {
			continue
		}
		schemas = append(schemas, dbInfo)
	}
	return
}

// GetRegionsTableInfo returns a map maps region id to its tables or indices.
// Assuming tables or indices key ranges never intersect.
// Regions key ranges can intersect.
func (h *Helper) GetRegionsTableInfo(regionsInfo *pd.RegionsInfo, is infoschema.SchemaAndTable, filter func([]*model.DBInfo) []*model.DBInfo) map[int64][]TableInfo {
	tables := h.GetTablesInfoWithKeyRange(is, filter)

	regions := make([]*pd.RegionInfo, 0, len(regionsInfo.Regions))
	for i := range regionsInfo.Regions {
		regions = append(regions, &regionsInfo.Regions[i])
	}

	tableInfos := h.ParseRegionsTableInfos(regions, tables)
	return tableInfos
}

func newTableInfoWithKeyRange(db *model.DBInfo, table *model.TableInfo, partition *model.PartitionDefinition, index *model.IndexInfo) TableInfoWithKeyRange {
	var sk, ek []byte
	if partition == nil && index == nil {
		sk, ek = tablecodec.GetTableHandleKeyRange(table.ID)
	} else if partition != nil && index == nil {
		sk, ek = tablecodec.GetTableHandleKeyRange(partition.ID)
	} else if partition == nil && index != nil {
		sk, ek = tablecodec.GetTableIndexKeyRange(table.ID, index.ID)
	} else {
		sk, ek = tablecodec.GetTableIndexKeyRange(partition.ID, index.ID)
	}
	startKey := bytesKeyToHex(codec.EncodeBytes(nil, sk))
	endKey := bytesKeyToHex(codec.EncodeBytes(nil, ek))
	return TableInfoWithKeyRange{
		&TableInfo{
			DB:          db,
			Table:       table,
			IsPartition: partition != nil,
			Partition:   partition,
			IsIndex:     index != nil,
			Index:       index,
		},
		startKey,
		endKey,
	}
}

// GetTablesInfoWithKeyRange returns a slice containing tableInfos with key ranges of all tables in schemas.
func (*Helper) GetTablesInfoWithKeyRange(is infoschema.SchemaAndTable, filter func([]*model.DBInfo) []*model.DBInfo) []TableInfoWithKeyRange {
	tables := []TableInfoWithKeyRange{}
	dbInfos := is.AllSchemas()
	if filter != nil {
		dbInfos = filter(dbInfos)
	}
	for _, db := range dbInfos {
		tableInfos, _ := is.SchemaTableInfos(context.Background(), db.Name)
		for _, table := range tableInfos {
			if table.Partition != nil {
				for i := range table.Partition.Definitions {
					tables = append(tables, newTableInfoWithKeyRange(db, table, &table.Partition.Definitions[i], nil))
				}
			} else {
				tables = append(tables, newTableInfoWithKeyRange(db, table, nil, nil))
			}
			for _, index := range table.Indices {
				if table.Partition == nil || index.Global {
					tables = append(tables, newTableInfoWithKeyRange(db, table, nil, index))
					continue
				}
				for i := range table.Partition.Definitions {
					tables = append(tables, newTableInfoWithKeyRange(db, table, &table.Partition.Definitions[i], index))
				}
			}
		}
	}
	slices.SortFunc(tables, func(i, j TableInfoWithKeyRange) int {
		return cmp.Compare(i.StartKey, j.StartKey)
	})
	return tables
}

// ParseRegionsTableInfos parses the tables or indices in regions according to key range.
func (*Helper) ParseRegionsTableInfos(regionsInfo []*pd.RegionInfo, tables []TableInfoWithKeyRange) map[int64][]TableInfo {
	tableInfos := make(map[int64][]TableInfo, len(regionsInfo))

	if len(tables) == 0 || len(regionsInfo) == 0 {
		return tableInfos
	}
	// tables is sorted in GetTablesInfoWithKeyRange func
	slices.SortFunc(regionsInfo, func(i, j *pd.RegionInfo) int {
		return cmp.Compare(i.StartKey, j.StartKey)
	})

	idx := 0
OutLoop:
	for _, region := range regionsInfo {
		id := region.ID
		tableInfos[id] = []TableInfo{}
		for isBehind(region, &tables[idx]) {
			idx++
			if idx >= len(tables) {
				break OutLoop
			}
		}
		for i := idx; i < len(tables) && isIntersecting(region, &tables[i]); i++ {
			tableInfos[id] = append(tableInfos[id], *tables[i].TableInfo)
		}
	}

	return tableInfos
}

func bytesKeyToHex(key []byte) string {
	return strings.ToUpper(hex.EncodeToString(key))
}

// GetPDAddr return the PD Address.
func (h *Helper) GetPDAddr() ([]string, error) {
	etcd, ok := h.Store.(kv.EtcdBackend)
	if !ok {
		return nil, errors.New("not implemented")
	}
	pdAddrs, err := etcd.EtcdAddrs()
	if err != nil {
		return nil, err
	}
	if len(pdAddrs) == 0 {
		return nil, errors.New("pd unavailable")
	}
	return pdAddrs, nil
}

// GetPDRegionStats get the RegionStats by tableID from PD by HTTP API.
func (h *Helper) GetPDRegionStats(ctx context.Context, tableID int64, noIndexStats bool) (*pd.RegionStats, error) {
	pdCli, err := h.TryGetPDHTTPClient()
	if err != nil {
		return nil, err
	}

	var startKey, endKey []byte
	if noIndexStats {
		startKey = tablecodec.GenTableRecordPrefix(tableID)
		endKey = kv.Key(startKey).PrefixNext()
	} else {
		startKey = tablecodec.EncodeTablePrefix(tableID)
		endKey = kv.Key(startKey).PrefixNext()
	}
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)

	return pdCli.GetRegionStatusByKeyRange(ctx, pd.NewKeyRange(startKey, endKey), false)
}

// GetTiFlashTableIDFromEndKey computes tableID from pd rule's endKey.
func GetTiFlashTableIDFromEndKey(endKey string) int64 {
	e, _ := hex.DecodeString(endKey)
	_, decodedEndKey, _ := codec.DecodeBytes(e, []byte{})
	tableID := tablecodec.DecodeTableID(decodedEndKey)
	tableID--
	return tableID
}

// ComputeTiFlashStatus is helper function for CollectTiFlashStatus.
func ComputeTiFlashStatus(reader *bufio.Reader, regionReplica *map[int64]int) error {
	ns, err := reader.ReadString('\n')
	if err != nil {
		return errors.Trace(err)
	}
	// The count
	ns = strings.Trim(ns, "\r\n\t")
	n, err := strconv.ParseInt(ns, 10, 64)
	if err != nil {
		return errors.Trace(err)
	}
	// The regions
	regions, err := reader.ReadString('\n')
	if err != nil {
		return errors.Trace(err)
	}
	regions = strings.Trim(regions, "\r\n\t")
	splits := strings.Split(regions, " ")
	realN := int64(0)
	for _, s := range splits {
		// For (`table`, `store`), has region `r`
		if s == "" {
			continue
		}
		realN++
		r, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		if c, ok := (*regionReplica)[r]; ok {
			(*regionReplica)[r] = c + 1
		} else {
			(*regionReplica)[r] = 1
		}
	}
	if n != realN {
		logutil.BgLogger().Warn("ComputeTiFlashStatus count check failed", zap.Int64("claim", n), zap.Int64("real", realN))
	}
	return nil
}

// CollectTiFlashStatus query sync status of one table from TiFlash store.
// `regionReplica` is a map from RegionID to count of TiFlash Replicas in this region.
func CollectTiFlashStatus(statusAddress string, keyspaceID tikv.KeyspaceID, tableID int64, regionReplica *map[int64]int) error {
	// The new query schema is like: http://<host>/tiflash/sync-status/keyspace/<keyspaceID>/table/<tableID>.
	// For TiDB forward compatibility, we define the Nullspace as the "keyspace" of the old table.
	// The query URL is like: http://<host>/sync-status/keyspace/<NullspaceID>/table/<tableID>
	// The old query schema is like: http://<host>/sync-status/<tableID>
	// This API is preserved in TiFlash for compatibility with old versions of TiDB.
	statURL := fmt.Sprintf("%s://%s/tiflash/sync-status/keyspace/%d/table/%d",
		util.InternalHTTPSchema(),
		statusAddress,
		keyspaceID,
		tableID,
	)
	resp, err := util.InternalHTTPClient().Get(statURL)
	if err != nil {
		return errors.Trace(err)
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logutil.BgLogger().Error("close body failed", zap.Error(err))
		}
	}()

	reader := bufio.NewReader(resp.Body)
	if err = ComputeTiFlashStatus(reader, regionReplica); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// SyncTableSchemaToTiFlash query sync schema of one table to TiFlash store.
func SyncTableSchemaToTiFlash(statusAddress string, keyspaceID tikv.KeyspaceID, tableID int64) error {
	// The new query schema is like: http://<host>/tiflash/sync-schema/keyspace/<keyspaceID>/table/<tableID>.
	// For TiDB forward compatibility, we define the Nullspace as the "keyspace" of the old table.
	// The query URL is like: http://<host>/sync-schema/keyspace/<NullspaceID>/table/<tableID>
	statURL := fmt.Sprintf("%s://%s/tiflash/sync-schema/keyspace/%d/table/%d",
		util.InternalHTTPSchema(),
		statusAddress,
		keyspaceID,
		tableID,
	)
	resp, err := util.InternalHTTPClient().Get(statURL)
	if err != nil {
		return errors.Trace(err)
	}

	err = resp.Body.Close()
	if err != nil {
		logutil.BgLogger().Error("close body failed", zap.Error(err))
	}
	return nil
}
