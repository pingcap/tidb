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

package stream

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
)

// TableMappingManager iterates on log backup meta kvs and generate new id for DB, table and partition for
// downstream cluster. It maintains the id mapping and passes down later to the rewrite logic.
type TableMappingManager struct {
	DbReplaceMap  map[UpstreamID]*DBReplace
	globalIdMap   map[UpstreamID]DownstreamID
	genGlobalIdFn func(ctx context.Context) (int64, error)
}

func NewTableMappingManager(
	dbReplaceMap map[UpstreamID]*DBReplace,
	genGlobalIdFn func(ctx context.Context) (int64, error)) *TableMappingManager {
	if dbReplaceMap == nil {
		dbReplaceMap = make(map[UpstreamID]*DBReplace)
	}

	globalTableIdMap := make(map[UpstreamID]DownstreamID)
	for _, dr := range dbReplaceMap {
		for tblID, tr := range dr.TableMap {
			globalTableIdMap[tblID] = tr.TableID
			for oldpID, newpID := range tr.PartitionMap {
				globalTableIdMap[oldpID] = newpID
			}
		}
	}

	return &TableMappingManager{
		DbReplaceMap:  dbReplaceMap,
		globalIdMap:   globalTableIdMap,
		genGlobalIdFn: genGlobalIdFn,
	}
}

// ParseMetaKvAndUpdateIdMapping collect table information
func (tc *TableMappingManager) ParseMetaKvAndUpdateIdMapping(e *kv.Entry, cf string) error {
	if cf == WriteCF {
		return nil
	}

	if !IsMetaDBKey(e.Key) {
		return nil
	}

	rawKey, err := ParseTxnMetaKeyFrom(e.Key)
	if err != nil {
		return errors.Trace(err)
	}

	if meta.IsDBkey(rawKey.Field) {
		return tc.parseDBEntryAndUpdateIdMapping(e)
	} else if !meta.IsDBkey(rawKey.Key) {
		return nil
	}

	if meta.IsTableKey(rawKey.Field) {
		return tc.parseTableEntryAndUpdateIdMapping(e)
	}
	return nil
}

func (tc *TableMappingManager) parseDBEntryAndUpdateIdMapping(e *kv.Entry) error {
	value := e.Value

	dbInfo := new(model.DBInfo)
	if err := json.Unmarshal(value, dbInfo); err != nil {
		return errors.Trace(err)
	}

	if dr, exist := tc.DbReplaceMap[dbInfo.ID]; !exist {
		newID, err := tc.genGlobalIdFn(context.Background())
		if err != nil {
			return errors.Trace(err)
		}
		tc.DbReplaceMap[dbInfo.ID] = NewDBReplace(dbInfo.Name.O, newID)
		tc.globalIdMap[dbInfo.ID] = newID
	} else {
		dr.Name = dbInfo.Name.O
	}
	return nil
}

func (tc *TableMappingManager) parseTableEntryAndUpdateIdMapping(e *kv.Entry) error {
	var (
		tableInfo    model.TableInfo
		err          error
		exist        bool
		dbReplace    *DBReplace
		tableReplace *TableReplace
	)

	dbID, err := ParseDBIDFromTableKey(e.Key)
	if err != nil {
		return errors.Trace(err)
	}

	if err := json.Unmarshal(e.Value, &tableInfo); err != nil {
		return errors.Trace(err)
	}

	// construct or find the id map.
	dbReplace, exist = tc.DbReplaceMap[dbID]
	if !exist {
		newID, err := tc.genGlobalIdFn(context.Background())
		if err != nil {
			return errors.Trace(err)
		}
		tc.globalIdMap[dbID] = newID
		dbReplace = NewDBReplace("", newID)
		tc.DbReplaceMap[dbID] = dbReplace
	}

	tableReplace, exist = dbReplace.TableMap[tableInfo.ID]
	if !exist {
		newID, exist := tc.globalIdMap[tableInfo.ID]
		if !exist {
			newID, err = tc.genGlobalIdFn(context.Background())
			if err != nil {
				return errors.Trace(err)
			}
			tc.globalIdMap[tableInfo.ID] = newID
		}

		tableReplace = NewTableReplace(tableInfo.Name.O, newID)
		dbReplace.TableMap[tableInfo.ID] = tableReplace
	} else {
		tableReplace.Name = tableInfo.Name.O
	}

	// update table ID and partition ID.
	tableInfo.ID = tableReplace.TableID
	partitions := tableInfo.GetPartitionInfo()
	if partitions != nil {
		for i, partition := range partitions.Definitions {
			newID, exist := tableReplace.PartitionMap[partition.ID]
			if !exist {
				newID, exist = tc.globalIdMap[partition.ID]
				if !exist {
					newID, err = tc.genGlobalIdFn(context.Background())
					if err != nil {
						return errors.Trace(err)
					}
					tc.globalIdMap[partition.ID] = newID
				}
				tableReplace.PartitionMap[partition.ID] = newID
			}
			partitions.Definitions[i].ID = newID
		}
	}
	return nil
}

// ToProto produces schemas id maps from up-stream to down-stream.
func (tc *TableMappingManager) ToProto() []*backuppb.PitrDBMap {
	dbMaps := make([]*backuppb.PitrDBMap, 0, len(tc.DbReplaceMap))

	for dbID, dr := range tc.DbReplaceMap {
		dbm := backuppb.PitrDBMap{
			Name: dr.Name,
			IdMap: &backuppb.IDMap{
				UpstreamId:   dbID,
				DownstreamId: dr.DbID,
			},
			Tables: make([]*backuppb.PitrTableMap, 0, len(dr.TableMap)),
		}

		for tblID, tr := range dr.TableMap {
			tm := backuppb.PitrTableMap{
				Name: tr.Name,
				IdMap: &backuppb.IDMap{
					UpstreamId:   tblID,
					DownstreamId: tr.TableID,
				},
				Partitions: make([]*backuppb.IDMap, 0, len(tr.PartitionMap)),
			}

			for upID, downID := range tr.PartitionMap {
				pm := backuppb.IDMap{
					UpstreamId:   upID,
					DownstreamId: downID,
				}
				tm.Partitions = append(tm.Partitions, &pm)
			}
			dbm.Tables = append(dbm.Tables, &tm)
		}
		dbMaps = append(dbMaps, &dbm)
	}

	return dbMaps
}

func FromDBMapProto(dbMaps []*backuppb.PitrDBMap) map[UpstreamID]*DBReplace {
	dbReplaces := make(map[UpstreamID]*DBReplace)

	for _, db := range dbMaps {
		dr := NewDBReplace(db.Name, db.IdMap.DownstreamId)
		dbReplaces[db.IdMap.UpstreamId] = dr

		for _, tbl := range db.Tables {
			tr := NewTableReplace(tbl.Name, tbl.IdMap.DownstreamId)
			dr.TableMap[tbl.IdMap.UpstreamId] = tr
			for _, p := range tbl.Partitions {
				tr.PartitionMap[p.UpstreamId] = p.DownstreamId
			}
		}
	}

	return dbReplaces
}
