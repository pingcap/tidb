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
	"fmt"
	"sort"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/consts"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
)

const InitialTempId int64 = 0

// TableMappingManager processes each log backup meta kv and generate new id for DB, table and partition for
// downstream cluster. It maintains the id mapping and passes down later to the rewrite logic.
//
// The usage in the code base is listed below
// 1. during PiTR, it runs before snapshot restore to collect table id mapping information. For each id to map it
// generates a dummy downstream id first, this is because we can only generate global id after running snapshot restore
// 2. at log restore phase, it merges the db replace map generated from the full backup or previous PiTR task, it will
// replace some dummy id at this step.
// 3. it runs a filter to filter out tables that we don't need
// 4. after all above steps, it uses the genGenGlobalIDs method to generate a batch of ids in one call and replace
// the dummy ids, it builds the final state of the db replace map
type TableMappingManager struct {
	DBReplaceMap map[UpstreamID]*DBReplace
	globalIdMap  map[UpstreamID]DownstreamID

	// a counter for temporary IDs, need to get real global id
	// once full restore completes
	tempIDCounter DownstreamID
}

func NewTableMappingManager() *TableMappingManager {
	return &TableMappingManager{
		DBReplaceMap:  make(map[UpstreamID]*DBReplace),
		globalIdMap:   make(map[UpstreamID]DownstreamID),
		tempIDCounter: InitialTempId,
	}
}

func (tm *TableMappingManager) FromDBReplaceMap(dbReplaceMap map[UpstreamID]*DBReplace) error {
	if !tm.IsEmpty() {
		return errors.Annotate(berrors.ErrRestoreInvalidRewrite,
			"expect table mapping manager empty when need to load ID map")
	}

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
	tm.globalIdMap = globalTableIdMap
	tm.DBReplaceMap = dbReplaceMap

	return nil
}

func (tm *TableMappingManager) ProcessDBValueAndUpdateIdMapping(dbInfo model.DBInfo) error {
	if dr, exist := tm.DBReplaceMap[dbInfo.ID]; !exist {
		newID := tm.generateTempID()
		tm.DBReplaceMap[dbInfo.ID] = NewDBReplace(dbInfo.Name.O, newID)
		tm.globalIdMap[dbInfo.ID] = newID
	} else {
		dr.Name = dbInfo.Name.O
	}
	return nil
}

func (tm *TableMappingManager) ProcessTableValueAndUpdateIdMapping(dbID int64, tableInfo model.TableInfo) error {
	var (
		exist        bool
		dbReplace    *DBReplace
		tableReplace *TableReplace
	)

	// construct or find the id map.
	dbReplace, exist = tm.DBReplaceMap[dbID]
	if !exist {
		newID := tm.generateTempID()
		tm.globalIdMap[dbID] = newID
		dbReplace = NewDBReplace("", newID)
		tm.DBReplaceMap[dbID] = dbReplace
	}

	tableReplace, exist = dbReplace.TableMap[tableInfo.ID]
	if !exist {
		newID, exist := tm.globalIdMap[tableInfo.ID]
		if !exist {
			newID = tm.generateTempID()
			tm.globalIdMap[tableInfo.ID] = newID
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
				newID, exist = tm.globalIdMap[partition.ID]
				if !exist {
					newID = tm.generateTempID()
					tm.globalIdMap[partition.ID] = newID
				}
				tableReplace.PartitionMap[partition.ID] = newID
			}
			partitions.Definitions[i].ID = newID
		}
	}
	return nil
}

func (tm *TableMappingManager) MergeBaseDBReplace(baseMap map[UpstreamID]*DBReplace) {
	// update globalIdMap
	for upstreamID, dbReplace := range baseMap {
		tm.globalIdMap[upstreamID] = dbReplace.DbID

		for tableUpID, tableReplace := range dbReplace.TableMap {
			tm.globalIdMap[tableUpID] = tableReplace.TableID
			for partUpID, partDownID := range tableReplace.PartitionMap {
				tm.globalIdMap[partUpID] = partDownID
			}
		}
	}

	// merge baseMap to DBReplaceMap
	for upstreamID, baseDBReplace := range baseMap {
		if existingDBReplace, exists := tm.DBReplaceMap[upstreamID]; exists {
			existingDBReplace.DbID = baseDBReplace.DbID

			for tableUpID, baseTableReplace := range baseDBReplace.TableMap {
				if existingTableReplace, tableExists := existingDBReplace.TableMap[tableUpID]; tableExists {
					existingTableReplace.TableID = baseTableReplace.TableID

					for partUpID, basePartDownID := range baseTableReplace.PartitionMap {
						existingTableReplace.PartitionMap[partUpID] = basePartDownID
					}
				} else {
					existingDBReplace.TableMap[tableUpID] = baseTableReplace
				}
			}
		} else {
			tm.DBReplaceMap[upstreamID] = baseDBReplace
		}
	}
}

func (tm *TableMappingManager) IsEmpty() bool {
	return len(tm.DBReplaceMap) == 0 && len(tm.globalIdMap) == 0
}

func (tm *TableMappingManager) ReplaceTemporaryIDs(
	ctx context.Context, genGenGlobalIDs func(ctx context.Context, n int) ([]int64, error)) error {
	if tm.tempIDCounter == InitialTempId {
		// no temporary IDs were allocated
		return nil
	}

	// find actually used temporary IDs
	usedTempIDs := make(map[DownstreamID]struct{})

	// check DBReplaceMap for used temporary IDs
	// any value less than 0 is temporary ID
	for _, dr := range tm.DBReplaceMap {
		if dr.DbID < 0 {
			usedTempIDs[dr.DbID] = struct{}{}
		}
		for _, tr := range dr.TableMap {
			if tr.TableID < 0 {
				usedTempIDs[tr.TableID] = struct{}{}
			}
			for _, partID := range tr.PartitionMap {
				if partID < 0 {
					usedTempIDs[partID] = struct{}{}
				}
			}
		}
	}

	// check in globalIdMap as well just be safe
	for _, downID := range tm.globalIdMap {
		if downID < 0 {
			usedTempIDs[downID] = struct{}{}
		}
	}

	tempIDs := make([]DownstreamID, 0, len(usedTempIDs))
	// convert to sorted slice
	for id := range usedTempIDs {
		tempIDs = append(tempIDs, id)
	}

	// sort to -1, -2, -4 ... etc
	sort.Slice(tempIDs, func(i, j int) bool {
		return tempIDs[i] > tempIDs[j]
	})

	// early return if no temp id used
	if len(tempIDs) == 0 {
		tm.tempIDCounter = InitialTempId
		return nil
	}

	// generate real global IDs only for actually used temporary IDs
	newIDs, err := genGenGlobalIDs(ctx, len(tempIDs))
	if err != nil {
		return errors.Trace(err)
	}

	// create mapping from temp IDs to new IDs
	idMapping := make(map[DownstreamID]DownstreamID, len(tempIDs))
	for i, tempID := range tempIDs {
		idMapping[tempID] = newIDs[i]
	}

	// replace temp id in globalIdMap
	for upID, downID := range tm.globalIdMap {
		if newID, exists := idMapping[downID]; exists {
			tm.globalIdMap[upID] = newID
		}
	}

	// replace temp id in DBReplaceMap
	for _, dr := range tm.DBReplaceMap {
		if newID, exists := idMapping[dr.DbID]; exists {
			dr.DbID = newID
		}

		for _, tr := range dr.TableMap {
			if newID, exists := idMapping[tr.TableID]; exists {
				tr.TableID = newID
			}

			for oldPID, tempPID := range tr.PartitionMap {
				if newID, exists := idMapping[tempPID]; exists {
					tr.PartitionMap[oldPID] = newID
				}
			}
		}
	}

	tm.tempIDCounter = InitialTempId
	return nil
}

func (tm *TableMappingManager) FilterDBReplaceMap(filter *utils.PiTRTableFilter) {
	// collect all IDs that should be kept
	keepIDs := make(map[UpstreamID]struct{})

	// iterate through existing DBReplaceMap
	for dbID, dbReplace := range tm.DBReplaceMap {
		// remove entire database if not in filter
		if !filter.ContainsDB(dbID) {
			delete(tm.DBReplaceMap, dbID)
			continue
		}

		keepIDs[dbID] = struct{}{}

		// filter tables in this database
		for tableID, tableReplace := range dbReplace.TableMap {
			if !filter.ContainsTable(dbID, tableID) {
				delete(dbReplace.TableMap, tableID)
			} else {
				keepIDs[tableID] = struct{}{}
				for partitionID := range tableReplace.PartitionMap {
					keepIDs[partitionID] = struct{}{}
				}
			}
		}
	}

	// remove any ID from globalIdMap that isn't in keepIDs
	for id := range tm.globalIdMap {
		if _, ok := keepIDs[id]; !ok {
			delete(tm.globalIdMap, id)
		}
	}
}

// ToProto produces schemas id maps from up-stream to down-stream.
func (tm *TableMappingManager) ToProto() []*backuppb.PitrDBMap {
	dbMaps := make([]*backuppb.PitrDBMap, 0, len(tm.DBReplaceMap))

	for dbID, dr := range tm.DBReplaceMap {
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

func ExtractValue(e *kv.Entry, cf string) ([]byte, error) {
	switch cf {
	case consts.DefaultCF:
		return e.Value, nil
	case consts.WriteCF:
		rawWriteCFValue := new(RawWriteCFValue)
		if err := rawWriteCFValue.ParseFrom(e.Value); err != nil {
			return nil, errors.Trace(err)
		}
		if rawWriteCFValue.HasShortValue() {
			return rawWriteCFValue.shortValue, nil
		}
		return nil, nil
	default:
		panic(fmt.Sprintf("not support cf:%s", cf))
	}
}

func (tm *TableMappingManager) generateTempID() DownstreamID {
	tm.tempIDCounter--
	return tm.tempIDCounter
}
