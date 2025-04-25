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
	"fmt"
	"sort"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/consts"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
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

	// used during scanning log to identify already seen id mapping. For example after exchange partition, the
	// exchanged-in table already had an id mapping can be identified in the partition so don't allocate a new id.
	globalIdMap map[UpstreamID]DownstreamID

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

	// doesn't even need to build globalIdMap since loading DBReplaceMap from saved checkpoint
	tm.DBReplaceMap = dbReplaceMap
	return nil
}

// ParseMetaKvAndUpdateIdMapping collect table information
// the keys and values that are selected to parse here follows the implementation in rewrite_meta_rawkv. Maybe
// parsing a subset of these keys/values would suffice, but to make it safe we decide to parse exactly same as
// in rewrite_meta_rawkv.
func (tm *TableMappingManager) ParseMetaKvAndUpdateIdMapping(e *kv.Entry, cf string) error {
	if !utils.IsMetaDBKey(e.Key) {
		return nil
	}

	rawKey, err := ParseTxnMetaKeyFrom(e.Key)
	if err != nil {
		return errors.Trace(err)
	}

	if meta.IsDBkey(rawKey.Field) {
		// parse db key
		err := tm.parseDBKeyAndUpdateIdMapping(rawKey.Field)
		if err != nil {
			return errors.Trace(err)
		}

		// parse value and update if exists
		value, err := ExtractValue(e, cf)
		if err != nil {
			return errors.Trace(err)
		}
		if value != nil {
			return tm.parseDBValueAndUpdateIdMapping(value)
		}
	} else if !meta.IsDBkey(rawKey.Key) {
		return nil
	}

	if meta.IsTableKey(rawKey.Field) {
		dbID, err := meta.ParseDBKey(rawKey.Key)
		if err != nil {
			return errors.Trace(err)
		}

		// parse table key and update
		err = tm.parseTableIdAndUpdateIdMapping(rawKey.Key, rawKey.Field, meta.ParseTableKey)
		if err != nil {
			return errors.Trace(err)
		}

		// parse value and update if exists
		value, err := ExtractValue(e, cf)
		if err != nil {
			return errors.Trace(err)
		}
		if value != nil {
			return tm.parseTableValueAndUpdateIdMapping(dbID, value)
		}
	} else if meta.IsAutoIncrementIDKey(rawKey.Field) {
		// parse auto increment key and update
		err = tm.parseTableIdAndUpdateIdMapping(rawKey.Key, rawKey.Field, meta.ParseAutoIncrementIDKey)
		if err != nil {
			return errors.Trace(err)
		}
	} else if meta.IsAutoTableIDKey(rawKey.Field) {
		// parse auto table key and update
		err = tm.parseTableIdAndUpdateIdMapping(rawKey.Key, rawKey.Field, meta.ParseAutoTableIDKey)
		if err != nil {
			return errors.Trace(err)
		}
	} else if meta.IsSequenceKey(rawKey.Field) {
		// parse sequence key and update
		err = tm.parseTableIdAndUpdateIdMapping(rawKey.Key, rawKey.Field, meta.ParseSequenceKey)
		if err != nil {
			return errors.Trace(err)
		}
	} else if meta.IsAutoRandomTableIDKey(rawKey.Field) {
		// parse sequence key and update
		err = tm.parseTableIdAndUpdateIdMapping(rawKey.Key, rawKey.Field, meta.ParseAutoRandomTableIDKey)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (tm *TableMappingManager) parseDBKeyAndUpdateIdMapping(field []byte) error {
	dbID, err := meta.ParseDBKey(field)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = tm.getOrCreateDBReplace(dbID)
	return errors.Trace(err)
}

func (tm *TableMappingManager) parseDBValueAndUpdateIdMapping(value []byte) error {
	dbInfo := new(model.DBInfo)
	if err := json.Unmarshal(value, dbInfo); err != nil {
		return errors.Trace(err)
	}

	dbReplace, err := tm.getOrCreateDBReplace(dbInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	dbReplace.Name = dbInfo.Name.O
	return nil
}

// getOrCreateDBReplace gets an existing DBReplace or creates a new one if not found
func (tm *TableMappingManager) getOrCreateDBReplace(dbID int64) (*DBReplace, error) {
	dbReplace, exist := tm.DBReplaceMap[dbID]
	if !exist {
		newID := tm.generateTempID()
		tm.globalIdMap[dbID] = newID
		dbReplace = NewDBReplace("", newID)
		tm.DBReplaceMap[dbID] = dbReplace
	}
	return dbReplace, nil
}

// getOrCreateTableReplace gets an existing TableReplace or creates a new one if not found
func (tm *TableMappingManager) getOrCreateTableReplace(dbReplace *DBReplace, tableID int64) (*TableReplace, error) {
	tableReplace, exist := dbReplace.TableMap[tableID]
	if !exist {
		newID, exist := tm.globalIdMap[tableID]
		if !exist {
			newID = tm.generateTempID()
			tm.globalIdMap[tableID] = newID
		}
		tableReplace = NewTableReplace("", newID)
		dbReplace.TableMap[tableID] = tableReplace
	}
	return tableReplace, nil
}

func (tm *TableMappingManager) parseTableIdAndUpdateIdMapping(
	key []byte,
	field []byte,
	parseField func([]byte) (tableID int64, err error)) error {
	dbID, err := meta.ParseDBKey(key)
	if err != nil {
		return errors.Trace(err)
	}

	tableID, err := parseField(field)
	if err != nil {
		return errors.Trace(err)
	}

	dbReplace, err := tm.getOrCreateDBReplace(dbID)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = tm.getOrCreateTableReplace(dbReplace, tableID)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (tm *TableMappingManager) parseTableValueAndUpdateIdMapping(dbID int64, value []byte) error {
	var tableInfo model.TableInfo
	if err := json.Unmarshal(value, &tableInfo); err != nil {
		return errors.Trace(err)
	}

	dbReplace, err := tm.getOrCreateDBReplace(dbID)
	if err != nil {
		return errors.Trace(err)
	}

	tableReplace, err := tm.getOrCreateTableReplace(dbReplace, tableInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	tableReplace.Name = tableInfo.Name.O

	// update table ID and partition ID.
	partitions := tableInfo.GetPartitionInfo()
	if partitions != nil {
		for _, partition := range partitions.Definitions {
			_, exist := tableReplace.PartitionMap[partition.ID]
			if !exist {
				newID, exist := tm.globalIdMap[partition.ID]
				if !exist {
					newID = tm.generateTempID()
					tm.globalIdMap[partition.ID] = newID
				}
				tableReplace.PartitionMap[partition.ID] = newID
			}
		}
	}
	return nil
}

func (tm *TableMappingManager) MergeBaseDBReplace(baseMap map[UpstreamID]*DBReplace) {
	// first pass: update all global IDs
	for upstreamID, baseDBReplace := range baseMap {
		tm.globalIdMap[upstreamID] = baseDBReplace.DbID

		for tableUpID, baseTableReplace := range baseDBReplace.TableMap {
			tm.globalIdMap[tableUpID] = baseTableReplace.TableID

			for partUpID, basePartDownID := range baseTableReplace.PartitionMap {
				tm.globalIdMap[partUpID] = basePartDownID
			}
		}
	}

	// second pass: update the DBReplaceMap structure
	// first update all existing entries using the global ID map
	for upDBID, existingDBReplace := range tm.DBReplaceMap {
		if newID, exists := tm.globalIdMap[upDBID]; exists {
			existingDBReplace.DbID = newID
		}

		// db replace in `TableMappingManager` has no name yet, it is determined by baseMap.
		// TODO: update the name of the db replace that is not exists in baseMap.
		// Now it is OK because user tables' name is not used.
		if existingDBReplace.Name == "" {
			if baseDBReplace, exists := baseMap[upDBID]; exists && baseDBReplace.Name != "" {
				existingDBReplace.Name = baseDBReplace.Name
			}
		}

		for upTableID, existingTableReplace := range existingDBReplace.TableMap {
			if newID, exists := tm.globalIdMap[upTableID]; exists {
				existingTableReplace.TableID = newID
			}

			// table replace in `TableMappingManager` has no name yet, it is determined by baseMap.
			// TODO: update the name of the table replace that is not exists in baseMap.
			// Now it is OK because user tables' name is not used.
			if existingTableReplace.Name == "" {
				if baseDBReplace, dbExists := baseMap[upDBID]; dbExists {
					if baseTableReplace, tableExists := baseDBReplace.TableMap[upTableID]; tableExists && baseTableReplace.Name != "" {
						existingTableReplace.Name = baseTableReplace.Name
					}
				}
			}

			for partUpID := range existingTableReplace.PartitionMap {
				if newID, exists := tm.globalIdMap[partUpID]; exists {
					existingTableReplace.PartitionMap[partUpID] = newID
				}
			}
		}
	}

	// then add any new entries from the base map
	for upstreamID, baseDBReplace := range baseMap {
		if _, exists := tm.DBReplaceMap[upstreamID]; !exists {
			tm.DBReplaceMap[upstreamID] = baseDBReplace
		} else {
			existingDBReplace := tm.DBReplaceMap[upstreamID]
			for tableUpID, baseTableReplace := range baseDBReplace.TableMap {
				if _, exists := existingDBReplace.TableMap[tableUpID]; !exists {
					existingDBReplace.TableMap[tableUpID] = baseTableReplace
				} else {
					// merge partition mappings for existing tables
					existingTableReplace := existingDBReplace.TableMap[tableUpID]
					for partUpID, partDownID := range baseTableReplace.PartitionMap {
						if _, exists := existingTableReplace.PartitionMap[partUpID]; !exists {
							existingTableReplace.PartitionMap[partUpID] = partDownID
						}
					}
				}
			}
		}
	}
}

func (tm *TableMappingManager) IsEmpty() bool {
	return len(tm.DBReplaceMap) == 0
}

func (tm *TableMappingManager) ReplaceTemporaryIDs(
	ctx context.Context, genGenGlobalIDs func(ctx context.Context, n int) ([]int64, error)) error {
	if tm.tempIDCounter == InitialTempId {
		// no temporary IDs were allocated
		return nil
	}

	// find actually used temporary IDs
	usedTempIDs := make(map[DownstreamID]UpstreamID)

	// helper function to check and add temporary ID
	addTempIDIfNeeded := func(downID DownstreamID, upID UpstreamID) error {
		if downID < 0 {
			if prevUpID, exists := usedTempIDs[downID]; exists {
				// ok if point to the same upstream
				if prevUpID == upID {
					return nil
				}
				return errors.Annotate(berrors.ErrRestoreInvalidRewrite,
					fmt.Sprintf("found duplicate temporary ID %d, existing upstream ID: %d, new upstream ID: %d",
						downID, prevUpID, upID))
			}
			usedTempIDs[downID] = upID
		}
		return nil
	}

	// check DBReplaceMap for used temporary IDs
	// any value less than 0 is temporary ID
	for upDBId, dr := range tm.DBReplaceMap {
		if err := addTempIDIfNeeded(dr.DbID, upDBId); err != nil {
			return err
		}
		for upTableID, tr := range dr.TableMap {
			if err := addTempIDIfNeeded(tr.TableID, upTableID); err != nil {
				return err
			}
			for upPartID, partID := range tr.PartitionMap {
				if err := addTempIDIfNeeded(partID, upPartID); err != nil {
					return err
				}
			}
		}
	}

	tempIDs := make([]DownstreamID, 0, len(usedTempIDs))
	// convert to sorted slice
	for id := range usedTempIDs {
		tempIDs = append(tempIDs, id)
	}

	// sort to -1, -2, -4, -8 ... etc
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

func (tm *TableMappingManager) ApplyFilterToDBReplaceMap(tracker *utils.PiTRIdTracker) {
	// iterate through existing DBReplaceMap
	for dbID, dbReplace := range tm.DBReplaceMap {
		if !tracker.ContainsDB(dbID) {
			dbReplace.FilteredOut = true
		}

		// filter tables in this database
		for tableID, tableReplace := range dbReplace.TableMap {
			if !tracker.ContainsTableId(dbID, tableID) {
				tableReplace.FilteredOut = true
			}
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
			Tables:      make([]*backuppb.PitrTableMap, 0, len(dr.TableMap)),
			FilteredOut: dr.FilteredOut,
		}

		for tblID, tr := range dr.TableMap {
			tm := backuppb.PitrTableMap{
				Name: tr.Name,
				IdMap: &backuppb.IDMap{
					UpstreamId:   tblID,
					DownstreamId: tr.TableID,
				},
				Partitions:  make([]*backuppb.IDMap, 0, len(tr.PartitionMap)),
				FilteredOut: tr.FilteredOut,
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
		dr.FilteredOut = db.FilteredOut
		dbReplaces[db.IdMap.UpstreamId] = dr

		for _, tbl := range db.Tables {
			tr := NewTableReplace(tbl.Name, tbl.IdMap.DownstreamId)
			tr.FilteredOut = tbl.FilteredOut
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
		// have to be consistent with rewrite_meta_rawkv.go otherwise value like p/xxx/xxx will fall through
		// and fail to parse
		if rawWriteCFValue.IsDelete() || rawWriteCFValue.IsRollback() || !rawWriteCFValue.HasShortValue() {
			return nil, nil
		}
		return rawWriteCFValue.GetShortValue(), nil
	default:
		return nil, errors.Errorf("unsupported column family: %s", cf)
	}
}

func (tm *TableMappingManager) generateTempID() DownstreamID {
	tm.tempIDCounter--
	return tm.tempIDCounter
}
