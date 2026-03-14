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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"sort"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/consts"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/zap"
)

const InitialTempId int64 = 0

const (
	errMsgDefaultCFKVLost = "the default cf kv is lost when there is its write cf kv"
)

type tableMetaKey struct {
	dbId    int64
	tableId int64
	ts      uint64
}

type tableSimpleInfo struct {
	Name         string
	PartitionIds []int64
}

type dbMetaKey struct {
	dbId int64
	ts   uint64
}

type dbMetaValue struct {
	name  string
	count int
}

type tableMetaValue struct {
	info  *tableSimpleInfo
	count int
}

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
	DBReplaceMap  map[UpstreamID]*DBReplace
	fromPitrIdMap bool

	// used during scanning log to identify already seen id mapping. For example after exchange partition, the
	// exchanged-in table already had an id mapping can be identified in the partition so don't allocate a new id.
	globalIdMap map[UpstreamID]DownstreamID

	// a counter for temporary IDs, need to get real global id
	// once full restore completes
	tempIDCounter DownstreamID

	tempDefaultKVTableMap map[tableMetaKey]*tableMetaValue
	tempDefaultKVDbMap    map[dbMetaKey]*dbMetaValue

	noDefaultKVErrorMap map[uint64]error

	// preallocated ID range from snapshot restore for scheduler pausing
	// [start, end) where end is exclusive
	PreallocatedRange [2]int64
}

func (tm *TableMappingManager) SetFromPiTRIDMap() {
	tm.fromPitrIdMap = true
}

func (tm *TableMappingManager) IsFromPiTRIDMap() bool {
	return tm.fromPitrIdMap
}

func NewTableMappingManager() *TableMappingManager {
	return &TableMappingManager{
		DBReplaceMap:          make(map[UpstreamID]*DBReplace),
		fromPitrIdMap:         false,
		globalIdMap:           make(map[UpstreamID]DownstreamID),
		tempIDCounter:         InitialTempId,
		tempDefaultKVTableMap: make(map[tableMetaKey]*tableMetaValue),
		tempDefaultKVDbMap:    make(map[dbMetaKey]*dbMetaValue),
		noDefaultKVErrorMap:   make(map[uint64]error),
	}
}

func (tm *TableMappingManager) CleanTempKV() {
	tm.tempDefaultKVDbMap = nil
	tm.tempDefaultKVTableMap = nil
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

// MetaInfoCollector is an interface for collecting metadata information during parsing
type MetaInfoCollector interface {
	// OnDatabaseInfo is called when database information is found in a value
	OnDatabaseInfo(dbId int64, dbName string, commitTs uint64)
	// OnTableInfo is called when table information is found in a value
	OnTableInfo(dbID, tableId int64, tableSimpleInfo *tableSimpleInfo, commitTs uint64)
}

// ParseMetaKvAndUpdateIdMapping collect table information
// the keys and values that are selected to parse here follows the implementation in rewrite_meta_rawkv. Maybe
// parsing a subset of these keys/values would suffice, but to make it safe we decide to parse exactly same as
// in rewrite_meta_rawkv.
func (tm *TableMappingManager) ParseMetaKvAndUpdateIdMapping(
	e *kv.Entry, cf string, ts uint64, collector MetaInfoCollector) error {
	if !utils.IsMetaDBKey(e.Key) {
		return nil
	}

	rawKey, err := ParseTxnMetaKeyFrom(e.Key)
	if err != nil {
		return errors.Trace(err)
	}

	if meta.IsDBkey(rawKey.Field) {
		// parse db key
		dbID, err := tm.parseDBKeyAndUpdateIdMapping(rawKey.Field)
		if err != nil {
			return errors.Trace(err)
		}

		// parse value and update if exists
		switch cf {
		case consts.DefaultCF:
			return tm.parseDBValueAndUpdateIdMappingForDefaultCf(dbID, e.Value, ts)
		case consts.WriteCF:
			return tm.parseDBValueAndUpdateIdMappingForWriteCf(dbID, e.Value, ts, collector)
		default:
			return errors.Errorf("unsupported column family: %s", cf)
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
		switch cf {
		case consts.DefaultCF:
			return tm.parseTableValueAndUpdateIdMappingForDefaultCf(dbID, e.Value, ts)
		case consts.WriteCF:
			tableId, err := meta.ParseTableKey(rawKey.Field)
			if err != nil {
				return errors.Trace(err)
			}
			return tm.parseTableValueAndUpdateIdMappingForWriteCf(dbID, tableId, e.Value, ts, collector)
		default:
			return errors.Errorf("unsupported column family: %s", cf)
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

func (tm *TableMappingManager) parseDBKeyAndUpdateIdMapping(field []byte) (int64, error) {
	dbID, err := meta.ParseDBKey(field)
	if err != nil {
		return 0, errors.Trace(err)
	}

	_, err = tm.getOrCreateDBReplace(dbID)
	return dbID, errors.Trace(err)
}

func extractDBName(value []byte) (string, error) {
	dbInfo := new(model.DBInfo)
	if err := json.Unmarshal(value, dbInfo); err != nil {
		return "", errors.Trace(err)
	}
	return dbInfo.Name.O, nil
}

func (tm *TableMappingManager) parseDBValueAndUpdateIdMappingForDefaultCf(
	dbId int64, value []byte, startTs uint64) error {
	dbName, err := extractDBName(value)
	if err != nil {
		return errors.Trace(err)
	}
	key := dbMetaKey{
		dbId: dbId,
		ts:   startTs,
	}
	if existingValue, exists := tm.tempDefaultKVDbMap[key]; exists {
		existingValue.count++
		return nil
	}

	tm.tempDefaultKVDbMap[key] = &dbMetaValue{
		name:  dbName,
		count: 1,
	}
	return nil
}

func (tm *TableMappingManager) parseDBValueAndUpdateIdMappingForWriteCf(
	dbId int64, value []byte, commitTs uint64, collector MetaInfoCollector) error {
	rawWriteCFValue := new(RawWriteCFValue)
	if err := rawWriteCFValue.ParseFrom(value); err != nil {
		return errors.Trace(err)
	}

	// handle different write types
	if rawWriteCFValue.IsDelete() || rawWriteCFValue.IsRollback() {
		// for delete operations, we should clear the tempDBInfo if it exists
		// but not process it as a database creation/update
		idx := dbMetaKey{
			dbId: dbId,
			ts:   rawWriteCFValue.GetStartTs(),
		}
		delete(tm.tempDefaultKVDbMap, idx)
		return nil
	}

	if !rawWriteCFValue.IsPut() {
		// skip other write types (like lock)
		return nil
	}

	startTs := rawWriteCFValue.GetStartTs()
	var dbValue []byte
	if rawWriteCFValue.HasShortValue() {
		dbValue = rawWriteCFValue.GetShortValue()
	}

	if len(dbValue) > 0 {
		dbName, err := extractDBName(dbValue)
		if err != nil {
			return errors.Trace(err)
		}
		return tm.parseDBValueAndUpdateIdMapping(dbId, dbName, commitTs, collector)
	}

	idx := dbMetaKey{
		dbId: dbId,
		ts:   startTs,
	}

	if dbValue, exists := tm.tempDefaultKVDbMap[idx]; exists {
		dbValue.count--
		if dbValue.count < 0 {
			log.Warn("write cf kvs are more than default cf kvs for database",
				zap.Int64("db-id", dbId),
				zap.Uint64("start-ts", startTs),
				zap.Uint64("commit-ts", commitTs),
				zap.String("value", base64.StdEncoding.EncodeToString(value)))
		}
		return tm.parseDBValueAndUpdateIdMapping(dbId, dbValue.name, commitTs, collector)
	}
	log.Warn("default cf kv is lost when processing write cf kv for database",
		zap.Int64("db-id", dbId),
		zap.Uint64("start-ts", startTs),
		zap.Uint64("commit-ts", commitTs),
		zap.String("value", base64.StdEncoding.EncodeToString(value)),
		zap.Int("temp-default-kv-db-map-size", len(tm.tempDefaultKVDbMap)))
	tm.noDefaultKVErrorMap[commitTs] = errors.Errorf(
		errMsgDefaultCFKVLost+"(db id:%d, value %s)",
		dbId, base64.StdEncoding.EncodeToString(value),
	)
	return nil
}

func (tm *TableMappingManager) parseDBValueAndUpdateIdMapping(
	dbId int64, dbName string, commitTs uint64, collector MetaInfoCollector) error {
	dbReplace, err := tm.getOrCreateDBReplace(dbId)
	if err != nil {
		return errors.Trace(err)
	}
	if dbName != "" {
		dbReplace.Name = dbName
	}
	collector.OnDatabaseInfo(dbId, dbName, commitTs)
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

func extractTableSimpleInfo(value []byte) (int64, *tableSimpleInfo, error) {
	var tableInfo model.TableInfo
	if err := json.Unmarshal(value, &tableInfo); err != nil {
		return 0, nil, errors.Trace(err)
	}
	var partitionIds []int64
	partitions := tableInfo.GetPartitionInfo()
	if partitions != nil {
		partitionIds = make([]int64, 0, len(partitions.Definitions))
		for _, def := range partitions.Definitions {
			partitionIds = append(partitionIds, def.ID)
		}
	}
	return tableInfo.ID, &tableSimpleInfo{
		Name:         tableInfo.Name.O,
		PartitionIds: partitionIds,
	}, nil
}

func (tm *TableMappingManager) parseTableValueAndUpdateIdMappingForDefaultCf(
	dbID int64, value []byte, ts uint64) error {
	tableId, tableSimpleInfo, err := extractTableSimpleInfo(value)
	if err != nil {
		return errors.Trace(err)
	}
	key := tableMetaKey{
		dbId:    dbID,
		tableId: tableId,
		ts:      ts,
	}
	if existingValue, exists := tm.tempDefaultKVTableMap[key]; exists {
		existingValue.count++
		return nil
	}

	tm.tempDefaultKVTableMap[key] = &tableMetaValue{
		info:  tableSimpleInfo,
		count: 1,
	}
	return nil
}

func (tm *TableMappingManager) parseTableValueAndUpdateIdMappingForWriteCf(
	dbId, tableId int64, value []byte, commitTs uint64, collector MetaInfoCollector) error {
	rawWriteCFValue := new(RawWriteCFValue)
	if err := rawWriteCFValue.ParseFrom(value); err != nil {
		return errors.Trace(err)
	}

	// handle different write types
	if rawWriteCFValue.IsDelete() || rawWriteCFValue.IsRollback() {
		// for delete operations, we should clear the tempTableInfo if it exists
		// but not process it as a table creation/update
		idx := tableMetaKey{
			dbId:    dbId,
			tableId: tableId,
			ts:      rawWriteCFValue.GetStartTs(),
		}
		delete(tm.tempDefaultKVTableMap, idx)
		return nil
	}

	if !rawWriteCFValue.IsPut() {
		// skip other write types (like lock)
		return nil
	}

	startTs := rawWriteCFValue.GetStartTs()
	var tableValue []byte
	if rawWriteCFValue.HasShortValue() {
		tableValue = rawWriteCFValue.GetShortValue()
	}

	if len(tableValue) > 0 {
		tableId, tableSimpleInfo, err := extractTableSimpleInfo(tableValue)
		if err != nil {
			return errors.Trace(err)
		}
		return tm.parseTableValueAndUpdateIdMapping(dbId, tableId, commitTs, tableSimpleInfo, collector)
	}

	idx := tableMetaKey{
		dbId:    dbId,
		tableId: tableId,
		ts:      startTs,
	}
	if tableValue, exists := tm.tempDefaultKVTableMap[idx]; exists {
		tableValue.count--
		if tableValue.count < 0 {
			log.Warn("write cf kvs are more than default cf kvs for table",
				zap.Int64("db-id", dbId),
				zap.Int64("table-id", tableId),
				zap.Uint64("start-ts", startTs),
				zap.Uint64("commit-ts", commitTs),
				zap.String("value", base64.StdEncoding.EncodeToString(value)))
		}
		return tm.parseTableValueAndUpdateIdMapping(dbId, tableId, commitTs, tableValue.info, collector)
	}
	log.Warn("default cf kv is lost when processing write cf kv for table",
		zap.Int64("db-id", dbId),
		zap.Int64("table-id", tableId),
		zap.Uint64("start-ts", startTs),
		zap.Uint64("commit-ts", commitTs),
		zap.String("value", base64.StdEncoding.EncodeToString(value)),
		zap.Int("temp-default-kv-table-map-size", len(tm.tempDefaultKVTableMap)))
	tm.noDefaultKVErrorMap[commitTs] = errors.Errorf(
		errMsgDefaultCFKVLost+"(db id:%d, table id:%d, value %s)",
		dbId, tableId, base64.StdEncoding.EncodeToString(value),
	)
	return nil
}

func (tm *TableMappingManager) parseTableValueAndUpdateIdMapping(
	dbId, tableId int64, commitTs uint64, tableSimpleInfo *tableSimpleInfo, collector MetaInfoCollector) error {
	dbReplace, err := tm.getOrCreateDBReplace(dbId)
	if err != nil {
		return errors.Trace(err)
	}

	tableReplace, err := tm.getOrCreateTableReplace(dbReplace, tableId)
	if err != nil {
		return errors.Trace(err)
	}
	if tableSimpleInfo.Name != "" {
		tableReplace.Name = tableSimpleInfo.Name
	}

	// update table ID and partition ID.
	for _, partitionId := range tableSimpleInfo.PartitionIds {
		_, exist := tableReplace.PartitionMap[partitionId]
		if !exist {
			newID, exist := tm.globalIdMap[partitionId]
			if !exist {
				newID = tm.generateTempID()
				tm.globalIdMap[partitionId] = newID
			}
			tableReplace.PartitionMap[partitionId] = newID
		}
	}
	collector.OnTableInfo(dbId, tableId, tableSimpleInfo, commitTs)
	return nil
}

func (tm *TableMappingManager) CleanError(rewriteTs uint64) {
	delete(tm.noDefaultKVErrorMap, rewriteTs)
}

func (tm *TableMappingManager) ReportIfError() error {
	for _, err := range tm.noDefaultKVErrorMap {
		if err != nil {
			return errors.Trace(err)
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

			maps.Copy(tm.globalIdMap, baseTableReplace.PartitionMap)
		}
	}

	// second pass: update the DBReplaceMap structure
	// first update all existing entries using the global ID map
	for upDBID, existingDBReplace := range tm.DBReplaceMap {
		if newID, exists := tm.globalIdMap[upDBID]; exists {
			existingDBReplace.DbID = newID
		}

		if baseDBReplace, exists := baseMap[upDBID]; exists {
			// db replace in `TableMappingManager` has no name yet, it is determined by baseMap.
			// TODO: update the name of the db replace that is not exists in baseMap.
			// Now it is OK because user tables' name is not used.
			if existingDBReplace.Name == "" && baseDBReplace.Name != "" {
				existingDBReplace.Name = baseDBReplace.Name
			}
			// update the reused flag of the db replace, maybe it is reused in snapshot restore.
			if baseDBReplace.Reused {
				existingDBReplace.Reused = true
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
						existingTableReplace.PartitionMap[partUpID] = partDownID
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

	if len(usedTempIDs) == 0 {
		// no temp id allocated
		return nil
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

func (tm *TableMappingManager) ReuseExistingDatabaseIDs(infoschema infoschema.InfoSchema) {
	for dbID, dbReplace := range tm.DBReplaceMap {
		if dbReplace.FilteredOut || dbReplace.DbID > 0 {
			continue
		}
		if dbInfo, exists := infoschema.SchemaByName(ast.NewCIStr(dbReplace.Name)); exists {
			dbReplace.DbID = dbInfo.ID
			dbReplace.Reused = true
			log.Info("reuse existing database id",
				zap.String("db-name", dbReplace.Name),
				zap.Int64("upstream-db-id", dbID),
				zap.Int64("downstream-db-id", dbReplace.DbID))
		}
	}
}

func (tm *TableMappingManager) ApplyFilterToDBReplaceMap(tracker *utils.PiTRIdTracker) {
	// iterate through existing DBReplaceMap
	for dbID, dbReplace := range tm.DBReplaceMap {
		if !tracker.ContainsDB(dbID) {
			dbReplace.FilteredOut = true
		}

		// filter tables in this database
		for tableID, tableReplace := range dbReplace.TableMap {
			if !tracker.ContainsDBAndTableId(dbID, tableID) {
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

func (tm *TableMappingManager) generateTempID() DownstreamID {
	tm.tempIDCounter--
	return tm.tempIDCounter
}

// UpdateDownstreamIds updates the mapping from old table ID to new table ID.
// this is necessary since we override the table name during full restore directly to its end name, so we need to
// figure out the id mapping upfront.
func (tm *TableMappingManager) UpdateDownstreamIds(dbs []*metautil.Database, tables []*restoreutils.CreatedTable,
	dom *domain.Domain) error {
	dbReplaces := make(map[UpstreamID]*DBReplace)

	for _, oldDB := range dbs {
		newDBInfo, exists := dom.InfoSchema().SchemaByName(oldDB.Info.Name)
		if !exists {
			return errors.New("db not exist in snapshot stage UpdateDownstreamIds")
		}
		_, exist := dbReplaces[oldDB.Info.ID]
		if !exist {
			dbReplace := NewDBReplace(newDBInfo.Name.O, newDBInfo.ID)
			dbReplace.Reused = oldDB.IsReusedByPITR()
			if dbReplace.Reused {
				log.Info("the database is reused by snapshot restore",
					zap.Stringer("db", newDBInfo.Name),
					zap.Int64("upstream-db-id", oldDB.Info.ID),
					zap.Int64("downstream-db-id", newDBInfo.ID))
			}
			dbReplaces[oldDB.Info.ID] = dbReplace
		}
	}

	for _, t := range tables {
		oldTable := t.OldTable
		newTable := t.Table

		dbReplace, exist := dbReplaces[oldTable.DB.ID]
		if !exist {
			return errors.New("table exists but db not exist in UpdateDownstreamIds")
		}

		dbReplace.TableMap[oldTable.Info.ID] = &TableReplace{
			Name:         newTable.Name.O,
			TableID:      newTable.ID,
			PartitionMap: restoreutils.GetPartitionIDMap(newTable, oldTable.Info),
		}
	}
	tm.MergeBaseDBReplace(dbReplaces)
	return nil
}

// SetPreallocatedRange sets the preallocated ID range from snapshot restore
// This range will be used for fine-grained scheduler pausing during log restore
func (tm *TableMappingManager) SetPreallocatedRange(start, end int64) {
	tm.PreallocatedRange = [2]int64{start, end}
	log.Info("set preallocated range for scheduler pausing",
		zap.Int64("start", start),
		zap.Int64("end", end))
}
