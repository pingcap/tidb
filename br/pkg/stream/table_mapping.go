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
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
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
	Name                 string
	PartitionIds         []int64
	HasForeignKeys       bool
	ForeignKeyReferences []ForeignKeyReference
	IsView               bool
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
	fkRefs := make([]ForeignKeyReference, 0, len(tableInfo.ForeignKeys))
	for _, fk := range tableInfo.ForeignKeys {
		if fk == nil {
			continue
		}
		fkRefs = append(fkRefs, ForeignKeyReference{Schema: fk.RefSchema.O, Table: fk.RefTable.O})
	}
	return tableInfo.ID, &tableSimpleInfo{
		Name:                 tableInfo.Name.O,
		PartitionIds:         partitionIds,
		HasForeignKeys:       len(tableInfo.ForeignKeys) > 0,
		ForeignKeyReferences: fkRefs,
		IsView:               tableInfo.View != nil,
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
	// Keep the union across all table-info versions in the restore interval. A
	// routed object is unsupported if any replayed version is a view or carries
	// a foreign key, even if a later DDL removes that dependency.
	tableReplace.HasForeignKeys = tableReplace.HasForeignKeys || tableSimpleInfo.HasForeignKeys
	for _, ref := range tableSimpleInfo.ForeignKeyReferences {
		if ref.Schema == "" {
			ref.Schema = dbReplace.Name
		}
		found := false
		for _, existing := range tableReplace.ForeignKeyReferences {
			if strings.EqualFold(existing.Schema, ref.Schema) && strings.EqualFold(existing.Table, ref.Table) {
				found = true
				break
			}
		}
		if !found {
			tableReplace.ForeignKeyReferences = append(tableReplace.ForeignKeyReferences, ref)
		}
	}
	tableReplace.IsView = tableReplace.IsView || tableSimpleInfo.IsView

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

func mergeForeignKeyReferences(dst, src *TableReplace) {
	for _, ref := range src.ForeignKeyReferences {
		found := false
		for _, existing := range dst.ForeignKeyReferences {
			if strings.EqualFold(existing.Schema, ref.Schema) && strings.EqualFold(existing.Table, ref.Table) {
				found = true
				break
			}
		}
		if !found {
			dst.ForeignKeyReferences = append(dst.ForeignKeyReferences, ref)
		}
	}
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
	type baseTable struct {
		table *TableReplace
		db    *DBReplace
	}
	baseTableMap := make(map[UpstreamID]baseTable)
	// first pass: update all global IDs
	for upstreamID, baseDBReplace := range baseMap {
		tm.globalIdMap[upstreamID] = baseDBReplace.DbID

		for tableUpID, baseTableReplace := range baseDBReplace.TableMap {
			tm.globalIdMap[tableUpID] = baseTableReplace.TableID
			baseTableMap[tableUpID] = baseTable{table: baseTableReplace, db: baseDBReplace}

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
			// Keep the log scan's latest source (or explicitly routed target)
			// name. Snapshot metadata can be older than the latest log metadata.
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

			// A table can move between databases in the upstream history. Apply
			// its final snapshot target by table ID instead of requiring it to
			// remain under the same source DBReplace.
			if base, exists := baseTableMap[upTableID]; exists {
				if existingTableReplace.Name == "" && base.table.Name != "" {
					existingTableReplace.Name = base.table.Name
				}
				// TargetDBName marks an explicit route. The snapshot stage owns
				// the allocated downstream ID, but must not introduce a route for
				// an identity mapping or replace the route's stable target name.
				if existingTableReplace.TargetDBName != "" {
					existingTableReplace.TargetDBID = base.table.EffectiveDBID(base.db)
				}
				existingTableReplace.HasForeignKeys = existingTableReplace.HasForeignKeys || base.table.HasForeignKeys
				mergeForeignKeyReferences(existingTableReplace, base.table)
				existingTableReplace.IsView = existingTableReplace.IsView || base.table.IsView
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
					if existingTableReplace.Name == "" && baseTableReplace.Name != "" {
						existingTableReplace.Name = baseTableReplace.Name
					}
					if existingTableReplace.TargetDBName != "" {
						existingTableReplace.TargetDBID = baseTableReplace.EffectiveDBID(baseDBReplace)
					}
					existingTableReplace.HasForeignKeys = existingTableReplace.HasForeignKeys || baseTableReplace.HasForeignKeys
					mergeForeignKeyReferences(existingTableReplace, baseTableReplace)
					existingTableReplace.IsView = existingTableReplace.IsView || baseTableReplace.IsView
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
	if err := tm.assignSharedTargetDatabaseIDs(); err != nil {
		return err
	}

	// find actually used temporary IDs
	type tempIDOwner struct {
		kind string
		id   UpstreamID
		name string
	}
	usedTempIDs := make(map[DownstreamID]tempIDOwner)

	// helper function to check and add temporary ID
	addTempIDIfNeeded := func(downID DownstreamID, owner tempIDOwner) error {
		if downID < 0 {
			if previous, exists := usedTempIDs[downID]; exists {
				// A DBReplace and a table-level target database can be two
				// references to the same canonical schema alias.
				previousIsDatabase := previous.kind == "database" || previous.kind == "target database"
				ownerIsDatabase := owner.kind == "database" || owner.kind == "target database"
				if previous.name != "" && previous.name == owner.name && previousIsDatabase && ownerIsDatabase {
					return nil
				}
				return errors.Annotate(berrors.ErrRestoreInvalidRewrite,
					fmt.Sprintf("found duplicate temporary ID %d, existing owner: %+v, new owner: %+v",
						downID, previous, owner))
			}
			usedTempIDs[downID] = owner
		}
		return nil
	}

	// check DBReplaceMap for used temporary IDs
	// any value less than 0 is temporary ID
	for upDBId, dr := range tm.DBReplaceMap {
		if err := addTempIDIfNeeded(dr.DbID, tempIDOwner{
			kind: "database",
			id:   upDBId,
			name: ast.NewCIStr(dr.Name).L,
		}); err != nil {
			return err
		}
		for upTableID, tr := range dr.TableMap {
			if err := addTempIDIfNeeded(tr.TableID, tempIDOwner{kind: "table", id: upTableID}); err != nil {
				return err
			}
			if tr.TargetDBName != "" {
				if err := addTempIDIfNeeded(tr.TargetDBID, tempIDOwner{
					kind: "target database",
					name: ast.NewCIStr(tr.TargetDBName).L,
				}); err != nil {
					return err
				}
			}
			for upPartID, partID := range tr.PartitionMap {
				if err := addTempIDIfNeeded(partID, tempIDOwner{kind: "partition", id: upPartID}); err != nil {
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
			if newID, exists := idMapping[tr.TargetDBID]; exists {
				tr.TargetDBID = newID
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

// assignSharedTargetDatabaseIDs makes every table route and DBReplace that name
// the same target schema share one downstream database ID. A unique positive ID
// takes precedence over temporary IDs. Multiple positive IDs are incompatible;
// multiple temporary IDs are normalized deterministically.
func (tm *TableMappingManager) assignSharedTargetDatabaseIDs() error {
	activeTargets := make(map[string]struct{})
	for _, dbReplace := range tm.DBReplaceMap {
		if dbReplace.FilteredOut {
			continue
		}
		for _, tableReplace := range dbReplace.TableMap {
			if tableReplace.FilteredOut || tableReplace.TargetDBName == "" {
				continue
			}
			activeTargets[ast.NewCIStr(tableReplace.TargetDBName).L] = struct{}{}
		}
	}

	targetNames := make([]string, 0, len(activeTargets))
	for name := range activeTargets {
		targetNames = append(targetNames, name)
	}
	sort.Strings(targetNames)

	targetIDs := make(map[string]DownstreamID, len(targetNames))
	for _, name := range targetNames {
		positiveIDs := make(map[DownstreamID]struct{})
		negativeIDs := make(map[DownstreamID]struct{})
		addCandidate := func(id DownstreamID) {
			switch {
			case id > 0:
				positiveIDs[id] = struct{}{}
			case id < 0:
				negativeIDs[id] = struct{}{}
			}
		}

		for _, dbReplace := range tm.DBReplaceMap {
			if ast.NewCIStr(dbReplace.Name).L == name {
				addCandidate(dbReplace.DbID)
			}
			for _, tableReplace := range dbReplace.TableMap {
				if ast.NewCIStr(tableReplace.TargetDBName).L == name {
					addCandidate(tableReplace.TargetDBID)
				}
			}
		}

		if len(positiveIDs) > 1 {
			ids := make([]DownstreamID, 0, len(positiveIDs))
			for id := range positiveIDs {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			return errors.Annotatef(berrors.ErrRestoreInvalidRewrite,
				"target database %s has conflicting downstream IDs %d and %d", name, ids[0], ids[1])
		}
		for id := range positiveIDs {
			targetIDs[name] = id
		}
		if targetIDs[name] != 0 {
			continue
		}
		for id := range negativeIDs {
			if targetIDs[name] == 0 || id > targetIDs[name] {
				targetIDs[name] = id
			}
		}
		if targetIDs[name] == 0 {
			targetIDs[name] = tm.generateTempID()
		}
	}

	for upstreamDBID, dbReplace := range tm.DBReplaceMap {
		if id, exists := targetIDs[ast.NewCIStr(dbReplace.Name).L]; exists {
			dbReplace.DbID = id
			tm.globalIdMap[upstreamDBID] = id
		}
		for _, tableReplace := range dbReplace.TableMap {
			if id, exists := targetIDs[ast.NewCIStr(tableReplace.TargetDBName).L]; exists {
				tableReplace.TargetDBID = id
			}
		}
	}
	return nil
}

// TargetDatabase describes a table-level target schema that is independent of
// the source database's DBInfo lifecycle.
type TargetDatabase struct {
	Name string
	ID   DownstreamID
}

// TableRoute is the stable downstream identity bound to one upstream table ID.
// A table can appear under more than one source DBReplace after an upstream
// cross-schema rename, but every occurrence must resolve to this same route.
type TableRoute struct {
	TargetDBName    string
	TargetDBID      DownstreamID
	TargetTableName string
	TargetTableID   DownstreamID
}

// LookupTableRoute finds the route for an upstream table ID across all source
// database buckets. It returns an error if history merging left inconsistent
// routes for the same stable table ID.
func (tm *TableMappingManager) LookupTableRoute(upstreamTableID UpstreamID) (TableRoute, bool, error) {
	var result TableRoute
	found := false
	for _, dbReplace := range tm.DBReplaceMap {
		tableReplace, exists := dbReplace.TableMap[upstreamTableID]
		if !exists || tableReplace.FilteredOut {
			continue
		}
		candidate := TableRoute{
			TargetDBName:    tableReplace.EffectiveDBName(dbReplace),
			TargetDBID:      tableReplace.EffectiveDBID(dbReplace),
			TargetTableName: tableReplace.Name,
			TargetTableID:   tableReplace.TableID,
		}
		if !found {
			result = candidate
			found = true
			continue
		}
		if ast.NewCIStr(result.TargetDBName).L != ast.NewCIStr(candidate.TargetDBName).L ||
			ast.NewCIStr(result.TargetTableName).L != ast.NewCIStr(candidate.TargetTableName).L ||
			result.TargetDBID != candidate.TargetDBID || result.TargetTableID != candidate.TargetTableID {
			return TableRoute{}, false, errors.Annotatef(berrors.ErrRestoreInvalidRewrite,
				"upstream table %d has inconsistent downstream routes %+v and %+v",
				upstreamTableID, result, candidate)
		}
	}
	return result, found, nil
}

// ValidateRoutedDependencies preserves the conservative behavior for persisted
// mappings that do not carry source FK reference names. The flags are
// accumulated while scanning log metadata and persisted in the PiTR ID map so
// phased restores make the same decision.
func (tm *TableMappingManager) ValidateRoutedDependencies() error {
	return tm.validateRoutedDependencies(nil)
}

// ValidateRoutedDependenciesWithRoute applies the first-stage FK rename
// policy with source-name route information. It allows an identity-mapped
// table whose FK references are also identity-mapped, while retaining the
// conservative rejection for routed tables and old ID maps without reference
// details.
func (tm *TableMappingManager) ValidateRoutedDependenciesWithRoute(
	route func(schema, table string) (targetSchema, targetTable string, matched bool),
) error {
	return tm.validateRoutedDependencies(route)
}

func (tm *TableMappingManager) validateRoutedDependencies(
	route func(schema, table string) (targetSchema, targetTable string, matched bool),
) error {
	hasTableRoute := false
	for _, dbReplace := range tm.DBReplaceMap {
		if dbReplace.FilteredOut {
			continue
		}
		for _, tableReplace := range dbReplace.TableMap {
			if !tableReplace.FilteredOut && tableReplace.TargetDBName != "" {
				hasTableRoute = true
				break
			}
		}
		if hasTableRoute {
			break
		}
	}
	if !hasTableRoute {
		return nil
	}

	for upstreamDBID, dbReplace := range tm.DBReplaceMap {
		if dbReplace.FilteredOut {
			continue
		}
		for upstreamTableID, tableReplace := range dbReplace.TableMap {
			if tableReplace.FilteredOut {
				continue
			}
			if tableReplace.IsView {
				return errors.Annotatef(berrors.ErrInvalidArgument,
					"restore rename does not support routed view with upstream database ID %d and table ID %d",
					upstreamDBID, upstreamTableID)
			}
			if tableReplace.HasForeignKeys {
				// Old persisted maps only retain the boolean flag. Keep their
				// previous fail-fast behavior because the referenced source names
				// are unavailable for a safe route check.
				if route == nil || len(tableReplace.ForeignKeyReferences) == 0 {
					return errors.Annotatef(berrors.ErrInvalidArgument,
						"restore rename does not support routed table with foreign keys, upstream database ID %d and table ID %d",
						upstreamDBID, upstreamTableID)
				}
				for _, ref := range tableReplace.ForeignKeyReferences {
					targetSchema, targetTable, matched := route(ref.Schema, ref.Table)
					if matched && (!strings.EqualFold(targetSchema, ref.Schema) || !strings.EqualFold(targetTable, ref.Table)) {
						return errors.Annotatef(berrors.ErrInvalidArgument,
							"restore rename does not support selected table with foreign key reference %s.%s",
							ref.Schema, ref.Table)
					}
				}
				continue
			}
		}
	}
	return nil
}

// TableRouteTargetDatabases returns the distinct target schemas that must exist
// before replaying table metadata. A same-schema table rename falls back to its
// parent DBReplace and therefore doesn't need a separately managed schema.
func (tm *TableMappingManager) TableRouteTargetDatabases() ([]TargetDatabase, error) {
	targets := make(map[string]TargetDatabase)
	for _, dbReplace := range tm.DBReplaceMap {
		if dbReplace.FilteredOut {
			continue
		}
		for _, tableReplace := range dbReplace.TableMap {
			if tableReplace.FilteredOut || tableReplace.TargetDBName == "" {
				continue
			}
			if tableReplace.TargetDBID == dbReplace.DbID &&
				ast.NewCIStr(tableReplace.TargetDBName).L == ast.NewCIStr(dbReplace.Name).L {
				continue
			}
			name := ast.NewCIStr(tableReplace.TargetDBName).L
			if existing, ok := targets[name]; ok && existing.ID != tableReplace.TargetDBID {
				return nil, errors.Annotatef(berrors.ErrRestoreInvalidRewrite,
					"target database %s has conflicting downstream IDs %d and %d",
					tableReplace.TargetDBName, existing.ID, tableReplace.TargetDBID)
			}
			targets[name] = TargetDatabase{Name: tableReplace.TargetDBName, ID: tableReplace.TargetDBID}
		}
	}

	result := make([]TargetDatabase, 0, len(targets))
	for _, target := range targets {
		if target.ID <= 0 {
			return nil, errors.Annotatef(berrors.ErrRestoreInvalidRewrite,
				"target database %s has unresolved downstream ID %d", target.Name, target.ID)
		}
		result = append(result, target)
	}
	sort.Slice(result, func(i, j int) bool {
		return ast.NewCIStr(result[i].Name).L < ast.NewCIStr(result[j].Name).L
	})
	return result, nil
}

// RebindTableRouteTargetDatabaseID replaces the provisional downstream ID of
// every database alias and table route for targetName with the ID assigned by
// CREATE DATABASE.
func (tm *TableMappingManager) RebindTableRouteTargetDatabaseID(
	targetName string,
	expectedID, actualID DownstreamID,
) error {
	name := ast.NewCIStr(targetName).L
	foundRoute := false
	checkID := func(currentID DownstreamID) error {
		if currentID != expectedID && currentID != actualID {
			return errors.Annotatef(berrors.ErrRestoreInvalidRewrite,
				"target database %s has downstream ID %d, expected provisional ID %d or actual ID %d",
				targetName, currentID, expectedID, actualID)
		}
		return nil
	}
	for _, dbReplace := range tm.DBReplaceMap {
		if ast.NewCIStr(dbReplace.Name).L == name {
			if err := checkID(dbReplace.DbID); err != nil {
				return err
			}
		}
		for _, tableReplace := range dbReplace.TableMap {
			if ast.NewCIStr(tableReplace.TargetDBName).L != name {
				continue
			}
			if err := checkID(tableReplace.TargetDBID); err != nil {
				return err
			}
			foundRoute = true
		}
	}
	if !foundRoute {
		return errors.Annotatef(berrors.ErrRestoreInvalidRewrite,
			"target database %s has no routed tables", targetName)
	}
	for upstreamDBID, dbReplace := range tm.DBReplaceMap {
		if ast.NewCIStr(dbReplace.Name).L == name {
			dbReplace.DbID = actualID
			tm.globalIdMap[upstreamDBID] = actualID
		}
		for _, tableReplace := range dbReplace.TableMap {
			if ast.NewCIStr(tableReplace.TargetDBName).L == name {
				tableReplace.TargetDBID = actualID
			}
		}
	}
	return nil
}

func (tm *TableMappingManager) ReuseExistingDatabaseIDs(infoschema infoschema.InfoSchema) {
	for dbID, dbReplace := range tm.DBReplaceMap {
		if dbReplace.FilteredOut {
			continue
		}
		if dbReplace.DbID <= 0 {
			if dbInfo, exists := infoschema.SchemaByName(ast.NewCIStr(dbReplace.Name)); exists {
				dbReplace.DbID = dbInfo.ID
				dbReplace.Reused = true
				log.Info("reuse existing database id",
					zap.String("db-name", dbReplace.Name),
					zap.Int64("upstream-db-id", dbID),
					zap.Int64("downstream-db-id", dbReplace.DbID))
			}
		}

		for _, tableReplace := range dbReplace.TableMap {
			if tableReplace.FilteredOut || tableReplace.TargetDBName == "" || tableReplace.TargetDBID > 0 {
				continue
			}
			if dbInfo, exists := infoschema.SchemaByName(ast.NewCIStr(tableReplace.TargetDBName)); exists {
				tableReplace.TargetDBID = dbInfo.ID
			}
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
				Name:             tr.Name,
				DownstreamDbId:   tr.TargetDBID,
				DownstreamDbName: tr.TargetDBName,
				HasForeignKeys:   tr.HasForeignKeys,
				IsView:           tr.IsView,
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
			tr.TargetDBID = tbl.DownstreamDbId
			tr.TargetDBName = tbl.DownstreamDbName
			tr.HasForeignKeys = tbl.HasForeignKeys
			tr.IsView = tbl.IsView
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
func (tm *TableMappingManager) UpdateDownstreamIds(dbs []*restoreutils.DatabaseRestorePlan, tables []*restoreutils.CreatedTable,
	dom *domain.Domain) error {
	dbReplaces := make(map[UpstreamID]*DBReplace)
	resolvedTargetDBs := make(map[string]*model.DBInfo, len(dbs))
	sourceDBReused := make(map[UpstreamID]bool, len(dbs))

	for _, dbPlan := range dbs {
		newDBInfo, exists := dom.InfoSchema().SchemaByName(dbPlan.Target.Name)
		if !exists {
			return errors.New("db not exist in snapshot stage UpdateDownstreamIds")
		}
		upstreamDBID := dbPlan.Source.Info.ID
		resolvedTargetDBs[newDBInfo.Name.L] = newDBInfo
		sourceDBReused[upstreamDBID] = sourceDBReused[upstreamDBID] || dbPlan.Source.IsReusedByPITR()
		dbReplace, exist := dbReplaces[upstreamDBID]
		if !exist {
			if existing, ok := tm.DBReplaceMap[upstreamDBID]; ok {
				dbReplace = NewDBReplace(existing.Name, existing.DbID)
				dbReplace.Reused = existing.Reused
			} else {
				dbReplace = NewDBReplace(newDBInfo.Name.O, newDBInfo.ID)
			}
			dbReplaces[upstreamDBID] = dbReplace
		}
	}

	// applyNameRoutesToTableMapping has already bound a schema route by setting
	// DBReplace.Name to its target, while an exact-table-only route deliberately
	// keeps the source parent name. Resolve every parent whose bound name matches
	// a created target. This also covers multiple source schemas merged into one
	// target even though the snapshot create plan contains that target only once.
	for upstreamDBID, existing := range tm.DBReplaceMap {
		newDBInfo, mappedParent := resolvedTargetDBs[ast.NewCIStr(existing.Name).L]
		if !mappedParent {
			continue
		}
		dbReplace, exists := dbReplaces[upstreamDBID]
		if !exists {
			dbReplace = NewDBReplace(existing.Name, existing.DbID)
			dbReplace.Reused = existing.Reused
			dbReplaces[upstreamDBID] = dbReplace
		}
		dbReplace.Name = newDBInfo.Name.O
		dbReplace.DbID = newDBInfo.ID
		dbReplace.Reused = dbReplace.Reused || sourceDBReused[upstreamDBID]
		// MergeBaseDBReplace intentionally preserves a non-empty name from the
		// log scan. Once the bound parent is resolved, normalize that persisted
		// route to the actual downstream schema identity before merging.
		existing.Name = newDBInfo.Name.O
		existing.DbID = newDBInfo.ID
		if dbReplace.Reused {
			log.Info("the database is reused by snapshot restore",
				zap.Stringer("db", newDBInfo.Name),
				zap.Int64("upstream-db-id", upstreamDBID),
				zap.Int64("downstream-db-id", dbReplace.DbID))
		}
	}
	// Snapshot DDL resolves target schema IDs before the log phase allocates
	// remaining temporary IDs. Propagate those real IDs to every routed table,
	// including log-only tables that have no CreatedTable entry below.
	for _, dbReplace := range tm.DBReplaceMap {
		for _, tableReplace := range dbReplace.TableMap {
			if tableReplace.TargetDBName == "" {
				continue
			}
			if targetDB, ok := resolvedTargetDBs[ast.NewCIStr(tableReplace.TargetDBName).L]; ok {
				tableReplace.TargetDBID = targetDB.ID
			}
		}
	}

	for _, t := range tables {
		oldTable := t.OldTable
		newTable := t.Table
		targetDBName := t.TargetDBName()
		newDBInfo, exists := dom.InfoSchema().SchemaByName(targetDBName)
		if !exists {
			return errors.Errorf("target db %s does not exist in snapshot stage UpdateDownstreamIds", targetDBName.O)
		}

		dbReplace, exist := dbReplaces[oldTable.DB.ID]
		if !exist {
			existing, ok := tm.DBReplaceMap[oldTable.DB.ID]
			if !ok {
				return errors.New("table exists but db not exist in UpdateDownstreamIds")
			}
			dbReplace = NewDBReplace(existing.Name, existing.DbID)
			dbReplace.Reused = existing.Reused
			dbReplaces[oldTable.DB.ID] = dbReplace
		}

		dbReplace.TableMap[oldTable.Info.ID] = &TableReplace{
			Name:         newTable.Name.O,
			TableID:      newTable.ID,
			PartitionMap: restoreutils.GetPartitionIDMap(newTable, oldTable.Info),
		}
		// An explicit route is already bound to the stable upstream table ID
		// before snapshot restore. Preserve that marker in the snapshot base map
		// so MergeBaseDBReplace can attach the actual target database ID without
		// pinning identity routes to snapshot-era names.
		if route, routed := tm.explicitTableRoute(oldTable.Info.ID); routed {
			dbReplace.TableMap[oldTable.Info.ID].TargetDBName = route.TargetDBName
			dbReplace.TableMap[oldTable.Info.ID].TargetDBID = newDBInfo.ID
		}
	}
	tm.MergeBaseDBReplace(dbReplaces)
	return nil
}

func (tm *TableMappingManager) explicitTableRoute(upstreamTableID UpstreamID) (TableRoute, bool) {
	for _, dbReplace := range tm.DBReplaceMap {
		tableReplace, exists := dbReplace.TableMap[upstreamTableID]
		if !exists || tableReplace.FilteredOut || tableReplace.TargetDBName == "" {
			continue
		}
		return TableRoute{
			TargetDBName:    tableReplace.TargetDBName,
			TargetDBID:      tableReplace.TargetDBID,
			TargetTableName: tableReplace.Name,
			TargetTableID:   tableReplace.TableID,
		}, true
	}
	return TableRoute{}, false
}

// SetPreallocatedRange sets the preallocated ID range from snapshot restore
// This range will be used for fine-grained scheduler pausing during log restore
func (tm *TableMappingManager) SetPreallocatedRange(start, end int64) {
	tm.PreallocatedRange = [2]int64{start, end}
	log.Info("set preallocated range for scheduler pausing",
		zap.Int64("start", start),
		zap.Int64("end", end))
}
