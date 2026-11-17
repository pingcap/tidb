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
	info  *model.DBInfo
	count int
}

type tableMetaValue struct {
	info  *tableSimpleInfo
	count int
}

// metadataEvent is the latest committed state of one source metadata key.
// Rollbacks and default-CF values never establish an object's lifetime.
type metadataEvent struct {
	ts      uint64
	deleted bool
}

type sourceTableLocation struct {
	dbID int64
	name string
	ts   uint64
}

type tableLocationKey struct {
	dbID    int64
	tableID int64
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

	// These maps describe the scanned interval independently of routed names
	// and of downstream metadata changed by a partially completed restore.
	databaseEvents     map[UpstreamID]metadataEvent
	tableEvents        map[tableLocationKey]metadataEvent
	latestTableSources map[UpstreamID]sourceTableLocation
	baseTableIDs       map[UpstreamID]struct{}

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

func extractDBInfo(value []byte) (*model.DBInfo, error) {
	dbInfo := new(model.DBInfo)
	if err := json.Unmarshal(value, dbInfo); err != nil {
		return nil, errors.Trace(err)
	}
	return dbInfo, nil
}

func (tm *TableMappingManager) parseDBValueAndUpdateIdMappingForDefaultCf(
	dbId int64, value []byte, startTs uint64) error {
	dbInfo, err := extractDBInfo(value)
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
		info:  dbInfo,
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
		if rawWriteCFValue.IsDelete() {
			tm.recordDatabaseEvent(dbId, commitTs, true)
		}
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
		dbInfo, err := extractDBInfo(dbValue)
		if err != nil {
			return errors.Trace(err)
		}
		return tm.parseDBValueAndUpdateIdMapping(dbId, dbInfo, commitTs, collector)
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
		return tm.parseDBValueAndUpdateIdMapping(dbId, dbValue.info, commitTs, collector)
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
	dbId int64, dbInfo *model.DBInfo, commitTs uint64, collector MetaInfoCollector) error {
	dbReplace, err := tm.getOrCreateDBReplace(dbId)
	if err != nil {
		return errors.Trace(err)
	}
	dbName := ""
	if dbInfo != nil {
		dbName = dbInfo.Name.O
	}
	if dbName != "" {
		dbReplace.Name = dbName
		dbReplace.SourceName = dbName
		dbReplace.SourceDBInfo = dbInfo
		normalizeForeignKeyReferences(dbReplace)
	}
	tm.recordDatabaseEvent(dbId, commitTs, false)
	collector.OnDatabaseInfo(dbId, dbName, commitTs)
	return nil
}

// normalizeForeignKeyReferences fills same-schema FK references after the
// database metadata becomes available. Table metadata can be observed before
// its DBInfo in the log stream, so references initially collected without a
// schema must not remain unroutable.
func normalizeForeignKeyReferences(dbReplace *DBReplace) {
	for _, tableReplace := range dbReplace.TableMap {
		seen := make(map[string]struct{}, len(tableReplace.ForeignKeyReferences))
		references := tableReplace.ForeignKeyReferences[:0]
		for _, ref := range tableReplace.ForeignKeyReferences {
			if ref.Schema == "" {
				ref.Schema = dbReplace.SourceName
				if ref.Schema == "" && !dbReplace.SchemaRouted {
					ref.Schema = dbReplace.Name
				}
			}
			key := strings.ToLower(ref.Schema) + "\x00" + strings.ToLower(ref.Table)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			references = append(references, ref)
		}
		tableReplace.ForeignKeyReferences = references
	}
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
		if rawWriteCFValue.IsDelete() {
			tm.recordTableEvent(dbId, tableId, commitTs, true)
		}
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
		tableReplace.SourceName = tableSimpleInfo.Name
		if tm.latestTableSources == nil {
			tm.latestTableSources = make(map[UpstreamID]sourceTableLocation)
		}
		if previous, ok := tm.latestTableSources[tableId]; !ok || commitTs >= previous.ts {
			tm.latestTableSources[tableId] = sourceTableLocation{dbID: dbId, name: tableSimpleInfo.Name, ts: commitTs}
		}
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
	tm.recordTableEvent(dbId, tableId, commitTs, false)
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
	if tm.baseTableIDs == nil {
		tm.baseTableIDs = make(map[UpstreamID]struct{})
	}
	// first pass: update all global IDs
	for upstreamID, baseDBReplace := range baseMap {
		tm.globalIdMap[upstreamID] = baseDBReplace.DbID

		for tableUpID, baseTableReplace := range baseDBReplace.TableMap {
			tm.baseTableIDs[tableUpID] = struct{}{}
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
			if existingDBReplace.SourceName == "" {
				existingDBReplace.SourceName = baseDBReplace.SourceName
			}
			existingDBReplace.SchemaRouted = existingDBReplace.SchemaRouted || baseDBReplace.SchemaRouted
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
				if existingTableReplace.SourceName == "" {
					existingTableReplace.SourceName = base.table.SourceName
				}
				// A route persisted in the ID map is the table's stable downstream
				// identity. The log scan can lose it (a table DDL cannot name its
				// schema) or re-resolve it to a different rule after an upstream
				// rename, so adopt the persisted target name and database ID as one
				// unit instead of mixing them with the scanned target.
				if base.table.TargetDBName != "" {
					existingTableReplace.TargetDBName = base.table.TargetDBName
					existingTableReplace.Name = base.table.Name
					existingTableReplace.TargetDBID = base.table.EffectiveDBID(base.db)
				} else if existingTableReplace.Name == "" && base.table.Name != "" {
					existingTableReplace.Name = base.table.Name
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
					if baseTableReplace.TargetDBName != "" {
						existingTableReplace.TargetDBName = baseTableReplace.TargetDBName
						existingTableReplace.Name = baseTableReplace.Name
						existingTableReplace.TargetDBID = baseTableReplace.EffectiveDBID(baseDBReplace)
					} else if existingTableReplace.Name == "" && baseTableReplace.Name != "" {
						existingTableReplace.Name = baseTableReplace.Name
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
	for _, dbReplace := range tm.DBReplaceMap {
		normalizeForeignKeyReferences(dbReplace)
	}
}

// SetSourceDatabaseName seeds a selected snapshot schema even when the log
// interval contains no DBInfo or table metadata for it.
func (tm *TableMappingManager) SetSourceDatabaseName(dbID UpstreamID, name string) error {
	dbReplace, err := tm.getOrCreateDBReplace(dbID)
	if err != nil {
		return err
	}
	dbReplace.SourceName = name
	if dbReplace.Name == "" {
		dbReplace.Name = name
	}
	normalizeForeignKeyReferences(dbReplace)
	return nil
}

func (tm *TableMappingManager) recordDatabaseEvent(dbID UpstreamID, ts uint64, deleted bool) {
	if tm.databaseEvents == nil {
		tm.databaseEvents = make(map[UpstreamID]metadataEvent)
	}
	if previous, ok := tm.databaseEvents[dbID]; !ok || ts >= previous.ts {
		tm.databaseEvents[dbID] = metadataEvent{ts: ts, deleted: deleted}
	}
}

// IsSourceDatabaseDeleted reports a committed Delete at the end of the scanned
// interval. Snapshot containers may still be needed for historical table data,
// but an expired schema must not contribute an independent creation plan.
func (tm *TableMappingManager) IsSourceDatabaseDeleted(dbID UpstreamID) bool {
	return tm.databaseEvents[dbID].deleted
}

func (tm *TableMappingManager) recordTableEvent(dbID, tableID UpstreamID, ts uint64, deleted bool) {
	if tm.tableEvents == nil {
		tm.tableEvents = make(map[tableLocationKey]metadataEvent)
	}
	key := tableLocationKey{dbID: dbID, tableID: tableID}
	if previous, ok := tm.tableEvents[key]; !ok || ts >= previous.ts {
		tm.tableEvents[key] = metadataEvent{ts: ts, deleted: deleted}
	}
}

// BindUnresolvedTableRoutes binds new upstream table IDs after the base ID map
// supplies source schema names missing from table-only log intervals. Historical
// database buckets use the table's latest source location, not their own name.
// Persisted table identities are left intact.
func (tm *TableMappingManager) BindUnresolvedTableRoutes(
	route func(schema, table string) (targetSchema, targetTable string, matched bool),
	hasRules bool,
) error {
	if !hasRules {
		return nil
	}
	for dbID, dbReplace := range tm.DBReplaceMap {
		if dbReplace.FilteredOut {
			continue
		}
		for tableID, tableReplace := range dbReplace.TableMap {
			_, persisted := tm.baseTableIDs[tableID]
			_, observed := tm.latestTableSources[tableID]
			if !persisted && !observed && tableReplace.SourceName == "" && tableReplace.Name == "" {
				// Auto-ID and delete-only metadata can allocate placeholders for
				// tables excluded from the baseline. Without a TableInfo in the
				// interval, there is no new table to bind or restore.
				tableReplace.FilteredOut = true
				continue
			}
			if tableReplace.FilteredOut || tableReplace.TargetDBName != "" {
				continue
			}
			if persisted {
				continue
			}
			sourceDBID, sourceName := dbID, tableReplace.SourceName
			if latest, ok := tm.latestTableSources[tableID]; ok {
				sourceDBID, sourceName = latest.dbID, latest.name
			}
			sourceDB := tm.DBReplaceMap[sourceDBID]
			// Legacy maps may have a table-route target in the parent's Name
			// even without SchemaRouted. Only SourceName or scanned DBInfo can
			// prove source identity; never infer it from a persisted target.
			if sourceDB == nil || sourceDB.SourceName == "" || sourceName == "" {
				return errors.Annotatef(berrors.ErrInvalidArgument,
					"cannot resolve source identity for upstream database ID %d and table ID %d "+
						"(observed table name %q) with restore rename rules; "+
						"the baseline must preserve source_name or the interval must contain DBInfo",
					sourceDBID, tableID, sourceName)
			}
			targetDB, targetTable, matched := route(sourceDB.SourceName, sourceName)
			if !matched {
				continue
			}
			tableReplace.Name = targetTable
			tableReplace.TargetDBName = targetDB
			// Resolve the ID for the newly bound name; a previous parent ID may
			// belong to a different schema.
			tableReplace.TargetDBID = 0
			if strings.EqualFold(targetDB, dbReplace.Name) {
				tableReplace.TargetDBID = dbReplace.DbID
			}
		}
		if dbReplace.Name == "" && dbReplace.SourceName == "" {
			// A nameless parent containing only excluded placeholders has no
			// database metadata to refresh after the replay either.
			dbReplace.FilteredOut = true
			for _, tableReplace := range dbReplace.TableMap {
				if !tableReplace.FilteredOut {
					dbReplace.FilteredOut = false
					break
				}
			}
		}
	}
	// Different source locations of one routed table can collapse to one
	// downstream metadata key. Keep its latest Put, not the old location's
	// relocation Delete, regardless of their order within the log batch.
	for dbID, dbReplace := range tm.DBReplaceMap {
		for tableID, tableReplace := range dbReplace.TableMap {
			tableReplace.SkipMetaDelete = false
			latest, ok := tm.latestTableSources[tableID]
			if !ok || latest.dbID == dbID || tableReplace.FilteredOut || dbReplace.FilteredOut {
				continue
			}
			latestDB := tm.DBReplaceMap[latest.dbID]
			if latestDB == nil || latestDB.FilteredOut {
				continue
			}
			latestTable := latestDB.TableMap[tableID]
			if latestTable == nil || latestTable.FilteredOut || latestTable.TableID != tableReplace.TableID {
				continue
			}
			name := tableReplace.EffectiveDBName(dbReplace)
			tableReplace.SkipMetaDelete = name != "" && strings.EqualFold(name, latestTable.EffectiveDBName(latestDB))
		}
	}
	return nil
}

// validateUniqueTargetDatabaseNames checks independent schema owners, not
// containers shared only by table routes. A committed Delete expires a source
// schema; an unobserved baseline owner participates only while its saved target
// schema still exists with the saved ID. Persisted aliases of the same source
// schema share one owner, but an unknown source is never inferred from Name.
func (tm *TableMappingManager) validateUniqueTargetDatabaseNames(infoSchema infoschema.InfoSchema) error {
	type schemaOwner struct {
		id         UpstreamID
		sourceName string
	}
	owners := make(map[string]schemaOwner)
	for dbID, dbReplace := range tm.DBReplaceMap {
		if dbReplace.Name == "" || !dbReplace.RestoresDatabaseMetadata() || tm.IsSourceDatabaseDeleted(dbID) {
			continue
		}
		if _, observed := tm.databaseEvents[dbID]; !observed {
			if infoSchema == nil {
				continue
			}
			dbInfo, exists := infoSchema.SchemaByName(ast.NewCIStr(dbReplace.Name))
			if !exists || dbInfo.ID != dbReplace.DbID {
				continue
			}
		}
		targetName := strings.ToLower(dbReplace.Name)
		sourceName := strings.ToLower(dbReplace.SourceName)
		if previous, exists := owners[targetName]; exists {
			if sourceName == "" || previous.sourceName == "" || sourceName != previous.sourceName {
				return errors.Annotatef(berrors.ErrInvalidArgument,
					"upstream database IDs %d (%q) and %d (%q) survive at the same restore target database %s",
					previous.id, previous.sourceName, dbID, sourceName, targetName)
			}
			continue
		}
		owners[targetName] = schemaOwner{id: dbID, sourceName: sourceName}
	}
	return nil
}

// ValidateUniqueTargetNames rejects different surviving upstream objects with
// one effective target name. Schema owners are checked before ID reuse, even
// when their tables have distinct names or all use table-level overrides.
// Committed deletes remove historical table locations; unchanged baseline
// tables are live only if their downstream table exists. Source-name equality
// never substitutes for a committed table deletion.
func (tm *TableMappingManager) ValidateUniqueTargetNames(ctx context.Context, infoSchema infoschema.InfoSchema) error {
	if err := tm.validateUniqueTargetDatabaseNames(infoSchema); err != nil {
		return err
	}
	type targetKey struct {
		db    string
		table string
	}
	targets := make(map[targetKey]UpstreamID)
	identities := make(map[UpstreamID]targetKey)
	for dbID, dbReplace := range tm.DBReplaceMap {
		if dbReplace.FilteredOut {
			continue
		}
		for tableID, tableReplace := range dbReplace.TableMap {
			if tableReplace.FilteredOut {
				continue
			}
			event, scanned := tm.tableEvents[tableLocationKey{dbID: dbID, tableID: tableID}]
			dbEvent := tm.databaseEvents[dbID]
			if (scanned && event.deleted) || (dbEvent.deleted && (!scanned || dbEvent.ts >= event.ts)) {
				continue
			}
			if !scanned {
				if infoSchema == nil {
					continue
				}
				tbl, exists := infoSchema.TableByID(ctx, tableReplace.TableID)
				if !exists || tbl.Meta().DBID != tableReplace.EffectiveDBID(dbReplace) {
					continue
				}
			}
			target := targetKey{
				db: strings.ToLower(tableReplace.EffectiveDBName(dbReplace)), table: strings.ToLower(tableReplace.Name),
			}
			if previous, ok := identities[tableID]; ok && previous != target {
				return errors.Annotatef(berrors.ErrInvalidArgument,
					"upstream table ID %d has conflicting target identities %s.%s and %s.%s",
					tableID, previous.db, previous.table, target.db, target.table)
			}
			if previousID, ok := targets[target]; ok && previousID != tableID {
				return errors.Annotatef(berrors.ErrInvalidArgument,
					"upstream table IDs %d and %d survive at the same restore target %s.%s",
					previousID, tableID, target.db, target.table)
			}
			identities[tableID], targets[target] = target, tableID
		}
	}
	return nil
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
				// The same upstream object can legitimately appear in more than one
				// DBReplace after an upstream RENAME/EXCHANGE, and a partition can
				// also be observed as a standalone table entry (same upstream ID,
				// different kind). Both share one downstream ID, so tolerate any
				// repeat of the same non-zero upstream ID.
				if owner.id != 0 && previous.id == owner.id {
					return nil
				}
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
		if name, routed := dbReplace.SchemaRouteTarget(); routed {
			activeTargets[ast.NewCIStr(name).L] = struct{}{}
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
	// SourceDBInfo is the metadata of the source schema that owns this target.
	// It is used to create the target schema instead of relying on defaults. It
	// is nil for legacy persisted maps that don't carry source DBInfo.
	SourceDBInfo *model.DBInfo
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
//
// When several source schemas route tables into the same target schema, their
// database-level settings must be compatible; otherwise this returns an error
// instead of silently picking defaults. A deterministic winner (by source
// schema name/id) provides the target metadata.
func (tm *TableMappingManager) TableRouteTargetDatabases() ([]TargetDatabase, error) {
	targets := make(map[string]TargetDatabase)
	mergeTarget := func(name, displayName string, id DownstreamID, sourceDBInfo *model.DBInfo) error {
		target, ok := targets[name]
		if !ok {
			target = TargetDatabase{Name: displayName}
		}
		if target.ID != 0 && id != 0 && target.ID != id {
			return errors.Annotatef(berrors.ErrRestoreInvalidRewrite,
				"target database %s has conflicting downstream IDs %d and %d",
				displayName, target.ID, id)
		}
		if id != 0 {
			target.ID = id
		}
		if err := mergeTargetDBSourceInfo(&target, sourceDBInfo); err != nil {
			return err
		}
		targets[name] = target
		return nil
	}

	type schemaRouteOwner struct {
		sourceName string
		targetName string
	}
	ownerKey := func(dbReplace *DBReplace) schemaRouteOwner {
		return schemaRouteOwner{
			sourceName: ast.NewCIStr(dbReplace.SourceName).L,
			targetName: ast.NewCIStr(dbReplace.Name).L,
		}
	}
	liveOwners := make(map[schemaRouteOwner]struct{})
	for dbID, dbReplace := range tm.DBReplaceMap {
		if _, routed := dbReplace.SchemaRouteTarget(); routed && dbReplace.SourceName != "" &&
			!tm.IsSourceDatabaseDeleted(dbID) {
			liveOwners[ownerKey(dbReplace)] = struct{}{}
		}
	}

	for dbID, dbReplace := range tm.DBReplaceMap {
		if dbReplace.FilteredOut {
			continue
		}
		// A schema-level rename rule restores the schema itself, even when every
		// selected table is overridden to another target schema.
		if name, routed := dbReplace.SchemaRouteTarget(); routed {
			sourceDBInfo := dbReplace.SourceDBInfo
			if _, replaced := liveOwners[ownerKey(dbReplace)]; replaced && tm.IsSourceDatabaseDeleted(dbID) {
				// DROP/CREATE of the same source schema starts a new lifetime.
				// Its live owner supplies creation settings; the historical alias
				// still contributes its ID so conflicting mappings remain errors.
				sourceDBInfo = nil
			}
			if err := mergeTarget(ast.NewCIStr(name).L, name, dbReplace.DbID,
				sourceDBInfo); err != nil {
				return nil, err
			}
		}
		for tableID, tableReplace := range dbReplace.TableMap {
			if tableReplace.FilteredOut || tableReplace.TargetDBName == "" {
				continue
			}
			if tableReplace.TargetDBID == dbReplace.DbID &&
				ast.NewCIStr(tableReplace.TargetDBName).L == ast.NewCIStr(dbReplace.Name).L {
				continue
			}
			sourceDBInfo := dbReplace.SourceDBInfo
			if latest, ok := tm.latestTableSources[tableID]; ok {
				// Every historical alias rewrites to this table's stable target.
				// Only its latest parent owns the target creation settings. If
				// those settings are unavailable, never borrow an older parent's.
				sourceDBInfo = nil
				if sourceDB := tm.DBReplaceMap[latest.dbID]; sourceDB != nil {
					sourceDBInfo = sourceDB.SourceDBInfo
				}
			}
			if err := mergeTarget(ast.NewCIStr(tableReplace.TargetDBName).L, tableReplace.TargetDBName,
				tableReplace.TargetDBID, sourceDBInfo); err != nil {
				return nil, err
			}
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

// mergeTargetDBSourceInfo folds one source schema's metadata into a target
// schema. Sources routed to the same target must agree on charset, collation,
// and placement policy; the winner is deterministic so resumes and retries
// create the same target metadata.
func mergeTargetDBSourceInfo(target *TargetDatabase, sourceDBInfo *model.DBInfo) error {
	if sourceDBInfo == nil {
		return nil
	}
	if target.SourceDBInfo == nil {
		target.SourceDBInfo = sourceDBInfo
		return nil
	}
	if !restoreutils.DatabaseSettingsCompatible(target.SourceDBInfo, sourceDBInfo) {
		return errors.Annotatef(berrors.ErrRestoreInvalidRewrite,
			"source schemas %s and %s routed to target schema %s have incompatible charset, collation, or placement policy",
			target.SourceDBInfo.Name.O, sourceDBInfo.Name.O, target.Name)
	}
	if restoreutils.SourceDBPrecedes(sourceDBInfo, target.SourceDBInfo) {
		target.SourceDBInfo = sourceDBInfo
	}
	return nil
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
			// A schema-level route binds the target on the DBReplace itself rather
			// than on a table's TargetDBName.
			if dbReplace.SchemaRouted {
				foundRoute = true
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
			Name:       dr.Name,
			SourceName: dr.SourceName,
			IdMap: &backuppb.IDMap{
				UpstreamId:   dbID,
				DownstreamId: dr.DbID,
			},
			Tables:      make([]*backuppb.PitrTableMap, 0, len(dr.TableMap)),
			FilteredOut: dr.FilteredOut,
			// Persist whether the downstream database already existed, so a resumed
			// task keeps skipping its DBInfo replay instead of overwriting it.
			Reused: dr.Reused,
			// Persist whether an explicit schema-level route owns this schema, so a
			// resumed task keeps replaying its DBInfo even when all of its tables
			// are routed elsewhere.
			SchemaRouted: dr.SchemaRouted,
		}

		for tblID, tr := range dr.TableMap {
			tm := backuppb.PitrTableMap{
				Name:             tr.Name,
				DownstreamDbId:   tr.TargetDBID,
				DownstreamDbName: tr.TargetDBName,
				HasForeignKeys:   tr.HasForeignKeys,
				IsView:           tr.IsView,
				ForeignKeyReferences: func() []*backuppb.PitrForeignKeyReference {
					refs := make([]*backuppb.PitrForeignKeyReference, 0, len(tr.ForeignKeyReferences))
					for _, ref := range tr.ForeignKeyReferences {
						refs = append(refs, &backuppb.PitrForeignKeyReference{Schema: ref.Schema, Table: ref.Table})
					}
					return refs
				}(),
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
		dr.SourceName = db.SourceName
		dr.FilteredOut = db.FilteredOut
		dr.Reused = db.Reused
		dr.SchemaRouted = db.SchemaRouted
		dbReplaces[db.IdMap.UpstreamId] = dr

		for _, tbl := range db.Tables {
			tr := NewTableReplace(tbl.Name, tbl.IdMap.DownstreamId)
			tr.TargetDBID = tbl.DownstreamDbId
			tr.TargetDBName = tbl.DownstreamDbName
			tr.HasForeignKeys = tbl.HasForeignKeys
			tr.IsView = tbl.IsView
			for _, ref := range tbl.ForeignKeyReferences {
				if ref != nil {
					tr.ForeignKeyReferences = append(tr.ForeignKeyReferences,
						ForeignKeyReference{Schema: ref.Schema, Table: ref.Table})
				}
			}
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
func (tm *TableMappingManager) UpdateDownstreamIds(
	dbs []*restoreutils.DatabaseRestorePlan, tables []*restoreutils.CreatedTable,
	dom *domain.Domain) error {
	dbReplaces := make(map[UpstreamID]*DBReplace)
	resolvedTargetDBs := make(map[string]*model.DBInfo, len(dbs))
	// Reuse is a property of the target schema, not of a source database: one
	// source database can be split across several target schemas. Key it by the
	// resolved target name so a reused sibling target cannot mark the source's
	// own target as reused.
	targetDBReused := make(map[string]bool, len(dbs))

	for _, dbPlan := range dbs {
		newDBInfo, exists := dom.InfoSchema().SchemaByName(dbPlan.Target.Name)
		if !exists {
			return errors.New("db not exist in snapshot stage UpdateDownstreamIds")
		}
		upstreamDBID := dbPlan.Source.Info.ID
		resolvedTargetDBs[newDBInfo.Name.L] = newDBInfo
		targetDBReused[newDBInfo.Name.L] = targetDBReused[newDBInfo.Name.L] || dbPlan.Reused
		dbReplace, exist := dbReplaces[upstreamDBID]
		if !exist {
			if existing, ok := tm.DBReplaceMap[upstreamDBID]; ok {
				dbReplace = NewDBReplace(existing.Name, existing.DbID)
				dbReplace.Reused = existing.Reused
				dbReplace.SchemaRouted = existing.SchemaRouted
				if dbReplace.Name == "" {
					// Table-only logs cannot name their parent schema. Recover the
					// source parent here; its own target ID is resolved below after
					// all sibling target creation plans have been collected.
					dbReplace.Name = dbPlan.Source.Info.Name.O
				}
			} else {
				// A source can have several table-route targets. None of those
				// creation plans owns the parent schema's identity.
				dbReplace = NewDBReplace(dbPlan.Source.Info.Name.O, tm.generateTempID())
			}
			dbReplace.SourceName = dbPlan.Source.Info.Name.O
			dbReplaces[upstreamDBID] = dbReplace
		}
	}

	// applyNameRoutesToTableMapping has already bound a schema route by setting
	// DBReplace.Name to its target, while an exact-table-only route deliberately
	// keeps the source parent name. Resolve every parent whose bound name matches
	// a created target. This also covers multiple source schemas merged into one
	// target even though the snapshot create plan contains that target only once.
	for upstreamDBID, existing := range tm.DBReplaceMap {
		if _, exists := dbReplaces[upstreamDBID]; !exists {
			dbReplace := NewDBReplace(existing.Name, existing.DbID)
			dbReplace.Reused = existing.Reused
			dbReplace.SchemaRouted = existing.SchemaRouted
			dbReplace.SourceName = existing.SourceName
			dbReplaces[upstreamDBID] = dbReplace
		}
	}
	for upstreamDBID, dbReplace := range dbReplaces {
		newDBInfo, mappedParent := resolvedTargetDBs[ast.NewCIStr(dbReplace.Name).L]
		if !mappedParent {
			continue
		}
		dbReplace.Name = newDBInfo.Name.O
		dbReplace.DbID = newDBInfo.ID
		dbReplace.Reused = dbReplace.Reused || targetDBReused[newDBInfo.Name.L]
		if existing, ok := tm.DBReplaceMap[upstreamDBID]; ok {
			existing.Name = dbReplace.Name
			existing.DbID = dbReplace.DbID
		}
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
			dbReplace.SchemaRouted = existing.SchemaRouted
			dbReplace.SourceName = existing.SourceName
			dbReplaces[oldTable.DB.ID] = dbReplace
		}

		dbReplace.TableMap[oldTable.Info.ID] = &TableReplace{
			Name:         newTable.Name.O,
			SourceName:   oldTable.Info.Name.O,
			TableID:      newTable.ID,
			PartitionMap: restoreutils.GetPartitionIDMap(newTable, oldTable.Info),
		}
		// An explicit route renames the table or moves it to another schema, so
		// its downstream database identity must be recorded. Derive it from the
		// created target and the source table: the log scan never sees a table
		// that was created before the log window, so a route marker bound by the
		// log mapping is not available for those snapshot-only tables. The marker
		// is needed by MergeBaseDBReplace, EffectiveDBName/EffectiveDBID and the
		// table mode reset during the log phase.
		if targetDBName.L != oldTable.DB.Name.L || newTable.Name.L != oldTable.Info.Name.L {
			dbReplace.TableMap[oldTable.Info.ID].TargetDBName = targetDBName.O
			dbReplace.TableMap[oldTable.Info.ID].TargetDBID = newDBInfo.ID
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
