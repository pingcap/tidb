// Copyright 2015 PingCAP, Inc.
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

package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/structure"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/partialjson"
)

func (m *Mutator) IterDatabases(fn func(info *model.DBInfo) error) error {
	err := m.txn.HGetIter(mDBs, func(r structure.HashPair) error {
		dbInfo := &model.DBInfo{}
		err := json.Unmarshal(r.Value, dbInfo)
		if err != nil {
			return errors.Trace(err)
		}
		return fn(dbInfo)
	})
	return errors.Trace(err)
}

// IterTables iterates all the table at once, in order to avoid oom.
func (m *Mutator) IterTables(dbID int64, fn func(info *model.TableInfo) error) error {
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	err := m.txn.HGetIter(dbKey, func(r structure.HashPair) error {
		// only handle table meta
		tableKey := string(r.Field)
		if !strings.HasPrefix(tableKey, mTablePrefix) {
			return nil
		}

		tbInfo := &model.TableInfo{}
		err := json.Unmarshal(r.Value, tbInfo)
		if err != nil {
			return errors.Trace(err)
		}
		tbInfo.DBID = dbID

		err = fn(tbInfo)
		return errors.Trace(err)
	})
	return errors.Trace(err)
}

func splitRangeInt64Max(n int64) [][]string {
	ranges := make([][]string, n)

	// 9999999999999999999 is the max number than maxInt64 in string format.
	batch := 9999999999999999999 / uint64(n)

	for k := range n {
		start := batch * uint64(k)
		end := batch * uint64(k+1)

		startStr := fmt.Sprintf("%019d", start)
		if k == 0 {
			startStr = "0"
		}
		endStr := fmt.Sprintf("%019d", end)

		ranges[k] = []string{startStr, endStr}
	}

	return ranges
}

// IterAllTables iterates all the table at once, in order to avoid oom. It can use at most 15 concurrency to iterate.
// This function is optimized for 'many databases' scenario. Only 1 concurrency can work for 'many tables in one database' scenario.
func IterAllTables(ctx context.Context, store kv.Storage, startTs uint64, concurrency int, fn func(info *model.TableInfo) error) error {
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workGroup, egCtx := util.NewErrorGroupWithRecoverWithCtx(cancelCtx)

	// In case of too many goroutines or 0 concurrency. fetchAllTablesAndBuildAnalysisJobs may pass 0 concurrency on 1C machine.
	concurrency = max(1, min(15, concurrency))
	kvRanges := splitRangeInt64Max(int64(concurrency))

	mu := sync.Mutex{}
	for i := range concurrency {
		snapshot := store.GetSnapshot(kv.NewVersion(startTs))
		snapshot.SetOption(kv.RequestSourceInternal, true)
		snapshot.SetOption(kv.RequestSourceType, kv.InternalTxnMeta)
		t := structure.NewStructure(snapshot, nil, mMetaPrefix)
		workGroup.Go(func() error {
			startKey := fmt.Appendf(nil, "%s:", mDBPrefix)
			startKey = codec.EncodeBytes(startKey, []byte(kvRanges[i][0]))
			endKey := fmt.Appendf(nil, "%s:", mDBPrefix)
			endKey = codec.EncodeBytes(endKey, []byte(kvRanges[i][1]))

			return t.IterateHashWithBoundedKey(startKey, endKey, func(key []byte, field []byte, value []byte) error {
				select {
				case <-egCtx.Done():
					return egCtx.Err()
				default:
				}
				// only handle table meta
				tableKey := string(field)
				if !strings.HasPrefix(tableKey, mTablePrefix) {
					return nil
				}

				tbInfo := &model.TableInfo{}
				err := json.Unmarshal(value, tbInfo)
				if err != nil {
					return errors.Trace(err)
				}
				dbID, err := ParseDBKey(key)
				if err != nil {
					return errors.Trace(err)
				}
				tbInfo.DBID = dbID

				mu.Lock()
				err = fn(tbInfo)
				mu.Unlock()
				return errors.Trace(err)
			})
		})
	}

	return errors.Trace(workGroup.Wait())
}

// GetMetasByDBID return all meta information of a database.
// Note(dongmen): This method is used by TiCDC to reduce the time of changefeed initialization.
// Ref: https://github.com/pingcap/tiflow/issues/11109
func (m *Mutator) GetMetasByDBID(dbID int64) ([]structure.HashPair, error) {
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return nil, errors.Trace(err)
	}
	res, err := m.txn.HGetAll(dbKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return res, nil
}

// foreign key info contain null and [] two situations
var checkForeignKeyAttributesNil = `"fk_info":null`
var checkForeignKeyAttributesZero = `"fk_info":[]`
var checkAttributesInOrder = []string{
	`"partition":null`,
	`"Lock":null`,
	`"tiflash_replica":null`,
	`"temp_table_type":0`,
	`"policy_ref_info":null`,
	`"ttl_info":null`,
}

// isTableInfoMustLoad checks whether the table info needs to be loaded.
// If the byte representation contains all the given attributes,
// then it does not need to be loaded and this function will return false.
// Otherwise, it will return true, indicating that the table info should be loaded.
// Since attributes are checked in sequence, it's important to choose the order carefully.
// isCheckForeignKeyAttrsInOrder check foreign key or not, since fk_info contains two null situations.
func isTableInfoMustLoad(json []byte, isCheckForeignKeyAttrsInOrder bool, filterAttrs ...string) bool {
	idx := 0
	if isCheckForeignKeyAttrsInOrder {
		idx = bytes.Index(json, hack.Slice(checkForeignKeyAttributesNil))
		if idx == -1 {
			idx = bytes.Index(json, hack.Slice(checkForeignKeyAttributesZero))
			if idx == -1 {
				return true
			}
		}
		json = json[idx:]
	}
	for _, substr := range filterAttrs {
		idx = bytes.Index(json, hack.Slice(substr))
		if idx == -1 {
			return true
		}
		json = json[idx:]
	}
	return false
}

// IsTableInfoMustLoad checks whether the table info needs to be loaded.
// Exported for testing.
func IsTableInfoMustLoad(json []byte) bool {
	return isTableInfoMustLoad(json, true, checkAttributesInOrder...)
}

// NameExtractRegexp is exported for testing.
const NameExtractRegexp = `"O":"([^"\\]*(?:\\.[^"\\]*)*)",`

// Unescape is exported for testing.
func Unescape(s string) string {
	s = strings.ReplaceAll(s, `\"`, `"`)
	s = strings.ReplaceAll(s, `\\`, `\`)
	return s
}

// GetAllNameToIDAndTheMustLoadedTableInfo gets all the fields and values and table info for special attributes in a hash.
// It's used to get some infos for information schema cache in a faster way.
// If a table contains any of the attributes listed in checkSubstringsInOrder, it must be loaded during schema full load.
// hasSpecialAttributes() is a subset of it, the difference is that:
// If a table need to be resident in-memory, its table info MUST be loaded.
// If a table info is loaded, it's NOT NECESSARILY to be keep in-memory.
func (m *Mutator) GetAllNameToIDAndTheMustLoadedTableInfo(dbID int64) (map[string]int64, []*model.TableInfo, error) {
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return nil, nil, errors.Trace(err)
	}

	res := make(map[string]int64)
	idRegex := regexp.MustCompile(`"id":(\d+)`)
	nameLRegex := regexp.MustCompile(NameExtractRegexp)

	tableInfos := make([]*model.TableInfo, 0)

	err := m.txn.IterateHash(dbKey, func(field []byte, value []byte) error {
		if !strings.HasPrefix(string(hack.String(field)), "Table") {
			return nil
		}

		idMatch := idRegex.FindStringSubmatch(string(hack.String(value)))
		nameLMatch := nameLRegex.FindStringSubmatch(string(hack.String(value)))
		id, err := strconv.Atoi(idMatch[1])
		if err != nil {
			return errors.Trace(err)
		}

		key := Unescape(nameLMatch[1])
		res[strings.Clone(key)] = int64(id)
		if isTableInfoMustLoad(value, true, checkAttributesInOrder...) {
			tbInfo := &model.TableInfo{}
			err = json.Unmarshal(value, tbInfo)
			if err != nil {
				return errors.Trace(err)
			}
			tbInfo.DBID = dbID
			tableInfos = append(tableInfos, tbInfo)
		}
		return nil
	})

	return res, tableInfos, errors.Trace(err)
}

// GetTableInfoWithAttributes retrieves all the table infos for a given db.
// The filterAttrs are used to filter out any table that is not needed.
func GetTableInfoWithAttributes(m *Mutator, dbID int64, filterAttrs ...string) ([]*model.TableInfo, error) {
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return nil, errors.Trace(err)
	}

	tableInfos := make([]*model.TableInfo, 0)
	err := m.txn.IterateHash(dbKey, func(field []byte, value []byte) error {
		if !strings.HasPrefix(string(hack.String(field)), "Table") {
			return nil
		}

		if isTableInfoMustLoad(value, false, filterAttrs...) {
			tbInfo := &model.TableInfo{}
			err := json.Unmarshal(value, tbInfo)
			if err != nil {
				return errors.Trace(err)
			}
			tbInfo.DBID = dbID
			tableInfos = append(tableInfos, tbInfo)
		}
		return nil
	})

	return tableInfos, errors.Trace(err)
}

// ListTables shows all tables in database.
func (m *Mutator) ListTables(ctx context.Context, dbID int64) ([]*model.TableInfo, error) {
	res, err := m.GetMetasByDBID(dbID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tables := make([]*model.TableInfo, 0, len(res)/2)
	for _, r := range res {
		// only handle table meta
		tableKey := string(r.Field)
		if !strings.HasPrefix(tableKey, mTablePrefix) {
			continue
		}
		if ctx.Err() != nil {
			return nil, errors.Trace(ctx.Err())
		}

		tbInfo := &model.TableInfo{}
		err = json.Unmarshal(r.Value, tbInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tbInfo.DBID = dbID

		tables = append(tables, tbInfo)
	}

	return tables, nil
}

// ListSimpleTables shows all simple tables in database.
func (m *Mutator) ListSimpleTables(dbID int64) ([]*model.TableNameInfo, error) {
	res, err := m.GetMetasByDBID(dbID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tables := make([]*model.TableNameInfo, 0, len(res)/2)
	for _, r := range res {
		// only handle table meta
		tableKey := string(r.Field)
		if !strings.HasPrefix(tableKey, mTablePrefix) {
			continue
		}

		tbInfo, err2 := FastUnmarshalTableNameInfo(r.Value)
		if err2 != nil {
			return nil, errors.Trace(err2)
		}

		tables = append(tables, tbInfo)
	}

	return tables, nil
}

var tableNameInfoFields = []string{"id", "name"}

// FastUnmarshalTableNameInfo is exported for testing.
func FastUnmarshalTableNameInfo(data []byte) (*model.TableNameInfo, error) {
	m, err := partialjson.ExtractTopLevelMembers(data, tableNameInfoFields)
	if err != nil {
		return nil, errors.Trace(err)
	}

	idTokens, ok := m["id"]
	if !ok {
		return nil, errors.New("id field not found in JSON")
	}
	if len(idTokens) != 1 {
		return nil, errors.Errorf("unexpected id field in JSON, %v", idTokens)
	}
	num, ok := idTokens[0].(json.Number)
	if !ok {
		return nil, errors.Errorf(
			"id field is not a number, got %T %v", idTokens[0], idTokens[0],
		)
	}
	id, err := num.Int64()
	if err != nil {
		return nil, errors.Trace(err)
	}

	nameTokens, ok := m["name"]
	if !ok {
		return nil, errors.New("name field not found in JSON")
	}
	// 6 tokens; {, O, ..., L, ..., }, the data looks like this {123,"O","t","L","t",125}
	if len(nameTokens) != 6 {
		return nil, errors.Errorf("unexpected name field in JSON, %v", nameTokens)
	}
	name, ok := nameTokens[2].(string)
	if !ok {
		return nil, errors.Errorf("unexpected name field in JSON, %v", nameTokens)
	}

	return &model.TableNameInfo{
		ID:   id,
		Name: ast.NewCIStr(name),
	}, nil
}

// ListDatabases shows all databases.
func (m *Mutator) ListDatabases() ([]*model.DBInfo, error) {
	res, err := m.txn.HGetAll(mDBs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbs := make([]*model.DBInfo, 0, len(res))
	for _, r := range res {
		dbInfo := &model.DBInfo{}
		err = json.Unmarshal(r.Value, dbInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		dbs = append(dbs, dbInfo)
	}
	return dbs, nil
}

// GetDatabase gets the database value with ID.
func (m *Mutator) GetDatabase(dbID int64) (*model.DBInfo, error) {
	dbKey := m.dbKey(dbID)
	value, err := m.txn.HGet(mDBs, dbKey)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	dbInfo := &model.DBInfo{}
	err = json.Unmarshal(value, dbInfo)
	return dbInfo, errors.Trace(err)
}

// ListPolicies shows all policies.
func (m *Mutator) ListPolicies() ([]*model.PolicyInfo, error) {
	res, err := m.txn.HGetAll(mPolicies)
	if err != nil {
		return nil, errors.Trace(err)
	}

	policies := make([]*model.PolicyInfo, 0, len(res))
	for _, r := range res {
		value, err := detachMagicByte(r.Value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		policy := &model.PolicyInfo{}
		err = json.Unmarshal(value, policy)
		if err != nil {
			return nil, errors.Trace(err)
		}
		policies = append(policies, policy)
	}
	return policies, nil
}

// GetPolicy gets the database value with ID.
func (m *Mutator) GetPolicy(policyID int64) (*model.PolicyInfo, error) {
	policyKey := m.policyKey(policyID)
	value, err := m.txn.HGet(mPolicies, policyKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if value == nil {
		return nil, ErrPolicyNotExists.GenWithStack("policy id : %d doesn't exist", policyID)
	}

	value, err = detachMagicByte(value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	policy := &model.PolicyInfo{}
	err = json.Unmarshal(value, policy)
	return policy, errors.Trace(err)
}

// ListResourceGroups shows all resource groups.
func (m *Mutator) ListResourceGroups() ([]*model.ResourceGroupInfo, error) {
	res, err := m.txn.HGetAll(mResourceGroups)
	if err != nil {
		return nil, errors.Trace(err)
	}

	hasDefault := false
	groups := make([]*model.ResourceGroupInfo, 0, len(res))
	for _, r := range res {
		value, err := detachMagicByte(r.Value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		group := &model.ResourceGroupInfo{}
		err = json.Unmarshal(value, group)
		if err != nil {
			return nil, errors.Trace(err)
		}
		groups = append(groups, group)
		hasDefault = hasDefault || (group.Name.L == resourcegroup.DefaultResourceGroupName)
	}
	if !hasDefault {
		groups = append(groups, defaultRGroupMeta)
	}
	return groups, nil
}

// DefaultGroupMeta4Test return the default group info for test usage.
func DefaultGroupMeta4Test() *model.ResourceGroupInfo {
	return defaultRGroupMeta
}

// GetResourceGroup gets the database value with ID.
func (m *Mutator) GetResourceGroup(groupID int64) (*model.ResourceGroupInfo, error) {
	groupKey := m.resourceGroupKey(groupID)
	value, err := m.txn.HGet(mResourceGroups, groupKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if value == nil {
		// the default group is not persistent to tikv by default.
		if groupID == defaultGroupID {
			return defaultRGroupMeta, nil
		}
		return nil, ErrResourceGroupNotExists.GenWithStack("resource group id : %d doesn't exist", groupID)
	}

	value, err = detachMagicByte(value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	group := &model.ResourceGroupInfo{}
	err = json.Unmarshal(value, group)
	return group, errors.Trace(err)
}

func attachMagicByte(data []byte) []byte {
	data = append(data, 0)
	copy(data[1:], data)
	data[0] = mPolicyMagicByte
	return data
}

func detachMagicByte(value []byte) ([]byte, error) {
	magic, data := value[:1], value[1:]
	switch whichMagicType(magic[0]) {
	case typeJSON:
		if magic[0] != CurrentMagicByteVer {
			return nil, errors.New("incompatible magic type handling module")
		}
		return data, nil
	default:
		return nil, errors.New("unknown magic type handling module")
	}
}

func whichMagicType(b byte) int {
	if b <= 0x3F {
		return typeJSON
	}
	return typeUnknown
}

// GetTable gets the table value in database with tableID.
func (m *Mutator) GetTable(dbID int64, tableID int64) (*model.TableInfo, error) {
	// Check if db exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return nil, errors.Trace(err)
	}

	tableKey := m.tableKey(tableID)
	value, err := m.txn.HGet(dbKey, tableKey)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	tableInfo := &model.TableInfo{}
	err = json.Unmarshal(value, tableInfo)
	tableInfo.DBID = dbID
	return tableInfo, errors.Trace(err)
}

// CheckTableExists checks if the table is existed with dbID and tableID.
func (m *Mutator) CheckTableExists(dbID int64, tableID int64) (bool, error) {
	// Check if db exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return false, errors.Trace(err)
	}

	// Check if table exists.
	tableKey := m.tableKey(tableID)
	v, err := m.txn.HGet(dbKey, tableKey)
	if err != nil {
		return false, errors.Trace(err)
	}
	if v != nil {
		return true, nil
	}

	return false, nil
}

