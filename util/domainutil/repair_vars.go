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
// See the License for the specific language governing permissions and
// limitations under the License.

package domainutil

import (
	"strings"
	"sync/atomic"

	"github.com/pingcap/parser/model"
)

type repairInfo struct {
	repairMode      atomic.Value
	repairTableList atomic.Value
	repairDBInfoMap atomic.Value
}

// RepairInfo indicates the repaired table info.
var RepairInfo repairInfo

// InRepairMode indicates whether TiDB is in repairMode.
func (r *repairInfo) InRepairMode() bool {
	return r.repairMode.Load().(bool)
}

// SetRepairMode sets whether TiDB is in repairMode.
func (r *repairInfo) SetRepairMode(mode bool) {
	r.repairMode.Store(mode)
}

// GetRepairTableList gets repairing table list.
func (r *repairInfo) GetRepairTableList() []string {
	return r.repairTableList.Load().([]string)
}

// SetRepairTableList sets repairing table list.
func (r *repairInfo) SetRepairTableList(list []string) {
	for i, one := range list {
		list[i] = strings.ToLower(one)
	}
	r.repairTableList.Store(list)
}

// GetTablesInRepair return the map of repaired table in repair.
func (r *repairInfo) GetTablesInRepair() map[int64]*model.DBInfo {
	return r.repairDBInfoMap.Load().(map[int64]*model.DBInfo)
}

// GetRepairCleanFunc return a func for call back when repair action done.
func (r *repairInfo) GetRepairCleanFunc() func(a, b string) {
	return r.RemoveFromRepairList
}

// CheckAndFetchRepairedTable fetches the repairing table list from meta, true indicates fetch success.
func (r *repairInfo) CheckAndFetchRepairedTable(di *model.DBInfo, tbl *model.TableInfo) bool {
	if !r.repairMode.Load().(bool) {
		return false
	}
	isRepair := false
	ls := r.repairTableList.Load().([]string)
	for _, tn := range ls {
		// Use dbName and tableName to specify a table.
		if strings.ToLower(tn) == di.Name.L+"."+tbl.Name.L {
			isRepair = true
			break
		}
	}
	if isRepair {
		mp := r.repairDBInfoMap.Load().(map[int64]*model.DBInfo)
		// Record the repaired table in Map.
		if repairedDB, ok := mp[di.ID]; ok {
			repairedDB.Tables = append(repairedDB.Tables, tbl)
		} else {
			// Shallow copy the DBInfo.
			repairedDB := di.Copy()
			// Clean the tables and set repaired table.
			repairedDB.Tables = []*model.TableInfo{tbl}
			mp[di.ID] = repairedDB
		}
		r.repairDBInfoMap.Store(mp)
		return true
	}
	return false
}

// GetRepairedTableInfoByTableName is exported for test.
func (r *repairInfo) GetRepairedTableInfoByTableName(schemaLowerName, tableLowerName string) (*model.TableInfo, *model.DBInfo) {
	mp := r.repairDBInfoMap.Load().(map[int64]*model.DBInfo)
	for _, db := range mp {
		if db.Name.L != schemaLowerName {
			continue
		}
		for _, t := range db.Tables {
			if t.Name.L == tableLowerName {
				return t, db
			}
		}
		return nil, db
	}
	return nil, nil
}

// RemoveFromRepairList remove the table from repair info when repaired.
func (r *repairInfo) RemoveFromRepairList(schemaLowerName, tableLowerName string) {
	repairedLowerName := schemaLowerName + "." + tableLowerName
	// Remove from the repair list.
	ls := r.repairTableList.Load().([]string)
	lsCopy := make([]string, 0, len(ls))
	copy(lsCopy, ls)
	for i, rt := range lsCopy {
		if strings.ToLower(rt) == repairedLowerName {
			lsCopy = append(lsCopy[:i], lsCopy[i+1:]...)
			break
		}
	}
	r.repairTableList.Store(lsCopy)
	// Remove from the repair map.
	mp := r.repairDBInfoMap.Load().(map[int64]*model.DBInfo)
	mpCopy := make(map[int64]*model.DBInfo, len(mp))
	for i, db := range mp {
		mpCopy[i] = db.Copy()
	}
	for _, db := range mpCopy {
		if db.Name.L == schemaLowerName {
			for j, t := range db.Tables {
				if t.Name.L == tableLowerName {
					db.Tables = append(db.Tables[:j], db.Tables[j+1:]...)
					break
				}
			}
			if len(db.Tables) == 0 {
				delete(mpCopy, db.ID)
			}
			break
		}
	}
	if len(mpCopy) == 0 {
		r.repairMode.Store(false)
	}
	r.repairDBInfoMap.Store(mpCopy)
}

// repairKeyType is keyType for admin repair table.
type repairKeyType int

const (
	// RepairedTable is the key type, caching the target repaired table in sessionCtx.
	RepairedTable repairKeyType = iota
	// RepairedDatabase is the key type, caching the target repaired database in sessionCtx.
	RepairedDatabase
)

func (t repairKeyType) String() (res string) {
	switch t {
	case RepairedTable:
		res = "RepairedTable"
	case RepairedDatabase:
		res = "RepairedDatabase"
	}
	return res
}

func init() {
	RepairInfo = repairInfo{}
	RepairInfo.repairMode.Store(false)
	RepairInfo.repairTableList.Store([]string{})
	RepairInfo.repairDBInfoMap.Store(make(map[int64]*model.DBInfo))
}
