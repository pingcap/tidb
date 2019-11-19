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

// InRepairMode get whether TiDB is in repairMode.
func (r *repairInfo) GetRepairMode() bool {
	return r.repairMode.Load().(bool)
}

// InRepairMode set whether TiDB is in repairMode.
func (r *repairInfo) SetRepairMode(mode bool) {
	r.repairMode.Store(mode)
}

// InRepairMode tet the simple repaired table list.
func (r *repairInfo) GetRepairTableList() []string {
	return r.repairTableList.Load().([]string)
}

// InRepairMode set the simple repaired table list.
func (r *repairInfo) SetRepairTableList(list []string) {
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

// FetchRepairedTableList fetch the repaired table list from meta.
func (r *repairInfo) FetchRepairedTableList(di *model.DBInfo, tbl *model.TableInfo) bool {
	if r.repairMode.Load().(bool) {
		isRepair := false
		ls := r.repairTableList.Load().([]string)
		for _, tn := range ls {
			// Use dbName and tableName to specified a table.
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
				repairedDB.Tables = []*model.TableInfo{}
				repairedDB.Tables = append(repairedDB.Tables, tbl)
				mp[di.ID] = repairedDB
			}
			r.repairDBInfoMap.Store(mp)
			return true
		}
	}
	return false
}

// GetRepairedTableInfoByTableName is exported for test.
func (r *repairInfo) GetRepairedTableInfoByTableName(schemaLowerName, tableLowerName string) *model.TableInfo {
	mp := r.repairDBInfoMap.Load().(map[int64]*model.DBInfo)
	for _, db := range mp {
		if db.Name.L == schemaLowerName {
			for _, t := range db.Tables {
				if t.Name.L == tableLowerName {
					return t
				}
			}
		}
	}
	return nil
}

// RemoveFromRepairList remove the table from repair info when repaired.
func (r *repairInfo) RemoveFromRepairList(schemaLowerName, tableLowerName string) {
	repairedLowerName := schemaLowerName + "." + tableLowerName
	// Remove from the repair list.
	ls := r.repairTableList.Load().([]string)
	for i, rt := range ls {
		if strings.ToLower(rt) == repairedLowerName {
			ls = append(ls[:i], ls[i+1:]...)
			break
		}
	}
	r.repairTableList.Store(ls)
	// Remove from the repair map.
	mp := r.repairDBInfoMap.Load().(map[int64]*model.DBInfo)
	for _, db := range mp {
		if db.Name.L == schemaLowerName {
			for j, t := range db.Tables {
				if t.Name.L == tableLowerName {
					db.Tables = append(db.Tables[:j], db.Tables[j+1:]...)
					break
				}
			}
			if len(db.Tables) == 0 {
				delete(mp, db.ID)
			}
			break
		}
	}
	r.repairDBInfoMap.Store(mp)
}

// repairKeyType is keyType for admin repair table.
type repairKeyType int

const (
	// RepairedTable is the key type, caching the target repaired table in sessionCtx.
	RepairedTable repairKeyType = iota
	// RepairedDatabase is the key type, caching the target repaired database in sessionCtx.
	RepairedDatabase
	// RepairedCallBack is the key type, caching the callback func of repair list in sessionCtx in case of import circle.
	RepairedCallBack
)

func (t repairKeyType) String() (res string) {
	switch t {
	case RepairedTable:
		res = "RepairedTable"
	case RepairedDatabase:
		res = "RepairedDatabase"
	case RepairedCallBack:
		res = "RepairedCallBack"
	}
	return res
}

func init() {
	RepairInfo = repairInfo{}
	RepairInfo.repairMode.Store(false)
	RepairInfo.repairTableList.Store([]string{})
	RepairInfo.repairDBInfoMap.Store(make(map[int64]*model.DBInfo, 0))
}
