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
	"sync"

	"github.com/pingcap/parser/model"
)

type repairInfo struct {
	sync.RWMutex
	repairMode      bool
	repairTableList []string
	repairDBInfoMap map[int64]*model.DBInfo
}

// RepairInfo indicates the repaired table info.
var RepairInfo repairInfo

// InRepairMode indicates whether TiDB is in repairMode.
func (r *repairInfo) InRepairMode() bool {
	r.RLock()
	defer r.RUnlock()
	return r.repairMode
}

// SetRepairMode sets whether TiDB is in repairMode.
func (r *repairInfo) SetRepairMode(mode bool) {
	r.Lock()
	defer r.Unlock()
	r.repairMode = mode
}

// GetRepairTableList gets repairing table list.
func (r *repairInfo) GetRepairTableList() []string {
	r.RLock()
	defer r.RUnlock()
	return r.repairTableList
}

// SetRepairTableList sets repairing table list.
func (r *repairInfo) SetRepairTableList(list []string) {
	for i, one := range list {
		list[i] = strings.ToLower(one)
	}
	r.Lock()
	defer r.Unlock()
	r.repairTableList = list
}

// CheckAndFetchRepairedTable fetches the repairing table list from meta, true indicates fetch success.
func (r *repairInfo) CheckAndFetchRepairedTable(di *model.DBInfo, tbl *model.TableInfo) bool {
	r.Lock()
	defer r.Unlock()
	if !r.repairMode {
		return false
	}
	isRepair := false
	for _, tn := range r.repairTableList {
		// Use dbName and tableName to specify a table.
		if strings.ToLower(tn) == di.Name.L+"."+tbl.Name.L {
			isRepair = true
			break
		}
	}
	if isRepair {
		// Record the repaired table in Map.
		if repairedDB, ok := r.repairDBInfoMap[di.ID]; ok {
			repairedDB.Tables = append(repairedDB.Tables, tbl)
		} else {
			// Shallow copy the DBInfo.
			repairedDB := di.Copy()
			// Clean the tables and set repaired table.
			repairedDB.Tables = []*model.TableInfo{tbl}
			r.repairDBInfoMap[di.ID] = repairedDB
		}
		return true
	}
	return false
}

// GetRepairedTableInfoByTableName is exported for test.
func (r *repairInfo) GetRepairedTableInfoByTableName(schemaLowerName, tableLowerName string) (*model.TableInfo, *model.DBInfo) {
	r.RLock()
	defer r.RUnlock()
	for _, db := range r.repairDBInfoMap {
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

// RemoveFromRepairInfo remove the table from repair info when repaired.
func (r *repairInfo) RemoveFromRepairInfo(schemaLowerName, tableLowerName string) {
	repairedLowerName := schemaLowerName + "." + tableLowerName
	// Remove from the repair list.
	r.Lock()
	defer r.Unlock()
	for i, rt := range r.repairTableList {
		if strings.ToLower(rt) == repairedLowerName {
			r.repairTableList = append(r.repairTableList[:i], r.repairTableList[i+1:]...)
			break
		}
	}
	// Remove from the repair map.
	for _, db := range r.repairDBInfoMap {
		if db.Name.L == schemaLowerName {
			for j, t := range db.Tables {
				if t.Name.L == tableLowerName {
					db.Tables = append(db.Tables[:j], db.Tables[j+1:]...)
					break
				}
			}
			if len(db.Tables) == 0 {
				delete(r.repairDBInfoMap, db.ID)
			}
			break
		}
	}
	if len(r.repairDBInfoMap) == 0 {
		r.repairMode = false
	}
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
	RepairInfo.repairMode = false
	RepairInfo.repairTableList = []string{}
	RepairInfo.repairDBInfoMap = make(map[int64]*model.DBInfo)
}
