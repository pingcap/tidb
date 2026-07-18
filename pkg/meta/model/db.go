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

package model

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// DBInfo provides meta data describing a DB.
type DBInfo struct {
	ID         int64     `json:"id"`      // Database ID
	Name       ast.CIStr `json:"db_name"` // DB name.
	Charset    string    `json:"charset"`
	Collate    string    `json:"collate"`
	Deprecated struct {  // Tables is not set in infoschema v2, use infoschema SchemaTableInfos() instead.
		Tables []*TableInfo `json:"-"` // Tables in the DB.
	}
	State              SchemaState      `json:"state"`
	PlacementPolicyRef *PolicyRefInfo   `json:"policy_ref_info"`
	TableName2ID       map[string]int64 `json:"-"`
}

// Clone clones DBInfo.
func (db *DBInfo) Clone() *DBInfo {
	newInfo := *db
	newInfo.Deprecated.Tables = make([]*TableInfo, len(db.Deprecated.Tables))
	for i := range db.Deprecated.Tables {
		newInfo.Deprecated.Tables[i] = db.Deprecated.Tables[i].Clone()
	}
	return &newInfo
}

// Copy shallow copies DBInfo.
func (db *DBInfo) Copy() *DBInfo {
	newInfo := *db
	newInfo.Deprecated.Tables = make([]*TableInfo, len(db.Deprecated.Tables))
	copy(newInfo.Deprecated.Tables, db.Deprecated.Tables)
	return &newInfo
}

// LessDBInfo is used for sorting DBInfo by DBInfo.Name.
func LessDBInfo(a *DBInfo, b *DBInfo) int {
	return strings.Compare(a.Name.L, b.Name.L)
}
