// Copyright 2017 PingCAP, Inc.
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

package types

import (
	"bytes"

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

// Json is for MySQL Json type.
type Json interface {
	// ParseFromString parses a json from string.
	ParseFromString(s string) error
	// DumpToString dumps itself to string.
	DumpToString() string
	// Serialize means serialize itself into bytes.
	Serialize() ([]byte, error)
	// Deserialize means deserialize a json from bytes.
	Deserialize(bytes []byte) error

	// Extract is used for json_extract function.
	Extract(pathExpr string) (Json, error)
}

// CreateJson will create a json with bson as serde format and nil as data.
func CreateJson(j interface{}) Json {
	return &jsonImpl{
		serde: bsonImplFlag,
		json:  j,
	}
}

// CompareJson compares two json object.
func CompareJson(j1 Json, j2 Json) (int, error) {
	s1, _ := j1.Serialize()
	s2, _ := j2.Serialize()
	return bytes.Compare(s1, s2), nil
}

var (
	errInvalidJsonText = terror.ClassJson.New(mysql.ErrInvalidJsonText, mysql.MySQLErrName[mysql.ErrInvalidJsonText])
)

func init() {
	terror.ErrClassToMySQLCodes[terror.ClassJson] = map[terror.ErrCode]uint16{
		mysql.ErrInvalidJsonText: mysql.ErrInvalidJsonText,
	}
}
