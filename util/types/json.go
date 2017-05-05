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

type Json interface {
	ParseFromString(s string) error
	DumpToString() string
	Serialize() ([]byte, error)
	Deserialize(bytes []byte) error

	Extract(pathExpr string) (Json, error)
}

func CreateJson(j interface{}) Json {
	return &jsonImpl{
		serde: bsonImplFlag,
		json:  j,
	}
}

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
