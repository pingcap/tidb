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

package types

import (
	"strings"

	"github.com/pingcap/parser/model"
)

// FieldName records the names used for mysql protocol.
type FieldName struct {
	OrigTblName model.CIStr
	OrigColName model.CIStr
	DBName      model.CIStr
	TblName     model.CIStr
	ColName     model.CIStr

	Hidden bool
}

const emptyName = "EMPTY_NAME"

// String implements Stringer interface.
func (name *FieldName) String() string {
	builder := strings.Builder{}
	if name.Hidden {
		return emptyName
	}
	if name.DBName.L != "" {
		builder.WriteString(name.DBName.L + ".")
	}
	if name.TblName.L != "" {
		builder.WriteString(name.TblName.L + ".")
	}
	builder.WriteString(name.ColName.L)
	return builder.String()
}

// NameSlice is the slice of the *fieldName
type NameSlice []*FieldName

// Shallow is a shallow copy, only making a new slice.
func (s NameSlice) Shallow() NameSlice {
	ret := make(NameSlice, len(s))
	copy(ret, s)
	return ret
}

// EmptyName is to occupy the position in the name slice. If it's set, that column's name is hidden.
var EmptyName = &FieldName{Hidden: true}
