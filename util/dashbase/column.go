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

package dashbase

// ColumnType is the column type
type ColumnType string

const (
	// TypeMeta is the `meta` type in Dashbase
	TypeMeta ColumnType = "meta"

	// TypeTime is the `time` type in Dashbase
	TypeTime ColumnType = "time"

	// TypeNumeric is the `numeric` type in Dashbase
	TypeNumeric ColumnType = "numeric"

	// TypeText is the `text` type in Dashbase
	TypeText ColumnType = "text"
)

type Column struct {
	Name string
	Type ColumnType
}
