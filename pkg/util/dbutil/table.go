// Copyright 2022 PingCAP, Inc.
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

package dbutil

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/model"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver" // for parser driver
)

// FindColumnByName finds column by name.
func FindColumnByName(cols []*model.ColumnInfo, name string) *model.ColumnInfo {
	// column name don't distinguish capital and small letter
	name = strings.ToLower(name)
	for _, col := range cols {
		if col.Name.L == name {
			return col
		}
	}

	return nil
}
