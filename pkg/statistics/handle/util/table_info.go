// Copyright 2023 PingCAP, Inc.
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

package util

import (
	"context"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/table"
)

// TableInfoGetter is used to get table meta info.
type TableInfoGetter interface {
	// TableInfoByID returns the table info specified by the physicalID.
	// If the physicalID is corresponding to a partition, return its parent table.
	TableInfoByID(is infoschema.InfoSchema, physicalID int64) (table.Table, bool)
}

// tableInfoGetterImpl is used to get table meta info.
type tableInfoGetterImpl struct {
}

// NewTableInfoGetter creates a TableInfoGetter.
func NewTableInfoGetter() TableInfoGetter {
	return &tableInfoGetterImpl{}
}

// TableInfoByID returns the table info specified by the physicalID.
// If the physicalID is corresponding to a partition, return its parent table.
func (*tableInfoGetterImpl) TableInfoByID(is infoschema.InfoSchema, physicalID int64) (table.Table, bool) {
	tbl, ok := is.TableByID(context.Background(), physicalID)
	if ok {
		return tbl, true
	}
	tbl, _, _ = is.FindTableByPartitionID(physicalID)
	return tbl, tbl != nil
}
