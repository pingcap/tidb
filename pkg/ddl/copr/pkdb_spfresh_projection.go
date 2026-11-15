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

package copr

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
)

func indexColumnsForSPFreshProjection(idxInfo *model.IndexInfo, tblInfo *model.TableInfo) ([]*model.IndexColumn, error) {
	if idxInfo == nil {
		return nil, errors.New("index info is not set")
	}
	if tblInfo == nil {
		return nil, errors.New("table info is not set")
	}
	if !idxInfo.IsSPFreshIndex() ||
		len(idxInfo.VectorInfo.StoredColumnIDs) == 0 {
		return idxInfo.Columns, nil
	}
	storedColumns, err := model.ResolveSPFreshStoredColumns(tblInfo, idxInfo)
	if err != nil {
		return nil, err
	}
	projected := make([]*model.IndexColumn, 0, len(idxInfo.Columns)+len(storedColumns))
	projected = append(projected, idxInfo.Columns...)
	for _, colInfo := range storedColumns {
		projected = append(projected, &model.IndexColumn{
			Name:   colInfo.Name,
			Offset: colInfo.Offset,
		})
	}
	return projected, nil
}
