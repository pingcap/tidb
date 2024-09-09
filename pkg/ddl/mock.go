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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// SetBatchInsertDeleteRangeSize sets the batch insert/delete range size in the test
func SetBatchInsertDeleteRangeSize(i int) {
	batchInsertDeleteRangeSize = i
}

type mockDelRange struct {
}

// newMockDelRangeManager creates a mock delRangeManager only used for test.
func newMockDelRangeManager() delRangeManager {
	return &mockDelRange{}
}

// addDelRangeJob implements delRangeManager interface.
func (*mockDelRange) addDelRangeJob(_ context.Context, _ *model.Job) error {
	return nil
}

// removeFromGCDeleteRange implements delRangeManager interface.
func (*mockDelRange) removeFromGCDeleteRange(_ context.Context, _ int64) error {
	return nil
}

// start implements delRangeManager interface.
func (*mockDelRange) start() {}

// clear implements delRangeManager interface.
func (*mockDelRange) clear() {}

// MockTableInfo mocks a table info by create table stmt ast and a specified table id.
func MockTableInfo(ctx sessionctx.Context, stmt *ast.CreateTableStmt, tableID int64) (*model.TableInfo, error) {
	chs, coll := charset.GetDefaultCharsetAndCollate()
	cols, newConstraints, err := buildColumnsAndConstraints(ctx, stmt.Cols, stmt.Constraints, chs, coll)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tbl, err := BuildTableInfo(ctx, stmt.Table.Name, cols, newConstraints, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	tbl.ID = tableID

	if err = setTableAutoRandomBits(ctx, tbl, stmt.Cols); err != nil {
		return nil, errors.Trace(err)
	}

	// The specified charset will be handled in handleTableOptions
	if err = handleTableOptions(stmt.Options, tbl); err != nil {
		return nil, errors.Trace(err)
	}

	return tbl, nil
}
