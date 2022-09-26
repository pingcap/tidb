// Copyright 2018 PingCAP, Inc.
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

package testutil

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
)

// SessionExecInGoroutine export for testing.
func SessionExecInGoroutine(s kv.Storage, dbName, sql string, done chan error) {
	ExecMultiSQLInGoroutine(s, dbName, []string{sql}, done)
}

// ExecMultiSQLInGoroutine exports for testing.
func ExecMultiSQLInGoroutine(s kv.Storage, dbName string, multiSQL []string, done chan error) {
	go func() {
		se, err := session.CreateSession4Test(s)
		if err != nil {
			done <- errors.Trace(err)
			return
		}
		defer se.Close()
		_, err = se.Execute(context.Background(), "use "+dbName)
		if err != nil {
			done <- errors.Trace(err)
			return
		}
		for _, sql := range multiSQL {
			rs, err := se.Execute(context.Background(), sql)
			if err != nil {
				done <- errors.Trace(err)
				return
			}
			if rs != nil {
				done <- errors.Errorf("RecordSet should be empty")
				return
			}
			done <- nil
		}
	}()
}

// ExtractAllTableHandles extracts all handles of a given table.
func ExtractAllTableHandles(se session.Session, dbName, tbName string) ([]int64, error) {
	dom := domain.GetDomain(se)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tbName))
	if err != nil {
		return nil, err
	}
	err = sessiontxn.NewTxn(context.Background(), se)
	if err != nil {
		return nil, err
	}

	var allHandles []int64
	err = tables.IterRecords(tbl, se, nil,
		func(h kv.Handle, _ []types.Datum, _ []*table.Column) (more bool, err error) {
			allHandles = append(allHandles, h.IntValue())
			return true, nil
		})
	return allHandles, err
}
