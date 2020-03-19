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
// See the License for the specific language governing permissions and
// limitations under the License.

package testutil

import (
	"context"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
)

// SessionExecInGoroutine export for testing.
func SessionExecInGoroutine(c *check.C, s kv.Storage, sql string, done chan error) {
	ExecMultiSQLInGoroutine(c, s, "test_db", []string{sql}, done)
}

// ExecMultiSQLInGoroutine exports for testing.
func ExecMultiSQLInGoroutine(c *check.C, s kv.Storage, dbName string, multiSQL []string, done chan error) {
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
				done <- errors.Errorf("RecordSet should be empty.")
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
	err = se.NewTxn(context.Background())
	if err != nil {
		return nil, err
	}
	var allHandles []int64
	err = tbl.IterRecords(se, tbl.FirstKey(), nil,
		func(h int64, _ []types.Datum, _ []*table.Column) (more bool, err error) {
			allHandles = append(allHandles, h)
			return true, nil
		})
	return allHandles, err
}
