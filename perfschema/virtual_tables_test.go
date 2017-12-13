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

package perfschema_test

import (
	"testing"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/perfschema"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (*testSuite) TestSessionStatus(c *C) {
	store, err := tikv.NewMockTikvStore()
	c.Assert(err, IsNil)

	ctx := mock.NewContext()
	ctx.Store = store
	ps := perfschema.NewPerfHandle()

	testTableName := []string{perfschema.TableSessionStatus, perfschema.TableGlobalStatus}
	for _, tableName := range testTableName {
		tb, _ := ps.GetTable(tableName)
		meta, ok := ps.GetTableMeta(tableName)
		c.Assert(tb, NotNil)
		c.Assert(ok, IsTrue)

		sessionStatusHandle, _ := perfschema.CreateVirtualDataSource(tableName, meta)
		rows, err := sessionStatusHandle.GetRows(ctx)
		c.Assert(err, IsNil)

		c.Assert(findSpecialStatus(rows, "Ssl_cipher"), IsNil)
	}
}

func findSpecialStatus(rows [][]types.Datum, name string) error {
	err := errors.New("cant find the status " + name)
	for _, row := range rows {
		statusNames, _ := row[0].ToString()
		if statusNames == name {
			err = nil
			break
		}
	}

	return err
}
