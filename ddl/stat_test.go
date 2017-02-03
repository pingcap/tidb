// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testStatSuite{})

type testStatSuite struct {
}

func (s *testStatSuite) getDDLSchemaVer(c *C, d *ddl) int64 {
	m, err := d.Stats()
	c.Assert(err, IsNil)
	v := m[ddlSchemaVersion]
	return v.(int64)
}

func (s *testStatSuite) TestStat(c *C) {
	defer testleak.AfterTest(c)()
	store := testCreateStore(c, "test_stat")
	defer store.Close()

	d := newDDL(store, nil, nil, testLease)
	defer d.Stop()

	time.Sleep(testLease)

	dbInfo := testSchemaInfo(c, d, "test")
	testCreateSchema(c, testNewContext(d), d, dbInfo)

	m, err := d.Stats()
	c.Assert(err, IsNil)
	c.Assert(m[ddlOwnerID], Equals, d.uuid)

	job := &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo.Name},
	}

	ctx := mock.NewContext()
	ctx.Store = store
	done := make(chan error, 1)
	go func() {
		done <- d.doDDLJob(ctx, job)
	}()

	ticker := time.NewTicker(d.lease * 1)
	defer ticker.Stop()

	ver := s.getDDLSchemaVer(c, d)
LOOP:
	for {
		select {
		case <-ticker.C:
			d.close()
			c.Assert(s.getDDLSchemaVer(c, d), GreaterEqual, ver)
			d.start()
		case err := <-done:
			c.Assert(err, IsNil)
			m, err := d.Stats()
			c.Assert(err, IsNil)
			c.Assert(m[bgOwnerID], Equals, d.uuid)
			break LOOP
		}
	}
}
