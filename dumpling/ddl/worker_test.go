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
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/util/errors2"
)

var _ = Suite(&testDDLSuite{})

func createTestStore(c *C, name string) kv.Storage {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open(fmt.Sprintf("memory:%s", name))
	c.Assert(err, IsNil)
	return store
}

type testDDLSuite struct {
}

func (s *testDDLSuite) TestCheckOnwer(c *C) {
	store := createTestStore(c, "test_owner")
	defer store.Close()

	lease := 100 * time.Millisecond
	d1 := newDDL(store, nil, nil, lease)

	time.Sleep(lease)

	err := d1.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
		_, err1 := d1.checkOwner(t)
		return err1
	})
	c.Assert(err, IsNil)

	d2 := newDDL(store, nil, nil, lease)
	err = d2.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
		_, err1 := d2.checkOwner(t)
		return err1
	})
	c.Assert(err, NotNil)
	c.Assert(errors2.ErrorEqual(err, ErrNotOwner), IsTrue)

	d1.close()

	time.Sleep(6 * lease)

	err = d2.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
		_, err1 := d2.checkOwner(t)
		return err1
	})
	c.Assert(err, IsNil)
	d2.close()
}
