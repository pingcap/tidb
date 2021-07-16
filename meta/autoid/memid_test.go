// Copyright 2021 PingCAP, Inc.
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

package autoid_test

import (
	"context"
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/store/mockstore"
)

func (*testSuite) TestInMemoryAlloc(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer func() {
		err := store.Close()
		c.Assert(err, IsNil)
	}()

	columnInfo := &model.ColumnInfo{
		FieldType: types.FieldType{
			Flag: mysql.AutoIncrementFlag,
		},
	}
	tblInfo := &model.TableInfo{
		Columns: []*model.ColumnInfo{columnInfo},
	}
	alloc := autoid.NewAllocatorFromTempTblInfo(tblInfo)
	c.Assert(alloc, NotNil)

	// alloc 1
	ctx := context.Background()
	_, id, err := alloc.Alloc(ctx, 1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(1))
	_, id, err = alloc.Alloc(ctx, 1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(2))

	// alloc N
	_, id, err = alloc.Alloc(ctx, 1, 10, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(12))

	// increment > N
	_, id, err = alloc.Alloc(ctx, 1, 1, 10, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(21))

	// offset
	_, id, err = alloc.Alloc(ctx, 1, 1, 1, 30)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(30))

	// rebase
	err = alloc.Rebase(1, int64(40), true)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(41))
	err = alloc.Rebase(1, int64(10), true)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(42))

	// maxInt64
	err = alloc.Rebase(1, int64(math.MaxInt64-2), true)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(math.MaxInt64-1))
	_, _, err = alloc.Alloc(ctx, 1, 1, 1, 1)
	c.Assert(terror.ErrorEqual(err, autoid.ErrAutoincReadFailed), IsTrue)

	// test unsigned
	columnInfo.FieldType.Flag |= mysql.UnsignedFlag
	alloc = autoid.NewAllocatorFromTempTblInfo(tblInfo)
	c.Assert(alloc, NotNil)

	var n uint64 = math.MaxUint64 - 2
	err = alloc.Rebase(1, int64(n), true)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(n+1))
	_, _, err = alloc.Alloc(ctx, 1, 1, 1, 1)
	c.Assert(terror.ErrorEqual(err, autoid.ErrAutoincReadFailed), IsTrue)
}
