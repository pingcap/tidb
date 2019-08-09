// Copyright 2016 PingCAP, Inc.
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

package kv

import (
	"bytes"
	"context"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/terror"
)

type testBufferStoreSuite struct{}

var _ = Suite(testBufferStoreSuite{})

func (s testBufferStoreSuite) TestGetSet(c *C) {
	bs := NewBufferStore(&mockSnapshot{NewMemDbBuffer(DefaultTxnMembufCap)}, DefaultTxnMembufCap)
	key := Key("key")
	_, err := bs.Get(context.TODO(), key)
	c.Check(err, NotNil)

	err = bs.Set(key, []byte("value"))
	c.Check(err, IsNil)

	value, err := bs.Get(context.TODO(), key)
	c.Check(err, IsNil)
	c.Check(bytes.Compare(value, []byte("value")), Equals, 0)
}

func (s testBufferStoreSuite) TestSaveTo(c *C) {
	bs := NewBufferStore(&mockSnapshot{NewMemDbBuffer(DefaultTxnMembufCap)}, DefaultTxnMembufCap)
	var buf bytes.Buffer
	for i := 0; i < 10; i++ {
		fmt.Fprint(&buf, i)
		err := bs.Set(buf.Bytes(), buf.Bytes())
		c.Check(err, IsNil)
		buf.Reset()
	}
	err := bs.Set(Key("novalue"), []byte("novalue"))
	c.Check(err, IsNil)

	mutator := NewMemDbBuffer(DefaultTxnMembufCap)
	err = bs.SaveTo(mutator)
	c.Check(err, IsNil)

	iter, err := mutator.Iter(nil, nil)
	c.Check(err, IsNil)
	for iter.Valid() {
		cmp := bytes.Compare(iter.Key(), iter.Value())
		c.Check(cmp, Equals, 0)
		err = iter.Next()
		c.Check(err, IsNil)
	}
}

func (s testBufferStoreSuite) TestBufferStore(c *C) {
	bs := NewBufferStore(&mockSnapshot{NewMemDbBuffer(DefaultTxnMembufCap)}, -1)
	bs.SetCap(10)
	key := Key("key")
	err := bs.Set(key, []byte("value"))
	c.Check(err, IsNil)

	err = bs.Set(key, []byte(""))
	c.Check(terror.ErrorEqual(err, ErrCannotSetNilValue), IsTrue)

	err = bs.Delete(key)
	c.Check(err, IsNil)

	_, err = bs.Get(context.TODO(), key)
	c.Check(terror.ErrorEqual(err, ErrNotExist), IsTrue)

	bs.Reset()
	_, err = bs.Get(context.TODO(), key)
	c.Check(terror.ErrorEqual(err, ErrNotExist), IsTrue)

}
