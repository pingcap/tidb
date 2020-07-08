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

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/terror"
)

type testBufferStoreSuite struct{}

var _ = Suite(testBufferStoreSuite{})

func (s testBufferStoreSuite) TestGetSet(c *C) {
	bs := NewBufferStore(&mockSnapshot{NewMemDbBuffer()})
	key := Key("key")
	_, err := bs.Get(context.TODO(), key)
	c.Check(err, NotNil)

	err = bs.Set(key, []byte("value"))
	c.Check(err, IsNil)

	value, err := bs.Get(context.TODO(), key)
	c.Check(err, IsNil)
	c.Check(bytes.Compare(value, []byte("value")), Equals, 0)
}

func (s testBufferStoreSuite) TestBufferStore(c *C) {
	bs := NewBufferStore(&mockSnapshot{NewMemDbBuffer()})
	key := Key("key")
	err := bs.Set(key, []byte("value"))
	c.Check(err, IsNil)

	err = bs.Set(key, []byte(""))
	c.Check(terror.ErrorEqual(err, ErrCannotSetNilValue), IsTrue)

	err = bs.Delete(key)
	c.Check(err, IsNil)

	_, err = bs.Get(context.TODO(), key)
	c.Check(terror.ErrorEqual(err, ErrNotExist), IsTrue)

	bs.Discard()
	_, err = bs.Get(context.TODO(), key)
	c.Check(terror.ErrorEqual(err, ErrNotExist), IsTrue)

}
