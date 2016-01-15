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

package kvrpc

import (
	"bytes"
	"testing"

	. "github.com/pingcap/check"
	kp "github.com/pingcap/tidb/kvrpc/proto"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCodecSuite{})

type testCodecSuite struct {
}

func (s *testCodecSuite) TestCodec(c *C) {
	m1 := &kp.Request{}
	msgType := kp.MessageType_Get
	m1.Type = &msgType
	buf := new(bytes.Buffer)
	err := EncodeMessage(buf, 42, m1)
	c.Assert(err, IsNil)

	m2 := &kp.Request{}
	msgId, err := DecodeMessage(buf, m2)
	c.Assert(err, IsNil)
	c.Assert(msgId, Equals, uint64(42))
	c.Assert(*m1.Type, Equals, *m2.Type)
}
