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

package vitess

import (
	"encoding/hex"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
	"math"
	"strings"
	"testing"
)

var _ = Suite(&testVitessSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testVitessSuite struct {
}

func toHex(buf []byte) string {
	return strings.ToUpper(hex.EncodeToString(buf))
}

func fromHex(hexStr string) []byte {
	if buf, err := hex.DecodeString(hexStr); err == nil {
		return buf
	} else {
		panic(err)
	}
}

var _ = Suite(&testVitessSuite{})

func (s *testVitessSuite) TestVitessHash(c *C) {
	defer testleak.AfterTest(c)()

	hashed, err := VitessHashUint64(30375298039)
	c.Assert(err, IsNil)
	c.Assert(toHex(hashed), Equals, "031265661E5F1133")

	// Same as previous value but passed as a []byte instead
	hashed, err = VitessHash(fromHex("00000007128243F7"))
	c.Assert(err, IsNil)
	c.Assert(toHex(hashed), Equals, "031265661E5F1133")

	hashed, err = VitessHashUint64(1123)
	c.Assert(err, IsNil)
	c.Assert(toHex(hashed), Equals, "031B565D41BDF8CA")

	hashed, err = VitessHashUint64(30573721600)
	c.Assert(err, IsNil)
	c.Assert(toHex(hashed), Equals, "1EFD6439F2050FFD")

	hashed, err = VitessHashUint64(116)
	c.Assert(err, IsNil)
	c.Assert(toHex(hashed), Equals, "1E1788FF0FDE093C")

	hashed, err = VitessHashUint64(math.MaxUint64)
	c.Assert(err, IsNil)
	c.Assert(toHex(hashed), Equals, "355550B2150E2451")
}
