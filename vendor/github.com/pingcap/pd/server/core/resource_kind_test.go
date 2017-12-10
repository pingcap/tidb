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

package core

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestCore(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testResouceKindSuite{})

type testResouceKindSuite struct{}

func (s *testResouceKindSuite) TestString(c *C) {
	tbl := []struct {
		value ResourceKind
		name  string
	}{
		{UnKnownKind, "unknown"},
		{AdminKind, "admin"},
		{LeaderKind, "leader"},
		{RegionKind, "region"},
		{HotRegionKind, "hot"},
		{OtherKind, "other"},
		{ResourceKind(404), "unknown"},
	}
	for _, t := range tbl {
		c.Assert(t.value.String(), Equals, t.name)
	}
}

func (s *testResouceKindSuite) TestParseResouceKind(c *C) {
	tbl := []struct {
		name  string
		value ResourceKind
	}{
		{"unknown", UnKnownKind},
		{"admin", AdminKind},
		{"leader", LeaderKind},
		{"region", RegionKind},
		{"hot", HotRegionKind},
		{"other", OtherKind},
		{"test", UnKnownKind},
	}
	for _, t := range tbl {
		c.Assert(ParseResourceKind(t.name), Equals, t.value)
	}
}
