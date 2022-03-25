// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package filter

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

type testFilterSuite struct{}

var _ = Suite(&testFilterSuite{})

func (s *testFilterSuite) TestIsSystemSchema(c *C) {
	cases := []struct {
		name     string
		expected bool
	}{
		{"information_schema", true},
		{"performance_schema", true},
		{"mysql", true},
		{"sys", true},
		{"INFORMATION_SCHEMA", true},
		{"PERFORMANCE_SCHEMA", true},
		{"MYSQL", true},
		{"SYS", true},
		{"not_system_schema", false},
		{"METRICS_SCHEMA", true},
		{"INSPECTION_SCHEMA", true},
	}

	for _, t := range cases {
		c.Assert(IsSystemSchema(t.name), Equals, t.expected, Commentf("schema name = %s", t.name))
	}

}
