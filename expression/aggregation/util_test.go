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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregation

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestDistinct(t *testing.T) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	dc := createDistinctChecker(sc)
	testCases := []struct {
		vals   []interface{}
		expect bool
	}{
		{[]interface{}{1, 1}, true},
		{[]interface{}{1, 1}, false},
		{[]interface{}{1, 2}, true},
		{[]interface{}{1, 2}, false},
		{[]interface{}{1, nil}, true},
		{[]interface{}{1, nil}, false},
	}
	for _, tc := range testCases {
		d, err := dc.Check(types.MakeDatums(tc.vals...))
		require.NoError(t, err)
		require.Equal(t, tc.expect, d)
	}
}
