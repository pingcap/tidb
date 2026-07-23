// Copyright 2026 PingCAP, Inc.
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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestTableSplitRangeFromTableScan(t *testing.T) {
	ts := &PhysicalTableScan{}
	start, end, ok, err := tableSplitRangeFromTableScan(ts)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, start)
	require.Nil(t, end)

	ts.TableSplit = &ast.TableSplit{Start: "6161", End: "7a7a"}
	start, end, ok, err = tableSplitRangeFromTableScan(ts)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("aa"), start)
	require.Equal(t, []byte("zz"), end)

	ts.TableSplit = &ast.TableSplit{Start: "zz", End: "7a7a"}
	_, _, _, err = tableSplitRangeFromTableScan(ts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "encoding/hex")

	ts.TableSplit = &ast.TableSplit{Start: "6161", End: "bad-hex"}
	_, _, _, err = tableSplitRangeFromTableScan(ts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "encoding/hex")
}
