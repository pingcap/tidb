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

package mvdeltamergeagg

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestFirstNullRow(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeLonglong)
	col := chunk.NewColumn(ft, 3)
	col.AppendInt64(1)
	col.AppendNull()
	col.AppendInt64(2)
	require.Equal(t, 1, firstNullRow(col, 3))

	noNullCol := chunk.NewColumn(ft, 2)
	noNullCol.AppendInt64(1)
	noNullCol.AppendInt64(2)
	require.Equal(t, -1, firstNullRow(noNullCol, 2))
}
