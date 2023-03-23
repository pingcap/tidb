// Copyright 2023 PingCAP, Inc.
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

package mydump

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/stretchr/testify/require"
)

func TestORCParser(t *testing.T) {
	parser, err := NewORCParser(log.L(), "/Users/jujiajia/code/orc/examples/TestOrcFile.testTimestamp.orc")
	require.NoError(t, err)
	fmt.Println(parser.Columns())
	require.NoError(t, parser.ReadRow())
	row := parser.LastRow()
	require.Len(t, row.Row, len(parser.Columns()))
}
