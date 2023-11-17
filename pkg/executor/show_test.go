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

package executor

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func Test_fillOneImportJobInfo(t *testing.T) {
	typeBytes := []byte{mysql.TypeLonglong, mysql.TypeString, mysql.TypeString, mysql.TypeLonglong,
		mysql.TypeString, mysql.TypeString, mysql.TypeString, mysql.TypeLonglong,
		mysql.TypeString, mysql.TypeTimestamp, mysql.TypeTimestamp, mysql.TypeTimestamp, mysql.TypeString}
	fieldTypes := make([]*types.FieldType, 0, len(typeBytes))
	for _, tp := range typeBytes {
		fieldType := types.NewFieldType(tp)
		flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.SetFlen(flen)
		fieldType.SetDecimal(decimal)
		charset, collate := types.DefaultCharsetForType(tp)
		fieldType.SetCharset(charset)
		fieldType.SetCollate(collate)
		fieldTypes = append(fieldTypes, fieldType)
	}
	c := chunk.New(fieldTypes, 10, 10)
	jobInfo := &importer.JobInfo{
		Parameters: importer.ImportParameters{},
	}
	fillOneImportJobInfo(jobInfo, c, -1)
	require.True(t, c.GetRow(0).IsNull(7))
	require.True(t, c.GetRow(0).IsNull(10))
	require.True(t, c.GetRow(0).IsNull(11))

	fillOneImportJobInfo(jobInfo, c, 0)
	require.False(t, c.GetRow(1).IsNull(7))
	require.Equal(t, uint64(0), c.GetRow(1).GetUint64(7))
	require.True(t, c.GetRow(1).IsNull(10))
	require.True(t, c.GetRow(1).IsNull(11))

	jobInfo.Summary = &importer.JobSummary{ImportedRows: 123}
	jobInfo.StartTime = types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 0)
	jobInfo.EndTime = types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 0)
	fillOneImportJobInfo(jobInfo, c, 0)
	require.False(t, c.GetRow(2).IsNull(7))
	require.Equal(t, uint64(123), c.GetRow(2).GetUint64(7))
	require.False(t, c.GetRow(2).IsNull(10))
	require.False(t, c.GetRow(2).IsNull(11))
}
