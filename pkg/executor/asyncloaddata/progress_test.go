// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package asyncloaddata

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProgressMarshalUnmarshal(t *testing.T) {
	p := NewProgress(true)
	require.Nil(t, p.PhysicalImportProgress)
	p.SourceFileSize = 123
	p.LoadedFileSize.Store(456)
	p.LoadedRowCnt.Store(789)

	s := p.String()
	require.Equal(t, `{"SourceFileSize":123,"LoadedFileSize":456,"LoadedRowCnt":789}`, s)

	s2 := `{"SourceFileSize":111,"LoadedFileSize":222,"LoadedRowCnt":333}`
	p2, err := ProgressFromJSON([]byte(s2))
	require.NoError(t, err)
	require.Equal(t, int64(111), p2.SourceFileSize)
	require.Equal(t, int64(222), p2.LoadedFileSize.Load())
	require.Equal(t, uint64(333), p2.LoadedRowCnt.Load())

	p = NewProgress(false)
	require.Nil(t, p.LogicalImportProgress)
	p.SourceFileSize = 123
	p.ReadRowCnt.Store(790)
	p.EncodeFileSize.Store(100)
	p.LoadedRowCnt.Store(789)

	s = p.String()
	require.Equal(t, `{"SourceFileSize":123,"ReadRowCnt":790,"EncodeFileSize":100,"LoadedRowCnt":789}`, s)

	s2 = `{"SourceFileSize":111,"ReadRowCnt":790,"EncodeFileSize":222,"LoadedRowCnt":333}`
	p2, err = ProgressFromJSON([]byte(s2))
	require.NoError(t, err)
	require.Equal(t, int64(111), p2.SourceFileSize)
	require.Equal(t, uint64(790), p2.ReadRowCnt.Load())
	require.Equal(t, int64(222), p2.EncodeFileSize.Load())
	require.Equal(t, uint64(333), p2.LoadedRowCnt.Load())
}
