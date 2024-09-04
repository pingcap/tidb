// Copyright 2024 PingCAP, Inc.
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

package indexadvisor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCollectTableFromQuery(t *testing.T) {
	names, err := collectTableNamesFromQuery("test", "select * from t where a = 1")
	require.NoError(t, err)
	require.Equal(t, names[0], "test.t")

	names, err = collectTableNamesFromQuery("test", "select * from t1, t2")
	require.NoError(t, err)
	require.Equal(t, names[0], "test.t1")
	require.Equal(t, names[1], "test.t2")

	names, err = collectTableNamesFromQuery("test", "select * from t1 where t1.a < (select max(b) from t2)")
	require.NoError(t, err)
	require.Equal(t, names[0], "test.t1")
	require.Equal(t, names[1], "test.t2")

	names, err = collectTableNamesFromQuery("test", "select * from t1 where t1.a < (select max(b) from db2.t2)")
	require.NoError(t, err)
	require.Equal(t, names[0], "test.t1")
	require.Equal(t, names[1], "db2.t2")
}

func TestCollectSelectColumnsFromQuery(t *testing.T) {
	names, err := collectSelectColumnsFromQuery(Query{Text: "select a, b from test.t"})
	require.NoError(t, err)
	require.True(t, names.String() == "{test.t.a, test.t.b}")

	names, err = collectSelectColumnsFromQuery(Query{Text: "select a, b, c from test.t"})
	require.NoError(t, err)
	require.True(t, names.String() == "{test.t.a, test.t.b, test.t.c}")
}

func TestCollectOrderByColumnsFromQuery(t *testing.T) {
	cols, err := collectOrderByColumnsFromQuery(Query{Text: "select a, b from test.t order by a"})
	require.NoError(t, err)
	require.Equal(t, len(cols), 1)
	require.Equal(t, cols[0].Key(), "test.t.a")

	cols, err = collectOrderByColumnsFromQuery(Query{Text: "select a, b from test.t order by a, b"})
	require.NoError(t, err)
	require.Equal(t, len(cols), 2)
	require.Equal(t, cols[0].Key(), "test.t.a")
	require.Equal(t, cols[1].Key(), "test.t.b")
}

func TestCollectDNFColumnsFromQuery(t *testing.T) {
	cols, err := collectDNFColumnsFromQuery(Query{Text: "select a, b from test.t where a = 1 or b = 2"})
	require.NoError(t, err)
	require.Equal(t, cols.String(), "{test.t.a, test.t.b}")

	cols, err = collectDNFColumnsFromQuery(Query{Text: "select a, b from test.t where a = 1 or b = 2 or c=3"})
	require.NoError(t, err)
	require.Equal(t, cols.String(), "{test.t.a, test.t.b, test.t.c}")
}
