// Copyright 2016 PingCAP, Inc.
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

package util

import (
	"bytes"
	"crypto/x509/pkix"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/fastrand"
	"github.com/pingcap/tidb/util/memory"
	"github.com/stretchr/testify/require"
)

func TestRunWithRetry(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		cnt := 0
		err := RunWithRetry(3, 1, func() (bool, error) {
			cnt++
			if cnt < 2 {
				return true, errors.New("err")
			}
			return true, nil
		})
		require.Nil(t, err)
		require.Equal(t, 2, cnt)
	})

	t.Run("retry exceeds", func(t *testing.T) {
		cnt := 0
		err := RunWithRetry(3, 1, func() (bool, error) {
			cnt++
			if cnt < 4 {
				return true, errors.New("err")
			}
			return true, nil
		})
		require.NotNil(t, err)
		require.Equal(t, 3, cnt)
	})

	t.Run("failed result", func(t *testing.T) {
		cnt := 0
		err := RunWithRetry(3, 1, func() (bool, error) {
			cnt++
			if cnt < 2 {
				return false, errors.New("err")
			}
			return true, nil
		})
		require.NotNil(t, err)
		require.Equal(t, 1, cnt)
	})
}

func TestX509NameParseMatch(t *testing.T) {
	require.Equal(t, "", X509NameOnline(pkix.Name{}))

	check := pkix.Name{
		Names: []pkix.AttributeTypeAndValue{
			MockPkixAttribute(Country, "SE"),
			MockPkixAttribute(Province, "Stockholm2"),
			MockPkixAttribute(Locality, "Stockholm"),
			MockPkixAttribute(Organization, "MySQL demo client certificate"),
			MockPkixAttribute(OrganizationalUnit, "testUnit"),
			MockPkixAttribute(CommonName, "client"),
			MockPkixAttribute(Email, "client@example.com"),
		},
	}
	result := "/C=SE/ST=Stockholm2/L=Stockholm/O=MySQL demo client certificate/OU=testUnit/CN=client/emailAddress=client@example.com"
	require.Equal(t, result, X509NameOnline(check))
}

func TestBasicFuncWithRecovery(t *testing.T) {
	var recovery interface{}
	WithRecovery(func() {
		panic("test")
	}, func(r interface{}) {
		recovery = r
	})
	require.Equal(t, "test", recovery)
}

func TestBasicFuncSyntaxError(t *testing.T) {
	require.Nil(t, SyntaxError(nil))
	require.True(t, terror.ErrorEqual(SyntaxError(errors.New("test")), parser.ErrParse))
	require.True(t, terror.ErrorEqual(SyntaxError(parser.ErrSyntax.GenWithStackByArgs()), parser.ErrSyntax))
}

func TestBasicFuncSyntaxWarn(t *testing.T) {
	require.Nil(t, SyntaxWarn(nil))
	require.True(t, terror.ErrorEqual(SyntaxWarn(errors.New("test")), parser.ErrParse))
}

func TestBasicFuncProcessInfo(t *testing.T) {
	pi := ProcessInfo{
		ID:      1,
		User:    "test",
		Host:    "www",
		DB:      "db",
		Command: mysql.ComSleep,
		Plan:    nil,
		Time:    time.Now(),
		State:   3,
		Info:    "test",
		StmtCtx: &stmtctx.StatementContext{
			MemTracker: memory.NewTracker(-1, -1),
		},
	}
	row := pi.ToRowForShow(false)
	row2 := pi.ToRowForShow(true)
	require.Equal(t, row2, row)
	require.Len(t, row, 8)
	require.Equal(t, pi.ID, row[0])
	require.Equal(t, pi.User, row[1])
	require.Equal(t, pi.Host, row[2])
	require.Equal(t, pi.DB, row[3])
	require.Equal(t, "Sleep", row[4])
	require.Equal(t, uint64(0), row[5])
	require.Equal(t, "in transaction; autocommit", row[6])
	require.Equal(t, "test", row[7])

	row3 := pi.ToRow(time.UTC)
	require.Equal(t, row, row3[:8])
	require.Equal(t, int64(0), row3[9])
}

func TestBasicFuncRandomBuf(t *testing.T) {
	buf := fastrand.Buf(5)
	require.Len(t, buf, 5)
	require.False(t, bytes.Contains(buf, []byte("$")))
	require.False(t, bytes.Contains(buf, []byte{0}))
}

func TestToPB(t *testing.T) {
	column := &model.ColumnInfo{
		ID:           1,
		Name:         model.NewCIStr("c"),
		Offset:       0,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(0),
		Hidden:       true,
	}
	column.SetCollate("utf8mb4_general_ci")

	column2 := &model.ColumnInfo{
		ID:           1,
		Name:         model.NewCIStr("c"),
		Offset:       0,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(0),
		Hidden:       true,
	}
	column2.SetCollate("utf8mb4_bin")

	require.Equal(t, "column_id:1 collation:-45 columnLen:-1 decimal:-1 ", ColumnToProto(column, false).String())
	require.Equal(t, "column_id:1 collation:-45 columnLen:-1 decimal:-1 ", ColumnsToProto([]*model.ColumnInfo{column, column2}, false, false)[0].String())
}

func TestComposeURL(t *testing.T) {
	// TODO Setup config for TLS and verify https protocol output
	require.Equal(t, ComposeURL("server.example.com", ""), "http://server.example.com")
	require.Equal(t, ComposeURL("httpserver.example.com", ""), "http://httpserver.example.com")
	require.Equal(t, ComposeURL("http://httpserver.example.com", "/"), "http://httpserver.example.com/")
	require.Equal(t, ComposeURL("https://httpserver.example.com", "/api/test"), "https://httpserver.example.com/api/test")
	require.Equal(t, ComposeURL("http://server.example.com", ""), "http://server.example.com")
	require.Equal(t, ComposeURL("https://server.example.com", ""), "https://server.example.com")
}

func assertChannel[T any](t *testing.T, ch <-chan T, items ...T) {
	for i, item := range items {
		require.Equal(t, <-ch, item, "the %d-th item doesn't match", i)
	}
	select {
	case item, ok := <-ch:
		require.False(t, ok, "channel not closed: more item %v", item)
	default:
		t.Fatal("channel not closed: blocked")
	}
}

func TestChannelMap(t *testing.T) {
	ch := make(chan int, 4)
	ch <- 1
	ch <- 2
	ch <- 3

	tableCh := ChanMap(ch, func(i int) string {
		return fmt.Sprintf("table%d", i)
	})
	close(ch)

	assertChannel(t, tableCh, "table1", "table2", "table3")
}
