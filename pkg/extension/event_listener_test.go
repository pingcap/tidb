// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package extension_test

import (
	"context"
	"encoding/binary"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

type stmtEventRecord struct {
	tp              extension.StmtEventTp
	user            *auth.UserIdentity
	originalText    string
	redactText      string
	params          []types.Datum
	connInfo        *variable.ConnectionInfo
	sessionAlias    string
	err             string
	tables          []stmtctx.TableEntry
	affectedRows    uint64
	stmtNode        ast.StmtNode
	executeStmtNode *ast.ExecuteStmt
	preparedNode    ast.StmtNode
}

type sessionHandler struct {
	records []stmtEventRecord
}

func (h *sessionHandler) OnStmtEvent(tp extension.StmtEventTp, info extension.StmtEventInfo) {
	tables := make([]stmtctx.TableEntry, len(info.RelatedTables()))
	copy(tables, info.RelatedTables())

	redactText, _ := info.SQLDigest()
	r := stmtEventRecord{
		tp:              tp,
		user:            info.User(),
		originalText:    info.OriginalText(),
		redactText:      redactText,
		params:          info.PreparedParams(),
		connInfo:        info.ConnectionInfo(),
		sessionAlias:    info.SessionAlias(),
		tables:          tables,
		affectedRows:    info.AffectedRows(),
		stmtNode:        info.StmtNode(),
		executeStmtNode: info.ExecuteStmtNode(),
		preparedNode:    info.ExecutePreparedStmt(),
	}

	if err := info.GetError(); err != nil {
		r.err = err.Error()
	}

	h.records = append(h.records, r)
}

func (h *sessionHandler) Reset() {
	h.records = nil
}

func (h *sessionHandler) GetHandler() *extension.SessionHandler {
	return &extension.SessionHandler{
		OnStmtEvent: h.OnStmtEvent,
	}
}

func registerHandler(t *testing.T) *sessionHandler {
	h := &sessionHandler{}
	err := extension.Register(
		"test",
		extension.WithSessionHandlerFactory(h.GetHandler),
	)
	require.NoError(t, err)
	return h
}

func getPreparedID(t *testing.T, sctx sessionctx.Context) uint32 {
	sessStates := &sessionstates.SessionStates{}
	require.NoError(t, sctx.GetSessionVars().EncodeSessionStates(context.Background(), sessStates))
	return sessStates.PreparedStmtID
}

type stmtEventCase struct {
	sql           string
	binaryExecute uint32
	executeParams []paramInfo

	err             string
	originalText    string
	redactText      string
	affectedRows    uint64
	tables          []stmtctx.TableEntry
	parseError      bool
	prepareNotFound bool
	multiQueryCases []stmtEventCase
	dispatchData    []byte
	sessionAlias    string
}

func TestExtensionStmtEvents(t *testing.T) {
	defer extension.Reset()
	extension.Reset()
	h := registerHandler(t)
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	serv := server.CreateMockServer(t, store)
	defer serv.Close()
	conn := server.CreateMockConn(t, serv)
	defer conn.Close()

	require.NoError(t, conn.HandleQuery(context.Background(), "SET tidb_enable_non_prepared_plan_cache=0")) // sctx.InMultiStmts cannot be set correctly in this UT.
	require.NoError(t, conn.HandleQuery(context.Background(), "SET tidb_multi_statement_mode='ON'"))
	require.NoError(t, conn.HandleQuery(context.Background(), "use test"))
	require.NoError(t, conn.HandleQuery(context.Background(), "create table t1(a int, b int)"))
	require.NoError(t, conn.HandleQuery(context.Background(), "create table t2(id int primary key)"))
	require.NoError(t, conn.HandleQuery(context.Background(), "create database test2"))
	require.NoError(t, conn.HandleQuery(context.Background(), "create table test2.t1(c int, d int)"))
	require.NoError(t, conn.HandleQuery(context.Background(), "set @a=1"))
	require.NoError(t, conn.HandleQuery(context.Background(), "set @b=2"))

	cmd := append([]byte{mysql.ComStmtPrepare}, []byte("select ?")...)
	require.NoError(t, conn.Dispatch(context.Background(), cmd))
	stmtID1 := getPreparedID(t, conn.Context())

	cmd = append(
		[]byte{mysql.ComStmtPrepare},
		[]byte("select a, b from t1 left join test2.t1 as t2 on t2.c = t1.a where t1.a = 3 and t1.b = ? and t2.d = ?")...)
	require.NoError(t, conn.Dispatch(context.Background(), cmd))
	stmtID2 := getPreparedID(t, conn.Context())

	require.NoError(t, conn.HandleQuery(context.Background(), "create table tnoexist(n int)"))
	cmd = append([]byte{mysql.ComStmtPrepare}, []byte("select * from tnoexist where n=?")...)
	require.NoError(t, conn.Dispatch(context.Background(), cmd))
	stmtID3 := getPreparedID(t, conn.Context())
	require.NoError(t, conn.HandleQuery(context.Background(), "drop table tnoexist"))

	cmd = append([]byte{mysql.ComStmtPrepare}, []byte("insert into t2 values(?)")...)
	require.NoError(t, conn.Dispatch(context.Background(), cmd))
	stmtID4 := getPreparedID(t, conn.Context())

	connID := conn.Context().Session.GetSessionVars().ConnectionID
	require.NotEqual(t, uint64(0), connID)

	cases := []stmtEventCase{
		{
			sql:        "select 1",
			redactText: "select ?",
		},
		{
			sql:        "invalid sql",
			parseError: true,
			err:        "[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 7 near \"invalid sql\" ",
		},
		{
			binaryExecute: stmtID1,
			executeParams: []paramInfo{
				{value: 7},
			},
			originalText: "select ?",
			redactText:   "select ?",
		},
		{
			sql:        "select a, b from t1 where a > 1 and b < 2",
			redactText: "select `a` , `b` from `t1` where `a` > ? and `b` < ?",
			tables: []stmtctx.TableEntry{
				{DB: "test", Table: "t1"},
			},
		},
		{
			sql:          "insert into t2 values(1)",
			redactText:   "insert into `t2` values ( ? )",
			affectedRows: 1,
			tables: []stmtctx.TableEntry{
				{DB: "test", Table: "t2"},
			},
		},
		{
			binaryExecute: stmtID2,
			executeParams: []paramInfo{
				{value: 3},
				{value: 4},
			},
			originalText: "select a, b from t1 left join test2.t1 as t2 on t2.c = t1.a where t1.a = 3 and t1.b = ? and t2.d = ?",
			redactText:   "select `a` , `b` from `t1` left join `test2` . `t1` as `t2` on `t2` . `c` = `t1` . `a` where `t1` . `a` = ? and `t1` . `b` = ? and `t2` . `d` = ?",
			tables: []stmtctx.TableEntry{
				{DB: "test", Table: "t1"},
				{DB: "test2", Table: "t1"},
			},
		},
		{
			binaryExecute: stmtID3,
			executeParams: []paramInfo{
				{value: 5},
			},
			originalText: "select * from tnoexist where n=?",
			redactText:   "select * from `tnoexist` where `n` = ?",
			tables: []stmtctx.TableEntry{
				{DB: "test", Table: "tnoexist"},
			},
			err: "select * from tnoexist where n=? [arguments: 5]: [planner:8113]Schema change caused error: [schema:1146]Table 'test.tnoexist' doesn't exist",
		},
		{
			binaryExecute: stmtID4,
			executeParams: []paramInfo{
				{value: 3},
			},
			originalText: "insert into t2 values(?)",
			redactText:   "insert into `t2` values ( ? )",
			affectedRows: 1,
			tables: []stmtctx.TableEntry{
				{DB: "test", Table: "t2"},
			},
		},
		{
			sql:        "prepare s from 'select * from t1 where a=1 and b>? and b<?'",
			redactText: "prepare `s` from ?",
		},
		{
			sql:          "execute s using @a, @b",
			originalText: "select * from t1 where a=1 and b>? and b<?",
			redactText:   "select * from `t1` where `a` = ? and `b` > ? and `b` < ?",
			executeParams: []paramInfo{
				{value: 1},
				{value: 2},
			},
			tables: []stmtctx.TableEntry{
				{DB: "test", Table: "t1"},
			},
		},
		{
			sql:        "execute sn using @a, @b",
			redactText: "execute `sn` using @a , @b",
			executeParams: []paramInfo{
				{value: 1},
				{value: 2},
			},
			prepareNotFound: true,
			err:             "[planner:8111]Prepared statement not found",
		},
		{
			sql:          "insert into t1 values(1, 10), (2, 20)",
			redactText:   "insert into `t1` values ( ... )",
			affectedRows: 2,
			tables: []stmtctx.TableEntry{
				{DB: "test", Table: "t1"},
			},
		},
		{
			sql:          "insert into t2 values(1)",
			redactText:   "insert into `t2` values ( ? )",
			affectedRows: 0,
			err:          "[kv:1062]Duplicate entry '1' for key 't2.PRIMARY'",
			tables: []stmtctx.TableEntry{
				{DB: "test", Table: "t2"},
			},
		},
		{
			sql: "select 1;select * from t1 where a > 1",
			multiQueryCases: []stmtEventCase{
				{
					originalText: "select 1;",
					redactText:   "select ?",
				},
				{
					originalText: "select * from t1 where a > 1",
					redactText:   "select * from `t1` where `a` > ?",
					tables: []stmtctx.TableEntry{
						{DB: "test", Table: "t1"},
					},
				},
			},
		},
		{
			binaryExecute: stmtID4,
			executeParams: []paramInfo{
				{value: 3},
			},
			err:          "insert into t2 values(?) [arguments: 3]: [kv:1062]Duplicate entry '3' for key 't2.PRIMARY'",
			originalText: "insert into t2 values(?)",
			redactText:   "insert into `t2` values ( ? )",
			affectedRows: 0,
			tables: []stmtctx.TableEntry{
				{DB: "test", Table: "t2"},
			},
		},
		{
			sql:        "create database db1",
			redactText: "create database `db1`",
			tables: []stmtctx.TableEntry{
				{DB: "db1", Table: ""},
			},
		},
		{
			sql:        "kill query 1",
			redactText: "kill query ?",
		},
		{
			sql:        "create placement policy p1 followers=1",
			redactText: "create placement policy `p1` followers = ?",
		},
		{
			dispatchData: append([]byte{mysql.ComInitDB}, []byte("db1")...),
			originalText: "use `db1`",
			redactText:   "use `db1`",
			tables: []stmtctx.TableEntry{
				{DB: "db1", Table: ""},
			},
		},
		{
			dispatchData: append([]byte{mysql.ComInitDB}, []byte("noexistdb")...),
			originalText: "use `noexistdb`",
			redactText:   "use `noexistdb`",
			err:          "[schema:1049]Unknown database 'noexistdb'",
			tables: []stmtctx.TableEntry{
				{DB: "noexistdb", Table: ""},
			},
		},
		{
			sql:          "set @@tidb_session_alias='alias123'",
			redactText:   "set @@tidb_session_alias = ?",
			sessionAlias: "alias123",
		},
		{
			sql:          "select 123",
			redactText:   "select ?",
			sessionAlias: "alias123",
		},
		{
			sql:          "set @@tidb_session_alias=''",
			redactText:   "set @@tidb_session_alias = ?",
			sessionAlias: "",
		},
		{
			sql:          "select 123",
			redactText:   "select ?",
			sessionAlias: "",
		},
	}

	for i, c := range cases {
		h.Reset()
		conn.Context().SetProcessInfo("", time.Now(), mysql.ComSleep, 0)

		var err error
		switch {
		case c.sql != "":
			err = conn.HandleQuery(context.Background(), c.sql)
			if c.originalText == "" {
				c.originalText = c.sql
			}
			if c.redactText == "" {
				c.redactText = c.sql
			}
		case c.binaryExecute != 0:
			err = conn.Dispatch(context.Background(), getExecuteBytes(c.binaryExecute, false, true, c.executeParams...))
		case c.dispatchData != nil:
			err = conn.Dispatch(context.Background(), c.dispatchData)
		}

		if c.err != "" {
			require.EqualError(t, err, c.err)
		} else {
			require.NoError(t, err)
		}

		subCases := c.multiQueryCases
		if subCases == nil {
			subCases = []stmtEventCase{c}
		}

		require.Equal(t, len(subCases), len(h.records), "%d", i)
		for j, subCase := range subCases {
			record := h.records[j]
			if subCase.err != "" {
				require.Equal(t, subCase.err, record.err)
				require.Equal(t, extension.StmtError, record.tp)
			} else {
				require.Empty(t, record.err)
				require.Equal(t, extension.StmtSuccess, record.tp)
			}

			require.NotNil(t, record.connInfo)
			if subCase.parseError {
				require.Nil(t, record.stmtNode)
				require.Nil(t, record.executeStmtNode)
				require.Nil(t, record.preparedNode)
			} else {
				require.NotNil(t, record.stmtNode)
				if subCase.binaryExecute != 0 || strings.HasPrefix(strings.ToLower(subCase.sql), "execute ") {
					require.NotNil(t, record.executeStmtNode)
					require.Equal(t, record.stmtNode, record.executeStmtNode)
					if c.prepareNotFound {
						require.Nil(t, record.preparedNode)
					} else {
						require.NotNil(t, record.preparedNode)
						require.NotEqual(t, record.preparedNode, record.executeStmtNode)
					}
				} else {
					require.Nil(t, record.executeStmtNode)
					require.Nil(t, record.preparedNode)
				}
			}

			require.Equal(t, connID, record.connInfo.ConnectionID)
			require.Equal(t, "root", record.user.Username)
			require.Equal(t, "localhost", record.user.Hostname)
			require.Equal(t, "root", record.user.AuthUsername)
			require.Equal(t, "%", record.user.AuthHostname)
			require.Equal(t, subCase.sessionAlias, record.sessionAlias)

			require.Equal(t, subCase.originalText, record.originalText)
			require.Equal(t, subCase.redactText, record.redactText)
			require.Equal(t, subCase.affectedRows, record.affectedRows)
			if subCase.tables == nil {
				subCase.tables = []stmtctx.TableEntry{}
			}
			sort.Slice(subCase.tables, func(i, j int) bool {
				l := subCase.tables[i]
				r := subCase.tables[j]
				return l.DB < r.DB || (l.DB == r.DB && l.Table < r.Table)
			})
			sort.Slice(record.tables, func(i, j int) bool {
				l := record.tables[i]
				r := record.tables[j]
				return l.DB < r.DB || (l.DB == r.DB && l.Table < r.Table)
			})
			require.Equal(t, subCase.tables, record.tables,
				"sql: %s\noriginalText: %s\n", subCase.sql, subCase.originalText)

			require.Equal(t, len(subCase.executeParams), len(record.params))
			for k, param := range subCase.executeParams {
				require.Equal(t, uint64(param.value), record.params[k].GetUint64())
			}
		}
	}
}

type paramInfo struct {
	value  uint32
	isNull bool
}

// create bytes for COM_STMT_EXECUTE. It only supports int type for convenience.
func getExecuteBytes(stmtID uint32, useCursor bool, newParam bool, params ...paramInfo) []byte {
	nullBitmapLen := (len(params) + 7) >> 3
	buf := make([]byte, 11+nullBitmapLen+len(params)*6)
	pos := 0
	buf[pos] = mysql.ComStmtExecute
	pos++
	binary.LittleEndian.PutUint32(buf[pos:], stmtID)
	pos += 4
	if useCursor {
		buf[pos] = 1
	}
	pos++
	binary.LittleEndian.PutUint32(buf[pos:], 1)
	pos += 4
	for i, param := range params {
		if param.isNull {
			buf[pos+(i>>3)] |= 1 << (i % 8)
		}
	}
	pos += nullBitmapLen
	if newParam {
		buf[pos] = 1
		pos++
		for i := 0; i < len(params); i++ {
			buf[pos] = mysql.TypeLong
			pos++
			buf[pos] = 0
			pos++
		}
	} else {
		buf[pos] = 0
		pos++
	}
	for _, param := range params {
		if !param.isNull {
			binary.LittleEndian.PutUint32(buf[pos:], param.value)
			pos += 4
		}
	}
	return buf[:pos]
}
