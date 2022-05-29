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

package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/testutils"
)

type Issue33699CheckType struct {
	name              string
	defVal            string
	setVal            string
	isSessionVariable bool
}

func (c *Issue33699CheckType) toSetSessionVar() string {
	if c.isSessionVariable {
		return fmt.Sprintf("set session %s=%s", c.name, c.setVal)
	}
	return fmt.Sprintf("set @%s=%s", c.name, c.setVal)
}

func (c *Issue33699CheckType) toGetSessionVar() string {
	if c.isSessionVariable {
		return fmt.Sprintf("select @@session.%s", c.name)
	}
	return fmt.Sprintf("select @%s", c.name)
}

func TestIssue33699(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	var outBuffer bytes.Buffer
	tidbdrv := NewTiDBDriver(store)
	cfg := newTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	defer server.Close()

	cc := &clientConn{
		connectionID: 1,
		salt:         []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14},
		server:       server,
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(&outBuffer),
		},
		collation:  mysql.DefaultCollationID,
		peerHost:   "localhost",
		alloc:      arena.NewAllocator(512),
		chunkAlloc: chunk.NewAllocator(),
		capability: mysql.ClientProtocol41,
	}

	tk := testkit.NewTestKit(t, store)
	ctx := &TiDBContext{Session: tk.Session()}
	cc.setCtx(ctx)

	// change user.
	doChangeUser := func() {
		userData := append([]byte("root"), 0x0, 0x0)
		userData = append(userData, []byte("test")...)
		userData = append(userData, 0x0)
		changeUserReq := dispatchInput{
			com: mysql.ComChangeUser,
			in:  userData,
			err: nil,
			out: []byte{0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0},
		}
		inBytes := append([]byte{changeUserReq.com}, changeUserReq.in...)
		err = cc.dispatch(context.Background(), inBytes)
		require.Equal(t, changeUserReq.err, err)
		if err == nil {
			err = cc.flush(context.TODO())
			require.NoError(t, err)
			require.Equal(t, changeUserReq.out, outBuffer.Bytes())
		} else {
			_ = cc.flush(context.TODO())
		}
		outBuffer.Reset()
	}
	// check variable.
	checks := []Issue33699CheckType{
		{ // self define.
			"a",
			"<nil>",
			"1",
			false,
		},
		{ // session variable
			"net_read_timeout",
			"30",
			"1234",
			true,
		},
		{
			"net_write_timeout",
			"60",
			"1234",
			true,
		},
	}

	// default;
	for _, ck := range checks {
		tk.MustQuery(ck.toGetSessionVar()).Check(testkit.Rows(ck.defVal))
	}
	// set;
	for _, ck := range checks {
		tk.MustExec(ck.toSetSessionVar())
	}
	// check after set.
	for _, ck := range checks {
		tk.MustQuery(ck.toGetSessionVar()).Check(testkit.Rows(ck.setVal))
	}
	// check for issue-33892: maybe trigger panic when ChangeUser before fix.
	var stop uint32
	go func(stop *uint32) {
		for {
			if atomic.LoadUint32(stop) == 1 {
				break
			}
			cc.getCtx().ShowProcess()
		}
	}(&stop)
	time.Sleep(time.Millisecond)
	doChangeUser()
	atomic.StoreUint32(&stop, 1)
	time.Sleep(time.Millisecond)
	require.NotEqual(t, ctx, cc.getCtx())
	require.NotEqual(t, ctx.Session, cc.ctx.Session)
	// new session,so values is defaults;
	tk.SetSession(cc.ctx.Session) // set new session.
	for _, ck := range checks {
		tk.MustQuery(ck.toGetSessionVar()).Check(testkit.Rows(ck.defVal))
	}
}

func TestMalformHandshakeHeader(t *testing.T) {
	data := []byte{0x00}
	var p handshakeResponse41
	_, err := parseHandshakeResponseHeader(context.Background(), &p, data)
	require.Error(t, err)
}

func TestParseHandshakeResponse(t *testing.T) {
	// test data from http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41
	data := []byte{
		0x85, 0xa2, 0x1e, 0x00, 0x00, 0x00, 0x00, 0x40, 0x08, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x72, 0x6f, 0x6f, 0x74, 0x00, 0x14, 0x22, 0x50, 0x79, 0xa2, 0x12, 0xd4,
		0xe8, 0x82, 0xe5, 0xb3, 0xf4, 0x1a, 0x97, 0x75, 0x6b, 0xc8, 0xbe, 0xdb, 0x9f, 0x80, 0x6d, 0x79,
		0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77,
		0x6f, 0x72, 0x64, 0x00, 0x61, 0x03, 0x5f, 0x6f, 0x73, 0x09, 0x64, 0x65, 0x62, 0x69, 0x61, 0x6e,
		0x36, 0x2e, 0x30, 0x0c, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65,
		0x08, 0x6c, 0x69, 0x62, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x04, 0x5f, 0x70, 0x69, 0x64, 0x05, 0x32,
		0x32, 0x33, 0x34, 0x34, 0x0f, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x76, 0x65, 0x72,
		0x73, 0x69, 0x6f, 0x6e, 0x08, 0x35, 0x2e, 0x36, 0x2e, 0x36, 0x2d, 0x6d, 0x39, 0x09, 0x5f, 0x70,
		0x6c, 0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x06, 0x78, 0x38, 0x36, 0x5f, 0x36, 0x34, 0x03, 0x66,
		0x6f, 0x6f, 0x03, 0x62, 0x61, 0x72,
	}
	var p handshakeResponse41
	offset, err := parseHandshakeResponseHeader(context.Background(), &p, data)
	require.NoError(t, err)
	require.Equal(t, mysql.ClientConnectAtts, p.Capability&mysql.ClientConnectAtts)
	err = parseHandshakeResponseBody(context.Background(), &p, data, offset)
	require.NoError(t, err)
	eq := mapIdentical(p.Attrs, map[string]string{
		"_client_version": "5.6.6-m9",
		"_platform":       "x86_64",
		"foo":             "bar",
		"_os":             "debian6.0",
		"_client_name":    "libmysql",
		"_pid":            "22344"})
	require.True(t, eq)

	data = []byte{
		0x8d, 0xa6, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x01, 0x08, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x70, 0x61, 0x6d, 0x00, 0x14, 0xab, 0x09, 0xee, 0xf6, 0xbc, 0xb1, 0x32,
		0x3e, 0x61, 0x14, 0x38, 0x65, 0xc0, 0x99, 0x1d, 0x95, 0x7d, 0x75, 0xd4, 0x47, 0x74, 0x65, 0x73,
		0x74, 0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x70,
		0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x00,
	}
	p = handshakeResponse41{}
	offset, err = parseHandshakeResponseHeader(context.Background(), &p, data)
	require.NoError(t, err)
	capability := mysql.ClientProtocol41 |
		mysql.ClientPluginAuth |
		mysql.ClientSecureConnection |
		mysql.ClientConnectWithDB
	require.Equal(t, capability, p.Capability&capability)
	err = parseHandshakeResponseBody(context.Background(), &p, data, offset)
	require.NoError(t, err)
	require.Equal(t, "pam", p.User)
	require.Equal(t, "test", p.DBName)

	// Test for compatibility of Protocol::HandshakeResponse320
	data = []byte{
		0x00, 0x80, 0x00, 0x00, 0x01, 0x72, 0x6f, 0x6f, 0x74, 0x00, 0x00,
	}
	p = handshakeResponse41{}
	offset, err = parseOldHandshakeResponseHeader(context.Background(), &p, data)
	require.NoError(t, err)
	capability = mysql.ClientProtocol41 |
		mysql.ClientSecureConnection
	require.Equal(t, capability, p.Capability&capability)
	err = parseOldHandshakeResponseBody(context.Background(), &p, data, offset)
	require.NoError(t, err)
	require.Equal(t, "root", p.User)
}

func TestIssue1768(t *testing.T) {
	// this data is from captured handshake packet, using mysql client.
	// TiDB should handle authorization correctly, even mysql client set
	// the ClientPluginAuthLenencClientData capability.
	data := []byte{
		0x85, 0xa6, 0xff, 0x01, 0x00, 0x00, 0x00, 0x01, 0x21, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x74, 0x65, 0x73, 0x74, 0x00, 0x14, 0xe9, 0x7a, 0x2b, 0xec, 0x4a, 0xa8,
		0xea, 0x67, 0x8a, 0xc2, 0x46, 0x4d, 0x32, 0xa4, 0xda, 0x39, 0x77, 0xe5, 0x61, 0x1a, 0x65, 0x03,
		0x5f, 0x6f, 0x73, 0x05, 0x4c, 0x69, 0x6e, 0x75, 0x78, 0x0c, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e,
		0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x08, 0x6c, 0x69, 0x62, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x04,
		0x5f, 0x70, 0x69, 0x64, 0x04, 0x39, 0x30, 0x33, 0x30, 0x0f, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e,
		0x74, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x06, 0x35, 0x2e, 0x37, 0x2e, 0x31, 0x34,
		0x09, 0x5f, 0x70, 0x6c, 0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x06, 0x78, 0x38, 0x36, 0x5f, 0x36,
		0x34, 0x0c, 0x70, 0x72, 0x6f, 0x67, 0x72, 0x61, 0x6d, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x05, 0x6d,
		0x79, 0x73, 0x71, 0x6c,
	}
	p := handshakeResponse41{}
	offset, err := parseHandshakeResponseHeader(context.Background(), &p, data)
	require.NoError(t, err)
	require.Equal(t, mysql.ClientPluginAuthLenencClientData, p.Capability&mysql.ClientPluginAuthLenencClientData)
	err = parseHandshakeResponseBody(context.Background(), &p, data, offset)
	require.NoError(t, err)
	require.NotEmpty(t, p.Auth)
}

func TestAuthSwitchRequest(t *testing.T) {
	// this data is from a MySQL 8.0 client
	data := []byte{
		0x85, 0xa6, 0xff, 0x1, 0x0, 0x0, 0x0, 0x1, 0x21, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x72, 0x6f,
		0x6f, 0x74, 0x0, 0x0, 0x63, 0x61, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x5f, 0x73, 0x68, 0x61,
		0x32, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x0, 0x79, 0x4, 0x5f, 0x70,
		0x69, 0x64, 0x5, 0x37, 0x37, 0x30, 0x38, 0x36, 0x9, 0x5f, 0x70, 0x6c, 0x61, 0x74, 0x66,
		0x6f, 0x72, 0x6d, 0x6, 0x78, 0x38, 0x36, 0x5f, 0x36, 0x34, 0x3, 0x5f, 0x6f, 0x73, 0x5,
		0x4c, 0x69, 0x6e, 0x75, 0x78, 0xc, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x6e,
		0x61, 0x6d, 0x65, 0x8, 0x6c, 0x69, 0x62, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x7, 0x6f, 0x73,
		0x5f, 0x75, 0x73, 0x65, 0x72, 0xa, 0x6e, 0x75, 0x6c, 0x6c, 0x6e, 0x6f, 0x74, 0x6e, 0x69,
		0x6c, 0xf, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69,
		0x6f, 0x6e, 0x6, 0x38, 0x2e, 0x30, 0x2e, 0x32, 0x31, 0xc, 0x70, 0x72, 0x6f, 0x67, 0x72,
		0x61, 0x6d, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x5, 0x6d, 0x79, 0x73, 0x71, 0x6c,
	}

	var resp handshakeResponse41
	pos, err := parseHandshakeResponseHeader(context.Background(), &resp, data)
	require.NoError(t, err)
	err = parseHandshakeResponseBody(context.Background(), &resp, data, pos)
	require.NoError(t, err)
	require.Equal(t, "caching_sha2_password", resp.AuthPlugin)
}

func TestInitialHandshake(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	var outBuffer bytes.Buffer
	cfg := newTestConfig()
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	drv := NewTiDBDriver(store)
	srv, err := NewServer(cfg, drv)
	require.NoError(t, err)
	cc := &clientConn{
		connectionID: 1,
		salt:         []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14},
		server:       srv,
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(&outBuffer),
		},
	}

	err = cc.writeInitialHandshake(context.TODO())
	require.NoError(t, err)

	expected := new(bytes.Buffer)
	expected.WriteByte(0x0a)                                     // Protocol
	expected.WriteString(mysql.ServerVersion)                    // Version
	expected.WriteByte(0x00)                                     // NULL
	err = binary.Write(expected, binary.LittleEndian, uint32(1)) // Connection ID
	require.NoError(t, err)
	expected.Write([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00})        // Salt
	err = binary.Write(expected, binary.LittleEndian, uint16(defaultCapability&0xFFFF)) // Server Capability
	require.NoError(t, err)
	expected.WriteByte(uint8(mysql.DefaultCollationID))                             // Server Language
	err = binary.Write(expected, binary.LittleEndian, mysql.ServerStatusAutocommit) // Server Status
	require.NoError(t, err)
	err = binary.Write(expected, binary.LittleEndian, uint16((defaultCapability>>16)&0xFFFF)) // Extended Server Capability
	require.NoError(t, err)
	expected.WriteByte(0x15)                                                                             // Authentication Plugin Length
	expected.Write([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})                   // Unused
	expected.Write([]byte{0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x00}) // Salt
	expected.WriteString("mysql_native_password")                                                        // Authentication Plugin
	expected.WriteByte(0x00)                                                                             // NULL
	require.Equal(t, expected.Bytes(), outBuffer.Bytes()[4:])
}

type dispatchInput struct {
	com byte
	in  []byte
	err error
	out []byte
}

func TestDispatch(t *testing.T) {
	userData := append([]byte("root"), 0x0, 0x0)
	userData = append(userData, []byte("test")...)
	userData = append(userData, 0x0)

	inputs := []dispatchInput{
		{
			com: mysql.ComSleep,
			in:  nil,
			err: nil,
			out: nil,
		},
		{
			com: mysql.ComQuit,
			in:  nil,
			err: io.EOF,
			out: nil,
		},
		{
			com: mysql.ComQuery,
			in:  []byte("do 1"),
			err: nil,
			out: []byte{0x3, 0x0, 0x0, 0x0, 0x0, 0x00, 0x0},
		},
		{
			com: mysql.ComInitDB,
			in:  []byte("test"),
			err: nil,
			out: []byte{0x3, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0},
		},
		{
			com: mysql.ComPing,
			in:  nil,
			err: nil,
			out: []byte{0x3, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0},
		},
		{
			com: mysql.ComStmtPrepare,
			in:  []byte("select 1"),
			err: nil,
			out: []byte{
				0xc, 0x0, 0x0, 0x3, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x18,
				0x0, 0x0, 0x4, 0x3, 0x64, 0x65, 0x66, 0x0, 0x0, 0x0, 0x1, 0x31, 0x1, 0x31, 0xc, 0x3f,
				0x0, 0x1, 0x0, 0x0, 0x0, 0x8, 0x81, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x5, 0xfe,
			},
		},
		{
			com: mysql.ComStmtExecute,
			in:  []byte{0x1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x1, 0x0},
			err: nil,
			out: []byte{
				0x1, 0x0, 0x0, 0x6, 0x1, 0x18, 0x0, 0x0, 0x7, 0x3, 0x64, 0x65, 0x66, 0x0, 0x0, 0x0,
				0x1, 0x31, 0x1, 0x31, 0xc, 0x3f, 0x0, 0x1, 0x0, 0x0, 0x0, 0x8, 0x81, 0x0, 0x0, 0x0,
				0x0, 0x1, 0x0, 0x0, 0x8, 0xfe,
			},
		},
		{
			com: mysql.ComStmtFetch,
			in:  []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
			err: nil,
			out: []byte{0x1, 0x0, 0x0, 0x9, 0xfe},
		},
		{
			com: mysql.ComStmtReset,
			in:  []byte{0x1, 0x0, 0x0, 0x0},
			err: nil,
			out: []byte{0x3, 0x0, 0x0, 0xa, 0x0, 0x0, 0x0},
		},
		{
			com: mysql.ComSetOption,
			in:  []byte{0x1, 0x0, 0x0, 0x0},
			err: nil,
			out: []byte{0x1, 0x0, 0x0, 0xb, 0xfe},
		},
		{
			com: mysql.ComStmtClose,
			in:  []byte{0x1, 0x0, 0x0, 0x0},
			err: nil,
			out: []byte{},
		},
		{
			com: mysql.ComFieldList,
			in:  []byte("t"),
			err: nil,
			out: []byte{
				0x26, 0x0, 0x0, 0xc, 0x3, 0x64, 0x65, 0x66, 0x4, 0x74, 0x65, 0x73, 0x74, 0x1, 0x74,
				0x1, 0x74, 0x1, 0x61, 0x1, 0x61, 0xc, 0x3f, 0x0, 0xb, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0xd, 0xfe,
			},
		},
		{
			com: mysql.ComChangeUser,
			in:  userData,
			err: nil,
			out: []byte{0x3, 0x0, 0x0, 0xe, 0x0, 0x0, 0x0},
		},
		{
			com: mysql.ComRefresh, // flush privileges
			in:  []byte{0x01},
			err: nil,
			out: []byte{0x3, 0x0, 0x0, 0xf, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x10, 0x0, 0x0, 0x0},
		},
		{
			com: mysql.ComRefresh, // flush logs etc
			in:  []byte{0x02},
			err: nil,
			out: []byte{0x3, 0x0, 0x0, 0x11, 0x0, 0x0, 0x0},
		},
		{
			com: mysql.ComResetConnection,
			in:  nil,
			err: nil,
			out: []byte{0x3, 0x0, 0x0, 0x12, 0x0, 0x0, 0x0},
		},
	}

	testDispatch(t, inputs, 0)
}

func TestDispatchClientProtocol41(t *testing.T) {
	userData := append([]byte("root"), 0x0, 0x0)
	userData = append(userData, []byte("test")...)
	userData = append(userData, 0x0)

	inputs := []dispatchInput{
		{
			com: mysql.ComSleep,
			in:  nil,
			err: nil,
			out: nil,
		},
		{
			com: mysql.ComQuit,
			in:  nil,
			err: io.EOF,
			out: nil,
		},
		{
			com: mysql.ComQuery,
			in:  []byte("do 1"),
			err: nil,
			out: []byte{0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0},
		},
		{
			com: mysql.ComInitDB,
			in:  []byte("test"),
			err: nil,
			out: []byte{0x7, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0},
		},
		{
			com: mysql.ComPing,
			in:  nil,
			err: nil,
			out: []byte{0x7, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0},
		},
		{
			com: mysql.ComStmtPrepare,
			in:  []byte("select 1"),
			err: nil,
			out: []byte{
				0xc, 0x0, 0x0, 0x3, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x18,
				0x0, 0x0, 0x4, 0x3, 0x64, 0x65, 0x66, 0x0, 0x0, 0x0, 0x1, 0x31, 0x1, 0x31, 0xc, 0x3f,
				0x0, 0x1, 0x0, 0x0, 0x0, 0x8, 0x81, 0x0, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x5, 0xfe,
				0x0, 0x0, 0x2, 0x0,
			},
		},
		{
			com: mysql.ComStmtExecute,
			in:  []byte{0x1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x1, 0x0},
			err: nil,
			out: []byte{
				0x1, 0x0, 0x0, 0x6, 0x1, 0x18, 0x0, 0x0, 0x7, 0x3, 0x64, 0x65, 0x66, 0x0, 0x0, 0x0,
				0x1, 0x31, 0x1, 0x31, 0xc, 0x3f, 0x0, 0x1, 0x0, 0x0, 0x0, 0x8, 0x81, 0x0, 0x0, 0x0,
				0x0, 0x5, 0x0, 0x0, 0x8, 0xfe, 0x0, 0x0, 0x42, 0x0,
			},
		},
		{
			com: mysql.ComStmtFetch,
			in:  []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
			err: nil,
			out: []byte{0x5, 0x0, 0x0, 0x9, 0xfe, 0x0, 0x0, 0x82, 0x0},
		},
		{
			com: mysql.ComStmtReset,
			in:  []byte{0x1, 0x0, 0x0, 0x0},
			err: nil,
			out: []byte{0x7, 0x0, 0x0, 0xa, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0},
		},
		{
			com: mysql.ComSetOption,
			in:  []byte{0x1, 0x0, 0x0, 0x0},
			err: nil,
			out: []byte{0x5, 0x0, 0x0, 0xb, 0xfe, 0x0, 0x0, 0x2, 0x0},
		},
		{
			com: mysql.ComStmtClose,
			in:  []byte{0x1, 0x0, 0x0, 0x0},
			err: nil,
			out: []byte{},
		},
		{
			com: mysql.ComFieldList,
			in:  []byte("t"),
			err: nil,
			out: []byte{
				0x26, 0x0, 0x0, 0xc, 0x3, 0x64, 0x65, 0x66, 0x4, 0x74, 0x65, 0x73, 0x74, 0x1, 0x74,
				0x1, 0x74, 0x1, 0x61, 0x1, 0x61, 0xc, 0x3f, 0x0, 0xb, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0xd, 0xfe,
				0x0, 0x0, 0x2, 0x0,
			},
		},
		{
			com: mysql.ComChangeUser,
			in:  userData,
			err: nil,
			out: []byte{0x7, 0x0, 0x0, 0xe, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0},
		},
		{
			com: mysql.ComRefresh, // flush privileges
			in:  []byte{0x01},
			err: nil,
			out: []byte{0x7, 0x0, 0x0, 0xf, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x7, 0x0, 0x0, 0x10, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0},
		},
		{
			com: mysql.ComRefresh, // flush logs etc
			in:  []byte{0x02},
			err: nil,
			out: []byte{0x7, 0x0, 0x0, 0x11, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0},
		},
		{
			com: mysql.ComResetConnection,
			in:  nil,
			err: nil,
			out: []byte{0x7, 0x0, 0x0, 0x12, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0},
		},
	}

	testDispatch(t, inputs, mysql.ClientProtocol41)
}

func testDispatch(t *testing.T, inputs []dispatchInput, capability uint32) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	tc := &TiDBContext{
		Session: se,
		stmts:   make(map[int]*TiDBStatement),
	}
	_, err = se.Execute(context.Background(), "create table test.t(a int)")
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "insert into test.t values (1)")
	require.NoError(t, err)

	var outBuffer bytes.Buffer
	tidbdrv := NewTiDBDriver(store)
	cfg := newTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	defer server.Close()

	cc := &clientConn{
		connectionID: 1,
		salt:         []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14},
		server:       server,
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(&outBuffer),
		},
		collation:  mysql.DefaultCollationID,
		peerHost:   "localhost",
		alloc:      arena.NewAllocator(512),
		chunkAlloc: chunk.NewAllocator(),
		capability: capability,
	}
	cc.setCtx(tc)
	for _, cs := range inputs {
		inBytes := append([]byte{cs.com}, cs.in...)
		err := cc.dispatch(context.Background(), inBytes)
		require.Equal(t, cs.err, err)
		if err == nil {
			err = cc.flush(context.TODO())
			require.NoError(t, err)
			require.Equal(t, cs.out, outBuffer.Bytes())
		} else {
			_ = cc.flush(context.TODO())
		}
		outBuffer.Reset()
	}
}

func TestGetSessionVarsWaitTimeout(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	tc := &TiDBContext{
		Session: se,
		stmts:   make(map[int]*TiDBStatement),
	}
	cc := &clientConn{
		connectionID: 1,
		server: &Server{
			capability: defaultCapability,
		},
	}
	cc.setCtx(tc)
	require.Equal(t, uint64(variable.DefWaitTimeout), cc.getSessionVarsWaitTimeout(context.Background()))
}

func mapIdentical(m1, m2 map[string]string) bool {
	return mapBelong(m1, m2) && mapBelong(m2, m1)
}

func mapBelong(m1, m2 map[string]string) bool {
	for k1, v1 := range m1 {
		v2, ok := m2[k1]
		if !ok && v1 != v2 {
			return false
		}
	}
	return true
}

func TestConnExecutionTimeout(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	// There is no underlying netCon, use failpoint to avoid panic
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/server/FakeClientConn", "return(1)"))

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	connID := uint64(1)
	se.SetConnectionID(connID)
	tc := &TiDBContext{
		Session: se,
		stmts:   make(map[int]*TiDBStatement),
	}
	cc := &clientConn{
		connectionID: connID,
		server: &Server{
			capability: defaultCapability,
		},
		alloc:      arena.NewAllocator(32 * 1024),
		chunkAlloc: chunk.NewAllocator(),
	}
	cc.setCtx(tc)
	srv := &Server{
		clients: map[uint64]*clientConn{
			connID: cc,
		},
		dom: dom,
	}
	handle := dom.ExpensiveQueryHandle().SetSessionManager(srv)
	go handle.Run()

	_, err = se.Execute(context.Background(), "use test;")
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "CREATE TABLE testTable2 (id bigint PRIMARY KEY,  age int)")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		str := fmt.Sprintf("insert into testTable2 values(%d, %d)", i, i%80)
		_, err = se.Execute(context.Background(), str)
		require.NoError(t, err)
	}

	_, err = se.Execute(context.Background(), "select SLEEP(1);")
	require.NoError(t, err)

	_, err = se.Execute(context.Background(), "set @@max_execution_time = 500;")
	require.NoError(t, err)

	err = cc.handleQuery(context.Background(), "select * FROM testTable2 WHERE SLEEP(1);")
	require.NoError(t, err)

	_, err = se.Execute(context.Background(), "set @@max_execution_time = 1500;")
	require.NoError(t, err)

	_, err = se.Execute(context.Background(), "set @@tidb_expensive_query_time_threshold = 1;")
	require.NoError(t, err)

	records, err := se.Execute(context.Background(), "select SLEEP(2);")
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.ResultSetToResult(records[0], fmt.Sprintf("%v", records[0])).Check(testkit.Rows("1"))

	_, err = se.Execute(context.Background(), "set @@max_execution_time = 0;")
	require.NoError(t, err)

	err = cc.handleQuery(context.Background(), "select * FROM testTable2 WHERE SLEEP(1);")
	require.NoError(t, err)

	err = cc.handleQuery(context.Background(), "select /*+ MAX_EXECUTION_TIME(100)*/  * FROM testTable2 WHERE  SLEEP(1);")
	require.NoError(t, err)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/server/FakeClientConn"))
}

func TestShutDown(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	cc := &clientConn{}
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	tc := &TiDBContext{Session: se}
	cc.setCtx(tc)
	// set killed flag
	cc.status = connStatusShutdown
	// assert ErrQueryInterrupted
	err = cc.handleQuery(context.Background(), "select 1")
	require.Equal(t, executor.ErrQueryInterrupted, err)
}

func TestShutdownOrNotify(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	tc := &TiDBContext{
		Session: se,
		stmts:   make(map[int]*TiDBStatement),
	}
	cc := &clientConn{
		connectionID: 1,
		server: &Server{
			capability: defaultCapability,
		},
		status: connStatusWaitShutdown,
	}
	cc.setCtx(tc)
	require.False(t, cc.ShutdownOrNotify())
	cc.status = connStatusReading
	require.True(t, cc.ShutdownOrNotify())
	require.Equal(t, connStatusShutdown, cc.status)
	cc.status = connStatusDispatching
	require.False(t, cc.ShutdownOrNotify())
	require.Equal(t, connStatusWaitShutdown, cc.status)
}

type snapshotCache interface {
	SnapCacheHitCount() int
}

func TestPrefetchPointKeys(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	cc := &clientConn{
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
	}
	tk := testkit.NewTestKit(t, store)
	cc.setCtx(&TiDBContext{Session: tk.Session()})
	ctx := context.Background()
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("use test")
	tk.MustExec("create table prefetch (a int, b int, c int, primary key (a, b))")
	tk.MustExec("insert prefetch values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk.MustExec("begin optimistic")
	tk.MustExec("update prefetch set c = c + 1 where a = 2 and b = 2")

	// enable multi-statement
	capabilities := cc.ctx.GetSessionVars().ClientCapability
	capabilities ^= mysql.ClientMultiStatements
	cc.ctx.SetClientCapability(capabilities)
	query := "update prefetch set c = c + 1 where a = 1 and b = 1;" +
		"update prefetch set c = c + 1 where a = 2 and b = 2;" +
		"update prefetch set c = c + 1 where a = 3 and b = 3;"
	err := cc.handleQuery(ctx, query)
	require.NoError(t, err)
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	snap := txn.GetSnapshot()
	require.Equal(t, 4, snap.(snapshotCache).SnapCacheHitCount())
	tk.MustExec("commit")
	tk.MustQuery("select * from prefetch").Check(testkit.Rows("1 1 2", "2 2 4", "3 3 4"))

	tk.MustExec("begin pessimistic")
	tk.MustExec("update prefetch set c = c + 1 where a = 2 and b = 2")
	require.Equal(t, 1, tk.Session().GetSessionVars().TxnCtx.PessimisticCacheHit)
	err = cc.handleQuery(ctx, query)
	require.NoError(t, err)
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	require.Equal(t, 5, tk.Session().GetSessionVars().TxnCtx.PessimisticCacheHit)
	tk.MustExec("commit")
	tk.MustQuery("select * from prefetch").Check(testkit.Rows("1 1 3", "2 2 6", "3 3 5"))
}

func TestTiFlashFallback(t *testing.T) {
	store, clean := testkit.CreateMockStore(t,
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockCluster := c.(*unistore.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			store := c.AllocID()
			peer := c.AllocID()
			mockCluster.AddStore(store, "tiflash0", &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
			mockCluster.AddPeer(region1, store, peer)
		}),
		mockstore.WithStoreType(mockstore.EmbedUnistore),
	)
	defer clean()

	cc := &clientConn{
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
	}

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	cc.setCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	dml := "insert into t values"
	for i := 0; i < 50; i++ {
		dml += fmt.Sprintf("(%v, 0)", i)
		if i != 49 {
			dml += ","
		}
	}
	tk.MustExec(dml)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("50"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/copr/ReduceCopNextMaxBackoff", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/copr/ReduceCopNextMaxBackoff"))
	}()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/BatchCopRpcErrtiflash0", "return(\"tiflash0\")"))
	// test COM_STMT_EXECUTE
	ctx := context.Background()
	tk.MustExec("set @@tidb_allow_fallback_to_tikv='tiflash'")
	tk.MustExec("set @@tidb_allow_mpp=OFF")
	require.NoError(t, cc.handleStmtPrepare(ctx, "select sum(a) from t"))
	require.NoError(t, cc.handleStmtExecute(ctx, []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0}))
	tk.MustQuery("show warnings").Check(testkit.Rows("Error 9012 TiFlash server timeout"))

	// test COM_STMT_FETCH (cursor mode)
	require.NoError(t, cc.handleStmtExecute(ctx, []byte{0x1, 0x0, 0x0, 0x0, 0x1, 0x1, 0x0, 0x0, 0x0}))
	require.Error(t, cc.handleStmtFetch(ctx, []byte{0x1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0}))
	tk.MustExec("set @@tidb_allow_fallback_to_tikv=''")
	require.Error(t, cc.handleStmtExecute(ctx, []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0}))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/BatchCopRpcErrtiflash0"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/server/fetchNextErr", "return(\"firstNext\")"))
	// test COM_STMT_EXECUTE (cursor mode)
	tk.MustExec("set @@tidb_allow_fallback_to_tikv='tiflash'")
	require.NoError(t, cc.handleStmtExecute(ctx, []byte{0x1, 0x0, 0x0, 0x0, 0x1, 0x1, 0x0, 0x0, 0x0}))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/server/fetchNextErr"))

	// test that TiDB would not retry if the first execution already sends data to client
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/server/fetchNextErr", "return(\"secondNext\")"))
	tk.MustExec("set @@tidb_allow_fallback_to_tikv='tiflash'")
	require.Error(t, cc.handleQuery(ctx, "select * from t t1 join t t2 on t1.a = t2.a"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/server/fetchNextErr"))

	// simple TiFlash query (unary + non-streaming)
	tk.MustExec("set @@tidb_allow_batch_cop=0; set @@tidb_allow_mpp=0;")
	require.NoError(t, failpoint.Enable("tikvclient/tikvStoreSendReqResult", "return(\"requestTiFlashError\")"))
	testFallbackWork(t, tk, cc, "select sum(a) from t")
	require.NoError(t, failpoint.Disable("tikvclient/tikvStoreSendReqResult"))

	// TiFlash query based on batch cop (batch + streaming)
	tk.MustExec("set @@tidb_allow_batch_cop=1; set @@tidb_allow_mpp=0;")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/BatchCopRpcErrtiflash0", "return(\"tiflash0\")"))
	testFallbackWork(t, tk, cc, "select count(*) from t")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/BatchCopRpcErrtiflash0"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/batchCopRecvTimeout", "return(true)"))
	testFallbackWork(t, tk, cc, "select count(*) from t")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/batchCopRecvTimeout"))

	// TiFlash MPP query (MPP + streaming)
	tk.MustExec("set @@tidb_allow_batch_cop=0; set @@tidb_allow_mpp=1;")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/mppDispatchTimeout", "return(true)"))
	testFallbackWork(t, tk, cc, "select * from t t1 join t t2 on t1.a = t2.a")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/mppDispatchTimeout"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/mppRecvTimeout", "return(-1)"))
	testFallbackWork(t, tk, cc, "select * from t t1 join t t2 on t1.a = t2.a")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/mppRecvTimeout"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/establishMppConnectionErr", "return(true)"))
	testFallbackWork(t, tk, cc, "select * from t t1 join t t2 on t1.a = t2.a")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/establishMppConnectionErr"))

	// When fallback is not set, TiFlash mpp will return the original error message
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/mppDispatchTimeout", "return(true)"))
	tk.MustExec("set @@tidb_allow_fallback_to_tikv=''")
	tk.MustExec("set @@tidb_allow_mpp=ON")
	tk.MustExec("set @@tidb_enforce_mpp=ON")
	tk.MustExec("set @@tidb_isolation_read_engines='tiflash,tidb'")
	err = cc.handleQuery(ctx, "select count(*) from t")
	require.Error(t, err)
	require.NotEqual(t, err.Error(), tikverr.ErrTiFlashServerTimeout.Error())
}

func testFallbackWork(t *testing.T, tk *testkit.TestKit, cc *clientConn, sql string) {
	ctx := context.Background()
	tk.MustExec("set @@tidb_allow_fallback_to_tikv=''")
	require.Error(t, tk.QueryToErr(sql))
	tk.MustExec("set @@tidb_allow_fallback_to_tikv='tiflash'")

	require.NoError(t, cc.handleQuery(ctx, sql))
	tk.MustQuery("show warnings").Check(testkit.Rows("Error 9012 TiFlash server timeout"))
}

// For issue https://github.com/pingcap/tidb/issues/25069
func TestShowErrors(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	cc := &clientConn{
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
	}
	ctx := context.Background()
	tk := testkit.NewTestKit(t, store)
	cc.setCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})

	err := cc.handleQuery(ctx, "create database if not exists test;")
	require.NoError(t, err)
	err = cc.handleQuery(ctx, "use test;")
	require.NoError(t, err)

	stmts, err := cc.ctx.Parse(ctx, "drop table idontexist")
	require.NoError(t, err)
	_, err = cc.ctx.ExecuteStmt(ctx, stmts[0])
	require.Error(t, err)
	tk.MustQuery("show errors").Check(testkit.Rows("Error 1051 Unknown table 'test.idontexist'"))
}

func TestHandleAuthPlugin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	cfg := newTestConfig()
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	drv := NewTiDBDriver(store)
	srv, err := NewServer(cfg, drv)
	require.NoError(t, err)
	ctx := context.Background()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER unativepassword")
	defer func() {
		tk.MustExec("DROP USER unativepassword")
	}()

	// 5.7 or newer client trying to authenticate with mysql_native_password
	cc := &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
		server: srv,
		user:   "unativepassword",
	}
	resp := handshakeResponse41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthNativePassword,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)

	// 8.0 or newer client trying to authenticate with caching_sha2_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
		server: srv,
		user:   "unativepassword",
	}
	resp = handshakeResponse41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthCachingSha2Password,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, resp.Auth, []byte(mysql.AuthNativePassword))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/server/FakeAuthSwitch"))

	// MySQL 5.1 or older client, without authplugin support
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
		server: srv,
		user:   "unativepassword",
	}
	resp = handshakeResponse41{
		Capability: mysql.ClientProtocol41,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)

	// === Target account has mysql_native_password ===
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/server/FakeUser", "return(\"mysql_native_password\")"))

	// 5.7 or newer client trying to authenticate with mysql_native_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
		server: srv,
		user:   "unativepassword",
	}
	resp = handshakeResponse41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthNativePassword,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthNativePassword), resp.Auth)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/server/FakeAuthSwitch"))

	// 8.0 or newer client trying to authenticate with caching_sha2_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
		server: srv,
		user:   "unativepassword",
	}
	resp = handshakeResponse41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthCachingSha2Password,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthNativePassword), resp.Auth)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/server/FakeAuthSwitch"))

	// MySQL 5.1 or older client, without authplugin support
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
		server: srv,
		user:   "unativepassword",
	}
	resp = handshakeResponse41{
		Capability: mysql.ClientProtocol41,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/server/FakeUser"))

	// === Target account has caching_sha2_password ===
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/server/FakeUser", "return(\"caching_sha2_password\")"))

	// 5.7 or newer client trying to authenticate with mysql_native_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
		server: srv,
		user:   "unativepassword",
	}
	resp = handshakeResponse41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthNativePassword,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthCachingSha2Password), resp.Auth)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/server/FakeAuthSwitch"))

	// 8.0 or newer client trying to authenticate with caching_sha2_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
		server: srv,
		user:   "unativepassword",
	}
	resp = handshakeResponse41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthCachingSha2Password,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthCachingSha2Password), resp.Auth)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/server/FakeAuthSwitch"))

	// MySQL 5.1 or older client, without authplugin support
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
		server: srv,
		user:   "unativepassword",
	}
	resp = handshakeResponse41{
		Capability: mysql.ClientProtocol41,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/server/FakeUser"))
}

func TestAuthPlugin2(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	cfg := newTestConfig()
	cfg.Port = 0
	cfg.Status.StatusPort = 0

	drv := NewTiDBDriver(store)
	srv, err := NewServer(cfg, drv)
	require.NoError(t, err)

	cc := &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
		server: srv,
		user:   "root",
	}
	ctx := context.Background()
	se, _ := session.CreateSession4Test(store)
	tc := &TiDBContext{
		Session: se,
		stmts:   make(map[int]*TiDBStatement),
	}
	cc.setCtx(tc)

	resp := handshakeResponse41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
	}

	cc.isUnixSocket = true
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/server/FakeAuthSwitch", "return(1)"))
	respAuthSwitch, err := cc.checkAuthPlugin(ctx, &resp)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/server/FakeAuthSwitch"))
	require.Equal(t, respAuthSwitch, []byte(mysql.AuthNativePassword))
	require.NoError(t, err)

}

func TestMaxAllowedPacket(t *testing.T) {
	// Test cases from issue 31422: https://github.com/pingcap/tidb/issues/31422
	// The string "SELECT length('') as len;" has 25 chars,
	// so if the string inside '' has a length of 999, the total query reaches the max allowed packet size.

	const maxAllowedPacket = 1024
	var (
		inBuffer  bytes.Buffer
		readBytes []byte
	)

	// The length of total payload is (25 + 999 = 1024).
	bytes := append([]byte{0x00, 0x04, 0x00, 0x00}, []byte(fmt.Sprintf("SELECT length('%s') as len;", strings.Repeat("a", 999)))...)
	_, err := inBuffer.Write(bytes)
	require.NoError(t, err)
	brc := newBufferedReadConn(&bytesConn{inBuffer})
	pkt := newPacketIO(brc)
	pkt.setMaxAllowedPacket(maxAllowedPacket)
	readBytes, err = pkt.readPacket()
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("SELECT length('%s') as len;", strings.Repeat("a", 999)), string(readBytes))
	require.Equal(t, uint8(1), pkt.sequence)

	// The length of total payload is (25 + 1000 = 1025).
	inBuffer.Reset()
	bytes = append([]byte{0x01, 0x04, 0x00, 0x00}, []byte(fmt.Sprintf("SELECT length('%s') as len;", strings.Repeat("a", 1000)))...)
	_, err = inBuffer.Write(bytes)
	require.NoError(t, err)
	brc = newBufferedReadConn(&bytesConn{inBuffer})
	pkt = newPacketIO(brc)
	pkt.setMaxAllowedPacket(maxAllowedPacket)
	_, err = pkt.readPacket()
	require.Error(t, err)

	// The length of total payload is (25 + 488 = 513).
	// Two separate packets would NOT exceed the limitation of maxAllowedPacket.
	inBuffer.Reset()
	bytes = append([]byte{0x01, 0x02, 0x00, 0x00}, []byte(fmt.Sprintf("SELECT length('%s') as len;", strings.Repeat("a", 488)))...)
	_, err = inBuffer.Write(bytes)
	require.NoError(t, err)
	brc = newBufferedReadConn(&bytesConn{inBuffer})
	pkt = newPacketIO(brc)
	pkt.setMaxAllowedPacket(maxAllowedPacket)
	readBytes, err = pkt.readPacket()
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("SELECT length('%s') as len;", strings.Repeat("a", 488)), string(readBytes))
	require.Equal(t, uint8(1), pkt.sequence)
	inBuffer.Reset()
	bytes = append([]byte{0x01, 0x02, 0x00, 0x01}, []byte(fmt.Sprintf("SELECT length('%s') as len;", strings.Repeat("b", 488)))...)
	_, err = inBuffer.Write(bytes)
	require.NoError(t, err)
	brc = newBufferedReadConn(&bytesConn{inBuffer})
	pkt.setBufferedReadConn(brc)
	readBytes, err = pkt.readPacket()
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("SELECT length('%s') as len;", strings.Repeat("b", 488)), string(readBytes))
	require.Equal(t, uint8(2), pkt.sequence)
}
