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
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server/internal"
	"github.com/pingcap/tidb/pkg/server/internal/handshake"
	"github.com/pingcap/tidb/pkg/server/internal/parse"
	"github.com/pingcap/tidb/pkg/server/internal/testutil"
	serverutil "github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/arena"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	promtestutils "github.com/prometheus/client_golang/prometheus/testutil"
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
	store := testkit.CreateMockStore(t)

	var outBuffer bytes.Buffer
	tidbdrv := NewTiDBDriver(store)
	cfg := serverutil.NewTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	defer server.Close()

	cc := &clientConn{
		connectionID: 1,
		salt:         []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14},
		server:       server,
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(&outBuffer)),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		alloc:        arena.NewAllocator(512),
		chunkAlloc:   chunk.NewAllocator(),
		capability:   mysql.ClientProtocol41,
	}

	tk := testkit.NewTestKit(t, store)
	ctx := &TiDBContext{Session: tk.Session()}
	cc.SetCtx(ctx)

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
	var p handshake.Response41
	_, err := parse.HandshakeResponseHeader(context.Background(), &p, data)
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
	var p handshake.Response41
	offset, err := parse.HandshakeResponseHeader(context.Background(), &p, data)
	require.NoError(t, err)
	require.Equal(t, mysql.ClientConnectAtts, p.Capability&mysql.ClientConnectAtts)
	err = parse.HandshakeResponseBody(context.Background(), &p, data, offset)
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
	p = handshake.Response41{}
	offset, err = parse.HandshakeResponseHeader(context.Background(), &p, data)
	require.NoError(t, err)
	capability := mysql.ClientProtocol41 |
		mysql.ClientPluginAuth |
		mysql.ClientSecureConnection |
		mysql.ClientConnectWithDB
	require.Equal(t, capability, p.Capability&capability)
	err = parse.HandshakeResponseBody(context.Background(), &p, data, offset)
	require.NoError(t, err)
	require.Equal(t, "pam", p.User)
	require.Equal(t, "test", p.DBName)
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
	p := handshake.Response41{}
	offset, err := parse.HandshakeResponseHeader(context.Background(), &p, data)
	require.NoError(t, err)
	require.Equal(t, mysql.ClientPluginAuthLenencClientData, p.Capability&mysql.ClientPluginAuthLenencClientData)
	err = parse.HandshakeResponseBody(context.Background(), &p, data, offset)
	require.NoError(t, err)
	require.NotEmpty(t, p.Auth)
}

func TestInitialHandshake(t *testing.T) {
	store := testkit.CreateMockStore(t)

	var outBuffer bytes.Buffer
	cfg := serverutil.NewTestConfig()
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	drv := NewTiDBDriver(store)
	srv, err := NewServer(cfg, drv)
	require.NoError(t, err)
	cc := &clientConn{
		connectionID: 1,
		salt:         []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14},
		server:       srv,
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(&outBuffer)),
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
			err: mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", nil, mysql.ComSleep),
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
				0xc, 0x0, 0x0, 0x3, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x17,
				0x0, 0x0, 0x4, 0x3, 0x64, 0x65, 0x66, 0x0, 0x0, 0x0, 0x1, 0x31, 0x0, 0xc, 0x3f,
				0x0, 0x1, 0x0, 0x0, 0x0, 0x8, 0x81, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x5, 0xfe,
			},
		},
		{
			com: mysql.ComStmtExecute,
			in:  []byte{0x1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x1, 0x0},
			err: nil,
			out: []byte{
				0x1, 0x0, 0x0, 0x6, 0x1, 0x17, 0x0, 0x0, 0x7, 0x3, 0x64, 0x65, 0x66, 0x0, 0x0, 0x0,
				0x1, 0x31, 0x0, 0xc, 0x3f, 0x0, 0x1, 0x0, 0x0, 0x0, 0x8, 0x81, 0x0, 0x0, 0x0,
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
				0x1f, 0x0, 0x0, 0xc, 0x3, 0x64, 0x65, 0x66, 0x4, 0x74, 0x65, 0x73, 0x74, 0x1, 0x74,
				0x1, 0x74, 0x1, 0x61, 0x1, 0x61, 0xc, 0x3f, 0x0, 0xb, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0,
				0x0, 0x0, 0x0, 0xfb, 0x1, 0x0, 0x0, 0xd, 0xfe,
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
			err: mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", nil, mysql.ComSleep),
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
				0xc, 0x0, 0x0, 0x3, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x17,
				0x0, 0x0, 0x4, 0x3, 0x64, 0x65, 0x66, 0x0, 0x0, 0x0, 0x1, 0x31, 0x0, 0xc, 0x3f,
				0x0, 0x1, 0x0, 0x0, 0x0, 0x8, 0x81, 0x0, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x5, 0xfe,
				0x0, 0x0, 0x2, 0x0,
			},
		},
		{
			com: mysql.ComStmtExecute,
			in:  []byte{0x1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x1, 0x0},
			err: nil,
			out: []byte{
				0x1, 0x0, 0x0, 0x6, 0x1, 0x17, 0x0, 0x0, 0x7, 0x3, 0x64, 0x65, 0x66, 0x0, 0x0, 0x0,
				0x1, 0x31, 0x0, 0xc, 0x3f, 0x0, 0x1, 0x0, 0x0, 0x0, 0x8, 0x81, 0x0, 0x0, 0x0,
				0x0, 0x5, 0x0, 0x0, 0x8, 0xfe, 0x0, 0x0, 0x42, 0x0,
			},
		},
		{
			com: mysql.ComStmtFetch,
			in:  []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
			err: nil,
			out: []byte{0x5, 0x0, 0x0, 0x9, 0xfe, 0x0, 0x0, 0x42, 0x0},
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
				0x1f, 0x0, 0x0, 0xc, 0x3, 0x64, 0x65, 0x66, 0x4, 0x74, 0x65, 0x73, 0x74, 0x1, 0x74,
				0x1, 0x74, 0x1, 0x61, 0x1, 0x61, 0xc, 0x3f, 0x0, 0xb, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0,
				0x0, 0x0, 0x0, 0xfb, 0x5, 0x0, 0x0, 0x0d, 0xfe, 0x0, 0x0, 0x2, 0x0,
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

func TestQueryEndWithZero(t *testing.T) {
	inputs := []dispatchInput{
		{
			com: mysql.ComStmtPrepare,
			in:  append([]byte("select 1"), 0x0),
			err: nil,
			out: []byte{
				0xc, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x17,
				0x0, 0x0, 0x1, 0x3, 0x64, 0x65, 0x66, 0x0, 0x0, 0x0, 0x1, 0x31, 0x0, 0xc, 0x3f,
				0x0, 0x1, 0x0, 0x0, 0x0, 0x8, 0x81, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x2, 0xfe,
			},
		},
		{
			com: mysql.ComQuery,
			in:  append([]byte("select 1"), 0x0),
			err: nil,
			out: []byte{
				0x1, 0x0, 0x0, 0x3, 0x1, 0x17, 0x0, 0x0, 0x4, 0x3, 0x64, 0x65, 0x66, 0x0, 0x0, 0x0,
				0x1, 0x31, 0x0, 0xc, 0x3f, 0x0, 0x1, 0x0, 0x0, 0x0, 0x8, 0x81, 0x0, 0x0, 0x0,
				0x0, 0x1, 0x0, 0x0, 0x5, 0xfe, 0x2, 0x0, 0x0, 0x6, 0x1, 0x31, 0x1, 0x0, 0x0, 0x7, 0xfe,
			},
		},
	}

	testDispatch(t, inputs, 0)
}

func testDispatch(t *testing.T, inputs []dispatchInput, capability uint32) {
	store := testkit.CreateMockStore(t)

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
	cfg := serverutil.NewTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	defer server.Close()

	cc := &clientConn{
		connectionID: 1,
		salt:         []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14},
		server:       server,
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(&outBuffer)),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		alloc:        arena.NewAllocator(512),
		chunkAlloc:   chunk.NewAllocator(),
		capability:   capability,
	}
	cc.SetCtx(tc)
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
	store := testkit.CreateMockStore(t)

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
	cc.SetCtx(tc)
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
	store, dom := testkit.CreateMockStoreAndDomain(t)

	// There is no underlying netCon, use failpoint to avoid panic
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeClientConn", "return(1)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeClientConn"))
	}()
	tk := testkit.NewTestKit(t, store)

	connID := uint64(1)
	tk.Session().SetConnectionID(connID)
	tc := &TiDBContext{
		Session: tk.Session(),
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
	cc.SetCtx(tc)
	srv := &Server{
		clients: map[uint64]*clientConn{
			connID: cc,
		},
		dom: dom,
	}
	handle := dom.ExpensiveQueryHandle().SetSessionManager(srv)
	go handle.Run()

	tk.MustExec("use test;")
	tk.MustExec("CREATE TABLE testTable2 (id bigint PRIMARY KEY,  age int)")
	for i := 0; i < 10; i++ {
		str := fmt.Sprintf("insert into testTable2 values(%d, %d)", i, i%80)
		tk.MustExec(str)
	}

	tk.MustQuery("select SLEEP(1);").Check(testkit.Rows("0"))
	tk.MustExec("set @@max_execution_time = 500;")
	tk.MustQuery("select SLEEP(1);").Check(testkit.Rows("1"))
	tk.MustExec("set @@max_execution_time = 1500;")
	tk.MustExec("set @@tidb_expensive_query_time_threshold = 1;")
	tk.MustQuery("select SLEEP(1);").Check(testkit.Rows("0"))
	err := tk.QueryToErr("select * FROM testTable2 WHERE SLEEP(1);")
	require.Equal(t, "[executor:3024]Query execution was interrupted, maximum statement execution time exceeded", err.Error())
	// Test executor stats when execution time exceeded.
	tk.MustExec("set @@tidb_slow_log_threshold=300")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/unistoreRPCSlowByInjestSleep", `return(150)`))
	err = tk.QueryToErr("select /*+ max_execution_time(600), set_var(tikv_client_read_timeout=100) */ * from testTable2")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/unistoreRPCSlowByInjestSleep"))
	require.Error(t, err)
	require.Equal(t, "[executor:3024]Query execution was interrupted, maximum statement execution time exceeded", err.Error())
	planInfo, err := plancodec.DecodePlan(tk.Session().GetSessionVars().StmtCtx.GetEncodedPlan())
	require.NoError(t, err)
	require.Regexp(t, "TableReader.*cop_task: {num: .*num_rpc:.*, total_time:.*", planInfo)

	// Killed because of max execution time, reset Killed to 0.
	tk.Session().GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.MaxExecTimeExceeded)
	tk.MustExec("set @@max_execution_time = 0;")
	tk.MustQuery("select * FROM testTable2 WHERE SLEEP(1);").Check(testkit.Rows())
	err = tk.QueryToErr("select /*+ MAX_EXECUTION_TIME(100)*/  * FROM testTable2 WHERE  SLEEP(1);")
	require.Equal(t, "[executor:3024]Query execution was interrupted, maximum statement execution time exceeded", err.Error())

	// Killed because of max execution time, reset Killed to 0.
	tk.Session().GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.MaxExecTimeExceeded)
	err = cc.handleQuery(context.Background(), "select * FROM testTable2 WHERE SLEEP(1);")
	require.NoError(t, err)

	err = cc.handleQuery(context.Background(), "select /*+ MAX_EXECUTION_TIME(100)*/  * FROM testTable2 WHERE  SLEEP(1);")
	require.Equal(t, "[executor:3024]Query execution was interrupted, maximum statement execution time exceeded", err.Error())
	err = cc.handleQuery(context.Background(), "select /*+ set_var(max_execution_time=100) */ age, sleep(1) from testTable2 union all select age, 1 from testTable2")
	require.Equal(t, "[executor:3024]Query execution was interrupted, maximum statement execution time exceeded", err.Error())
	// Killed because of max execution time, reset Killed to 0.
	tk.Session().GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.MaxExecTimeExceeded)
	tk.MustExec("set @@max_execution_time = 500;")

	err = cc.handleQuery(context.Background(), "alter table testTable2 add index idx(age);")
	require.NoError(t, err)
}

func TestShutDown(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	cc := &clientConn{}
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	tc := &TiDBContext{Session: se}
	cc.SetCtx(tc)
	// set killed flag
	cc.status = connStatusShutdown
	// assert ErrQueryInterrupted
	err = cc.handleQuery(context.Background(), "select 1")
	require.Equal(t, exeerrors.ErrQueryInterrupted, err)

	cfg := serverutil.NewTestConfig()
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	drv := NewTiDBDriver(store)
	srv, err := NewServer(cfg, drv)
	require.NoError(t, err)
	srv.SetDomain(dom)

	cc = &clientConn{server: srv}
	cc.SetCtx(tc)

	// test in txn
	srv.clients[dom.NextConnID()] = cc
	cc.getCtx().GetSessionVars().SetInTxn(true)

	waitTime := 100 * time.Millisecond
	begin := time.Now()
	srv.DrainClients(waitTime, waitTime)
	require.Greater(t, time.Since(begin), waitTime)

	// test not in txn
	srv.clients[dom.NextConnID()] = cc
	cc.getCtx().GetSessionVars().SetInTxn(false)

	begin = time.Now()
	srv.DrainClients(waitTime, waitTime)
	require.Less(t, time.Since(begin), waitTime)
}

type snapshotCache interface {
	SnapCacheHitCount() int
}

func TestPrefetchPointKeys4Update(t *testing.T) {
	store := testkit.CreateMockStore(t)

	cc := &clientConn{
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt:        internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
	}
	tk := testkit.NewTestKit(t, store)
	cc.SetCtx(&TiDBContext{Session: tk.Session()})
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
	//nolint:forcetypeassert
	require.Equal(t, 6, snap.(snapshotCache).SnapCacheHitCount())
	tk.MustExec("commit")
	tk.MustQuery("select * from prefetch").Check(testkit.Rows("1 1 2", "2 2 4", "3 3 4"))

	tk.MustExec("begin pessimistic")
	tk.MustExec("update prefetch set c = c + 1 where a = 2 and b = 2")
	require.Equal(t, 2, tk.Session().GetSessionVars().TxnCtx.PessimisticCacheHit)
	err = cc.handleQuery(ctx, query)
	require.NoError(t, err)
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	require.Equal(t, 6, tk.Session().GetSessionVars().TxnCtx.PessimisticCacheHit)
	tk.MustExec("commit")
	tk.MustQuery("select * from prefetch").Check(testkit.Rows("1 1 3", "2 2 6", "3 3 5"))
}

func TestPrefetchPointKeys4Delete(t *testing.T) {
	store := testkit.CreateMockStore(t)

	cc := &clientConn{
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt:        internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
	}
	tk := testkit.NewTestKit(t, store)
	cc.SetCtx(&TiDBContext{Session: tk.Session()})
	ctx := context.Background()
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("use test")
	tk.MustExec("create table prefetch (a int, b int, c int, primary key (a, b))")
	tk.MustExec("insert prefetch values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk.MustExec("insert prefetch values (4, 4, 4), (5, 5, 5), (6, 6, 6)")
	tk.MustExec("begin optimistic")
	tk.MustExec("delete from prefetch where a = 2 and b = 2")

	// enable multi-statement
	capabilities := cc.ctx.GetSessionVars().ClientCapability
	capabilities ^= mysql.ClientMultiStatements
	cc.ctx.SetClientCapability(capabilities)
	query := "delete from prefetch where a = 1 and b = 1;" +
		"delete from prefetch where a = 2 and b = 2;" +
		"delete from prefetch where a = 3 and b = 3;"
	err := cc.handleQuery(ctx, query)
	require.NoError(t, err)
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	snap := txn.GetSnapshot()
	//nolint:forcetypeassert
	require.Equal(t, 6, snap.(snapshotCache).SnapCacheHitCount())
	tk.MustExec("commit")
	tk.MustQuery("select * from prefetch").Check(testkit.Rows("4 4 4", "5 5 5", "6 6 6"))

	tk.MustExec("begin pessimistic")
	tk.MustExec("delete from prefetch where a = 5 and b = 5")
	require.Equal(t, 2, tk.Session().GetSessionVars().TxnCtx.PessimisticCacheHit)
	query = "delete from prefetch where a = 4 and b = 4;" +
		"delete from prefetch where a = 5 and b = 5;" +
		"delete from prefetch where a = 6 and b = 6;"
	err = cc.handleQuery(ctx, query)
	require.NoError(t, err)
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	require.Equal(t, 6, tk.Session().GetSessionVars().TxnCtx.PessimisticCacheHit)
	tk.MustExec("commit")
	tk.MustQuery("select * from prefetch").Check(testkit.Rows())
}

func TestPrefetchBatchPointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)

	cc := &clientConn{
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt:        internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
	}
	tk := testkit.NewTestKit(t, store)
	cc.SetCtx(&TiDBContext{Session: tk.Session()})
	ctx := context.Background()
	tk.MustExec("use test")
	tk.MustExec("create table prefetch (a int primary key, b int)")
	tk.MustExec("insert prefetch values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
	tk.MustExec("begin optimistic")
	tk.MustExec("delete from prefetch where a = 1")

	// enable multi-statement
	capabilities := cc.ctx.GetSessionVars().ClientCapability
	capabilities ^= mysql.ClientMultiStatements
	cc.ctx.SetClientCapability(capabilities)
	query := "delete from prefetch where a in (2,3);" +
		"delete from prefetch where a in (4,5);"
	err := cc.handleQuery(ctx, query)
	require.NoError(t, err)
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	snap := txn.GetSnapshot()
	//nolint:forcetypeassert
	require.Equal(t, 4, snap.(snapshotCache).SnapCacheHitCount())
	tk.MustExec("commit")
	tk.MustQuery("select * from prefetch").Check(testkit.Rows())
}

func TestPrefetchPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	cc := &clientConn{
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt:        internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
	}
	tk := testkit.NewTestKit(t, store)
	cc.SetCtx(&TiDBContext{Session: tk.Session()})
	ctx := context.Background()
	tk.MustExec("use test")
	tk.MustExec("create table prefetch (a int primary key, b int) partition by hash(a) partitions 4")
	tk.MustExec("insert prefetch values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
	tk.MustExec("begin optimistic")
	tk.MustExec("delete from prefetch where a = 1")

	// enable multi-statement
	capabilities := cc.ctx.GetSessionVars().ClientCapability
	capabilities ^= mysql.ClientMultiStatements
	cc.ctx.SetClientCapability(capabilities)
	query := "delete from prefetch where a = 2;" +
		"delete from prefetch where a = 3;" +
		"delete from prefetch where a in (4,5);"
	// TODO: refactor the build code and extract the partition pruning one,
	// so it can be called in prefetchPointPlanKeys
	err := cc.handleQuery(ctx, query)
	require.NoError(t, err)
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	snap := txn.GetSnapshot()
	//nolint:forcetypeassert
	require.Equal(t, 4, snap.(snapshotCache).SnapCacheHitCount())
	tk.MustExec("commit")
	tk.MustQuery("select * from prefetch").Check(testkit.Rows())
}

func TestTiFlashFallback(t *testing.T) {
	store := testkit.CreateMockStore(t,
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			//nolint:forcetypeassert
			mockCluster := c.(*unistore.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			store := c.AllocID()
			peer := c.AllocID()
			mockCluster.AddStore(store, "tiflash0", &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
			mockCluster.AddPeer(region1, store, peer)
		}),
		mockstore.WithStoreType(mockstore.EmbedUnistore),
	)

	cc := &clientConn{
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt:        internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
	}

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")
	cc.SetCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})

	tk.MustExec("drop table if exists t")
	tk.MustExec("set tidb_cost_model_version=1")
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

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/ReduceCopNextMaxBackoff", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/ReduceCopNextMaxBackoff", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/ReduceCopNextMaxBackoff"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/ReduceCopNextMaxBackoff"))
	}()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash0", "return(\"tiflash0\")"))
	// test COM_STMT_EXECUTE
	ctx := context.Background()
	tk.MustExec("set @@tidb_allow_fallback_to_tikv='tiflash'")
	tk.MustExec("set @@tidb_allow_mpp=OFF")
	require.NoError(t, cc.HandleStmtPrepare(ctx, "select sum(a) from t"))
	require.NoError(t, cc.handleStmtExecute(ctx, []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0}))
	tk.MustQuery("show warnings").Check(testkit.Rows("Error 9012 TiFlash server timeout"))

	// test COM_STMT_FETCH (cursor mode)
	require.Error(t, cc.handleStmtExecute(ctx, []byte{0x1, 0x0, 0x0, 0x0, 0x1, 0x1, 0x0, 0x0, 0x0}))
	tk.MustExec("set @@tidb_allow_fallback_to_tikv=''")
	require.Error(t, cc.handleStmtExecute(ctx, []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0}))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash0"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/fetchNextErr", "return(\"firstNext\")"))
	// test COM_STMT_EXECUTE (cursor mode)
	tk.MustExec("set @@tidb_allow_fallback_to_tikv='tiflash'")
	require.NoError(t, cc.handleStmtExecute(ctx, []byte{0x1, 0x0, 0x0, 0x0, 0x1, 0x1, 0x0, 0x0, 0x0}))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/fetchNextErr"))

	// test that TiDB would not retry if the first execution already sends data to client
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/fetchNextErr", "return(\"secondNext\")"))
	tk.MustExec("set @@tidb_allow_fallback_to_tikv='tiflash'")
	require.Error(t, cc.handleQuery(ctx, "select * from t t1 join t t2 on t1.a = t2.a"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/fetchNextErr"))

	// simple TiFlash query (unary + non-streaming)
	tk.MustExec("set @@tidb_allow_batch_cop=0; set @@tidb_allow_mpp=0;")
	require.NoError(t, failpoint.Enable("tikvclient/tikvStoreSendReqResult", "return(\"requestTiFlashError\")"))
	testFallbackWork(t, tk, cc, "select sum(a) from t")
	require.NoError(t, failpoint.Disable("tikvclient/tikvStoreSendReqResult"))

	// TiFlash query based on batch cop (batch + streaming)
	tk.MustExec("set @@tidb_allow_batch_cop=1; set @@tidb_allow_mpp=0;")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash0", "return(\"tiflash0\")"))
	testFallbackWork(t, tk, cc, "select count(*) from t")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash0"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/batchCopRecvTimeout", "return(true)"))
	testFallbackWork(t, tk, cc, "select count(*) from t")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/batchCopRecvTimeout"))

	// TiFlash MPP query (MPP + streaming)
	tk.MustExec("set @@tidb_allow_batch_cop=0; set @@tidb_allow_mpp=1;")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppDispatchTimeout", "return(true)"))
	testFallbackWork(t, tk, cc, "select * from t t1 join t t2 on t1.a = t2.a")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppDispatchTimeout"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppRecvTimeout", "return(-1)"))
	testFallbackWork(t, tk, cc, "select * from t t1 join t t2 on t1.a = t2.a")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppRecvTimeout"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/establishMppConnectionErr", "return(true)"))
	testFallbackWork(t, tk, cc, "select * from t t1 join t t2 on t1.a = t2.a")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/establishMppConnectionErr"))

	// When fallback is not set, TiFlash mpp will return the original error message
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppDispatchTimeout", "return(true)"))
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
	store := testkit.CreateMockStore(t)
	cc := &clientConn{
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt:        internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
	}
	ctx := context.Background()
	tk := testkit.NewTestKit(t, store)
	cc.SetCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})

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
	store := testkit.CreateMockStore(t)

	cfg := serverutil.NewTestConfig()
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
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp := handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthNativePassword,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)

	// 8.0 or newer client trying to authenticate with caching_sha2_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp = handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthCachingSha2Password,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthNativePassword), resp.Auth)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch"))

	// client trying to authenticate with tidb_sm3_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp = handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthTiDBSM3Password,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthNativePassword), resp.Auth)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch"))

	// MySQL 5.1 or older client, without authplugin support
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp = handshake.Response41{
		Capability: mysql.ClientProtocol41,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)

	// === Target account has mysql_native_password ===
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeUser", "return(\"mysql_native_password\")"))

	// 5.7 or newer client trying to authenticate with mysql_native_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp = handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthNativePassword,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthNativePassword), resp.Auth)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch"))

	// 8.0 or newer client trying to authenticate with caching_sha2_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp = handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthCachingSha2Password,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthNativePassword), resp.Auth)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch"))

	// client trying to authenticate with tidb_sm3_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp = handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthTiDBSM3Password,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthNativePassword), resp.Auth)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch"))

	// MySQL 5.1 or older client, without authplugin support
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp = handshake.Response41{
		Capability: mysql.ClientProtocol41,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeUser"))

	// === Target account has caching_sha2_password ===
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeUser", "return(\"caching_sha2_password\")"))

	// 5.7 or newer client trying to authenticate with mysql_native_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp = handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthNativePassword,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthCachingSha2Password), resp.Auth)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch"))

	// 8.0 or newer client trying to authenticate with caching_sha2_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp = handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthCachingSha2Password,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthCachingSha2Password), resp.Auth)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch"))

	// client trying to authenticate with tidb_sm3_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp = handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthTiDBSM3Password,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthCachingSha2Password), resp.Auth)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch"))

	// MySQL 5.1 or older client, without authplugin support
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp = handshake.Response41{
		Capability: mysql.ClientProtocol41,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeUser"))

	// === Target account has tidb_sm3_password ===
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeUser", "return(\"tidb_sm3_password\")"))

	// 5.7 or newer client trying to authenticate with mysql_native_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp = handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthNativePassword,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthTiDBSM3Password), resp.Auth)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch"))

	// 8.0 or newer client trying to authenticate with caching_sha2_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp = handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthCachingSha2Password,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthTiDBSM3Password), resp.Auth)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch"))

	// client trying to authenticate with tidb_sm3_password
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch", "return(1)"))
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp = handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthTiDBSM3Password,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthTiDBSM3Password), resp.Auth)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch"))

	// MySQL 5.1 or older client, without authplugin support
	cc = &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "unativepassword",
	}
	resp = handshake.Response41{
		Capability: mysql.ClientProtocol41,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeUser"))
}

func TestChangeUserAuth(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user user1")

	cfg := serverutil.NewTestConfig()
	cfg.Port = 0
	cfg.Status.StatusPort = 0

	drv := NewTiDBDriver(store)
	srv, err := NewServer(cfg, drv)
	require.NoError(t, err)

	cc := &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		peerHost:     "localhost",
		collation:    mysql.DefaultCollationID,
		capability:   defaultCapability,
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "root",
	}
	ctx := context.Background()
	se, _ := session.CreateSession4Test(store)
	tc := &TiDBContext{
		Session: se,
		stmts:   make(map[int]*TiDBStatement),
	}
	cc.SetCtx(tc)

	data := []byte{}
	data = append(data, "user1"...)
	data = append(data, 0)
	data = append(data, 1)
	data = append(data, 0)
	data = append(data, "test"...)
	data = append(data, 0)
	data = append(data, 0, 0)
	data = append(data, "unknown"...)
	data = append(data, 0)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/ChangeUserAuthSwitch", fmt.Sprintf("return(\"%s\")", t.Name())))
	err = cc.handleChangeUser(ctx, data)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/ChangeUserAuthSwitch"))
	require.EqualError(t, err, t.Name())
}

func TestAuthPlugin2(t *testing.T) {
	store := testkit.CreateMockStore(t)

	cfg := serverutil.NewTestConfig()
	cfg.Port = 0
	cfg.Status.StatusPort = 0

	drv := NewTiDBDriver(store)
	srv, err := NewServer(cfg, drv)
	require.NoError(t, err)

	cc := &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "root",
	}
	ctx := context.Background()
	se, _ := session.CreateSession4Test(store)
	tc := &TiDBContext{
		Session: se,
		stmts:   make(map[int]*TiDBStatement),
	}
	cc.SetCtx(tc)

	resp := handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
	}

	cc.isUnixSocket = true
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch", "return(1)"))
	respAuthSwitch, err := cc.checkAuthPlugin(ctx, &resp)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch"))
	require.Equal(t, []byte(mysql.AuthNativePassword), respAuthSwitch)
	require.NoError(t, err)
}

func TestAuthSessionTokenPlugin(t *testing.T) {
	// create the cert
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "test1_cert.pem")
	keyPath := filepath.Join(tempDir, "test1_key.pem")
	err := util.CreateCertificates(certPath, keyPath, 1024, x509.RSA, x509.UnknownSignatureAlgorithm)
	require.NoError(t, err)

	cfg := config.GetGlobalConfig()
	cfg.Security.SessionTokenSigningCert = certPath
	cfg.Security.SessionTokenSigningKey = keyPath
	cfg.Port = 0
	cfg.Status.StatusPort = 0

	// The global config is read during creating the store.
	store := testkit.CreateMockStore(t)
	drv := NewTiDBDriver(store)
	srv, err := NewServer(cfg, drv)
	require.NoError(t, err)
	ctx := context.Background()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER auth_session_token")
	tk.MustExec("CREATE USER another_user")

	tc, err := drv.OpenCtx(uint64(0), 0, uint8(mysql.DefaultCollationID), "", nil, nil)
	require.NoError(t, err)
	cc := &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "auth_session_token",
	}
	cc.SetCtx(tc)
	// create a token without TLS
	tk1 := testkit.NewTestKitWithSession(t, store, tc.Session)
	tc.Session.GetSessionVars().ConnectionInfo = cc.connectInfo()
	tk1.Session().Auth(&auth.UserIdentity{Username: "auth_session_token", Hostname: "localhost"}, nil, nil, nil)
	tk1.MustQuery("show session_states")

	// create a token with TLS
	cc.tlsConn = tls.Client(nil, &tls.Config{})
	tc.Session.GetSessionVars().ConnectionInfo = cc.connectInfo()
	tk1.Session().Auth(&auth.UserIdentity{Username: "auth_session_token", Hostname: "localhost"}, nil, nil, nil)
	tk1.MustQuery("show session_states")

	// create a token with UnixSocket
	cc.tlsConn = nil
	cc.isUnixSocket = true
	tc.Session.GetSessionVars().ConnectionInfo = cc.connectInfo()
	rows := tk1.MustQuery("show session_states").Rows()
	//nolint:forcetypeassert
	tokenBytes := []byte(rows[0][1].(string))

	// auth with the token
	resp := handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthTiDBSessionToken,
		Auth:       tokenBytes,
	}
	err = cc.handleAuthPlugin(ctx, &resp)
	require.NoError(t, err)
	err = cc.openSessionAndDoAuth(resp.Auth, resp.AuthPlugin, resp.ZstdLevel)
	require.NoError(t, err)

	// login succeeds even if the password expires now
	tk.MustExec("ALTER USER auth_session_token PASSWORD EXPIRE")
	err = cc.openSessionAndDoAuth([]byte{}, mysql.AuthNativePassword, 0)
	require.ErrorContains(t, err, "Your password has expired")
	err = cc.openSessionAndDoAuth(resp.Auth, resp.AuthPlugin, resp.ZstdLevel)
	require.NoError(t, err)

	// wrong token should fail
	tokenBytes[0] ^= 0xff
	err = cc.openSessionAndDoAuth(resp.Auth, resp.AuthPlugin, resp.ZstdLevel)
	require.ErrorContains(t, err, "Access denied")
	tokenBytes[0] ^= 0xff

	// using the token to auth with another user should fail
	cc.user = "another_user"
	err = cc.openSessionAndDoAuth(resp.Auth, resp.AuthPlugin, resp.ZstdLevel)
	require.ErrorContains(t, err, "Access denied")
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
	brc := serverutil.NewBufferedReadConn(&testutil.BytesConn{Buffer: inBuffer})
	pkt := internal.NewPacketIO(brc)
	pkt.SetMaxAllowedPacket(maxAllowedPacket)
	readBytes, err = pkt.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("SELECT length('%s') as len;", strings.Repeat("a", 999)), string(readBytes))
	require.Equal(t, uint8(1), pkt.Sequence())

	// The length of total payload is (25 + 1000 = 1025).
	inBuffer.Reset()
	bytes = append([]byte{0x01, 0x04, 0x00, 0x00}, []byte(fmt.Sprintf("SELECT length('%s') as len;", strings.Repeat("a", 1000)))...)
	_, err = inBuffer.Write(bytes)
	require.NoError(t, err)
	brc = serverutil.NewBufferedReadConn(&testutil.BytesConn{Buffer: inBuffer})
	pkt = internal.NewPacketIO(brc)
	pkt.SetMaxAllowedPacket(maxAllowedPacket)
	_, err = pkt.ReadPacket()
	require.Error(t, err)

	// The length of total payload is (25 + 488 = 513).
	// Two separate packets would NOT exceed the limitation of maxAllowedPacket.
	inBuffer.Reset()
	bytes = append([]byte{0x01, 0x02, 0x00, 0x00}, []byte(fmt.Sprintf("SELECT length('%s') as len;", strings.Repeat("a", 488)))...)
	_, err = inBuffer.Write(bytes)
	require.NoError(t, err)
	brc = serverutil.NewBufferedReadConn(&testutil.BytesConn{Buffer: inBuffer})
	pkt = internal.NewPacketIO(brc)
	pkt.SetMaxAllowedPacket(maxAllowedPacket)
	readBytes, err = pkt.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("SELECT length('%s') as len;", strings.Repeat("a", 488)), string(readBytes))
	require.Equal(t, uint8(1), pkt.Sequence())
	inBuffer.Reset()
	bytes = append([]byte{0x01, 0x02, 0x00, 0x01}, []byte(fmt.Sprintf("SELECT length('%s') as len;", strings.Repeat("b", 488)))...)
	_, err = inBuffer.Write(bytes)
	require.NoError(t, err)
	brc = serverutil.NewBufferedReadConn(&testutil.BytesConn{Buffer: inBuffer})
	pkt.SetBufferedReadConn(brc)
	readBytes, err = pkt.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("SELECT length('%s') as len;", strings.Repeat("b", 488)), string(readBytes))
	require.Equal(t, uint8(2), pkt.Sequence())
}

func TestOkEof(t *testing.T) {
	store := testkit.CreateMockStore(t)

	var outBuffer bytes.Buffer
	tidbdrv := NewTiDBDriver(store)
	cfg := serverutil.NewTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	defer server.Close()

	cc := &clientConn{
		connectionID: 1,
		salt:         []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14},
		server:       server,
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(&outBuffer)),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		alloc:        arena.NewAllocator(512),
		chunkAlloc:   chunk.NewAllocator(),
		capability:   mysql.ClientProtocol41 | mysql.ClientDeprecateEOF,
	}

	tk := testkit.NewTestKit(t, store)
	ctx := &TiDBContext{Session: tk.Session()}
	cc.SetCtx(ctx)

	err = cc.writeOK(context.Background())
	require.NoError(t, err)
	require.Equal(t, []byte{0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0}, outBuffer.Bytes())

	outBuffer.Reset()
	err = cc.writeEOF(context.Background(), cc.ctx.Status())
	require.NoError(t, err)
	err = cc.flush(context.TODO())
	require.NoError(t, err)
	require.Equal(t, mysql.EOFHeader, outBuffer.Bytes()[4])
	require.Equal(t, []byte{0x7, 0x0, 0x0, 0x1, 0xfe, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0}, outBuffer.Bytes())
}

func TestExtensionChangeUser(t *testing.T) {
	defer extension.Reset()
	extension.Reset()

	logged := false
	var logTp extension.ConnEventTp
	var logInfo *extension.ConnEventInfo
	require.NoError(t, extension.Register("test", extension.WithSessionHandlerFactory(func() *extension.SessionHandler {
		return &extension.SessionHandler{
			OnConnectionEvent: func(tp extension.ConnEventTp, info *extension.ConnEventInfo) {
				require.False(t, logged)
				logTp = tp
				logInfo = info
				logged = true
			},
		}
	})))

	extensions, err := extension.GetExtensions()
	require.NoError(t, err)

	store := testkit.CreateMockStore(t)

	var outBuffer bytes.Buffer
	tidbdrv := NewTiDBDriver(store)
	cfg := serverutil.NewTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	defer server.Close()

	cc := &clientConn{
		connectionID: 1,
		salt:         []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14},
		server:       server,
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(&outBuffer)),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		alloc:        arena.NewAllocator(512),
		chunkAlloc:   chunk.NewAllocator(),
		capability:   mysql.ClientProtocol41,
		extensions:   extensions.NewSessionExtensions(),
	}

	tk := testkit.NewTestKit(t, store)
	ctx := &TiDBContext{Session: tk.Session()}
	cc.SetCtx(ctx)
	tk.MustExec("create user user1")
	tk.MustExec("create user user2")
	tk.MustExec("create database db1")
	tk.MustExec("create database db2")
	tk.MustExec("grant select on db1.* to user1@'%'")
	tk.MustExec("grant select on db2.* to user2@'%'")

	// change user.
	doDispatch := func(req dispatchInput) {
		inBytes := append([]byte{req.com}, req.in...)
		err = cc.dispatch(context.Background(), inBytes)
		require.Equal(t, req.err, err)
		if err == nil {
			err = cc.flush(context.TODO())
			require.NoError(t, err)
			require.Equal(t, req.out, outBuffer.Bytes())
		} else {
			_ = cc.flush(context.TODO())
		}
		outBuffer.Reset()
	}

	expectedConnInfo := extension.ConnEventInfo{
		ConnectionInfo: cc.connectInfo(),
		ActiveRoles:    []*auth.RoleIdentity{},
	}
	expectedConnInfo.User = "user1"
	expectedConnInfo.DB = "db1"

	require.False(t, logged)
	userData := append([]byte("user1"), 0x0, 0x0)
	userData = append(userData, []byte("db1")...)
	userData = append(userData, 0x0)
	doDispatch(dispatchInput{
		com: mysql.ComChangeUser,
		in:  userData,
		err: nil,
		out: []byte{0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0},
	})
	require.True(t, logged)
	require.Equal(t, extension.ConnReset, logTp)
	require.Equal(t, expectedConnInfo.ActiveRoles, logInfo.ActiveRoles)
	require.Equal(t, expectedConnInfo.Error, logInfo.Error)
	require.Equal(t, *(expectedConnInfo.ConnectionInfo), *(logInfo.ConnectionInfo))

	logged = false
	logTp = 0
	logInfo = nil
	expectedConnInfo.User = "user2"
	expectedConnInfo.DB = "db2"
	userData = append([]byte("user2"), 0x0, 0x0)
	userData = append(userData, []byte("db2")...)
	userData = append(userData, 0x0)
	doDispatch(dispatchInput{
		com: mysql.ComChangeUser,
		in:  userData,
		err: nil,
		out: []byte{0x7, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0},
	})
	require.True(t, logged)
	require.Equal(t, extension.ConnReset, logTp)
	require.Equal(t, expectedConnInfo.ActiveRoles, logInfo.ActiveRoles)
	require.Equal(t, expectedConnInfo.Error, logInfo.Error)
	require.Equal(t, *(expectedConnInfo.ConnectionInfo), *(logInfo.ConnectionInfo))

	logged = false
	logTp = 0
	logInfo = nil
	doDispatch(dispatchInput{
		com: mysql.ComResetConnection,
		in:  nil,
		err: nil,
		out: []byte{0x7, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0},
	})
	require.True(t, logged)
	require.Equal(t, extension.ConnReset, logTp)
	require.Equal(t, expectedConnInfo.ActiveRoles, logInfo.ActiveRoles)
	require.Equal(t, expectedConnInfo.Error, logInfo.Error)
	require.Equal(t, *(expectedConnInfo.ConnectionInfo), *(logInfo.ConnectionInfo))
}

func TestAuthSha(t *testing.T) {
	store := testkit.CreateMockStore(t)

	var outBuffer bytes.Buffer
	tidbdrv := NewTiDBDriver(store)
	cfg := serverutil.NewTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	defer server.Close()

	cc := &clientConn{
		connectionID: 1,
		salt:         []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14},
		server:       server,
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(&outBuffer)),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		alloc:        arena.NewAllocator(512),
		chunkAlloc:   chunk.NewAllocator(),
		capability:   mysql.ClientProtocol41,
	}

	tk := testkit.NewTestKit(t, store)
	ctx := &TiDBContext{Session: tk.Session()}
	cc.SetCtx(ctx)

	resp := handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		AuthPlugin: mysql.AuthCachingSha2Password,
		Auth:       []byte{}, // No password
	}

	authData, err := cc.authSha(context.Background(), resp)
	require.NoError(t, err)

	// If no password is specified authSha() should return an empty byte slice
	// which differs from when a password is specified as that should trigger
	// fastAuthFail and the rest of the auth process.
	require.Equal(t, authData, []byte{})
}

func TestProcessInfoForExecuteCommand(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	cc := &clientConn{
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt:        internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
	}
	ctx := context.Background()

	tk.MustExec("use test")
	cc.SetCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})

	tk.MustExec("create table t (col1 int)")

	// simple prepare and execute
	require.NoError(t, cc.HandleStmtPrepare(ctx, "select sum(col1) from t"))
	require.NoError(t, cc.handleStmtExecute(ctx, []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0}))
	require.Equal(t, cc.ctx.Session.ShowProcess().Info, "select sum(col1) from t")

	// prepare and execute with params
	require.NoError(t, cc.HandleStmtPrepare(ctx, "select sum(col1) from t where col1 < ? and col1 > 100"))
	// 1 params, length of nullBitMap is 1, `0x8, 0x0` represents the type, and the following `0x10, 0x0....` is the param
	// 10
	require.NoError(t, cc.handleStmtExecute(ctx, []byte{0x2, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0,
		0x1, 0x8, 0x0,
		0x0A, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}))
	require.Equal(t, cc.ctx.Session.ShowProcess().Info, "select sum(col1) from t where col1 < ? and col1 > 100")
}

func TestLDAPAuthSwitch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	cfg := serverutil.NewTestConfig()
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	drv := NewTiDBDriver(store)
	srv, err := NewServer(cfg, drv)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER test_simple_ldap IDENTIFIED WITH authentication_ldap_simple AS 'uid=test_simple_ldap,dc=example,dc=com'")

	cc := &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "test_simple_ldap",
	}
	se, _ := session.CreateSession4Test(store)
	tc := &TiDBContext{
		Session: se,
		stmts:   make(map[int]*TiDBStatement),
	}
	cc.SetCtx(tc)
	cc.isUnixSocket = true

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch", "return(1)"))
	respAuthSwitch, err := cc.checkAuthPlugin(context.Background(), &handshake.Response41{
		Capability: mysql.ClientProtocol41 | mysql.ClientPluginAuth,
		User:       "test_simple_ldap",
	})
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/FakeAuthSwitch"))
	require.NoError(t, err)
	require.Equal(t, []byte(mysql.AuthMySQLClearPassword), respAuthSwitch)
}

func TestEmptyOrgName(t *testing.T) {
	inputs := []dispatchInput{
		{
			com: mysql.ComQuery,
			in:  append([]byte("SELECT DATE_FORMAT(CONCAT('2023-07-0', a), '%Y')  AS 'YEAR' FROM test.t"), 0x0),
			err: nil,
			out: []byte{0x1, 0x0, 0x0, 0x0, 0x1, // 1 column
				0x1a, 0x0, 0x0,
				0x1, 0x3, 0x64, 0x65, 0x66, // catalog
				0x0,                         // schema
				0x0,                         // table name
				0x0,                         // org table
				0x4, 0x59, 0x45, 0x41, 0x52, // name 'YEAR'
				0x0, // org name
				0xc, 0x2e, 0x0, 0x2c, 0x0, 0x0, 0x0, 0xfd, 0x0, 0x0, 0x1f, 0x0, 0x0, 0x1, 0x0, 0x0, 0x2, 0xfe, 0x5, 0x0,
				0x0, 0x3, 0x4, 0x32, 0x30, 0x32, 0x33, 0x1, 0x0, 0x0, 0x4, 0xfe},
		},
	}

	testDispatch(t, inputs, 0)
}

func TestStats(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	stats := &compressionStats{}

	// No compression
	vars := tk.Session().GetSessionVars()
	m, err := stats.Stats(vars)
	require.NoError(t, err)
	require.Equal(t, "OFF", m["Compression"])
	require.Equal(t, "", m["Compression_algorithm"])
	require.Equal(t, 0, m["Compression_level"])

	// zlib compression
	vars.CompressionAlgorithm = mysql.CompressionZlib
	m, err = stats.Stats(vars)
	require.NoError(t, err)
	require.Equal(t, "ON", m["Compression"])
	require.Equal(t, "zlib", m["Compression_algorithm"])
	require.Equal(t, mysql.ZlibCompressDefaultLevel, m["Compression_level"])

	// zstd compression, with level 1
	vars.CompressionAlgorithm = mysql.CompressionZstd
	vars.CompressionLevel = 1
	m, err = stats.Stats(vars)
	require.NoError(t, err)
	require.Equal(t, "ON", m["Compression"])
	require.Equal(t, "zstd", m["Compression_algorithm"])
	require.Equal(t, 1, m["Compression_level"])
}

func TestCloseConn(t *testing.T) {
	var outBuffer bytes.Buffer

	store, _ := testkit.CreateMockStoreAndDomain(t)
	cfg := serverutil.NewTestConfig()
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	drv := NewTiDBDriver(store)
	server, err := NewServer(cfg, drv)
	require.NoError(t, err)

	cc := &clientConn{
		connectionID: 0,
		salt: []byte{
			0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
			0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14,
		},
		server:     server,
		pkt:        internal.NewPacketIOForTest(bufio.NewWriter(&outBuffer)),
		collation:  mysql.DefaultCollationID,
		peerHost:   "localhost",
		alloc:      arena.NewAllocator(512),
		chunkAlloc: chunk.NewAllocator(),
		capability: mysql.ClientProtocol41,
	}

	var wg sync.WaitGroup
	const numGoroutines = 10
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			err := closeConn(cc)
			require.NoError(t, err)
		}()
	}
	wg.Wait()
}

func TestConnAddMetrics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	re := require.New(t)
	tk := testkit.NewTestKit(t, store)
	cc := &clientConn{
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt:        internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
	}
	tk.MustExec("use test")
	cc.SetCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})

	// default
	cc.addMetrics(mysql.ComQuery, time.Now(), nil)
	counter := metrics.QueryTotalCounter
	v := promtestutils.ToFloat64(counter.WithLabelValues("Query", "OK", "default"))
	require.Equal(t, 1.0, v)

	// rg1
	cc.getCtx().GetSessionVars().ResourceGroupName = "test_rg1"
	cc.addMetrics(mysql.ComQuery, time.Now(), nil)
	re.Equal(promtestutils.ToFloat64(counter.WithLabelValues("Query", "OK", "default")), 1.0)
	re.Equal(promtestutils.ToFloat64(counter.WithLabelValues("Query", "OK", "test_rg1")), 1.0)
	/// inc the counter again
	cc.addMetrics(mysql.ComQuery, time.Now(), nil)
	re.Equal(promtestutils.ToFloat64(counter.WithLabelValues("Query", "OK", "test_rg1")), 2.0)

	// rg2
	cc.getCtx().GetSessionVars().ResourceGroupName = "test_rg2"
	// error
	cc.addMetrics(mysql.ComQuery, time.Now(), errors.New("unknown error"))
	re.Equal(promtestutils.ToFloat64(counter.WithLabelValues("Query", "Error", "test_rg2")), 1.0)
	// ok
	cc.addMetrics(mysql.ComStmtExecute, time.Now(), nil)
	re.Equal(promtestutils.ToFloat64(counter.WithLabelValues("StmtExecute", "OK", "test_rg2")), 1.0)
}
