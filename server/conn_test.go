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
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/arena"
)

type ConnTestSuite struct {
	dom   *domain.Domain
	store kv.Storage
}

var _ = Suite(ConnTestSuite{})

func (ts ConnTestSuite) TestMalformHandshakeHeader(c *C) {
	c.Parallel()
	data := []byte{0x00}
	var p handshakeResponse41
	_, err := parseHandshakeResponseHeader(context.Background(), &p, data)
	c.Assert(err, NotNil)
}

func (ts ConnTestSuite) TestParseHandshakeResponse(c *C) {
	c.Parallel()
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
	c.Assert(err, IsNil)
	c.Assert(p.Capability&mysql.ClientConnectAtts, Equals, mysql.ClientConnectAtts)
	err = parseHandshakeResponseBody(context.Background(), &p, data, offset)
	c.Assert(err, IsNil)
	eq := mapIdentical(p.Attrs, map[string]string{
		"_client_version": "5.6.6-m9",
		"_platform":       "x86_64",
		"foo":             "bar",
		"_os":             "debian6.0",
		"_client_name":    "libmysql",
		"_pid":            "22344"})
	c.Assert(eq, IsTrue)

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
	c.Assert(err, IsNil)
	capability := mysql.ClientProtocol41 |
		mysql.ClientPluginAuth |
		mysql.ClientSecureConnection |
		mysql.ClientConnectWithDB
	c.Assert(p.Capability&capability, Equals, capability)
	err = parseHandshakeResponseBody(context.Background(), &p, data, offset)
	c.Assert(err, IsNil)
	c.Assert(p.User, Equals, "pam")
	c.Assert(p.DBName, Equals, "test")

	// Test for compatibility of Protocol::HandshakeResponse320
	data = []byte{
		0x00, 0x80, 0x00, 0x00, 0x01, 0x72, 0x6f, 0x6f, 0x74, 0x00, 0x00,
	}
	p = handshakeResponse41{}
	offset, err = parseOldHandshakeResponseHeader(context.Background(), &p, data)
	c.Assert(err, IsNil)
	capability = mysql.ClientProtocol41 |
		mysql.ClientSecureConnection
	c.Assert(p.Capability&capability, Equals, capability)
	err = parseOldHandshakeResponseBody(context.Background(), &p, data, offset)
	c.Assert(err, IsNil)
	c.Assert(p.User, Equals, "root")
}

func (ts ConnTestSuite) TestIssue1768(c *C) {
	c.Parallel()
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
	c.Assert(err, IsNil)
	c.Assert(p.Capability&mysql.ClientPluginAuthLenencClientData, Equals, mysql.ClientPluginAuthLenencClientData)
	err = parseHandshakeResponseBody(context.Background(), &p, data, offset)
	c.Assert(err, IsNil)
	c.Assert(len(p.Auth) > 0, IsTrue)
}

func (ts ConnTestSuite) TestInitialHandshake(c *C) {
	c.Parallel()
	var outBuffer bytes.Buffer
	cc := &clientConn{
		connectionID: 1,
		salt:         []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14},
		server: &Server{
			capability: defaultCapability,
		},
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(&outBuffer),
		},
	}
	err := cc.writeInitialHandshake()
	c.Assert(err, IsNil)

	expected := new(bytes.Buffer)
	expected.WriteByte(0x0a)                                                                             // Protocol
	expected.WriteString(mysql.ServerVersion)                                                            // Version
	expected.WriteByte(0x00)                                                                             // NULL
	binary.Write(expected, binary.LittleEndian, uint32(1))                                               // Connection ID
	expected.Write([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00})                         // Salt
	binary.Write(expected, binary.LittleEndian, uint16(defaultCapability&0xFFFF))                        // Server Capability
	expected.WriteByte(uint8(mysql.DefaultCollationID))                                                  // Server Language
	binary.Write(expected, binary.LittleEndian, mysql.ServerStatusAutocommit)                            // Server Status
	binary.Write(expected, binary.LittleEndian, uint16((defaultCapability>>16)&0xFFFF))                  // Extended Server Capability
	expected.WriteByte(0x15)                                                                             // Authentication Plugin Length
	expected.Write([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})                   // Unused
	expected.Write([]byte{0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x00}) // Salt
	expected.WriteString("mysql_native_password")                                                        // Authentication Plugin
	expected.WriteByte(0x00)                                                                             // NULL
	c.Assert(outBuffer.Bytes()[4:], DeepEquals, expected.Bytes())
}

func (ts ConnTestSuite) testGetSessionVarsWaitTimeout(c *C) {
	c.Parallel()
	var err error
	ts.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	ts.dom, err = session.BootstrapSession(ts.store)
	c.Assert(err, IsNil)
	se, err := session.CreateSession4Test(ts.store)
	c.Assert(err, IsNil)
	tc := &TiDBContext{
		session: se,
		stmts:   make(map[int]*TiDBStatement),
	}
	cc := &clientConn{
		connectionID: 1,
		server: &Server{
			capability: defaultCapability,
		},
		ctx: tc,
	}
	c.Assert(cc.getSessionVarsWaitTimeout(context.Background()), Equals, 28800)
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

func (ts ConnTestSuite) TestConnExecutionTimeout(c *C) {
	//There is no underlying netCon, use failpoint to avoid panic
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/server/FakeClientConn", "return(1)"), IsNil)

	c.Parallel()
	var err error
	ts.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	ts.dom, err = session.BootstrapSession(ts.store)
	c.Assert(err, IsNil)
	se, err := session.CreateSession4Test(ts.store)
	c.Assert(err, IsNil)

	connID := 1
	se.SetConnectionID(uint64(connID))
	tc := &TiDBContext{
		session: se,
		stmts:   make(map[int]*TiDBStatement),
	}
	cc := &clientConn{
		connectionID: uint32(connID),
		server: &Server{
			capability: defaultCapability,
		},
		ctx:   tc,
		alloc: arena.NewAllocator(32 * 1024),
	}
	srv := &Server{
		clients: map[uint32]*clientConn{
			uint32(connID): cc,
		},
	}
	handle := ts.dom.ExpensiveQueryHandle().SetSessionManager(srv)
	go handle.Run()
	defer handle.Close()

	_, err = se.Execute(context.Background(), "use test;")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "CREATE TABLE testTable2 (id bigint PRIMARY KEY,  age int)")
	c.Assert(err, IsNil)
	for i := 0; i < 10; i++ {
		str := fmt.Sprintf("insert into testTable2 values(%d, %d)", i, i%80)
		_, err = se.Execute(context.Background(), str)
		c.Assert(err, IsNil)
	}

	_, err = se.Execute(context.Background(), "select SLEEP(1);")
	c.Assert(err, IsNil)

	_, err = se.Execute(context.Background(), "set @@max_execution_time = 500;")
	c.Assert(err, IsNil)

	now := time.Now()
	err = cc.handleQuery(context.Background(), "select * FROM testTable2 WHERE SLEEP(3);")
	c.Assert(err, IsNil)
	c.Assert(time.Since(now) < 3*time.Second, IsTrue)

	_, err = se.Execute(context.Background(), "set @@max_execution_time = 0;")
	c.Assert(err, IsNil)

	now = time.Now()
	err = cc.handleQuery(context.Background(), "select * FROM testTable2 WHERE SLEEP(1);")
	c.Assert(err, IsNil)
	c.Assert(time.Since(now) > 500*time.Millisecond, IsTrue)

	now = time.Now()
	err = cc.handleQuery(context.Background(), "select /*+ MAX_EXECUTION_TIME(100)*/  * FROM testTable2 WHERE  SLEEP(3);")
	c.Assert(err, IsNil)
	c.Assert(time.Since(now) < 3*time.Second, IsTrue)

	c.Assert(failpoint.Disable("github.com/pingcap/tidb/server/FakeClientConn"), IsNil)
}
