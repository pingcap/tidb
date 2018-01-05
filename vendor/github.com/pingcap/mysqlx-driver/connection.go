// A Go driver for the MySQL X protocol for Go's database/sql package
// Based heavily on:
// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
// Copyright 2016 Simon J Mudd.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql/driver"
	"fmt"
	"net"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tipb/go-mysqlx/Resultset"
	"github.com/pingcap/tipb/go-mysqlx/Sql"
)

type mysqlXConn struct {
	buf              buffer       // raw bytes pulled in from network
	pb               *netProtobuf // holds a possible protobuf message that still needs processing
	netConn          net.Conn
	affectedRows     uint64
	insertID         uint64
	cfg              *xconfig
	maxPacketAllowed int
	maxWriteSize     int
	parseTime        bool
	strict           bool
	state            queryState
	capabilities     ServerCapabilities
	systemVariable   []byte
}

func (mc *mysqlXConn) capabilityTestUnknownCapability() error {
	name := "randomCapability"
	return mc.setScalarBoolCapability(name, true)
}

// second stage of the open once the driver has been selecteed
func (mc *mysqlXConn) Open2() (driver.Conn, error) {
	var err error
	// Connect to Server
	if dial, ok := dials[mc.cfg.net]; ok {
		mc.netConn, err = dial(mc.cfg.addr)
	} else {
		nd := net.Dialer{Timeout: mc.cfg.timeout}
		mc.netConn, err = nd.Dial(mc.cfg.net, mc.cfg.addr)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Enable TCP Keepalives on TCP connections
	if tc, ok := mc.netConn.(*net.TCPConn); ok {
		if err = tc.SetKeepAlive(true); err != nil {
			// Don't send COM_QUIT before handshake.
			if err = mc.netConn.Close(); err != nil {
				return nil, errors.Trace(err)
			}
			mc.netConn = nil
			return nil, errors.Trace(err)
		}
	}

	mc.buf = newBuffer(mc.netConn)

	// could/should be optional for performance? e.g. dsn has get_capabilities=0
	if err = mc.getCapabilities(); err != nil {
		return nil, errors.Trace(err)
	}

	if !mc.capabilities.Exists("authentication.mechanisms") {
		return nil, errors.Errorf("mysqlXConn.Open2: did not find capability: authentication.mechanisms")
	}

	// Current known capabilities: as of 5.7.14
	//   "tls"                       (scalar string) only visible if TLS is configured
	//   "authentication.mechanisms" (array string)
	//   "doc.formats"               (scalar string)
	//   "node_type"                 (scalar string)
	//   "plugin.version"            (scalar string)
	//   "client.pwd_expire_ok"      (scalar bool)

	// Check and use the first one we can. SHOULD prioritise.
	values := mc.capabilities.Values("authentication.mechanisms")

	found := false
	for i := range values {
		if values[i].String() == "MYSQL41" {
			found = true
			if err = mc.AuthenticateMySQL41(); err != nil {
				return nil, errors.Trace(err)
			}
			break
		}
	}
	if !found {
		return nil, errors.Errorf("mysqlXConn.Open2: could not find authentication.mechanism I can deal with. Found: %+v", values)
	}

	//	// Get max allowed packet size
	//	maxap, err := mc.getSystemVar("mysqlx_max_allowed_packet") // NOT THE SAME AS max_allowed_packet !!
	//	if err != nil {
	//		mc.Close()
	//		return nil, err
	//	}
	//	mc.maxPacketAllowed = stringToInt(maxap) - 1
	//	if mc.maxPacketAllowed < maxPacketSize {
	//		mc.maxWriteSize = mc.maxPacketAllowed
	//	}

	// Handle DSN Params
	err = mc.handleParams()
	if err != nil {
		if err = mc.Close(); err != nil {
			return nil, errors.Trace(err)
		}
		return nil, errors.Trace(err)
	}

	return mc, nil
}

// Gets the value of the given MySQL System Variable
// FIXME FIXME FIXME
// - Note this is broken as we need to send a normal SQL statement to get the values.
// - Currently returning a constant to the client which is of course wrong.
// FIXME FIXME FIXME
func (mc *mysqlXConn) getSystemVar(name string) ([]byte, error) {

	// Required steps are:
	// - Send command  "SELECT @@"+name)
	// - check the values
	// - return them

	if name == "mysqlx_max_allowed_packet" {
		// hard-coded: should not be FIXME FIXME
		response := "1048576"
		return []byte(response), nil
	}

	return nil, errors.Errorf("mysqlXConn.getSystemVar(%s) not implemented", name)
}

// Handles parameters set in DSN after the connection is established
func (mc *mysqlXConn) handleParams() (err error) {
	for param, val := range mc.cfg.params {
		switch param {
		// Charset
		case "charset":
			// is this a code bug in upstream go-sql-drivers/mysql ?
			// not quite sure why we might want to set more than one charset here.
			charsets := strings.Split(val, ",")
			for i := range charsets {
				// ignore errors here - a charset may not exist
				err = mc.exec("SET NAMES "+charsets[i], nil)
				if err == nil {
					break
				}
			}
			if err != nil {
				return
			}

		// time.Time parsing
		case "parseTime":
			var isBool bool
			mc.parseTime, isBool = readBool(val)
			if !isBool {
				return errors.New("Invalid Bool value: " + val)
			}

		// Strict mode
		case "strict":
			var isBool bool
			mc.strict, isBool = readBool(val)
			if !isBool {
				return errors.New("Invalid Bool value: " + val)
			}

		// Compression
		case "compress":
			err = errors.New("Compression not implemented yet")
			return

		// TLS
		case "tls":
			// FIXME FIXME FIXME
			//
			// not sure about order and handling of TLS
			// - do we try to go into TLS mode first?
			// - do we go into TLS mode at the point that we process the parameter options?
			// - do we go into TLS mode at the end of processing parameters, having remembered that we want to use TLS mode?
			// - it may make sense to go into TLS mode early but that changes logic etc.
			//
			// FIXME FIXME FIXME

			// check if the server has advertised tls capabilities
			if !mc.capabilities.Exists("tls") {
				return errors.New("Server does not support TLS")
			}
			tls := mc.capabilities.Values("tls")
			if len(tls) == 1 {
				return errors.Errorf("server tls capability returns unexpected result: len(tls) = %d, expecting 1", len(tls))
			}
			tlsType := tls[0].Type()
			if tlsType != "bool" {
				return errors.Errorf("server tls capability type unexpected: %s, expecting bool", tlsType)
			}
			tlsValue := tls[0].Bool()

			// Setup or check the magic TLS config here
			// - should have been done by the app before

			// Tell the server we want to go in TLS mode.
			if err = mc.setScalarBoolCapability("tls", tlsValue); err != nil {
				return errors.Trace(err)
			}
			// wait for OK back and if we get it then we go into TLS mode

		// System Vars
		default:
			err = mc.exec("SET "+param+"="+val+"", nil)
			if err != nil {
				return
			}
		}
	}
	return
}

// Internal function to execute commands when we don't expect a resultset
// e.g. for sending commands.
func (mc *mysqlXConn) exec(query string, args []driver.Value) error {

	// Should be able to use normal "query logic" here
	rows, err := mc.Query(query, args)
	if err != nil {
		return errors.Trace(err)
	}

	// close the rows and handle any response packets received
	return rows.Close()
}

// methods needed to make things work
func (mc *mysqlXConn) Begin() (driver.Tx, error) {
	log.Info("Use begin.")
	return nil, errors.Errorf("cannot use 'begin', mysqlXConn does not support transaction")
}

// close the connection
func (mc *mysqlXConn) Close() error {
	log.Info("Closing connection.")
	if mc.netConn == nil {
		return nil
	}

	// we don't handle in mc that we are dealing with a query. If we are we need to drain the input.

	// send this message
	if err := mc.writeClose(); err != nil {
		return errors.Trace(err)
	}

	// wait for Ok or Error, and ignore others
	var err error
	var pb *netProtobuf
	done := false
	for !done {
		pb, err = mc.readMsg()
		if err != nil {
			return errors.Trace(err)
		}

		switch Mysqlx.ServerMessages_Type(pb.msgType) {
		case Mysqlx.ServerMessages_OK:
			// show any message
			ok := new(Mysqlx.Ok)
			if err = proto.Unmarshal(pb.payload, ok); err != nil {
				return errors.Trace(err)
			}
			done = true
		case Mysqlx.ServerMessages_ERROR:
			return errors.Trace(err)
		case Mysqlx.ServerMessages_NOTICE:
			// process the notice message
			if err = mc.processNotice("mysqlXConn.Close()"); err != nil {
				return errors.Trace(err)
			}
		}
	}

	if err = mc.netConn.Close(); err != nil {
		return errors.Trace(err)
	}
	mc.netConn = nil
	return nil
}

func (mc *mysqlXConn) Prepare(query string) (driver.Stmt, error) {
	log.Infof("Query: %d", query)
	return nil, errors.Errorf("Prepare statement '%s' error. mysqlXConn dose not support prepare.", query)
}

func (mc *mysqlXConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	log.Infof("Query: %d", query)
	mc.affectedRows = 0
	mc.insertID = 0
	if err := mc.exec(query, args); err != nil {
		return nil, errors.Trace(err)
	}
	return &mysqlResult{
		affectedRows: int64(mc.affectedRows),
		insertID:     int64(mc.insertID),
	}, nil
}

// Query is the public interface to making a query via database/sql
func (mc *mysqlXConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	log.Infof("Query: %d", query)
	if mc.netConn == nil {
		return nil, driver.ErrBadConn
	}

	nameSpace := "sql"
	var stmtArgs []*Mysqlx_Datatypes.Any
	if len(args) > 0 {
		str, ok := args[0].(string)
		if ok {
			nameSpace = str
		}
		for i := 1; i < len(args); i++ {
			var any Mysqlx_Datatypes.Any
			switch v := args[i].(type) {
			case string:
				any = setString([]byte(v))
			case int:
				any = setUint(uint64(v))
			default:
				continue
			}
			stmtArgs = append(stmtArgs, &any)
		}
	}

	stmtExecute := &Mysqlx_Sql.StmtExecute{
		Namespace: &nameSpace,
		Stmt:      []byte(query),
		Args:      stmtArgs,
	}

	// write a StmtExecute packet with the given query to the network
	// - we DO NOT process the result as this will be done later.
	if err := mc.writeStmtExecute(stmtExecute); err != nil {
		return nil, errors.Trace(err)
	}

	// return the iterator
	return &mysqlXRows{
		columns: nil, // be explicit about expectations
		err:     nil, // be explicit about expectations
		mc:      mc,
		state:   queryStateWaitingColumnMetaData,
	}, nil
}

func printableColumnMetaData(pb *netProtobuf) string {
	p := new(Mysqlx_Resultset.ColumnMetaData)
	if err := proto.Unmarshal(pb.payload, p); err != nil {
		log.Fatalf("error unmarshaling ColumnMetaData p: %v", err)
	}

	return fmt.Sprintf("Type: %v, Name: %q, OriginalName: %q, Table: %q, Schema: %q, Catalog: %q, Collation: %v, FractionalDigits: %v, Length: %v, Flags: %v, ContentType: %v",
		p.GetType(),
		string(p.GetName()),
		string(p.GetOriginalName()),
		string(p.GetTable()),
		string(p.GetSchema()),
		string(p.GetCatalog()),
		p.GetCollation(),
		p.GetFractionalDigits(),
		p.GetLength(),
		p.GetFlags(),
		p.GetContentType())
}
