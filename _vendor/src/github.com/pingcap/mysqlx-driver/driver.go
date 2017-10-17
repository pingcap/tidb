// Go driver for MySQL X Protocol
//
// Copyright 2016 Simon J Mudd.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.
//
// MySQL X protocol authentication using MYSQL41 method

package mysql

import (
	"database/sql"
	"database/sql/driver"
	"net"
)

// This struct is exported to make the driver directly accessible.
// In general the driver is used via the database/sql package.
type XDriver struct{}

// DialFunc is a function which can be used to establish the network connection.
// Custom dial functions must be registered with RegisterDial
type DialFunc func(addr string) (net.Conn, error)

var dials map[string]DialFunc

// RegisterDial registers a custom dial function. It can then be used by the
// network address mynet(addr), where mynet is the registered new network.
// addr is passed as a parameter to the dial function.
func RegisterDial(net string, dial DialFunc) {
	if dials == nil {
		dials = make(map[string]DialFunc)
	}
	dials[net] = dial
}

func (d XDriver) Open(dsn string) (driver.Conn, error) {
	var err error

	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.useXProtocol = true // force X protocol as this driver was called explicitly

	// New mysqlConn
	mc := &mysqlXConn{
		capabilities:     NewServerCapabilities(),
		cfg:              NewXconfigFromConfig(cfg),
		maxPacketAllowed: maxPacketSize,
		maxWriteSize:     maxPacketSize - 1,
	}
	return mc.Open2()
}

func init() {
	sql.Register("mysql/xprotocol", &XDriver{})
}
