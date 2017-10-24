// A Go driver for the MySQL X protocol for Go's database/sql package
// Based heavily on:
// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2014 The Go-MySQL-Driver Authors. All rights reserved.
// Copyright 2016 Simon J Mudd.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"crypto/tls"
	"time"
)

type config struct {
	user                    string
	passwd                  string
	net                     string
	addr                    string
	dbname                  string
	params                  map[string]string
	loc                     *time.Location
	tls                     *tls.Config
	timeout                 time.Duration
	collation               uint8
	allowAllFiles           bool
	allowOldPasswords       bool
	allowCleartextPasswords bool
	columnsWithAlias        bool
	interpolateParams       bool
	useXProtocol            bool // use X protocol rather than native protocol
}
