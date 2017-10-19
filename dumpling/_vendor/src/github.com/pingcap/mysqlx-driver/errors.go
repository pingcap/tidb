// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"github.com/juju/errors"
)

// Various errors the driver might return. Can change between driver versions.
var (
	ErrInvalidConn       = errors.New("Invalid Connection")
	ErrMalformPkt        = errors.New("Malformed Packet")
	ErrNoTLS             = errors.New("TLS encryption requested but server does not support TLS")
	ErrOldPassword       = errors.New("This user requires old password authentication. If you still want to use it, please add 'allowOldPasswords=1' to your DSN. See also https://github.com/go-sql-driver/mysql/wiki/old_passwords")
	ErrCleartextPassword = errors.New("This user requires clear text authentication. If you still want to use it, please add 'allowCleartextPasswords=1' to your DSN")
	ErrUnknownPlugin     = errors.New("The authentication plugin is not supported")
	ErrOldProtocol       = errors.New("MySQL-Server does not support required Protocol 41+")
	ErrPktSync           = errors.New("Commands out of sync. You can't run this command now")
	ErrPktSyncMul        = errors.New("Commands out of sync. Did you run multiple statements at once?")
	ErrPktTooLarge       = errors.New("Packet for query is too large. You can change this value on the server by adjusting the 'max_allowed_packet' variable")
	ErrBusyBuffer        = errors.New("Busy buffer")
)
