// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.
//
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

package tikv

import (
	"github.com/juju/errors"
	"google.golang.org/grpc"
)

// Conn is a simple wrapper of grpc.ClientConn.
type Conn struct {
	*grpc.ClientConn
	addr   string
	closed bool
}

// NewConnection creates a Conn with dial timeout.
func NewConnection(addr string) (*Conn, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Conn{
		ClientConn: conn,
		addr:       addr,
		closed:     false,
	}, nil
}

// Close closes the grpc.ClientConn.
func (c *Conn) Close() {
	if c.closed {
		return
	}
	c.closed = true
	if c.ClientConn != nil {
		c.ClientConn.Close()
		c.ClientConn = nil
	}
}
