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
	"sync"

	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/juju/errors"
	"google.golang.org/grpc"
)

// Conn is a simple wrapper of grpc.ClientConn.
type Conn struct {
	c *grpc.ClientConn
}

// ConnMap is a map that maintains the mapping from addresses to their connections.
// TODO: Implement background cleanup. It adds a backgroud goroutine to periodically check
// whether there is any connection is idle and then close and remove these idle connections.
type ConnMap struct {
	m struct {
		sync.RWMutex
		isClosed bool
		conns    map[string]*Conn
	}
}

// NewConnMap creates a ConnMap.
func NewConnMap() *ConnMap {
	p := new(ConnMap)
	p.m.conns = make(map[string]*Conn)
	return p
}

// Get takes a grpc.ClientConn out of the map by the specific addr.
func (p *ConnMap) Get(addr string) (*grpc.ClientConn, error) {
	p.m.RLock()
	if p.m.isClosed {
		p.m.RUnlock()
		return nil, errors.Errorf("ConnMap is closed")
	}
	conn, ok := p.m.conns[addr]
	p.m.RUnlock()
	if !ok {
		var err error
		conn, err = p.tryCreate(addr)
		if err != nil {
			return nil, err
		}
	}
	// TODO: increase refCount for conn.
	return conn.c, nil
}

func (p *ConnMap) tryCreate(addr string) (*Conn, error) {
	p.m.Lock()
	defer p.m.Unlock()
	conn, ok := p.m.conns[addr]
	if !ok {
		c, err := grpc.Dial(
			addr,
			grpc.WithInsecure(),
			grpc.WithTimeout(dialTimeout),
			grpc.WithUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
			grpc.WithStreamInterceptor(grpc_prometheus.StreamClientInterceptor))
		if err != nil {
			return nil, errors.Trace(err)
		}
		conn = &Conn{
			c: c,
		}
		p.m.conns[addr] = conn
	}
	return conn, nil
}

// Close closes the map.
func (p *ConnMap) Close() {
	p.m.Lock()
	if !p.m.isClosed {
		p.m.isClosed = true
		// close all connections
		for _, conn := range p.m.conns {
			conn.c.Close()
		}
	}
	p.m.Unlock()
}
