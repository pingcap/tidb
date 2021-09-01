// Copyright 2021 PingCAP, Inc.
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

package common

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// connPool is a lazy pool of gRPC channels.
// When `Get` called, it lazily allocates new connection if connection not full.
// If it's full, then it will return allocated channels round-robin.
type ConnPool struct {
	mu sync.Mutex

	conns   []*grpc.ClientConn
	next    int
	cap     int
	newConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (p *ConnPool) TakeConns() (conns []*grpc.ClientConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns, conns = nil, p.conns
	p.next = 0
	return conns
}

// Close closes the conn pool.
func (p *ConnPool) Close() {
	for _, c := range p.TakeConns() {
		if err := c.Close(); err != nil {
			log.L().Warn("failed to close clientConn", zap.String("target", c.Target()), log.ShortError(err))
		}
	}
}

// get tries to get an existing connection from the pool, or make a new one if the pool not full.
func (p *ConnPool) get(ctx context.Context) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.conns) < p.cap {
		c, err := p.newConn(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		p.conns = append(p.conns, c)
		return c, nil
	}

	conn := p.conns[p.next]
	p.next = (p.next + 1) % p.cap
	return conn, nil
}

// newConnPool creates a new connPool by the specified conn factory function and capacity.
func NewConnPool(cap int, newConn func(ctx context.Context) (*grpc.ClientConn, error)) *ConnPool {
	return &ConnPool{
		cap:     cap,
		conns:   make([]*grpc.ClientConn, 0, cap),
		newConn: newConn,

		mu: sync.Mutex{},
	}
}

type GRPCConns struct {
	mu    sync.Mutex
	conns map[uint64]*ConnPool
}

func (conns *GRPCConns) Close() {
	conns.mu.Lock()
	defer conns.mu.Unlock()

	for _, cp := range conns.conns {
		cp.Close()
	}
}

func (conns *GRPCConns) GetGrpcConn(ctx context.Context, storeID uint64, tcpConcurrency int, newConn func(ctx context.Context) (*grpc.ClientConn, error)) (*grpc.ClientConn, error) {
	conns.mu.Lock()
	defer conns.mu.Unlock()
	if _, ok := conns.conns[storeID]; !ok {
		conns.conns[storeID] = NewConnPool(tcpConcurrency, newConn)
	}
	return conns.conns[storeID].get(ctx)
}

func NewGRPCConns() GRPCConns {
	cons := GRPCConns{conns: make(map[uint64]*ConnPool)}
	return cons
}
