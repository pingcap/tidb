// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"database/sql"
)

type connectionsPool struct {
	conns        chan *sql.Conn
	createdConns []*sql.Conn
}

func newConnectionsPool(ctx context.Context, n int, pool *sql.DB) (*connectionsPool, error) {
	connectPool := &connectionsPool{
		conns:        make(chan *sql.Conn, n),
		createdConns: make([]*sql.Conn, 0, n+1),
	}
	for i := 0; i < n+1; i++ {
		conn, err := createConnWithConsistency(ctx, pool)
		if err != nil {
			connectPool.Close()
			return connectPool, err
		}
		if i != n {
			connectPool.releaseConn(conn)
		}
		connectPool.createdConns = append(connectPool.createdConns, conn)
	}
	return connectPool, nil
}

func (r *connectionsPool) getConn() *sql.Conn {
	return <-r.conns
}

func (r *connectionsPool) extraConn() *sql.Conn {
	return r.createdConns[len(r.createdConns)-1]
}

func (r *connectionsPool) Close() error {
	var err error
	for _, conn := range r.createdConns {
		err2 := conn.Close()
		if err2 != nil {
			err = err2
		}
	}
	return err
}

func (r *connectionsPool) releaseConn(conn *sql.Conn) {
	select {
	case r.conns <- conn:
	default:
		panic("put a redundant conn")
	}
}
