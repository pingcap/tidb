package export

import (
	"context"
	"database/sql"
)

type connectionsPool struct {
	conns chan *sql.Conn
}

func newConnectionsPool(ctx context.Context, n int, pool *sql.DB) (*connectionsPool, error) {
	connectPool := &connectionsPool{
		conns: make(chan *sql.Conn, n),
	}
	for i := 0; i < n; i++ {
		conn, err := createConnWithConsistency(ctx, pool)
		if err != nil {
			return nil, err
		}
		connectPool.releaseConn(conn)
	}
	return connectPool, nil
}

func (r *connectionsPool) getConn() *sql.Conn {
	return <-r.conns
}

func (r *connectionsPool) releaseConn(conn *sql.Conn) {
	select {
	case r.conns <- conn:
	default:
		panic("put a redundant conn")
	}
}
