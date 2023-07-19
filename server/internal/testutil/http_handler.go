package testutil

import (
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/server"
)

type BasicHTTPHandlerTestSuite struct {
	*testServerClient
	server  *server.Server
	store   kv.Storage
	domain  *domain.Domain
	tidbdrv *server.TiDBDriver
	sh      *server.StatsHandler
}

func CreateBasicHTTPHandlerTestSuite() *BasicHTTPHandlerTestSuite {
	ts := &BasicHTTPHandlerTestSuite{}
	ts.testServerClient = newTestServerClient()
	return ts
}
