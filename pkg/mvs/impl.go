package utils

import (
	"context"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	meta "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	basic "github.com/pingcap/tidb/pkg/util"
)

type serverHelper struct {
}

func (m *serverHelper) serverFilter(s serverInfo) bool {
	return true
}

func (m *serverHelper) getServerInfo() (serverInfo, error) {
	localSrv, err := infosync.GetServerInfo()
	if err != nil {
		return serverInfo{}, err
	}
	return serverInfo{
		ID: localSrv.ID,
	}, nil
}

func (m *serverHelper) getAllServerInfo(ctx context.Context) (map[string]serverInfo, error) {
	servers := make(map[string]serverInfo)
	allServers, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		return nil, err
	}
	for _, srv := range allServers {
		servers[srv.ID] = serverInfo{
			ID: srv.ID,
		}
	}
	return servers, nil
}

func (*serverHelper) RefreshMV(_ context.Context, _ string) (relatedMVLog []string, nextRefresh time.Time, err error) {
	return nil, time.Time{}, ErrMVRefreshHandlerNotRegistered
}

func (*serverHelper) PurgeMVLog(_ context.Context, _ string) (nextPurge time.Time, err error) {
	return time.Time{}, ErrMVLogPurgeHandlerNotRegistered
}

var mvs *MVService

// RegisterMVS registers a DDL event handler for MV-related events.
func RegisterMVS(ddlNotifier *notifier.DDLNotifier, se basic.SessionPool) {
	if ddlNotifier == nil {
		return
	}

	mvs = NewMVJobsManager(se, &serverHelper{})

	ddlNotifier.RegisterHandler(notifier.MVJobsHandlerID, func(_ context.Context, _ sessionctx.Context, event *notifier.SchemaChangeEvent) error {
		switch event.GetType() {
		case meta.ActionCreateMaterializedViewLog:
			mvs.ddlDirty.Store(true)
			mvs.notifier.Wake()
		}
		return nil
	})

	mvs.Start()
}
