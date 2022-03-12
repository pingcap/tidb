// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"database/sql"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
)

const tidbServerInformationPath = "/tidb/server/info"

func getPdDDLIDs(pCtx context.Context, cli *clientv3.Client) ([]string, error) {
	ctx, cancel := context.WithTimeout(pCtx, 10*time.Second)
	defer cancel()

	resp, err := cli.Get(ctx, tidbServerInformationPath, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Trace(err)
	}
	pdDDLIds := make([]string, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		items := strings.Split(string(kv.Key), "/")
		pdDDLIds[i] = items[len(items)-1]
	}
	return pdDDLIds, nil
}

func checkSameCluster(tctx *tcontext.Context, db *sql.DB, pdAddrs []string) (bool, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   pdAddrs,
		DialTimeout: defaultEtcdDialTimeOut,
	})
	if err != nil {
		return false, errors.Trace(err)
	}
	tidbDDLIDs, err := GetTiDBDDLIDs(tctx, db)
	if err != nil {
		return false, err
	}
	pdDDLIDs, err := getPdDDLIDs(tctx, cli)
	if err != nil {
		return false, err
	}
	sort.Strings(tidbDDLIDs)
	sort.Strings(pdDDLIDs)

	return sameStringArray(tidbDDLIDs, pdDDLIDs), nil
}

func sameStringArray(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func string2Map(a, b []string) map[string]string {
	a2b := make(map[string]string, len(a))
	for i, str := range a {
		a2b[str] = b[i]
	}
	return a2b
}

func needRepeatableRead(serverType version.ServerType, consistency string) bool {
	return consistency != consistencyTypeSnapshot || serverType != version.ServerTypeTiDB
}
