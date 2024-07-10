// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	clientv3 "go.etcd.io/etcd/client/v3"
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
		Endpoints:        pdAddrs,
		DialTimeout:      defaultEtcdDialTimeOut,
		AutoSyncInterval: 30 * time.Second,
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
	slices.Sort(tidbDDLIDs)
	slices.Sort(pdDDLIDs)

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
	return consistency != ConsistencyTypeSnapshot || serverType != version.ServerTypeTiDB
}

func infiniteChan[T any]() (chan<- T, <-chan T) {
	in, out := make(chan T), make(chan T)

	go func() {
		var (
			q  []T
			e  T
			ok bool
		)
		handleRead := func() bool {
			if !ok {
				for _, e = range q {
					out <- e
				}
				close(out)
				return true
			}
			q = append(q, e)
			return false
		}
		for {
			if len(q) > 0 {
				select {
				case e, ok = <-in:
					if handleRead() {
						return
					}
				case out <- q[0]:
					q = q[1:]
				}
			} else {
				e, ok = <-in
				if handleRead() {
					return
				}
			}
		}
	}()
	return in, out
}
