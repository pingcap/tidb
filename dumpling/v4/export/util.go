package export

import (
	"context"
	"database/sql"
	"sort"
	"strings"
	"time"

	"go.etcd.io/etcd/clientv3"
)

const tidbServerInformationPath = "/tidb/server/info"

func GetPdDDLIDs(pCtx context.Context, cli *clientv3.Client) ([]string, error) {
	ctx, cancel := context.WithTimeout(pCtx, 10*time.Second)
	defer cancel()

	var pdDDLIds []string
	resp, err := cli.Get(ctx, tidbServerInformationPath, clientv3.WithPrefix())
	if err != nil {
		return pdDDLIds, err
	}
	for _, kv := range resp.Kvs {
		items := strings.Split(string(kv.Key), "/")
		pdDDLIds = append(pdDDLIds, items[len(items)-1])
	}
	return pdDDLIds, nil
}

func checkSameCluster(ctx context.Context, db *sql.DB, pdAddrs []string) (bool, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   pdAddrs,
		DialTimeout: defaultEtcdDialTimeOut,
	})
	if err != nil {
		return false, err
	}
	tidbDDLIDs, err := GetTiDBDDLIDs(db)
	if err != nil {
		return false, err
	}
	pdDDLIDs, err := GetPdDDLIDs(ctx, cli)
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
