package resourcemanage

import "github.com/pingcap/tidb/resourcemanage/util"

func downclock(pool *util.PoolContainer) {
	capa := pool.Pool.Cap()
	pool.Pool.Tune(capa + 1)
}

func overclock(pool *util.PoolContainer) {
	capa := pool.Pool.Cap()
	pool.Pool.Tune(capa - 1)
}
