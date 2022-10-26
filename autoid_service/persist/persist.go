package persist

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/juju/errors"
	autoid "github.com/pingcap/tidb/autoid_service"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store"
)

var _ autoid.Persist = &tikvPersist{}

func NewPersist(tikvPath string) (*tikvPersist, []string, error) {
	fullPath := fmt.Sprintf("tikv://%s", tikvPath)
	store, err := store.New(fullPath)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	ebd, ok := store.(kv.EtcdBackend)
	if !ok {
		return nil, nil, nil
	}
	etcdAddr, err := ebd.EtcdAddrs()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return &tikvPersist{store}, etcdAddr, nil
}

type tikvPersist struct {
	kv.Storage
}

func (t *tikvPersist) SyncID(ctx context.Context, dbID, tblID int64, val uint64, addr *int64, done <-chan struct{}) error {
	err := kv.RunInNewTxn(ctx, t.Storage, true, func(ctx context.Context, txn kv.Transaction) error {
		idAcc := meta.NewMeta(txn).GetAutoIDAccessors(dbID, tblID).RowID()
		return idAcc.Put(int64(val))
	})
	atomic.StoreInt64(addr, int64(val))
	<-done
	return errors.Trace(err)
}

func (t *tikvPersist) LoadID(ctx context.Context, dbID, tblID int64) (uint64, error) {
	var res int64
	err := kv.RunInNewTxn(ctx, t.Storage, true, func(ctx context.Context, txn kv.Transaction) error {
		var err1 error
		idAcc := meta.NewMeta(txn).GetAutoIDAccessors(dbID, tblID).RowID()
		res, err1 = idAcc.Get()
		return err1
	})
	if err != nil {
		return 0, errors.Trace(err)
	}
	return uint64(res), nil
}
