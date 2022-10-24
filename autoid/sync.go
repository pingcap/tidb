package autoid

import (
	"context"
	"fmt"
	"sync/atomic"
	"strconv"
	"runtime/debug"

	"github.com/pingcap/tidb/kv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/meta"
)

type persist interface {
	syncID(ctx context.Context, dbID, tblID int64, val uint64, addr *int64, done <-chan struct{}) error
	loadID(ctx context.Context, dbID, tblID int64) (uint64, error)
}

type mockPersist struct {
	data map[autoIDKey]uint64
}

func (p *mockPersist) syncID(ctx context.Context, dbID, tblID int64, val uint64, addr *int64, done <-chan struct{}) error {
	key := autoIDKey{dbID: dbID, tblID: tblID}
	p.data[key] = val
	atomic.StoreInt64(addr, int64(val))
	<-done
	return nil
}

func (p *mockPersist) loadID(ctx context.Context, dbID, tblID int64) (uint64, error) {
	key := autoIDKey{dbID: dbID, tblID: tblID}
	return p.data[key], nil
}

const (
	autoIDKeyPattern = "tidb/autoid/db/%d/tbl/%d"
)

type etcdPersist struct {
	cli      *clientv3.Client
}

func (p *etcdPersist) syncID(ctx context.Context, dbID, tblID int64, val uint64, addr *int64, done <-chan struct{}) error {
	key := fmt.Sprintf(autoIDKeyPattern, dbID, tblID)
	fmt.Println("before syncID... put", key, val)
	_, err := p.cli.Put(ctx, key, strconv.FormatUint(val, 10))
	fmt.Println("after syncID... put", key, val)
	atomic.StoreInt64(addr, int64(val))
	<-done
	debug.PrintStack()
	if err != nil {
		fmt.Println("sync id error ==", err)
		return err
	}
	return nil
}

func (p *etcdPersist) loadID(ctx context.Context, dbID, tblID int64) (uint64, error) {
	key := fmt.Sprintf(autoIDKeyPattern, dbID, tblID)
	resp, err := p.cli.Get(ctx, key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	fmt.Println("load id get response ==", resp)
	if len(resp.Kvs) == 0 {
		fmt.Println(" LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLoad get empty response!!!!!!")
		return 0, nil
	}

	valStr := resp.Kvs[0].Value
	if len(valStr) == 0 {
		return 0, errors.New("invalid data")
	}
	val, err :=  strconv.ParseUint(string(valStr), 10, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

type tikvPersist struct {
	kv.Storage
}

func (t tikvPersist) syncID(ctx context.Context, dbID, tblID int64, val uint64, addr *int64, done <-chan struct{}) error {
	err := kv.RunInNewTxn(ctx, t.Storage, true, func(ctx context.Context, txn kv.Transaction) error {
		idAcc := meta.NewMeta(txn).GetAutoIDAccessors(dbID, tblID).RowID()
		return idAcc.Put(int64(val))
	})
	atomic.StoreInt64(addr, int64(val))
	<-done
	return errors.Trace(err)
}

func (t tikvPersist) loadID(ctx context.Context, dbID, tblID int64) (uint64, error) {
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
