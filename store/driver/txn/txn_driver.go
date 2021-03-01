package txn

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
)

type tikvTxn struct {
	*tikv.KVTxn
}

// NewTiKVTxn returns a new Transaction.
func NewTiKVTxn(txn *tikv.KVTxn) kv.Transaction {
	return &tikvTxn{txn}
}
