// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
)

const (
	// SchemaMetaPrefix is the prefix for database meta key prefix.
	SchemaMetaPrefix = "mDB:"
	// TableMetaPrefix is the prefix for table meta key prefix.
	TableMetaPrefix = "mTable:"
)

var (
	nextGlobalIDPrefix = []byte("mNextGlobalID")
	// SchemaMetaVersion is used as lock for changing schema
	SchemaMetaVersion = []byte("mSchemaVersion")
)

// GenID adds step to the value for key and returns the sum.
func GenID(txn kv.Transaction, key []byte, step int) (int64, error) {
	if len(key) == 0 {
		return 0, errors.New("Invalid key")
	}
	err := txn.LockKeys(key)
	if err != nil {
		return 0, err
	}
	id, err := txn.Inc(key, int64(step))
	if err != nil {
		return 0, errors.Trace(err)
	}

	return id, errors.Trace(err)
}

// DBMetaKey generates database meta key according to databaseID.
func DBMetaKey(databaseID int64) string {
	return fmt.Sprintf("%s:%d", SchemaMetaPrefix, databaseID)
}

// AutoIDKey generates table autoID meta key according to tableID.
func AutoIDKey(tableID int64) string {
	if tableID == 0 {
		log.Error("Invalid tableID")
	}
	return fmt.Sprintf("%s:%d_autoID", TableMetaPrefix, tableID)
}

// GenGlobalID generates the next id in the store scope.
func GenGlobalID(store kv.Storage) (ID int64, err error) {
	err = kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
		ID, err = GenID(txn, []byte(nextGlobalIDPrefix), 1)
		if err != nil {
			return errors.Trace(err)
		}

		log.Info("Generate global id", ID)

		return nil
	})

	return
}
