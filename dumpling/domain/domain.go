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

package domain

import (
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
)

// Domain represents a storage space. Different domains can use the same database name.
// Multiple domains can be used in parallel without synchronization.
type Domain struct {
	store       kv.Storage
	infoHandle  *infoschema.Handle
	ddl         ddl.DDL
	leaseCh     chan time.Duration
	lastLeaseTS int64
}

func (do *Domain) loadInfoSchema(txn kv.Transaction) (err error) {
	m := meta.NewMeta(txn)
	schemaMetaVersion, err := m.GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	info := do.infoHandle.Get()
	if info != nil && schemaMetaVersion > 0 && schemaMetaVersion == info.SchemaMetaVersion() {
		log.Debugf("schema version is still %d, no need reload", schemaMetaVersion)
		return nil
	}

	schemas, err := m.ListDatabases()
	if err != nil {
		return errors.Trace(err)
	}

	for _, di := range schemas {
		if di.State != model.StatePublic {
			// schema is not public, can't be used outside.
			continue
		}

		tables, err := m.ListTables(di.ID)
		if err != nil {
			return errors.Trace(err)
		}

		di.Tables = make([]*model.TableInfo, 0, len(tables))
		for _, tbl := range tables {
			if tbl.State != model.StatePublic {
				// schema is not public, can't be used outsiee.
				continue
			}
			di.Tables = append(di.Tables, tbl)
		}
	}

	log.Infof("loadInfoSchema %d", schemaMetaVersion)
	do.infoHandle.Set(schemas, schemaMetaVersion)
	return
}

// InfoSchema gets information schema from domain.
func (do *Domain) InfoSchema() infoschema.InfoSchema {
	return do.infoHandle.Get()
}

// DDL gets DDL from domain.
func (do *Domain) DDL() ddl.DDL {
	return do.ddl
}

// Store gets KV store from domain.
func (do *Domain) Store() kv.Storage {
	return do.store
}

// SetLease will reset the lease time for online DDL change.
func (do *Domain) SetLease(lease time.Duration) {
	do.leaseCh <- lease

	// let ddl to reset lease too.
	do.ddl.SetLease(lease)
}

// Stat returns the DDL statistic.
func (do *Domain) Stat() (map[string]interface{}, error) {
	m, err := do.ddl.Stat()
	if err != nil {
		return nil, errors.Trace(err)
	}

	m["DDL_last_reload_schema_ts"] = atomic.LoadInt64(&do.lastLeaseTS)

	return m, nil
}

func (do *Domain) onDDLChange(err error) error {
	if err != nil {
		return err
	}
	log.Warnf("on DDL change")

	return do.reload()
}

func (do *Domain) reload() error {
	err := kv.RunInNewTxn(do.store, false, do.loadInfoSchema)
	return errors.Trace(err)
}

// check schema every 300 seconds default.
const defaultLoadTime = 300 * time.Second

func (do *Domain) loadSchemaInLoop(lease time.Duration) {
	if lease <= 0 {
		lease = defaultLoadTime
	}

	ticker := time.NewTicker(lease)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := do.reload(); err != nil {
				log.Fatalf("reload schema err %v", err)
			}
			atomic.StoreInt64(&do.lastLeaseTS, time.Now().Unix())
		case newLease := <-do.leaseCh:
			if newLease <= 0 {
				newLease = defaultLoadTime
			}

			if lease == newLease {
				// nothing to do
				continue
			}

			lease = newLease
			// reset ticker too.
			ticker.Stop()
			ticker = time.NewTicker(lease)
		}
	}
}

// NewDomain creates a new domain.
func NewDomain(store kv.Storage, lease time.Duration) (d *Domain, err error) {
	d = &Domain{
		store:   store,
		leaseCh: make(chan time.Duration, 1),
	}

	d.infoHandle = infoschema.NewHandle(d.store)
	d.ddl = ddl.NewDDL(d.store, d.infoHandle, d.onDDLChange, lease)
	err = d.reload()
	if err != nil {
		log.Fatalf("load schema err %v", err)
	}

	go d.loadSchemaInLoop(lease)

	return d, nil
}
