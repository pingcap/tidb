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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/terror"
)

var ddlLastReloadSchemaTS = "ddl_last_reload_schema_ts"

// Domain represents a storage space. Different domains can use the same database name.
// Multiple domains can be used in parallel without synchronization.
type Domain struct {
	store      kv.Storage
	infoHandle *infoschema.Handle
	ddl        ddl.DDL
	leaseCh    chan time.Duration
	// nano seconds
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
	// try reload if possible.
	do.tryReload()
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

// Stats returns the domain statistic.
func (do *Domain) Stats() (map[string]interface{}, error) {
	m := make(map[string]interface{})
	m[ddlLastReloadSchemaTS] = atomic.LoadInt64(&do.lastLeaseTS) / 1e9

	return m, nil
}

// GetScope gets the status variables scope.
func (do *Domain) GetScope(status string) variable.ScopeFlag {
	// Now domain status variables scope are all default scope.
	return variable.DefaultScopeFlag
}

func (do *Domain) tryReload() {
	// if we don't have update the schema for a long time > lease, we must force reloading it.
	// Although we try to reload schema every lease time in a goroutine, sometimes it may not
	// run accurately, e.g, the machine has a very high load, running the ticker is delayed.
	last := atomic.LoadInt64(&do.lastLeaseTS)
	lease := do.ddl.GetLease()

	// if lease is 0, we use the local store, so no need to reload.
	if lease > 0 && time.Now().UnixNano()-last > lease.Nanoseconds() {
		do.mustReload()
	}
}

func (do *Domain) reload() error {
	err := kv.RunInNewTxn(do.store, false, do.loadInfoSchema)
	if err != nil {
		return errors.Trace(err)
	}

	atomic.StoreInt64(&do.lastLeaseTS, time.Now().UnixNano())
	return nil
}

func (do *Domain) mustReload() {
	// if reload error, we will terminate whole program to guarantee data safe.
	// TODO: retry some times if reload error.
	err := do.reload()
	if err != nil {
		log.Fatalf("reload schema err %v", err)
	}
}

const maxReloadTimeout = 60 * time.Second
const minReloadTimeout = 20 * time.Second

func getReloadTimeout(lease time.Duration) time.Duration {
	timeout := lease / 4

	if timeout >= maxReloadTimeout {
		return maxReloadTimeout
	}
	if timeout <= minReloadTimeout {
		return minReloadTimeout
	}

	return timeout
}

// check schema every 300 seconds default.
const defaultLoadTime = 300 * time.Second

func (do *Domain) loadSchemaInLoop(lease time.Duration) {
	if lease <= 0 {
		lease = defaultLoadTime
	}

	ticker := time.NewTicker(lease)
	defer ticker.Stop()

	reloadTimeout := getReloadTimeout(lease)
	reloadErrCh := make(chan error, 1)

	for {
		select {
		case <-ticker.C:
			go func() {
				reloadErrCh <- do.reload()
			}()
			select {
			case err := <-reloadErrCh:
				// we may close store in test, but the domain load schema loop is still checking,
				// so we can't panic for ErrDBClosed and just return here.
				if terror.ErrorEqual(err, localstore.ErrDBClosed) {
					return
				} else if err != nil {
					log.Fatalf("reload schema err %v", errors.ErrorStack(err))
				}
			case <-time.After(reloadTimeout):
				log.Fatalf("reload schema timeout:%d", reloadTimeout)
			}
		case newLease := <-do.leaseCh:
			if newLease <= 0 {
				newLease = defaultLoadTime
			}

			if lease == newLease {
				// nothing to do
				continue
			}

			lease = newLease
			reloadTimeout = getReloadTimeout(lease)
			// reset ticker too.
			ticker.Stop()
			ticker = time.NewTicker(lease)
		}
	}
}

type ddlCallback struct {
	ddl.BaseCallback
	do *Domain
}

func (c *ddlCallback) OnChanged(err error) error {
	if err != nil {
		return err
	}
	log.Warnf("on DDL change")

	c.do.mustReload()
	return nil
}

// NewDomain creates a new domain.
func NewDomain(store kv.Storage, lease time.Duration) (d *Domain, err error) {
	d = &Domain{
		store:   store,
		leaseCh: make(chan time.Duration, 1),
	}

	d.infoHandle = infoschema.NewHandle(d.store)
	d.ddl = ddl.NewDDL(d.store, d.infoHandle, &ddlCallback{do: d}, lease)
	d.mustReload()

	variable.RegisterStatistics(d)

	go d.loadSchemaInLoop(lease)

	return d, nil
}
