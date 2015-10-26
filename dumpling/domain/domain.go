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
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
)

// Domain represents a storage space. Different domains can use the same database name.
// Multiple domains can be used in parallel without synchronization.
type Domain struct {
	store      kv.Storage
	infoHandle *infoschema.Handle
	ddl        ddl.DDL
	meta       *meta.Meta
	lease      int
}

func (do *Domain) loadInfoSchema(m *meta.TMeta) (err error) {
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
		tables, err := m.ListTables(di.ID)
		if err != nil {
			return errors.Trace(err)
		}

		di.Tables = tables
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

func (do *Domain) onDDLChange(err error) error {
	if err != nil {
		return err
	}
	log.Warnf("on DDL change")

	return do.reload()
}

func (do *Domain) reload() error {
	err := do.meta.RunInNewTxn(false, do.loadInfoSchema)
	return errors.Trace(err)
}

func (do *Domain) loadSchemaInLoop() {
	if do.lease <= 0 {
		return
	}

	ticker := time.NewTicker(time.Duration(do.lease) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := do.reload(); err != nil {
				log.Fatalf("reload schema err %v", err)
			}
		}
	}
}

// NewDomain creates a new domain.
func NewDomain(store kv.Storage, lease int) (d *Domain, err error) {
	d = &Domain{
		store: store,
		meta:  meta.NewMeta(store),
		lease: lease,
	}

	d.infoHandle = infoschema.NewHandle(d.store)
	d.ddl = ddl.NewDDL(d.store, d.infoHandle, d.onDDLChange, lease)
	err = d.reload()
	if err != nil {
		log.Fatalf("load schema err %v", err)
	}

	go d.loadSchemaInLoop()

	return d, nil
}
