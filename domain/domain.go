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
	"encoding/json"

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
	store      kv.Storage
	infoHandle *infoschema.Handle
	ddl        ddl.DDL
	meta       *meta.Meta
}

func (do *Domain) loadInfoSchema(txn *meta.TMeta) (err error) {
	res, err := txn.ListDatabases()
	if err != nil {
		return errors.Trace(err)
	}

	schemas := make([]*model.DBInfo, 0, len(res))
	for _, value := range res {
		di := &model.DBInfo{}
		err = json.Unmarshal(value, di)
		if err != nil {
			log.Fatal(err)
		}
		schemas = append(schemas, di)
	}

	for _, di := range schemas {
		res, err = txn.ListTables(di.ID)
		if err != nil {
			return errors.Trace(err)
		}

		tables := make([]*model.TableInfo, 0, len(res))
		for _, value := range res {
			ti := &model.TableInfo{}
			err = json.Unmarshal(value, ti)
			if err != nil {
				log.Fatal(err)
			}
			tables = append(tables, ti)
		}
		di.Tables = tables
	}

	schemaMetaVersion, err := txn.GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
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

// NewDomain creates a new domain.
func NewDomain(store kv.Storage) (d *Domain, err error) {
	d = &Domain{
		store: store,
		meta:  meta.NewMeta(store),
	}

	d.infoHandle = infoschema.NewHandle(d.store)
	d.ddl = ddl.NewDDL(d.store, d.infoHandle, d.onDDLChange)
	err = d.reload()
	return d, errors.Trace(err)
}
