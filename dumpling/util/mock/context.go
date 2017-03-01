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

// Package mock is just for test only.
package mock

import (
	"fmt"
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
)

var _ context.Context = (*Context)(nil)

// Context represents mocked context.Context.
type Context struct {
	values map[fmt.Stringer]interface{}
	// mock global variable
	txn         kv.Transaction
	Store       kv.Storage
	sessionVars *variable.SessionVars
	// Fix data race in ddl test.
	mux sync.Mutex
}

// SetValue implements context.Context SetValue interface.
func (c *Context) SetValue(key fmt.Stringer, value interface{}) {
	c.values[key] = value
}

// Value implements context.Context Value interface.
func (c *Context) Value(key fmt.Stringer) interface{} {
	value := c.values[key]
	return value
}

// ClearValue implements context.Context ClearValue interface.
func (c *Context) ClearValue(key fmt.Stringer) {
	delete(c.values, key)
}

// GetSessionVars implements the context.Context GetSessionVars interface.
func (c *Context) GetSessionVars() *variable.SessionVars {
	return c.sessionVars
}

// Txn implements context.Context Txn interface.
func (c *Context) Txn() kv.Transaction {
	return c.txn
}

// GetClient implements context.Context GetClient interface.
func (c *Context) GetClient() kv.Client {
	if c.Store == nil {
		return nil
	}
	return c.Store.GetClient()
}

// GetGlobalSysVar implements GlobalVarAccessor GetGlobalSysVar interface.
func (c *Context) GetGlobalSysVar(ctx context.Context, name string) (string, error) {
	v := variable.GetSysVar(name)
	if v == nil {
		return "", variable.UnknownSystemVar.GenByArgs(name)
	}
	return v.Value, nil
}

// SetGlobalSysVar implements GlobalVarAccessor SetGlobalSysVar interface.
func (c *Context) SetGlobalSysVar(ctx context.Context, name string, value string) error {
	v := variable.GetSysVar(name)
	if v == nil {
		return variable.UnknownSystemVar.GenByArgs(name)
	}
	v.Value = value
	return nil
}

// NewTxn implements the context.Context interface.
func (c *Context) NewTxn() error {
	if c.Store == nil {
		return errors.New("store is not set")
	}
	if c.txn != nil && c.txn.Valid() {
		err := c.txn.Commit()
		if err != nil {
			return errors.Trace(err)
		}
	}
	txn, err := c.Store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	c.txn = txn
	return nil
}

// ActivePendingTxn implements the context.Context interface.
func (c *Context) ActivePendingTxn() error {
	if c.txn != nil {
		return nil
	}
	if c.Store != nil {
		txn, err := c.Store.Begin()
		if err != nil {
			return errors.Trace(err)
		}
		c.txn = txn
	}
	return nil
}

// InitTxnWithStartTS implements the context.Context interface with startTS.
func (c *Context) InitTxnWithStartTS(startTS uint64) error {
	if c.txn != nil {
		return nil
	}
	if c.Store != nil {
		txn, err := c.Store.BeginWithStartTS(startTS)
		if err != nil {
			return errors.Trace(err)
		}
		c.txn = txn
	}
	return nil
}

// GetSessionManager implements the context.Context interface.
func (c *Context) GetSessionManager() util.SessionManager {
	return nil
}

// Cancel implements the Session interface.
func (c *Context) Cancel() {
}

// Done implements the context.Context interface.
func (c *Context) Done() <-chan struct{} {
	return nil
}

// NewContext creates a new mocked context.Context.
func NewContext() *Context {
	return &Context{
		values:      make(map[fmt.Stringer]interface{}),
		sessionVars: variable.NewSessionVars(),
	}
}
