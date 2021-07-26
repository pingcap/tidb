// Copyright 2019 PingCAP, Inc.
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

package main

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx/variable"
)

// Accumulator of connection
// It can be increased by 1 after each OnConnectionEvent is triggered.
var connection int32

// Validate implements TiDB plugin's Validate SPI.
// It is called before OnInit
func Validate(ctx context.Context, m *plugin.Manifest) error {
	fmt.Println("## conn_ip_example Validate called ##")
	fmt.Printf("---- context: %s\n", ctx)
	return nil
}

// OnInit implements TiDB plugin's OnInit SPI.
func OnInit(ctx context.Context, manifest *plugin.Manifest) error {
	fmt.Println("## conn_ip_example OnInit called ##")
	fmt.Printf("---- context: %s\n", ctx)

	// Register an example system variable
	// With the server.
	sv := &variable.SysVar{
		Name:  "conn_ip_example_key",
		Scope: variable.ScopeGlobal | variable.ScopeSession,
		Value: "v1",
		Type:  variable.TypeStr, // default.
		// (Optional) specifying a validation function helps to normalize the value before setting it.
		// The "normalizedValue" applies if the value has a Type associated, where some formatting may have already
		// been applied. i.e. TypeBool: ON/oN/1/on -> ON
		Validation: func(vars *variable.SessionVars, normalizedValue string, originalValue string, scope variable.ScopeFlag) (string, error) {
			fmt.Println("The validation function was called")
			return strings.ToLower(normalizedValue), nil
		},
		// (Optional) the SetSession function is called when a session scoped variable is changed.
		// *And* when a new session is initialized.
		SetSession: func(vars *variable.SessionVars, value string) error {
			fmt.Println("The set session function was called")
			return nil
		},
		// (Optional) the SetGlobal function is called when a global variable is changed.
		// This will only be called on the TiDB server that the change is made on,
		// and not on the tidb-server peers which will also update their global variable eventually.
		SetGlobal: func(vars *variable.SessionVars, value string) error {
			fmt.Println("The set global function was called")
			return nil
		},
	}

	variable.RegisterSysVar(sv)

	fmt.Printf("---- read cfg in init [key: conn_ip_example_key, value: %s]\n", variable.GetSysVar("conn_ip_example_key").Value)
	atomic.SwapInt32(&connection, 0)
	return nil
}

// OnShutdown implements TiDB plugin's OnShutdown SPI.
func OnShutdown(ctx context.Context, manifest *plugin.Manifest) error {
	fmt.Println("## conn_ip_examples OnShutdown called ##")
	fmt.Printf("---- context: %s\n", ctx)
	fmt.Printf("---- read cfg in shutdown [key: conn_ip_example_key, value: %s]\n", variable.GetSysVar("conn_ip_example_key").Value)
	atomic.SwapInt32(&connection, 0)
	return nil
}

// OnGeneralEvent implements TiDB Audit plugin's OnGeneralEvent SPI.
func OnGeneralEvent(ctx context.Context, sctx *variable.SessionVars, event plugin.GeneralEvent, cmd string) {
	fmt.Println("## conn_ip_example OnGeneralEvent called ##")
	fmt.Printf("---- new connection by %s\n", ctx.Value("ip"))
	if sctx != nil {
		fmt.Printf("---- session status: %d\n", sctx.Status)
	}
	switch event {
	case plugin.Log:
		fmt.Println("---- event: Log")
	case plugin.Error:
		fmt.Println("---- event: Error")
	case plugin.Result:
		fmt.Println("---- event: Result")
	case plugin.Status:
		fmt.Println("---- event: Status")
	default:
		fmt.Println("---- event: unrecognized")
	}
	fmt.Printf("---- cmd: %s\n", cmd)
}

// OnConnectionEvent implements TiDB Audit plugin's OnConnectionEvent SPI.
func OnConnectionEvent(ctx context.Context, event plugin.ConnectionEvent, info *variable.ConnectionInfo) error {
	var reason string
	if r := ctx.Value(plugin.RejectReasonCtxValue{}); r != nil {
		reason = r.(string)
	}
	fmt.Println("## conn_ip_example onConnectionEvent called ##")
	fmt.Printf("---- conenct event: %s, reason: [%s]\n", event, reason)
	fmt.Printf("---- connection host: %s\n", info.Host)
	atomic.AddInt32(&connection, 1)
	return nil
}
