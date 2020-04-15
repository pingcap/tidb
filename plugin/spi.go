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

package plugin

import (
	"context"
	"reflect"
	"unsafe"

	"github.com/pingcap/tidb/v4/sessionctx/variable"
)

const (
	// LibrarySuffix defines TiDB plugin's file suffix.
	LibrarySuffix = ".so"
	// ManifestSymbol defines TiDB plugin's entrance symbol.
	// Plugin take manifest info from this symbol.
	ManifestSymbol = "PluginManifest"
)

// Manifest describes plugin info and how it can do by plugin itself.
type Manifest struct {
	Name           string
	Description    string
	RequireVersion map[string]uint16
	License        string
	BuildTime      string
	SysVars        map[string]*variable.SysVar
	// Validate defines the validate logic for plugin.
	// returns error will stop load plugin process and TiDB startup.
	Validate func(ctx context.Context, manifest *Manifest) error
	// OnInit defines the plugin init logic.
	// it will be called after domain init.
	// return error will stop load plugin process and TiDB startup.
	OnInit func(ctx context.Context, manifest *Manifest) error
	// OnShutDown defines the plugin cleanup logic.
	// return error will write log and continue shutdown.
	OnShutdown func(ctx context.Context, manifest *Manifest) error
	// OnFlush defines flush logic after executed `flush tidb plugins`.
	// it will be called after OnInit.
	// return error will write log and continue watch following flush.
	OnFlush      func(ctx context.Context, manifest *Manifest) error
	flushWatcher *flushWatcher

	Version uint16
	Kind    Kind
}

// ExportManifest exports a manifest to TiDB as a known format.
// it just casts sub-manifest to manifest.
func ExportManifest(m interface{}) *Manifest {
	v := reflect.ValueOf(m)
	return (*Manifest)(unsafe.Pointer(v.Pointer()))
}

// AuthenticationManifest presents a sub-manifest that every audit plugin must provide.
type AuthenticationManifest struct {
	Manifest
	AuthenticateUser             func()
	GenerateAuthenticationString func()
	ValidateAuthenticationString func()
	SetSalt                      func()
}

// SchemaManifest presents a sub-manifest that every schema plugins must provide.
type SchemaManifest struct {
	Manifest
}

// DaemonManifest presents a sub-manifest that every DaemonManifest plugins must provide.
type DaemonManifest struct {
	Manifest
}
