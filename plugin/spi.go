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

	"github.com/pingcap/tidb/sessionctx/variable"
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
	Kind           Kind
	Name           string
	Description    string
	Version        uint16
	RequireVersion map[string]uint16
	License        string
	BuildTime      string
	SysVars        map[string]*variable.SysVar
	Validate       func(ctx context.Context, manifest *Manifest) error
	OnInit         func(ctx context.Context, manifest *Manifest) error
	OnShutdown     func(ctx context.Context, manifest *Manifest) error
}

// ExportManifest exports a manifest to TiDB as a known format.
// it just casts sub-manifest to manifest.
func ExportManifest(m interface{}) *Manifest {
	v := reflect.ValueOf(m)
	return (*Manifest)(unsafe.Pointer(v.Pointer()))
}

// AuditManifest presents a sub-manifest that every audit plugin must provide.
type AuditManifest struct {
	Manifest
	NotifyEvent func(ctx context.Context, sctx *variable.SessionVars) error
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
