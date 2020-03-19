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
	"strings"
	"unsafe"
)

// DeclareAuditManifest declares manifest as AuditManifest.
func DeclareAuditManifest(m *Manifest) *AuditManifest {
	return (*AuditManifest)(unsafe.Pointer(m))
}

// DeclareAuthenticationManifest declares manifest as AuthenticationManifest.
func DeclareAuthenticationManifest(m *Manifest) *AuthenticationManifest {
	return (*AuthenticationManifest)(unsafe.Pointer(m))
}

// DeclareSchemaManifest declares manifest as SchemaManifest.
func DeclareSchemaManifest(m *Manifest) *SchemaManifest {
	return (*SchemaManifest)(unsafe.Pointer(m))
}

// DeclareDaemonManifest declares manifest as DaemonManifest.
func DeclareDaemonManifest(m *Manifest) *DaemonManifest {
	return (*DaemonManifest)(unsafe.Pointer(m))
}

// ID present plugin identity.
type ID string

// Decode decodes a plugin id into name, version parts.
func (n ID) Decode() (name string, version string, err error) {
	splits := strings.Split(string(n), "-")
	if len(splits) != 2 {
		err = errInvalidPluginID.GenWithStackByArgs(string(n))
		return
	}
	name = splits[0]
	version = splits[1]
	return
}
