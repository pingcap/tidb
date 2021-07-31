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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPluginDeclare(t *testing.T) {
	t.Parallel()
	auditRaw := &AuditManifest{Manifest: Manifest{}}
	auditExport := ExportManifest(auditRaw)
	audit2 := DeclareAuditManifest(auditExport)
	assert.Equalf(t, auditRaw, audit2, "declare audit fail")

	authRaw := &AuthenticationManifest{Manifest: Manifest{}}
	authExport := ExportManifest(authRaw)
	auth2 := DeclareAuthenticationManifest(authExport)
	assert.Equalf(t, authRaw, auth2, "declare auth fail")

	schemaRaw := &SchemaManifest{Manifest: Manifest{}}
	schemaExport := ExportManifest(schemaRaw)
	schema2 := DeclareSchemaManifest(schemaExport)
	assert.Equalf(t, schemaRaw, schema2, "declare schema fail")

	daemonRaw := &DaemonManifest{Manifest: Manifest{}}
	daemonExport := ExportManifest(daemonRaw)
	daemon2 := DeclareDaemonManifest(daemonExport)
	assert.Equalf(t, daemonRaw, daemon2, "declare daemon fail")
}

func TestDecode(t *testing.T) {
	t.Parallel()
	failID := ID("fail")
	_, _, err := failID.Decode()
	assert.Errorf(t, err, "'fail' should not decode success")
}
