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

import "testing"

func TestPluginDeclare(t *testing.T) {
	auditRaw := &AuditManifest{Manifest: Manifest{}}
	auditExport := ExportManifest(auditRaw)
	audit2 := DeclareAuditManifest(auditExport)
	if audit2 != auditRaw {
		t.Errorf("declare audit fail")
	}

	authRaw := &AuthenticationManifest{Manifest: Manifest{}}
	authExport := ExportManifest(authRaw)
	auth2 := DeclareAuthenticationManifest(authExport)
	if auth2 != authRaw {
		t.Errorf("declare auth fail")
	}

	schemaRaw := &SchemaManifest{Manifest: Manifest{}}
	schemaExport := ExportManifest(schemaRaw)
	schema2 := DeclareSchemaManifest(schemaExport)
	if schema2 != schemaRaw {
		t.Errorf("declare schema fail")
	}

	daemonRaw := &DaemonManifest{Manifest: Manifest{}}
	daemonExport := ExportManifest(daemonRaw)
	daemon2 := DeclareDaemonManifest(daemonExport)
	if daemon2 != daemonRaw {
		t.Errorf("declare daemon fail")
	}
}

func TestDecode(t *testing.T) {
	failID := ID("fail")
	_, _, err := failID.Decode()
	if err == nil {
		t.Errorf("'fail' should not decode success")
	}
}
