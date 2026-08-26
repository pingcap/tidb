// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base32"
	"encoding/binary"
	"hash"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
)

const (
	diagnosticRedactionVersion      = 1
	diagnosticRedactionDomain       = "tidb-diagnostic-identifier-v1"
	minDiagnosticRedactionKeyBytes  = 32
	maxDiagnosticRedactionKeyBytes  = 4096
	diagnosticIdentifierDigestBytes = 16
	diagnosticFieldClassMetadata    = "metadata"
	diagnosticFieldClassIdentifier  = "identifier"
	diagnosticFieldClassDerived     = "derived"
	diagnosticFieldClassStructured  = "structured"
	diagnosticFieldClassUserContent = "user_content"
	diagnosticTransformPass         = "pass"
	diagnosticTransformDigest       = "digest"
	diagnosticTransformOmit         = "omit"
	diagnosticTransformPseudonymize = "pseudonymize"
	diagnosticTransformStructured   = "structured"
)

var diagnosticAliasEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

type diagnosticRedactor struct {
	profile string
	version int
	keyID   string
	key     []byte
	pool    sync.Pool
}

type diagnosticHMACState struct {
	mac    hash.Hash
	input  []byte
	digest [sha256.Size]byte
	alias  [26]byte
}

type diagnosticRedactionCapability struct {
	Profile string `json:"profile"`
	Version int    `json:"version"`
	KeyID   string `json:"key_id,omitempty"`
}

type diagnosticFieldPolicy struct {
	Name      string `json:"name"`
	Class     string `json:"class"`
	Transform string `json:"transform"`
}

type diagnosticFieldDefinition struct {
	Name      string
	Class     string
	Transform string
	TopLevel  bool
}

type diagnosticDatasetDescriptor struct {
	SensitivityLevel string
	Fields           []diagnosticFieldDefinition
}

func newDiagnosticRedactor(cfg config.DiagnosticAPI) (*diagnosticRedactor, error) {
	if !config.IsSupportedDiagnosticRedactionProfile(cfg.RedactionProfile) {
		return nil, errors.Errorf("unsupported diagnostic redaction profile %q", cfg.RedactionProfile)
	}
	redactor := &diagnosticRedactor{
		profile: cfg.RedactionProfile,
		version: diagnosticRedactionVersion,
		keyID:   cfg.RedactionKeyID,
	}
	if cfg.RedactionProfile == config.DiagnosticRedactionProfileMetadataReadable {
		if cfg.RedactionKeyFile != "" || cfg.RedactionKeyID != "" {
			return nil, errors.New("metadata-readable-v1 must not configure a diagnostic redaction key")
		}
		return redactor, nil
	}
	if cfg.RedactionKeyFile == "" || cfg.RedactionKeyID == "" {
		return nil, errors.New("strict-v1 requires diagnostic redaction-key-file and redaction-key-id")
	}
	// The path is an operator-controlled TiDB configuration value.
	//nolint:gosec
	rawKey, err := os.ReadFile(filepath.Clean(cfg.RedactionKeyFile))
	if err != nil {
		return nil, errors.Annotate(err, "read diagnostic redaction key")
	}
	keyMaterial := bytes.TrimSpace(rawKey)
	if len(keyMaterial) < minDiagnosticRedactionKeyBytes || len(keyMaterial) > maxDiagnosticRedactionKeyBytes {
		clear(rawKey)
		return nil, errors.Errorf(
			"diagnostic redaction key must contain between %d and %d bytes after trimming whitespace",
			minDiagnosticRedactionKeyBytes,
			maxDiagnosticRedactionKeyBytes,
		)
	}
	redactor.key = append([]byte(nil), keyMaterial...)
	clear(rawKey)
	redactor.pool.New = func() any {
		return &diagnosticHMACState{
			mac:   hmac.New(sha256.New, redactor.key),
			input: make([]byte, 0, 128),
		}
	}
	return redactor, nil
}

func (r *diagnosticRedactor) capability() diagnosticRedactionCapability {
	return diagnosticRedactionCapability{
		Profile: r.profile,
		Version: r.version,
		KeyID:   r.keyID,
	}
}

func (r *diagnosticRedactor) identifier(objectType, rawName string, objectIDs ...int64) string {
	if r.profile == config.DiagnosticRedactionProfileMetadataReadable {
		return rawName
	}
	state := r.pool.Get().(*diagnosticHMACState)
	input := state.input[:0]
	input = appendDiagnosticHMACString(input, diagnosticRedactionDomain)
	input = appendDiagnosticHMACString(input, objectType)
	input = binary.BigEndian.AppendUint32(input, uint32(len(objectIDs)))
	for _, objectID := range objectIDs {
		input = binary.BigEndian.AppendUint64(input, uint64(objectID))
	}
	input = appendDiagnosticHMACString(input, rawName)
	state.mac.Reset()
	_, _ = state.mac.Write(input)
	sum := state.mac.Sum(state.digest[:0])
	diagnosticAliasEncoding.Encode(state.alias[:], sum[:diagnosticIdentifierDigestBytes])
	for i, char := range state.alias {
		if char >= 'A' && char <= 'Z' {
			state.alias[i] = char + ('a' - 'A')
		}
	}
	var alias strings.Builder
	alias.Grow(len(objectType) + 1 + len(state.alias))
	alias.WriteString(objectType)
	alias.WriteByte('_')
	_, _ = alias.Write(state.alias[:])
	result := alias.String()
	clear(input)
	clear(state.digest[:])
	state.input = input[:0]
	r.pool.Put(state)
	return result
}

func appendDiagnosticHMACString(dst []byte, value string) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...)
}

func diagnosticField(name, class, transform string) diagnosticFieldDefinition {
	return diagnosticFieldDefinition{Name: name, Class: class, Transform: transform, TopLevel: true}
}

func diagnosticNestedField(name, class, transform string) diagnosticFieldDefinition {
	return diagnosticFieldDefinition{Name: name, Class: class, Transform: transform}
}

func diagnosticOmittedField(name, class string) diagnosticFieldDefinition {
	return diagnosticNestedField(name, class, diagnosticTransformOmit)
}

func diagnosticIdentifierField(name string) diagnosticFieldDefinition {
	return diagnosticField(name, diagnosticFieldClassIdentifier, diagnosticTransformPass)
}

func diagnosticNestedIdentifierField(name string) diagnosticFieldDefinition {
	return diagnosticNestedField(name, diagnosticFieldClassIdentifier, diagnosticTransformPass)
}

var diagnosticDatasetDescriptors = map[string]diagnosticDatasetDescriptor{
	"schema.tables": {
		SensitivityLevel: "L2",
		Fields: []diagnosticFieldDefinition{
			diagnosticField("schema_id", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticIdentifierField("schema_name"),
			diagnosticField("table_id", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticIdentifierField("table_name"),
			diagnosticField("table_kind", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("state", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("charset", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("collation", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("pk_is_handle", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("is_common_handle", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("shard_row_id_bits", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("auto_random_bits", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("partitioned", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("update_ts", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticOmittedField("comment", diagnosticFieldClassUserContent),
			diagnosticOmittedField("view_select", diagnosticFieldClassUserContent),
			diagnosticOmittedField("ttl_expression", diagnosticFieldClassUserContent),
		},
	},
	"schema.columns": {
		SensitivityLevel: "L2",
		Fields: []diagnosticFieldDefinition{
			diagnosticField("schema_id", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticIdentifierField("schema_name"),
			diagnosticField("table_id", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticIdentifierField("table_name"),
			diagnosticField("column_id", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticIdentifierField("column_name"),
			diagnosticField("ordinal", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("type_code", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("type_name", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("flag", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("length", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("decimal", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("charset", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("collation", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("state", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("hidden", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("generated", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("generated_stored", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticOmittedField("default_value", diagnosticFieldClassUserContent),
			diagnosticOmittedField("default_expression", diagnosticFieldClassUserContent),
			diagnosticOmittedField("comment", diagnosticFieldClassUserContent),
			diagnosticOmittedField("generated_expression", diagnosticFieldClassUserContent),
		},
	},
	"schema.indexes": {
		SensitivityLevel: "L2",
		Fields: []diagnosticFieldDefinition{
			diagnosticField("schema_id", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticIdentifierField("schema_name"),
			diagnosticField("table_id", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticIdentifierField("table_name"),
			diagnosticField("index_id", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticIdentifierField("index_name"),
			diagnosticField("state", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("index_type", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("unique", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("primary", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("invisible", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("global", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("multi_valued", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("columns", diagnosticFieldClassStructured, diagnosticTransformStructured),
			diagnosticNestedField("columns[].column_id", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticNestedIdentifierField("columns[].column_name"),
			diagnosticNestedField("columns[].prefix_length", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticOmittedField("comment", diagnosticFieldClassUserContent),
			diagnosticOmittedField("expression", diagnosticFieldClassUserContent),
		},
	},
	"schema.partitions": {
		SensitivityLevel: "L2",
		Fields: []diagnosticFieldDefinition{
			diagnosticField("schema_id", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticIdentifierField("schema_name"),
			diagnosticField("table_id", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticIdentifierField("table_name"),
			diagnosticField("partition_id", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticIdentifierField("partition_name"),
			diagnosticField("ordinal", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("partition_type", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticOmittedField("less_than", diagnosticFieldClassUserContent),
			diagnosticOmittedField("in_values", diagnosticFieldClassUserContent),
			diagnosticOmittedField("placement_policy_ref", diagnosticFieldClassIdentifier),
		},
	},
	"binding.summary": {
		SensitivityLevel: "L2",
		Fields: []diagnosticFieldDefinition{
			diagnosticField("sql_digest", diagnosticFieldClassDerived, diagnosticTransformDigest),
			diagnosticField("plan_digest", diagnosticFieldClassDerived, diagnosticTransformDigest),
			diagnosticField("status", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("source", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("create_time", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("update_time", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticOmittedField("original_sql", diagnosticFieldClassUserContent),
			diagnosticOmittedField("bind_sql", diagnosticFieldClassUserContent),
			diagnosticOmittedField("default_db", diagnosticFieldClassIdentifier),
		},
	},
	"stats.health": {
		SensitivityLevel: "L2",
		Fields: []diagnosticFieldDefinition{
			diagnosticField("table_id", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("version", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("modify_count", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("row_count", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("snapshot", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("last_histograms_version", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticField("modify_ratio", diagnosticFieldClassMetadata, diagnosticTransformPass),
			diagnosticOmittedField("top_n", diagnosticFieldClassUserContent),
			diagnosticOmittedField("buckets", diagnosticFieldClassUserContent),
			diagnosticOmittedField("cm_sketch", diagnosticFieldClassUserContent),
			diagnosticOmittedField("fm_sketch", diagnosticFieldClassUserContent),
		},
	},
}

func diagnosticDatasetDescriptorFor(name string) (diagnosticDatasetDescriptor, bool) {
	descriptor, ok := diagnosticDatasetDescriptors[name]
	return descriptor, ok
}

func (d diagnosticDatasetDescriptor) fieldNames() []string {
	fields := make([]string, 0, len(d.Fields))
	for _, field := range d.Fields {
		if field.TopLevel {
			fields = append(fields, field.Name)
		}
	}
	return fields
}

func (d diagnosticDatasetDescriptor) fieldPolicies(profile string) []diagnosticFieldPolicy {
	policies := make([]diagnosticFieldPolicy, 0, len(d.Fields))
	for _, field := range d.Fields {
		transform := field.Transform
		if field.Class == diagnosticFieldClassIdentifier && transform == diagnosticTransformPass &&
			profile == config.DiagnosticRedactionProfileStrict {
			transform = diagnosticTransformPseudonymize
		}
		policies = append(policies, diagnosticFieldPolicy{
			Name:      field.Name,
			Class:     field.Class,
			Transform: transform,
		})
	}
	return policies
}
