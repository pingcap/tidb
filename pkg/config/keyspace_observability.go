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

package config

import (
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/prometheus/common/model"
)

// KeyspaceObservability maps metadata entries to observability outputs.
type KeyspaceObservability struct {
	Fields []KeyspaceObservabilityField `toml:"fields" json:"fields,omitempty"`
}

// KeyspaceObservabilityField describes one metadata entry mapping.
type KeyspaceObservabilityField struct {
	Source       string `toml:"source" json:"source,omitempty"`
	MetricLabel  string `toml:"metric-label" json:"metric-label,omitempty"`
	SlowLogField string `toml:"slow-log-field" json:"slow-log-field,omitempty"`
	StmtLogField string `toml:"stmt-log-field" json:"stmt-log-field,omitempty"`
	Required     bool   `toml:"required" json:"required,omitempty"`
}

// KeyspaceObservabilityValues stores resolved metadata values.
type KeyspaceObservabilityValues struct {
	MetricLabels  map[string]string               `toml:"-" json:"-"`
	SlowLogFields []KeyspaceObservabilityLogField `toml:"-" json:"-"`
	StmtLogFields map[string]string               `toml:"-" json:"-"`
}

// KeyspaceObservabilityLogField stores a resolved log field value.
type KeyspaceObservabilityLogField struct {
	Name  string
	Value string
}

const keyspaceObservabilityMetricLabelPrefix = "keyspace_meta_"

// Valid validates metadata observability mappings.
func (o KeyspaceObservability) Valid() error {
	metricLabels := make(map[string]struct{}, len(o.Fields))
	slowLogFields := make(map[string]struct{}, len(o.Fields))
	stmtLogFields := make(map[string]struct{}, len(o.Fields))
	for i, field := range o.Fields {
		if field.Source == "" {
			return fmt.Errorf("[keyspace-observability.fields.%d] source cannot be empty", i)
		}
		if field.MetricLabel == "" && field.SlowLogField == "" && field.StmtLogField == "" {
			return fmt.Errorf("[keyspace-observability.fields.%d] at least one output must be set", i)
		}
		if field.MetricLabel != "" {
			if !validPrometheusLabelName(field.MetricLabel) {
				return fmt.Errorf("[keyspace-observability.fields.%d] invalid metric-label %q", i, field.MetricLabel)
			}
			key := strings.ToLower(field.MetricLabel)
			if !strings.HasPrefix(key, keyspaceObservabilityMetricLabelPrefix) {
				return fmt.Errorf("[keyspace-observability.fields.%d] metric-label %q must start with %q", i, field.MetricLabel, keyspaceObservabilityMetricLabelPrefix)
			}
			if _, ok := metricLabels[key]; ok {
				return fmt.Errorf("[keyspace-observability.fields.%d] duplicated metric-label %q", i, field.MetricLabel)
			}
			metricLabels[key] = struct{}{}
		}
		if field.SlowLogField != "" {
			if !validKeyspaceObservabilityLogFieldName(field.SlowLogField) {
				return fmt.Errorf("[keyspace-observability.fields.%d] invalid slow-log-field %q", i, field.SlowLogField)
			}
			key := strings.ToLower(field.SlowLogField)
			if !strings.HasPrefix(key, keyspaceObservabilityMetricLabelPrefix) {
				return fmt.Errorf("[keyspace-observability.fields.%d] slow-log-field %q must start with %q", i, field.SlowLogField, keyspaceObservabilityMetricLabelPrefix)
			}
			if _, ok := slowLogFields[key]; ok {
				return fmt.Errorf("[keyspace-observability.fields.%d] duplicated slow-log-field %q", i, field.SlowLogField)
			}
			slowLogFields[key] = struct{}{}
		}
		if field.StmtLogField != "" {
			key := strings.ToLower(field.StmtLogField)
			if _, ok := stmtLogFields[key]; ok {
				return fmt.Errorf("[keyspace-observability.fields.%d] duplicated stmt-log-field %q", i, field.StmtLogField)
			}
			stmtLogFields[key] = struct{}{}
		}
	}
	return nil
}

func validKeyspaceObservabilityLogFieldName(field string) bool {
	return validPrometheusLabelName(field)
}

func validPrometheusLabelName(label string) bool {
	return model.LabelName(label).IsValid() && model.LabelName(label).IsValidLegacy()
}

// ResolveKeyspaceObservability resolves configured output values from metadata.
func (c *Config) ResolveKeyspaceObservability(values map[string]string) error {
	resolved := KeyspaceObservabilityValues{
		MetricLabels:  make(map[string]string),
		StmtLogFields: make(map[string]string),
	}
	for _, field := range c.KeyspaceObservability.Fields {
		value, ok := values[field.Source]
		if !ok {
			if field.Required {
				return fmt.Errorf("missing required keyspace metadata entry %q", field.Source)
			}
			continue
		}
		if field.MetricLabel != "" {
			resolved.MetricLabels[field.MetricLabel] = value
		}
		if field.SlowLogField != "" {
			resolved.SlowLogFields = append(resolved.SlowLogFields, KeyspaceObservabilityLogField{
				Name:  field.SlowLogField,
				Value: value,
			})
		}
		if field.StmtLogField != "" {
			resolved.StmtLogFields[field.StmtLogField] = value
		}
	}
	sort.SliceStable(resolved.SlowLogFields, func(i, j int) bool {
		return resolved.SlowLogFields[i].Name < resolved.SlowLogFields[j].Name
	})
	c.KeyspaceObservabilityValues = resolved
	return nil
}

// Clone returns a deep copy of resolved metadata observability values.
func (v KeyspaceObservabilityValues) Clone() KeyspaceObservabilityValues {
	res := KeyspaceObservabilityValues{}
	if len(v.MetricLabels) > 0 {
		res.MetricLabels = maps.Clone(v.MetricLabels)
	}
	if len(v.SlowLogFields) > 0 {
		res.SlowLogFields = append([]KeyspaceObservabilityLogField(nil), v.SlowLogFields...)
	}
	if len(v.StmtLogFields) > 0 {
		res.StmtLogFields = maps.Clone(v.StmtLogFields)
	}
	return res
}

// GetKeyspaceObservabilityMetricLabels returns resolved metric labels.
func (c *Config) GetKeyspaceObservabilityMetricLabels() map[string]string {
	return c.KeyspaceObservabilityValues.MetricLabels
}

// GetKeyspaceObservabilitySlowLogFields returns resolved slow log fields in stable order.
func (c *Config) GetKeyspaceObservabilitySlowLogFields() []KeyspaceObservabilityLogField {
	return c.KeyspaceObservabilityValues.SlowLogFields
}

// GetKeyspaceObservabilityStmtLogFields returns resolved statement log fields.
func (c *Config) GetKeyspaceObservabilityStmtLogFields() map[string]string {
	return c.KeyspaceObservabilityValues.StmtLogFields
}
