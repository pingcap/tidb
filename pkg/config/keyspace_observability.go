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
	MetricLabels  map[string]string `toml:"-" json:"-"`
	SlowLogFields map[string]string `toml:"-" json:"-"`
	StmtLogFields map[string]string `toml:"-" json:"-"`
}

const keyspaceObservabilityMetricLabelPrefix = "keyspace_meta_"

var reservedKeyspaceObservabilitySlowLogFields = map[string]struct{}{
	"backoff_detail":                {},
	"backoff_time":                  {},
	"backoff_total":                 {},
	"backoff_types":                 {},
	"binary_plan":                   {},
	"commit_backoff_time":           {},
	"commit_primary_rpc_detail":     {},
	"commit_time":                   {},
	"compile_time":                  {},
	"conn_id":                       {},
	"cop_backoff_":                  {},
	"cop_mvcc_read_amplification":   {},
	"cop_proc_addr":                 {},
	"cop_proc_avg":                  {},
	"cop_proc_max":                  {},
	"cop_proc_p90":                  {},
	"cop_time":                      {},
	"cop_wait_addr":                 {},
	"cop_wait_avg":                  {},
	"cop_wait_max":                  {},
	"cop_wait_p90":                  {},
	"db":                            {},
	"digest":                        {},
	"disk_max":                      {},
	"exec_retry_count":              {},
	"exec_retry_time":               {},
	"get_commit_ts_time":            {},
	"get_latest_ts_time":            {},
	"get_snapshot_time":             {},
	"has_more_results":              {},
	"host":                          {},
	"index_names":                   {},
	"is_internal":                   {},
	"isexplicittxn":                 {},
	"issyncstatsfailed":             {},
	"iswritecachetable":             {},
	"keyspace_id":                   {},
	"keyspace_name":                 {},
	"kv_total":                      {},
	"local_latch_wait_time":         {},
	"lockkeys_time":                 {},
	"mem_arbitration":               {},
	"mem_max":                       {},
	"num_cop_tasks":                 {},
	"opt_binding_match":             {},
	"opt_logical":                   {},
	"opt_physical":                  {},
	"opt_stats_derive":              {},
	"opt_stats_sync_wait":           {},
	"optimize_time":                 {},
	"parse_time":                    {},
	"pd_total":                      {},
	"plan":                          {},
	"plan_digest":                   {},
	"plan_from_binding":             {},
	"plan_from_cache":               {},
	"preproc_subqueries":            {},
	"preproc_subqueries_time":       {},
	"prepared":                      {},
	"prewrite_backoff_types":        {},
	"prewrite_region":               {},
	"prewrite_time":                 {},
	"prev_stmt":                     {},
	"process_keys":                  {},
	"process_time":                  {},
	"query":                         {},
	"query_time":                    {},
	"request_count":                 {},
	"request_unit_read":             {},
	"request_unit_v2":               {},
	"request_unit_v2_detail":        {},
	"request_unit_write":            {},
	"resolve_lock_time":             {},
	"resource_group":                {},
	"result_rows":                   {},
	"rewrite_time":                  {},
	"rocksdb_block_cache_hit_count": {},
	"rocksdb_block_read_byte":       {},
	"rocksdb_block_read_count":      {},
	"rocksdb_block_read_time":       {},
	"rocksdb_delete_skipped_count":  {},
	"rocksdb_key_skipped_count":     {},
	"session_alias":                 {},
	"session_connect_attrs":         {},
	"slowest_prewrite_rpc_detail":   {},
	"stats":                         {},
	"storage_from_kv":               {},
	"storage_from_mpp":              {},
	"succ":                          {},
	"tidb_cpu_time":                 {},
	"tikv_cpu_time":                 {},
	"time":                          {},
	"time_queued_by_rc":             {},
	"total_keys":                    {},
	"txn_retry":                     {},
	"txn_start_ts":                  {},
	"unpacked_bytes_received_tiflash_cross_zone": {},
	"unpacked_bytes_received_tiflash_total":      {},
	"unpacked_bytes_received_tikv_cross_zone":    {},
	"unpacked_bytes_received_tikv_total":         {},
	"unpacked_bytes_sent_tiflash_cross_zone":     {},
	"unpacked_bytes_sent_tiflash_total":          {},
	"unpacked_bytes_sent_tikv_cross_zone":        {},
	"unpacked_bytes_sent_tikv_total":             {},
	"user":                                       {},
	"user@host":                                  {},
	"wait_prewrite_binlog_time":                  {},
	"wait_time":                                  {},
	"wait_ts":                                    {},
	"warnings":                                   {},
	"write_keys":                                 {},
	"write_size":                                 {},
	"write_sql_response_total":                   {},
}

var reservedKeyspaceObservabilitySlowLogFieldPrefixes = []string{
	"cop_backoff_",
}

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
			if isReservedKeyspaceObservabilitySlowLogField(key) {
				return fmt.Errorf("[keyspace-observability.fields.%d] reserved slow-log-field %q", i, field.SlowLogField)
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

func isReservedKeyspaceObservabilitySlowLogField(field string) bool {
	if _, ok := reservedKeyspaceObservabilitySlowLogFields[field]; ok {
		return true
	}
	for _, prefix := range reservedKeyspaceObservabilitySlowLogFieldPrefixes {
		if strings.HasPrefix(field, prefix) {
			return true
		}
	}
	return false
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
		SlowLogFields: make(map[string]string),
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
			resolved.SlowLogFields[field.SlowLogField] = value
		}
		if field.StmtLogField != "" {
			resolved.StmtLogFields[field.StmtLogField] = value
		}
	}
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
		res.SlowLogFields = maps.Clone(v.SlowLogFields)
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

// GetKeyspaceObservabilitySlowLogFields returns resolved slow log fields.
func (c *Config) GetKeyspaceObservabilitySlowLogFields() map[string]string {
	return c.KeyspaceObservabilityValues.SlowLogFields
}

// GetKeyspaceObservabilityStmtLogFields returns resolved statement log fields.
func (c *Config) GetKeyspaceObservabilityStmtLogFields() map[string]string {
	return c.KeyspaceObservabilityValues.StmtLogFields
}
