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
	MetricLabels  map[string]string                `toml:"-" json:"-"`
	SlowLogFields []KeyspaceObservabilityFieldPair `toml:"-" json:"-"`
	StmtLogFields []KeyspaceObservabilityFieldPair `toml:"-" json:"-"`
}

// KeyspaceObservabilityFieldPair stores one resolved output field.
type KeyspaceObservabilityFieldPair struct {
	Key   string
	Value string
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

var reservedKeyspaceObservabilityStmtLogFields = map[string]struct{}{
	"auth_users":                         {},
	"backoff_types":                      {},
	"begin":                              {},
	"binding_digest":                     {},
	"binding_sql":                        {},
	"charset":                            {},
	"collation":                          {},
	"commit_count":                       {},
	"digest":                             {},
	"end":                                {},
	"exec_count":                         {},
	"exec_retry_count":                   {},
	"exec_retry_time":                    {},
	"first_seen":                         {},
	"index_names":                        {},
	"is_internal":                        {},
	"keyspace_id":                        {},
	"keyspace_name":                      {},
	"last_seen":                          {},
	"max_backoff_time":                   {},
	"max_commit_backoff_time":            {},
	"max_commit_time":                    {},
	"max_compile_latency":                {},
	"max_cop_process_address":            {},
	"max_cop_process_time":               {},
	"max_cop_wait_address":               {},
	"max_cop_wait_time":                  {},
	"max_disk":                           {},
	"max_get_commit_ts_time":             {},
	"max_latency":                        {},
	"max_local_latch_time":               {},
	"max_mem":                            {},
	"max_mem_arbitration":                {},
	"max_parse_latency":                  {},
	"max_prewrite_region_num":            {},
	"max_prewrite_time":                  {},
	"max_process_time":                   {},
	"max_processed_keys":                 {},
	"max_resolve_lock_time":              {},
	"max_result_rows":                    {},
	"max_rocksdb_block_cache_hit_count":  {},
	"max_rocksdb_block_read_byte":        {},
	"max_rocksdb_block_read_count":       {},
	"max_rocksdb_delete_skipped_count":   {},
	"max_rocksdb_key_skipped_count":      {},
	"max_rru":                            {},
	"max_ru_wait_duration":               {},
	"max_ruv2":                           {},
	"max_total_keys":                     {},
	"max_txn_retry":                      {},
	"max_wait_time":                      {},
	"max_write_keys":                     {},
	"max_write_size":                     {},
	"max_wru":                            {},
	"min_latency":                        {},
	"min_result_rows":                    {},
	"normalized_sql":                     {},
	"plan_cache_hits":                    {},
	"plan_cache_unqualified_count":       {},
	"plan_cache_unqualified_last_reason": {},
	"plan_digest":                        {},
	"plan_hint":                          {},
	"plan_in_binding":                    {},
	"plan_in_cache":                      {},
	"prepared":                           {},
	"prev_sql":                           {},
	"resource_group_name":                {},
	"sample_binary_plan":                 {},
	"sample_plan":                        {},
	"sample_sql":                         {},
	"schema_name":                        {},
	"stmt_type":                          {},
	"storage_kv":                         {},
	"storage_mpp":                        {},
	"sum_affected_rows":                  {},
	"sum_backoff_time":                   {},
	"sum_backoff_times":                  {},
	"sum_backoff_total":                  {},
	"sum_commit_backoff_time":            {},
	"sum_commit_time":                    {},
	"sum_compile_latency":                {},
	"sum_disk":                           {},
	"sum_errors":                         {},
	"sum_get_commit_ts_time":             {},
	"sum_kv_total":                       {},
	"sum_latency":                        {},
	"sum_local_latch_time":               {},
	"sum_mem":                            {},
	"sum_mem_arbitration":                {},
	"sum_num_cop_tasks":                  {},
	"sum_parse_latency":                  {},
	"sum_pd_total":                       {},
	"sum_prewrite_region_num":            {},
	"sum_prewrite_time":                  {},
	"sum_process_time":                   {},
	"sum_processed_keys":                 {},
	"sum_resolve_lock_time":              {},
	"sum_result_rows":                    {},
	"sum_rocksdb_block_cache_hit_count":  {},
	"sum_rocksdb_block_read_byte":        {},
	"sum_rocksdb_block_read_count":       {},
	"sum_rocksdb_delete_skipped_count":   {},
	"sum_rocksdb_key_skipped_count":      {},
	"sum_rru":                            {},
	"sum_ru_wait_duration":               {},
	"sum_ruv2":                           {},
	"sum_tidb_cpu":                       {},
	"sum_tikv_cpu":                       {},
	"sum_total_keys":                     {},
	"sum_txn_retry":                      {},
	"sum_wait_time":                      {},
	"sum_warnings":                       {},
	"sum_write_keys":                     {},
	"sum_write_size":                     {},
	"sum_write_sql_resp_total":           {},
	"sum_wru":                            {},
	"table_names":                        {},
	"unpacked_bytes_received_tiflash_cross_zone": {},
	"unpacked_bytes_received_tiflash_total":      {},
	"unpacked_bytes_received_tikv_cross_zone":    {},
	"unpacked_bytes_received_tikv_total":         {},
	"unpacked_bytes_send_tiflash_cross_zone":     {},
	"unpacked_bytes_send_tiflash_total":          {},
	"unpacked_bytes_send_tikv_cross_zone":        {},
	"unpacked_bytes_send_tikv_total":             {},
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
			if _, ok := reservedKeyspaceObservabilityStmtLogFields[key]; ok {
				return fmt.Errorf("[keyspace-observability.fields.%d] reserved stmt-log-field %q", i, field.StmtLogField)
			}
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
		MetricLabels: make(map[string]string),
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
			resolved.SlowLogFields = append(resolved.SlowLogFields, KeyspaceObservabilityFieldPair{Key: field.SlowLogField, Value: value})
		}
		if field.StmtLogField != "" {
			resolved.StmtLogFields = append(resolved.StmtLogFields, KeyspaceObservabilityFieldPair{Key: field.StmtLogField, Value: value})
		}
	}
	c.KeyspaceObservabilityValues = resolved
	return nil
}

// Clone returns a deep copy of resolved metadata observability values.
func (v KeyspaceObservabilityValues) Clone() KeyspaceObservabilityValues {
	res := KeyspaceObservabilityValues{}
	if len(v.MetricLabels) > 0 {
		res.MetricLabels = make(map[string]string, len(v.MetricLabels))
		for k, value := range v.MetricLabels {
			res.MetricLabels[k] = value
		}
	}
	res.SlowLogFields = append([]KeyspaceObservabilityFieldPair(nil), v.SlowLogFields...)
	res.StmtLogFields = append([]KeyspaceObservabilityFieldPair(nil), v.StmtLogFields...)
	return res
}

// GetKeyspaceObservabilityMetricLabels returns resolved metric labels.
func (c *Config) GetKeyspaceObservabilityMetricLabels() map[string]string {
	return c.KeyspaceObservabilityValues.Clone().MetricLabels
}

// GetKeyspaceObservabilitySlowLogFields returns resolved slow log fields.
func (c *Config) GetKeyspaceObservabilitySlowLogFields() []KeyspaceObservabilityFieldPair {
	return c.KeyspaceObservabilityValues.Clone().SlowLogFields
}

// GetKeyspaceObservabilityStmtLogFields returns resolved statement log fields.
func (c *Config) GetKeyspaceObservabilityStmtLogFields() []KeyspaceObservabilityFieldPair {
	return c.KeyspaceObservabilityValues.Clone().StmtLogFields
}
