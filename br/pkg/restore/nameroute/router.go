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

// Package nameroute parses and applies exact schema and table rename rules for
// BR restore operations.
package nameroute

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// ObjectName identifies a schema when Table is empty, or a table otherwise.
type ObjectName struct {
	Schema ast.CIStr
	Table  ast.CIStr
}

// IsTable reports whether the object identifies a table.
func (n ObjectName) IsTable() bool {
	return n.Table.O != ""
}

// Rule maps one exact source schema or table to one exact target.
type Rule struct {
	Source ObjectName
	Target ObjectName
}

// Router applies exact rules to original source names. Table rules take
// precedence over schema rules, and a Route call never routes its result again.
type Router struct {
	rules          []Rule
	schemaRules    map[string]ObjectName
	tableRules     map[objectKey]ObjectName
	canonicalRules []string
	fingerprint    [sha256.Size]byte
}

type objectKey struct {
	schema string
	table  string
}

// Parse parses and validates rename specifications.
func Parse(specs []string) (*Router, error) {
	rules, err := ParseRules(specs)
	if err != nil {
		return nil, err
	}
	return New(rules)
}

// New validates rules and creates a router.
func New(rules []Rule) (*Router, error) {
	normalized := make([]Rule, 0, len(rules))
	sources := make(map[objectKey]int, len(rules))
	targets := make(map[objectKey]int, len(rules))

	for i, rule := range rules {
		normalizedRule, err := normalizeRule(rule)
		if err != nil {
			return nil, fmt.Errorf("invalid rename rule %d: %w", i+1, err)
		}
		sourceKey := keyOf(normalizedRule.Source)
		if previous, ok := sources[sourceKey]; ok {
			return nil, fmt.Errorf("rename rules %d and %d have duplicate source %s", previous+1, i+1, formatObject(normalizedRule.Source))
		}
		targetKey := keyOf(normalizedRule.Target)
		if previous, ok := targets[targetKey]; ok {
			return nil, fmt.Errorf("rename rules %d and %d have duplicate target %s", previous+1, i+1, formatObject(normalizedRule.Target))
		}
		if sourceKey == targetKey {
			return nil, fmt.Errorf("rename rule %d maps %s to itself", i+1, formatObject(normalizedRule.Source))
		}
		sources[sourceKey] = i
		targets[targetKey] = i
		normalized = append(normalized, normalizedRule)
	}

	router := &Router{
		rules:       normalized,
		schemaRules: make(map[string]ObjectName),
		tableRules:  make(map[objectKey]ObjectName),
	}
	for _, rule := range normalized {
		if rule.Source.IsTable() {
			router.tableRules[keyOf(rule.Source)] = rule.Target
		} else {
			router.schemaRules[rule.Source.Schema.L] = rule.Target
		}
	}
	if err := router.validateRuleConflicts(); err != nil {
		return nil, err
	}
	router.canonicalRules = canonicalize(normalized)
	router.fingerprint = fingerprint(router.canonicalRules)
	return router, nil
}

// Rules returns a copy of the normalized rules.
func (r *Router) Rules() []Rule {
	return slices.Clone(r.rules)
}

// Route applies at most one rule to the original source name. It returns the
// source unchanged when no rule matches.
func (r *Router) Route(schema, table ast.CIStr) (targetSchema, targetTable ast.CIStr, matched bool) {
	if table.O != "" {
		if target, ok := r.tableRules[objectKey{schema: schema.L, table: table.L}]; ok {
			return target.Schema, target.Table, true
		}
	}
	if target, ok := r.schemaRules[schema.L]; ok {
		return target.Schema, table, true
	}
	return schema, table, false
}

// ValidateTargets verifies that a concrete set of source objects has unique
// targets after routing. This also detects a renamed object colliding with an
// unchanged object in the restore set.
func (r *Router) ValidateTargets(objects []ObjectName) error {
	targets := make(map[objectKey]ObjectName, len(objects))
	seenSources := make(map[objectKey]struct{}, len(objects))
	for _, object := range objects {
		normalized, err := normalizeObject(object)
		if err != nil {
			return fmt.Errorf("invalid restore object: %w", err)
		}
		sourceKey := keyOf(normalized)
		if _, ok := seenSources[sourceKey]; ok {
			continue
		}
		seenSources[sourceKey] = struct{}{}

		targetSchema, targetTable, _ := r.Route(normalized.Schema, normalized.Table)
		target := ObjectName{Schema: targetSchema, Table: targetTable}
		targetKey := keyOf(target)
		if previous, ok := targets[targetKey]; ok && keyOf(previous) != sourceKey {
			return fmt.Errorf("source objects %s and %s conflict at target %s",
				formatObject(previous), formatObject(normalized), formatObject(target))
		}
		targets[targetKey] = normalized
	}
	return nil
}

// CanonicalRules returns an order-independent, unambiguous representation of
// the rules suitable for persistence in restore configuration.
func (r *Router) CanonicalRules() []string {
	return slices.Clone(r.canonicalRules)
}

// Fingerprint returns an order-independent SHA-256 fingerprint of the rules.
func (r *Router) Fingerprint() [sha256.Size]byte {
	return r.fingerprint
}

func (r *Router) validateRuleConflicts() error {
	for source, target := range r.tableRules {
		schemaSource, ok := r.schemaSourceForTarget(target.Schema.L)
		if !ok {
			continue
		}
		implicitSource := objectKey{schema: schemaSource, table: target.Table.L}
		if source == implicitSource {
			continue
		}
		if _, overridden := r.tableRules[implicitSource]; overridden {
			continue
		}
		return fmt.Errorf("table rule from %s conflicts with schema rule from %s at target %s",
			formatKey(source), formatKey(objectKey{schema: schemaSource}), formatObject(target))
	}
	return nil
}

func (r *Router) schemaSourceForTarget(targetSchema string) (string, bool) {
	for source, target := range r.schemaRules {
		if target.Schema.L == targetSchema {
			return source, true
		}
	}
	return "", false
}

func normalizeRule(rule Rule) (Rule, error) {
	source, err := normalizeObject(rule.Source)
	if err != nil {
		return Rule{}, fmt.Errorf("invalid source: %w", err)
	}
	target, err := normalizeObject(rule.Target)
	if err != nil {
		return Rule{}, fmt.Errorf("invalid target: %w", err)
	}
	if source.IsTable() != target.IsTable() {
		return Rule{}, fmt.Errorf("source and target must both name a schema or both name a table")
	}
	return Rule{Source: source, Target: target}, nil
}

func normalizeObject(object ObjectName) (ObjectName, error) {
	if err := validateIdentifier(object.Schema.O, mysql.MaxDatabaseNameLength); err != nil {
		return ObjectName{}, fmt.Errorf("invalid schema name: %w", err)
	}
	normalized := ObjectName{Schema: ast.NewCIStr(object.Schema.O)}
	if object.Table.O == "" {
		return normalized, nil
	}
	if err := validateIdentifier(object.Table.O, mysql.MaxTableNameLength); err != nil {
		return ObjectName{}, fmt.Errorf("invalid table name: %w", err)
	}
	normalized.Table = ast.NewCIStr(object.Table.O)
	return normalized, nil
}

func keyOf(object ObjectName) objectKey {
	return objectKey{schema: object.Schema.L, table: object.Table.L}
}

func canonicalize(rules []Rule) []string {
	canonical := make([]string, 0, len(rules))
	for _, rule := range rules {
		canonical = append(canonical, formatObject(rule.Source)+":"+formatObject(rule.Target))
	}
	slices.Sort(canonical)
	return canonical
}

func fingerprint(canonical []string) [sha256.Size]byte {
	hash := sha256.New()
	var size [8]byte
	for _, rule := range canonical {
		binary.BigEndian.PutUint64(size[:], uint64(len(rule)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(rule))
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func formatObject(object ObjectName) string {
	formatted := quoteIdentifier(object.Schema.O)
	if object.IsTable() {
		formatted += "." + quoteIdentifier(object.Table.O)
	}
	return formatted
}

func formatKey(key objectKey) string {
	return formatObject(ObjectName{Schema: ast.NewCIStr(key.schema), Table: ast.NewCIStr(key.table)})
}

func quoteIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}
