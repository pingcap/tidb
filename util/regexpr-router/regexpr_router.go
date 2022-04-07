// Copyright 2022 PingCAP, Inc.
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

package regexprrouter

import (
	"regexp"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/filter"
	router "github.com/pingcap/tidb/util/table-router"
)

// FilterType is the type of filter
type FilterType = int32

const (
	// TblFilter is table filter
	TblFilter FilterType = iota + 1
	// SchmFilter is schema filter
	SchmFilter
)

type filterWrapper struct {
	filter *filter.Filter
	typ    FilterType
	target filter.Table

	rawRule *router.TableRule
}

// RouteTable is route table
type RouteTable struct {
	filters       []*filterWrapper
	caseSensitive bool
}

// NewRegExprRouter is to create RouteTable
func NewRegExprRouter(caseSensitive bool, rules []*router.TableRule) (*RouteTable, error) {
	r := &RouteTable{
		filters:       make([]*filterWrapper, 0),
		caseSensitive: caseSensitive,
	}
	for _, rule := range rules {
		if err := r.AddRule(rule); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// AddRule is to add rule
func (r *RouteTable) AddRule(rule *router.TableRule) error {
	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}
	if !r.caseSensitive {
		rule.ToLower()
	}
	newFilter := &filterWrapper{
		rawRule: rule,
	}
	newFilter.target = filter.Table{
		Schema: rule.TargetSchema,
		Name:   rule.TargetTable,
	}
	if len(rule.TablePattern) == 0 {
		// raw schema rule
		newFilter.typ = SchmFilter
		rawFilter, err := filter.New(r.caseSensitive, &filter.Rules{
			DoDBs: []string{rule.SchemaPattern},
		})
		if err != nil {
			return errors.Annotatef(err, "add rule %+v into table router", rule)
		}
		newFilter.filter = rawFilter
	} else {
		newFilter.typ = TblFilter
		rawFilter, err := filter.New(r.caseSensitive, &filter.Rules{
			DoTables: []*filter.Table{
				{
					Schema: rule.SchemaPattern,
					Name:   rule.TablePattern,
				},
			},
			DoDBs: []string{rule.SchemaPattern},
		})
		if err != nil {
			return errors.Annotatef(err, "add rule %+v into table router", rule)
		}
		newFilter.filter = rawFilter
	}
	r.filters = append(r.filters, newFilter)
	return nil
}

// Route is to route table
func (r *RouteTable) Route(schema, table string) (string, string, error) {
	curTable := &filter.Table{
		Schema: schema,
		Name:   table,
	}
	tblRules := make([]*filterWrapper, 0)
	schmRules := make([]*filterWrapper, 0)
	for _, filterWrapper := range r.filters {
		if filterWrapper.filter.Match(curTable) {
			if filterWrapper.typ == TblFilter {
				tblRules = append(tblRules, filterWrapper)
			} else {
				schmRules = append(schmRules, filterWrapper)
			}
		}
	}
	var (
		targetSchema string
		targetTable  string
	)
	if table == "" || len(tblRules) == 0 {
		// 1. no need to match table or
		// 2. match no table
		if len(schmRules) > 1 {
			return "", "", errors.Errorf("table %s.%s matches more than one rule", schema, table)
		}
		if len(schmRules) == 1 {
			targetSchema, targetTable = schmRules[0].target.Schema, schmRules[0].target.Name
		}
	} else {
		if len(tblRules) > 1 {
			return "", "", errors.Errorf("table %s.%s matches more than one rule", schema, table)
		}
		targetSchema, targetTable = tblRules[0].target.Schema, tblRules[0].target.Name
	}
	if len(targetSchema) == 0 {
		targetSchema = schema
	}
	if len(targetTable) == 0 {
		targetTable = table
	}
	return targetSchema, targetTable, nil
}

// AllRules is to get all rules
func (r *RouteTable) AllRules() ([]router.TableRule, []router.TableRule) {
	var (
		schmRouteRules  []router.TableRule
		tableRouteRules []router.TableRule
	)
	for _, filter := range r.filters {
		if filter.typ == SchmFilter {
			schmRouteRules = append(schmRouteRules, *filter.rawRule)
		} else {
			tableRouteRules = append(tableRouteRules, *filter.rawRule)
		}
	}
	return schmRouteRules, tableRouteRules
}

// FetchExtendColumn is to fetch extend column
func (r *RouteTable) FetchExtendColumn(schema, table, source string) ([]string, []string) {
	var cols []string
	var vals []string
	rules := []*filterWrapper{}
	curTable := &filter.Table{
		Schema: schema,
		Name:   table,
	}
	for _, filter := range r.filters {
		if filter.filter.Match(curTable) {
			rules = append(rules, filter)
		}
	}
	var (
		schemaRules = make([]*router.TableRule, 0, len(rules))
		tableRules  = make([]*router.TableRule, 0, len(rules))
	)
	for i := range rules {
		rule := rules[i].rawRule
		if rule.TablePattern == "" {
			schemaRules = append(schemaRules, rule)
		} else {
			tableRules = append(tableRules, rule)
		}
	}
	if len(tableRules) == 0 && len(schemaRules) == 0 {
		return cols, vals
	}
	var rule *router.TableRule
	if len(tableRules) == 0 {
		rule = schemaRules[0]
	} else {
		rule = tableRules[0]
	}
	if rule.TableExtractor != nil {
		cols = append(cols, rule.TableExtractor.TargetColumn)
		vals = append(vals, extractVal(table, rule.TableExtractor))
	}

	if rule.SchemaExtractor != nil {
		cols = append(cols, rule.SchemaExtractor.TargetColumn)
		vals = append(vals, extractVal(schema, rule.SchemaExtractor))
	}

	if rule.SourceExtractor != nil {
		cols = append(cols, rule.SourceExtractor.TargetColumn)
		vals = append(vals, extractVal(source, rule.SourceExtractor))
	}
	return cols, vals
}

func extractVal(s string, ext interface{}) string {
	var params []string
	switch e := ext.(type) {
	case *router.TableExtractor:
		if regExpr, err := regexp.Compile(e.TableRegexp); err == nil {
			params = regExpr.FindStringSubmatch(s)
		}
	case *router.SchemaExtractor:
		if regExpr, err := regexp.Compile(e.SchemaRegexp); err == nil {
			params = regExpr.FindStringSubmatch(s)
		}
	case *router.SourceExtractor:
		if regExpr, err := regexp.Compile(e.SourceRegexp); err == nil {
			params = regExpr.FindStringSubmatch(s)
		}
	}
	var val strings.Builder
	for idx, param := range params {
		if idx > 0 {
			val.WriteString(param)
		}
	}
	return val.String()
}
