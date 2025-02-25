// Copyright 2023 PingCAP, Inc.
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

package mydump

import (
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tidb/pkg/util/slice"
	"go.uber.org/zap"
)

// SourceType specifies the source file types.
type SourceType int

const (
	// SourceTypeIgnore means this source file is ignored.
	SourceTypeIgnore SourceType = iota
	// SourceTypeSchemaSchema means this source file is a schema file for the DB.
	SourceTypeSchemaSchema
	// SourceTypeTableSchema means this source file is a schema file for the table.
	SourceTypeTableSchema
	// SourceTypeSQL means this source file is a SQL data file.
	SourceTypeSQL
	// SourceTypeCSV means this source file is a CSV data file.
	SourceTypeCSV
	// SourceTypeParquet means this source file is a parquet data file.
	SourceTypeParquet
	// SourceTypeViewSchema means this source file is a schema file for the view.
	SourceTypeViewSchema
)

const (
	// SchemaSchema is the source type value for schema file for DB.
	SchemaSchema = "schema-schema"
	// TableSchema is the source type value for schema file for table.
	TableSchema = "table-schema"
	// ViewSchema is the source type value for schema file for view.
	ViewSchema = "view-schema"
	// TypeSQL is the source type value for sql data file.
	TypeSQL = "sql"
	// TypeCSV is the source type value for csv data file.
	TypeCSV = "csv"
	// TypeParquet is the source type value for parquet data file.
	TypeParquet = "parquet"
	// TypeIgnore is the source type value for a ignored data file.
	TypeIgnore = "ignore"
)

// Compression specifies the compression type.
type Compression int

const (
	// CompressionNone is the compression type that with no compression.
	CompressionNone Compression = iota
	// CompressionGZ is the compression type that uses GZ algorithm.
	CompressionGZ
	// CompressionLZ4 is the compression type that uses LZ4 algorithm.
	CompressionLZ4
	// CompressionZStd is the compression type that uses ZStd algorithm.
	CompressionZStd
	// CompressionXZ is the compression type that uses XZ algorithm.
	CompressionXZ
	// CompressionLZO is the compression type that uses LZO algorithm.
	CompressionLZO
	// CompressionSnappy is the compression type that uses Snappy algorithm.
	CompressionSnappy
)

// ToStorageCompressType converts Compression to storage.CompressType.
func ToStorageCompressType(compression Compression) (storage.CompressType, error) {
	switch compression {
	case CompressionGZ:
		return storage.Gzip, nil
	case CompressionSnappy:
		return storage.Snappy, nil
	case CompressionZStd:
		return storage.Zstd, nil
	case CompressionNone:
		return storage.NoCompression, nil
	default:
		return storage.NoCompression,
			errors.Errorf("compression %d doesn't have related storage compressType", compression)
	}
}

func parseSourceType(t string) (SourceType, error) {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case SchemaSchema:
		return SourceTypeSchemaSchema, nil
	case TableSchema:
		return SourceTypeTableSchema, nil
	case TypeSQL:
		return SourceTypeSQL, nil
	case TypeCSV:
		return SourceTypeCSV, nil
	case TypeParquet:
		return SourceTypeParquet, nil
	case TypeIgnore:
		return SourceTypeIgnore, nil
	case ViewSchema:
		return SourceTypeViewSchema, nil
	default:
		return SourceTypeIgnore, errors.Errorf("unknown source type '%s'", t)
	}
}

func (s SourceType) String() string {
	switch s {
	case SourceTypeSchemaSchema:
		return SchemaSchema
	case SourceTypeTableSchema:
		return TableSchema
	case SourceTypeCSV:
		return TypeCSV
	case SourceTypeSQL:
		return TypeSQL
	case SourceTypeParquet:
		return TypeParquet
	case SourceTypeViewSchema:
		return ViewSchema
	default:
		return TypeIgnore
	}
}

// ParseCompressionOnFileExtension parses the compression type from the file extension.
func ParseCompressionOnFileExtension(filename string) Compression {
	fileExt := strings.ToLower(filepath.Ext(filename))
	if len(fileExt) == 0 {
		return CompressionNone
	}
	tp, err := parseCompressionType(fileExt[1:])
	if err != nil {
		// file extension is not a compression type, just ignore it
		return CompressionNone
	}
	return tp
}

func parseCompressionType(t string) (Compression, error) {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "gz", "gzip":
		return CompressionGZ, nil
	case "lz4":
		return CompressionLZ4, nil
	case "zstd", "zst":
		return CompressionZStd, nil
	case "xz":
		return CompressionXZ, nil
	case "lzo":
		return CompressionLZO, nil
	case "snappy":
		return CompressionSnappy, nil
	case "":
		return CompressionNone, nil
	default:
		return CompressionNone, errors.Errorf("invalid compression type '%s'", t)
	}
}

var expandVariablePattern = regexp.MustCompile(`\$(?:\$|[\pL\p{Nd}_]+|\{[\pL\p{Nd}_]+\})`)

var defaultFileRouteRules = []*config.FileRouteRule{
	// ignore *-schema-trigger.sql, *-schema-post.sql files
	{Pattern: `(?i).*(-schema-trigger|-schema-post)\.sql(?:\.(\w*?))?$`, Type: "ignore"},
	// ignore backup files
	{Pattern: `(?i).*\.(sql|csv|parquet)(\.(\w+))?\.(bak|BAK)$`, Type: "ignore"},
	// db schema create file pattern, matches files like '{schema}-schema-create.sql[.{compress}]'
	{Pattern: `(?i)^(?:[^/]*/)*([^/.]+)-schema-create\.sql(?:\.(\w*?))?$`,
		Schema: "$1", Table: "", Type: SchemaSchema, Compression: "$2", Unescape: true},
	// table schema create file pattern, matches files like '{schema}.{table}-schema.sql[.{compress}]'
	{Pattern: `(?i)^(?:[^/]*/)*([^/.]+)\.(.*?)-schema\.sql(?:\.(\w*?))?$`,
		Schema: "$1", Table: "$2", Type: TableSchema, Compression: "$3", Unescape: true},
	// view schema create file pattern, matches files like '{schema}.{table}-schema-view.sql[.{compress}]'
	{Pattern: `(?i)^(?:[^/]*/)*([^/.]+)\.(.*?)-schema-view\.sql(?:\.(\w*?))?$`,
		Schema: "$1", Table: "$2", Type: ViewSchema, Compression: "$3", Unescape: true},
	// source file pattern, matches files like '{schema}.{table}.0001.{sql|csv}[.{compress}]'
	{Pattern: `(?i)^(?:[^/]*/)*([^/.]+)\.(.*?)(?:\.([0-9]+))?\.(sql|csv|parquet)(?:\.(\w+))?$`,
		Schema: "$1", Table: "$2", Type: "$4", Key: "$3", Compression: "$5", Unescape: true},
}

// FileRouter provides some operations to apply a rule to route file path to target schema/table
type FileRouter interface {
	// Route apply rule to path. Return nil if path doesn't match route rule;
	// return error if path match route rule but the captured value for field is invalid
	Route(path string) (*RouteResult, error)
}

// chainRouters aggregates multi `FileRouter` as a router
type chainRouters []FileRouter

func (c chainRouters) Route(path string) (*RouteResult, error) {
	for _, r := range c {
		res, err := r.Route(path)
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}
	}
	return nil, nil
}

// NewFileRouter creates a new file router with the rule.
func NewFileRouter(cfg []*config.FileRouteRule, logger log.Logger) (FileRouter, error) {
	res := make([]FileRouter, 0, len(cfg))
	p := regexRouterParser{}
	for _, c := range cfg {
		rule, err := p.Parse(c, logger)
		if err != nil {
			return nil, err
		}
		res = append(res, rule)
	}
	return chainRouters(res), nil
}

// NewDefaultFileRouter creates a new file router with the default file route rules.
func NewDefaultFileRouter(logger log.Logger) (FileRouter, error) {
	return NewFileRouter(defaultFileRouteRules, logger)
}

// RegexRouter is a `FileRouter` implement that apply specific regex pattern to filepath.
// if regex pattern match, then each extractors with capture the matched regexp pattern and
// set value to target field in `RouteResult`
type RegexRouter struct {
	pattern    *regexp.Regexp
	extractors []patExpander
}

// Route routes a file path to a source file type.
func (r *RegexRouter) Route(path string) (*RouteResult, error) {
	indexes := r.pattern.FindStringSubmatchIndex(path)
	if len(indexes) == 0 {
		return nil, nil
	}
	result := &RouteResult{}
	for _, e := range r.extractors {
		err := e.Expand(r.pattern, path, indexes, result)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

type regexRouterParser struct{}

func (p regexRouterParser) Parse(r *config.FileRouteRule, logger log.Logger) (*RegexRouter, error) {
	rule := &RegexRouter{}
	if r.Path == "" && r.Pattern == "" {
		return nil, errors.New("`path` and `pattern` must not be both empty in [[mydumper.files]]")
	}
	if r.Path != "" && r.Pattern != "" {
		return nil, errors.New("can't set both `path` and `pattern` field in [[mydumper.files]]")
	}
	if r.Path != "" {
		// convert constant string as a regexp pattern
		r.Pattern = regexp.QuoteMeta(r.Path)
		// escape all '$' by '$$' in match templates
		quoteTmplFn := func(t string) string { return strings.ReplaceAll(t, "$", "$$") }
		r.Table = quoteTmplFn(r.Table)
		r.Schema = quoteTmplFn(r.Schema)
		r.Type = quoteTmplFn(r.Type)
		r.Compression = quoteTmplFn(r.Compression)
		r.Key = quoteTmplFn(r.Key)
	}
	pattern, err := regexp.Compile(r.Pattern)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rule.pattern = pattern

	err = p.parseFieldExtractor(rule, "type", r.Type, func(result *RouteResult, value string) error {
		ty, err := parseSourceType(value)
		if err != nil {
			return err
		}
		result.Type = ty
		return nil
	})
	if err != nil {
		return nil, err
	}
	// ignore pattern needn't parse other fields
	if r.Type == TypeIgnore {
		return rule, nil
	}

	setValue := func(target *string, value string, unescape bool) {
		if unescape {
			val, err := url.PathUnescape(value)
			if err != nil {
				logger.Warn("unescape string failed, will be ignored", zap.String("value", value),
					zap.Error(err))
			} else {
				value = val
			}
		}
		*target = value
	}

	err = p.parseFieldExtractor(rule, "schema", r.Schema, func(result *RouteResult, value string) error {
		setValue(&result.Schema, value, r.Unescape)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// special case: when the pattern is for db schema, should not parse table name
	if r.Type != SchemaSchema {
		err = p.parseFieldExtractor(rule, "table", r.Table, func(result *RouteResult, value string) error {
			setValue(&result.Name, value, r.Unescape)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	if len(r.Key) > 0 {
		err = p.parseFieldExtractor(rule, "key", r.Key, func(result *RouteResult, value string) error {
			result.Key = value
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	if len(r.Compression) > 0 {
		err = p.parseFieldExtractor(rule, "compression", r.Compression, func(result *RouteResult, value string) error {
			// TODO: should support restore compressed source files
			compression, err := parseCompressionType(value)
			if err != nil {
				return err
			}
			if result.Type == SourceTypeParquet && compression != CompressionNone {
				return errors.Errorf("can't support whole compressed parquet file, should compress parquet files by choosing correct parquet compress writer, path: %s", r.Path)
			}
			result.Compression = compression
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return rule, nil
}

// parse each field extractor in `p.r` and set them to p.rule
func (p regexRouterParser) parseFieldExtractor(
	rule *RegexRouter,
	field,
	fieldPattern string,
	applyFn func(result *RouteResult, value string) error,
) error {
	// pattern is empty, return default rule
	if len(fieldPattern) == 0 {
		return errors.Errorf("field '%s' match pattern can't be empty", field)
	}

	// check and parse regexp template
	if err := p.checkSubPatterns(rule.pattern, fieldPattern); err != nil {
		return errors.Trace(err)
	}
	rule.extractors = append(rule.extractors, patExpander{
		template: fieldPattern,
		applyFn:  applyFn,
	})
	return nil
}

func (regexRouterParser) checkSubPatterns(pat *regexp.Regexp, t string) error {
	subPats := expandVariablePattern.FindAllString(t, -1)
	for _, subVar := range subPats {
		var tmplName string
		switch {
		case subVar == "$$":
			continue
		case strings.HasPrefix(subVar, "${"):
			tmplName = subVar[2 : len(subVar)-1]
		default:
			tmplName = subVar[1:]
		}
		if number, err := strconv.Atoi(tmplName); err == nil {
			if number > pat.NumSubexp() {
				return errors.Errorf("sub pattern capture '%s' out of range", subVar)
			}
		} else if !slice.AnyOf(pat.SubexpNames(), func(i int) bool {
			// FIXME: we should use re.SubexpIndex here, but not supported in go1.13 yet
			return pat.SubexpNames()[i] == tmplName
		}) {
			return errors.Errorf("invalid named capture '%s'", subVar)
		}
	}

	return nil
}

// patExpander extract string by expanding template with the regexp pattern
type patExpander struct {
	template string
	applyFn  func(result *RouteResult, value string) error
}

func (p *patExpander) Expand(pattern *regexp.Regexp, path string, matchIndex []int, result *RouteResult) error {
	value := pattern.ExpandString([]byte{}, p.template, path, matchIndex)
	return p.applyFn(result, string(value))
}

// RouteResult contains the information for a file routing.
type RouteResult struct {
	filter.Table
	Key         string
	Compression Compression
	Type        SourceType
}
