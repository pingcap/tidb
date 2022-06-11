package mydump

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/filter"
	"github.com/pingcap/tidb/util/slice"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
)

type SourceType int

const (
	SourceTypeIgnore SourceType = iota
	SourceTypeSchemaSchema
	SourceTypeTableSchema
	SourceTypeSQL
	SourceTypeCSV
	SourceTypeParquet
	SourceTypeViewSchema
)

const (
	SchemaSchema = "schema-schema"
	TableSchema  = "table-schema"
	ViewSchema   = "view-schema"
	TypeSQL      = "sql"
	TypeCSV      = "csv"
	TypeParquet  = "parquet"
	TypeIgnore   = "ignore"
)

type Compression int

const (
	CompressionNone Compression = iota
	CompressionGZ
	CompressionLZ4
	CompressionZStd
	CompressionXZ
)

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

func parseCompressionType(t string) (Compression, error) {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "gz":
		return CompressionGZ, nil
	case "lz4":
		return CompressionLZ4, nil
	case "zstd":
		return CompressionZStd, nil
	case "xz":
		return CompressionXZ, nil
	case "":
		return CompressionNone, nil
	default:
		return CompressionNone, errors.Errorf("invalid compression type '%s'", t)
	}
}

var expandVariablePattern = regexp.MustCompile(`\$(?:\$|[\pL\p{Nd}_]+|\{[\pL\p{Nd}_]+\})`)

var defaultFileRouteRules = []*config.FileRouteRule{
	// ignore *-schema-trigger.sql, *-schema-post.sql files
	{Pattern: `(?i).*(-schema-trigger|-schema-post)\.sql$`, Type: "ignore"},
	// db schema create file pattern, matches files like '{schema}-schema-create.sql'
	{Pattern: `(?i)^(?:[^/]*/)*([^/.]+)-schema-create\.sql$`, Schema: "$1", Table: "", Type: SchemaSchema, Unescape: true},
	// table schema create file pattern, matches files like '{schema}.{table}-schema.sql'
	{Pattern: `(?i)^(?:[^/]*/)*([^/.]+)\.(.*?)-schema\.sql$`, Schema: "$1", Table: "$2", Type: TableSchema, Unescape: true},
	// view schema create file pattern, matches files like '{schema}.{table}-schema-view.sql'
	{Pattern: `(?i)^(?:[^/]*/)*([^/.]+)\.(.*?)-schema-view\.sql$`, Schema: "$1", Table: "$2", Type: ViewSchema, Unescape: true},
	// source file pattern, matches files like '{schema}.{table}.0001.{sql|csv}'
	{Pattern: `(?i)^(?:[^/]*/)*([^/.]+)\.(.*?)(?:\.([0-9]+))?\.(sql|csv|parquet)$`, Schema: "$1", Table: "$2", Type: "$4", Key: "$3", Unescape: true},
}

// // RouteRule is a rule to route file path to target schema/table
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

func NewFileRouter(cfg []*config.FileRouteRule) (FileRouter, error) {
	res := make([]FileRouter, 0, len(cfg))
	p := regexRouterParser{}
	for _, c := range cfg {
		rule, err := p.Parse(c)
		if err != nil {
			return nil, err
		}
		res = append(res, rule)
	}
	return chainRouters(res), nil
}

// `RegexRouter` is a `FileRouter` implement that apply specific regex pattern to filepath.
// if regex pattern match, then each extractors with capture the matched regexp pattern and
// set value to target field in `RouteResult`
type RegexRouter struct {
	pattern    *regexp.Regexp
	extractors []patExpander
}

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

func (p regexRouterParser) Parse(r *config.FileRouteRule) (*RegexRouter, error) {
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
				log.L().Warn("unescape string failed, will be ignored", zap.String("value", value),
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
			if compression != CompressionNone {
				return errors.New("Currently we don't support restore compressed source file yet")
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

func (p regexRouterParser) checkSubPatterns(pat *regexp.Regexp, t string) error {
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

type RouteResult struct {
	filter.Table
	Key         string
	Compression Compression
	Type        SourceType
}
