// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/pingcap/errors"

	tcontext "github.com/pingcap/tidb/dumpling/context"
)

const (
	outputFileTemplateSchema   = "schema"
	outputFileTemplateTable    = "table"
	outputFileTemplateView     = "view"
	outputFileTemplateSequence = "sequence"
	outputFileTemplateData     = "data"
	outputFileTemplatePolicy   = "placement-policy"

	defaultOutputFileTemplateBase = `
		{{- define "objectName" -}}
			{{fn .DB}}.{{fn .Table}}
		{{- end -}}
		{{- define "schema" -}}
			{{fn .DB}}-schema-create
		{{- end -}}
		{{- define "event" -}}
			{{template "objectName" .}}-schema-post
		{{- end -}}
		{{- define "function" -}}
			{{template "objectName" .}}-schema-post
		{{- end -}}
		{{- define "procedure" -}}
			{{template "objectName" .}}-schema-post
		{{- end -}}
		{{- define "sequence" -}}
			{{template "objectName" .}}-schema-sequence
		{{- end -}}
		{{- define "trigger" -}}
			{{template "objectName" .}}-schema-triggers
		{{- end -}}
		{{- define "view" -}}
			{{template "objectName" .}}-schema-view
		{{- end -}}
		{{- define "table" -}}
			{{template "objectName" .}}-schema
		{{- end -}}
		{{- define "data" -}}
			{{template "objectName" .}}.{{.Index}}
		{{- end -}}
		{{- define "placement-policy" -}}
            {{fn .Policy}}-placement-policy-create
		{{- end -}}
	`

	// DefaultAnonymousOutputFileTemplateText is the default anonymous output file templateText for dumpling's table data file name
	DefaultAnonymousOutputFileTemplateText = "result.{{.Index}}"
)

var (
	filenameEscapeRegexp = regexp.MustCompile(`[\x00-\x1f%"*./:<>?\\|]|-(?i:schema)`)
	// DefaultOutputFileTemplate is the default output file template for dumpling's table data file name
	DefaultOutputFileTemplate = template.Must(template.New("data").
					Option("missingkey=error").
					Funcs(template.FuncMap{
			"fn": func(input string) string {
				return filenameEscapeRegexp.ReplaceAllStringFunc(input, func(match string) string {
					return fmt.Sprintf("%%%02X%s", match[0], match[1:])
				})
			},
		}).
		Parse(defaultOutputFileTemplateBase))
)

// ParseOutputFileTemplate parses template from the specified text
func ParseOutputFileTemplate(text string) (*template.Template, error) {
	return template.Must(DefaultOutputFileTemplate.Clone()).Parse(text)
}

func prepareDumpingDatabases(tctx *tcontext.Context, conf *Config, db *sql.Conn) ([]string, error) {
	databases, err := ShowDatabases(db)
	if err != nil {
		return nil, err
	}
	databases = filterDatabases(tctx, conf, databases)
	if len(conf.Databases) == 0 {
		return databases, nil
	}
	dbMap := make(map[string]interface{}, len(databases))
	for _, database := range databases {
		dbMap[database] = struct{}{}
	}
	var notExistsDatabases []string
	for _, database := range conf.Databases {
		if _, ok := dbMap[database]; !ok {
			notExistsDatabases = append(notExistsDatabases, database)
		}
	}
	if len(notExistsDatabases) > 0 {
		return nil, errors.Errorf("Unknown databases [%s]", strings.Join(notExistsDatabases, ","))
	}
	return conf.Databases, nil
}

type databaseName = string

// TableType represents the type of table
type TableType int8

const (
	// TableTypeBase represents the basic table
	TableTypeBase TableType = iota
	// TableTypeView represents the view table
	TableTypeView
	// TableTypeSequence represents the view table
	// TODO: need to be supported
	TableTypeSequence
)

const (
	// TableTypeBaseStr represents the basic table string
	TableTypeBaseStr = "BASE TABLE"
	// TableTypeViewStr represents the view table string
	TableTypeViewStr = "VIEW"
	// TableTypeSequenceStr represents the view table string
	TableTypeSequenceStr = "SEQUENCE"
)

func (t TableType) String() string {
	switch t {
	case TableTypeBase:
		return TableTypeBaseStr
	case TableTypeView:
		return TableTypeViewStr
	case TableTypeSequence:
		return TableTypeSequenceStr
	default:
		return "UNKNOWN"
	}
}

// ParseTableType parses table type string to TableType
func ParseTableType(s string) (TableType, error) {
	switch s {
	case TableTypeBaseStr:
		return TableTypeBase, nil
	case TableTypeViewStr:
		return TableTypeView, nil
	case TableTypeSequenceStr:
		return TableTypeSequence, nil
	default:
		return TableTypeBase, errors.Errorf("unknown table type %s", s)
	}
}

// TableInfo is the table info for a table in database
type TableInfo struct {
	Name         string
	AvgRowLength uint64
	Type         TableType
}

// Equals returns true the table info is the same with another one
func (t *TableInfo) Equals(other *TableInfo) bool {
	return t.Name == other.Name && t.Type == other.Type
}

// DatabaseTables is the type that represents tables in a database
type DatabaseTables map[databaseName][]*TableInfo

// NewDatabaseTables returns a new DatabaseTables
func NewDatabaseTables() DatabaseTables {
	return DatabaseTables{}
}

// AppendTable appends a TableInfo to DatabaseTables
func (d DatabaseTables) AppendTable(dbName string, table *TableInfo) DatabaseTables {
	d[dbName] = append(d[dbName], table)
	return d
}

// AppendTables appends several basic tables to DatabaseTables
func (d DatabaseTables) AppendTables(dbName string, tableNames []string, avgRowLengths []uint64) DatabaseTables {
	for i, t := range tableNames {
		d[dbName] = append(d[dbName], &TableInfo{t, avgRowLengths[i], TableTypeBase})
	}
	return d
}

// AppendViews appends several views to DatabaseTables
func (d DatabaseTables) AppendViews(dbName string, viewNames ...string) DatabaseTables {
	for _, v := range viewNames {
		d[dbName] = append(d[dbName], &TableInfo{v, 0, TableTypeView})
	}
	return d
}

// Merge merges another DatabaseTables
func (d DatabaseTables) Merge(other DatabaseTables) {
	for name, infos := range other {
		d[name] = append(d[name], infos...)
	}
}

// Literal returns a user-friendly output for DatabaseTables
func (d DatabaseTables) Literal() string {
	var b strings.Builder
	b.WriteString("tables list\n")
	b.WriteString("\n")

	for dbName, tables := range d {
		b.WriteString("schema ")
		b.WriteString(dbName)
		b.WriteString(" :[")
		for _, tbl := range tables {
			b.WriteString(tbl.Name)
			b.WriteString(", ")
		}
		b.WriteString("]")
	}

	return b.String()
}

// DatabaseTablesToMap transfers DatabaseTables to Map
func DatabaseTablesToMap(d DatabaseTables) map[string]map[string]struct{} {
	mp := make(map[string]map[string]struct{}, len(d))
	for name, infos := range d {
		mp[name] = make(map[string]struct{}, len(infos))
		for _, info := range infos {
			if info.Type == TableTypeBase {
				mp[name][info.Name] = struct{}{}
			}
		}
	}
	return mp
}
