package mydump

import (
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
)

var _ = Suite(&testFileRouterSuite{})

type testFileRouterSuite struct{}

func (t *testFileRouterSuite) TestRouteParser(c *C) {
	// valid rules
	rules := []*config.FileRouteRule{
		{Pattern: `^(?:[^/]*/)*([^/.]+)\.([^./]+)(?:\.[0-9]+)?\.(csv|sql)`, Schema: "$1", Table: "$2", Type: "$3"},
		{Pattern: `^.+\.(csv|sql)`, Schema: "test", Table: "t", Type: "$1"},
		{Pattern: `^(?:[^/]*/)*(?P<schema>[^/.]+)\.(?P<table>[^./]+)(?:\.(?P<key>[0-9]+))?\.(?P<type>csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, Schema: "$schema", Table: "$table", Type: "$type", Key: "$key", Compression: "$cp"},
		{Pattern: `^(?:[^/]*/)*(?P<schema>[^/.]+)\.(?P<table>[^./]+)(?:\.([0-9]+))?\.(csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, Schema: "${schema}s", Table: "$table", Type: "${3}_0", Key: "$4", Compression: "$cp"},
		{Pattern: `^(?:[^/]*/)*([^/.]+)\.(?P<table>[^./]+)(?:\.([0-9]+))?\.(csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, Schema: "${1}s", Table: "$table", Type: "${3}_0", Key: "$4", Compression: "$cp"},
		{Pattern: `^(?:[^/]*/)*([^/.]+)\.([^./]+)(?:\.[0-9]+)?\.(csv|sql)`, Schema: "$1-schema", Table: "$1-table", Type: "$2"},
	}
	for _, r := range rules {
		_, err := NewFileRouter([]*config.FileRouteRule{r})
		c.Assert(err, IsNil)
	}

	// invalid rules
	invalidRules := []*config.FileRouteRule{
		{Pattern: `^(?:[^/]*/)*(?P<schema>\.(?P<table>[^./]+).*$`, Schema: "$test", Table: "$table"},
		{Pattern: `^(?:[^/]*/)*(?P<schema>[^/.]+)\.(?P<table>[^./]+).*$`, Schema: "$schemas", Table: "$table"},
		{Pattern: `^(?:[^/]*/)*([^/.]+)\.([^./]+)(?:\.[0-9]+)?\.(csv|sql)`, Schema: "$1", Table: "$2", Type: "$3", Key: "$4"},
	}
	for _, r := range invalidRules {
		_, err := NewFileRouter([]*config.FileRouteRule{r})
		c.Assert(err, NotNil)
	}
}

func (t *testFileRouterSuite) TestInvalidRouteRule(c *C) {
	rule := &config.FileRouteRule{}
	rules := []*config.FileRouteRule{rule}
	_, err := NewFileRouter(rules)
	c.Assert(err, ErrorMatches, "`path` and `pattern` must not be both empty in \\[\\[mydumper.files\\]\\]")

	rule.Pattern = `^(?:[^/]*/)*([^/.]+)\.(?P<table>[^./]+)(?:\.(?P<key>[0-9]+))?\.(?P<type>csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`
	_, err = NewFileRouter(rules)
	c.Assert(err, ErrorMatches, "field 'type' match pattern can't be empty")

	rule.Type = "$type"
	_, err = NewFileRouter(rules)
	c.Assert(err, ErrorMatches, "field 'schema' match pattern can't be empty")

	rule.Schema = "$schema"
	_, err = NewFileRouter(rules)
	c.Assert(err, ErrorMatches, "invalid named capture '\\$schema'")

	rule.Schema = "$1"
	_, err = NewFileRouter(rules)
	c.Assert(err, ErrorMatches, "field 'table' match pattern can't be empty")

	rule.Table = "$table"
	_, err = NewFileRouter(rules)
	c.Assert(err, IsNil)

	rule.Path = "/tmp/1.sql"
	_, err = NewFileRouter(rules)
	c.Assert(err, ErrorMatches, "can't set both `path` and `pattern` field in \\[\\[mydumper.files\\]\\]")
}

func (t *testFileRouterSuite) TestSingleRouteRule(c *C) {
	rules := []*config.FileRouteRule{
		{Pattern: `^(?:[^/]*/)*([^/.]+)\.(?P<table>[^./]+)(?:\.(?P<key>[0-9]+))?\.(?P<type>csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, Schema: "$1", Table: "$table", Type: "$type", Key: "$key", Compression: "$cp"},
	}

	r, err := NewFileRouter(rules)
	c.Assert(err, IsNil)

	inputOutputMap := map[string][]string{
		"my_schema.my_table.sql":           {"my_schema", "my_table", "", "", "sql"},
		"/test/123/my_schema.my_table.sql": {"my_schema", "my_table", "", "", "sql"},
		"my_dir/my_schema.my_table.csv":    {"my_schema", "my_table", "", "", "csv"},
		"my_schema.my_table.0001.sql":      {"my_schema", "my_table", "0001", "", "sql"},
	}
	for path, fields := range inputOutputMap {
		res, err := r.Route(path)
		c.Assert(err, IsNil)
		compress, e := parseCompressionType(fields[3])
		c.Assert(e, IsNil)
		ty, e := parseSourceType(fields[4])
		c.Assert(e, IsNil)
		exp := &RouteResult{filter.Table{Schema: fields[0], Name: fields[1]}, fields[2], compress, ty}
		c.Assert(res, DeepEquals, exp)
	}

	notMatchPaths := []string{
		"my_table.sql",
		"/schema/table.sql",
		"my_schema.my_table.txt",
		"my_schema.my_table.001.txt",
		"my_schema.my_table.0001-002.sql",
	}
	for _, p := range notMatchPaths {
		res, err := r.Route(p)
		c.Assert(res, IsNil)
		c.Assert(err, IsNil)
	}

	rule := &config.FileRouteRule{Pattern: `^(?:[^/]*/)*([^/.]+)\.(?P<table>[^./]+)(?:\.(?P<key>[0-9]+))?\.(?P<type>\w+)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, Schema: "$1", Table: "$table", Type: "$type", Key: "$key", Compression: "$cp"}
	r, err = NewFileRouter([]*config.FileRouteRule{rule})
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	invalidMatchPaths := []string{
		"my_schema.my_table.sql.gz",
		"my_schema.my_table.sql.rar",
		"my_schema.my_table.txt",
	}
	for _, p := range invalidMatchPaths {
		res, err := r.Route(p)
		c.Assert(res, IsNil)
		c.Assert(err, NotNil)
	}
}

func (t *testFileRouterSuite) TestMultiRouteRule(c *C) {
	// multi rule don't intersect with each other
	rules := []*config.FileRouteRule{
		{Pattern: `(?:[^/]*/)*([^/.]+)-schema-create\.sql`, Schema: "$1", Type: SchemaSchema},
		{Pattern: `(?:[^/]*/)*([^/.]+)\.([^/.]+)-schema\.sql$`, Schema: "$1", Table: "$2", Type: TableSchema},
		{Pattern: `(?:[^/]*/)*([^/.]+)\.([^/.]+)-schema-view\.sql$`, Schema: "$1", Table: "$2", Type: ViewSchema},
		{Pattern: `^(?:[^/]*/)*(?P<schema>[^/.]+)\.(?P<table>[^./]+)(?:\.(?P<key>[0-9]+))?\.(?P<type>csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, Schema: "$schema", Table: "$table", Type: "$type", Key: "$key", Compression: "$cp"},
	}

	r, err := NewFileRouter(rules)
	c.Assert(err, IsNil)

	inputOutputMap := map[string][]string{
		"test-schema-create.sql":           {"test", "", "", "", SchemaSchema},
		"test.t-schema.sql":                {"test", "t", "", "", TableSchema},
		"test.v1-schema-view.sql":          {"test", "v1", "", "", ViewSchema},
		"my_schema.my_table.sql":           {"my_schema", "my_table", "", "", "sql"},
		"/test/123/my_schema.my_table.sql": {"my_schema", "my_table", "", "", "sql"},
		"my_dir/my_schema.my_table.csv":    {"my_schema", "my_table", "", "", "csv"},
		"my_schema.my_table.0001.sql":      {"my_schema", "my_table", "0001", "", "sql"},
		// "my_schema.my_table.0001.sql.gz":      {"my_schema", "my_table", "0001", "gz", "sql"},
	}
	for path, fields := range inputOutputMap {
		res, err := r.Route(path)
		c.Assert(err, IsNil)
		if len(fields) == 0 {
			c.Assert(res, IsNil)
		} else {
			compress, e := parseCompressionType(fields[3])
			c.Assert(e, IsNil)
			ty, e := parseSourceType(fields[4])
			c.Assert(e, IsNil)
			exp := &RouteResult{filter.Table{Schema: fields[0], Name: fields[1]}, fields[2], compress, ty}
			c.Assert(res, DeepEquals, exp)
		}
	}

	// multi rule don't intersect with each other
	// add another rule that math same pattern with the third rule, the result should be no different
	p := &config.FileRouteRule{Pattern: `^(?P<schema>[^/.]+)\.(?P<table>[^./]+)(?:\.(?P<key>[0-9]+))?\.(?P<type>csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, Schema: "test_schema", Table: "test_table", Type: "$type", Key: "$key", Compression: "$cp"}
	rules = append(rules, p)
	r, err = NewFileRouter(rules)
	c.Assert(err, IsNil)
	for path, fields := range inputOutputMap {
		res, err := r.Route(path)
		c.Assert(err, IsNil)
		if len(fields) == 0 {
			c.Assert(res, IsNil)
		} else {
			compress, e := parseCompressionType(fields[3])
			c.Assert(e, IsNil)
			ty, e := parseSourceType(fields[4])
			c.Assert(e, IsNil)
			exp := &RouteResult{filter.Table{Schema: fields[0], Name: fields[1]}, fields[2], compress, ty}
			c.Assert(res, DeepEquals, exp)
		}
	}
}

func (t *testFileRouterSuite) TestRouteExpanding(c *C) {
	rule := &config.FileRouteRule{
		Pattern:     `^(?:[^/]*/)*(?P<schema>[^/.]+)\.(?P<table_name>[^./]+)(?:\.(?P<key>[0-9]+))?\.(?P<type>csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`,
		Schema:      "$schema",
		Type:        "$type",
		Key:         "$key",
		Compression: "$cp",
	}
	path := "db.table.001.sql"
	tablePatternResMap := map[string]string{
		"$schema":             "db",
		"$table_name":         "table",
		"$schema.$table_name": "db.table",
		"${1}":                "db",
		"${1}_$table_name":    "db_table",
		"${2}.schema":         "table.schema",
		"$${2}":               "${2}",
		"$$table_name":        "$table_name",
		"$table_name-123":     "table-123",
		"$$12$1$schema":       "$12dbdb",
		"${table_name}$$2":    "table$2",
		"${table_name}$$":     "table$",
		"{1}$$":               "{1}$",
		"my_table":            "my_table",
	}

	for pat, value := range tablePatternResMap {
		rule.Table = pat
		router, err := NewFileRouter([]*config.FileRouteRule{rule})
		c.Assert(err, IsNil)
		res, err := router.Route(path)
		c.Assert(err, IsNil)
		c.Assert(res, NotNil)
		c.Assert(res.Name, Equals, value)
	}

	invalidPatterns := []string{"$1_$schema", "$schema_$table_name", "$6"}
	for _, pat := range invalidPatterns {
		rule.Table = pat
		_, err := NewFileRouter([]*config.FileRouteRule{rule})
		c.Assert(err, NotNil)
	}
}

func (t *testFileRouterSuite) TestRouteWithPath(c *C) {
	fileName := "myschema.(my_table$1).000.sql"
	rule := &config.FileRouteRule{
		Path:   fileName,
		Schema: "schema",
		Table:  "my_table$1",
		Type:   "sql",
		Key:    "$key",
	}
	r := *rule
	router, err := NewFileRouter([]*config.FileRouteRule{&r})
	c.Assert(err, IsNil)
	res, err := router.Route(fileName)
	c.Assert(err, IsNil)
	c.Assert(res, NotNil)
	c.Assert(res.Schema, Equals, rule.Schema)
	c.Assert(res.Name, Equals, rule.Table)
	ty, _ := parseSourceType(rule.Type)
	c.Assert(res.Type, Equals, ty)
	c.Assert(res.Key, Equals, rule.Key)

	// replace all '.' by '-', if with plain regex pattern, will still match
	res, err = router.Route(strings.ReplaceAll(fileName, ".", "-"))
	c.Assert(err, IsNil)
	c.Assert(res, IsNil)
}
