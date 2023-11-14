# Column Mapping

## introduction

column mapping is a library to provide a simple and unified way to mapping columns of table:

- add prefix for one char/varchar/text column

- add suffix for one char/varchar/text column

- partition ID (used for sharding schema/table, would partition these tables with a custom ID), only for id(int64)

## column mapping rule

we define a rule `Rule` to show how to map column

```go
type Rule struct {
	PatternSchema    string   `yaml:"schema-pattern" json:"schema-pattern" toml:"schema-pattern"`
	PatternTable     string   `yaml:"table-pattern" json:"table-pattern" toml:"table-pattern"`
	SourceColumn     string   `yaml:"source-column" json:"source-column" toml:"source-column"` // modify, add refer column, ignore
	TargetColumn     string   `yaml:"target-column" json:"target-column" toml:"target-column"` // add column, modify
	Expression       Expr     `yaml:"expression" json:"expression" toml:"expression"`
	Arguments        []string `yaml:"arguments" json:"arguments" toml:"arguments"`
	CreateTableQuery string   `yaml:"create-table-query" json:"create-table-query" toml:"create-table-query"`
}
```

now we support following expressions

``` go
add prefix, with arguments[prefix]

add suffix, with arguments[suffix]

partition id, with arguments [instance_id, prefix of schema, prefix of table, separator]
[1:1 bit][2:9 bits][3:10 bits][4:44 bits] int64  (using default bits length)
- 1: useless, no reason
- 2: schema ID (schema suffix)
- 3: table ID (table suffix)
- 4: origin ID (>= 0, <= 17592186044415)
And
- schema = arguments[1] + arguments[3] + schema suffix    or    arguments[1]
- table  = arguments[2] + arguments[3] + table suffix     or    arguments[2]
The separator argument defaults to an empty string if omitted.
```

## notice
* only support above poor expressions now
* column mapping doesn't change column type and table structure now
