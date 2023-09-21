# Table Filter

A table filter is an interface which determines if a table or schema should be
accepted for some process or not given its name.

This package defines the format allowing users to specify the filter criteria
via command line or config files. This package is used by all tools in the TiDB
ecosystem.

## Examples

```go
package main

import (
    "fmt"

    "github.com/pingcap/tidb/util/table-filter"
    "github.com/spf13/pflag"
)

func main() {
    args := pflag.StringArrayP("filter", "f", []string{"*.*"}, "table filter")
    pflag.Parse()

    f, err := filter.Parse(*args)
    if err != nil {
            panic(err)
    }
    f = filter.CaseInsensitive(f)

    tables := []filter.Table{
        {Schema: "employees", Name: "employees"},
        {Schema: "employees", Name: "departments"},
        {Schema: "employees", Name: "dept_manager"},
        {Schema: "employees", Name: "dept_emp"},
        {Schema: "employees", Name: "titles"},
        {Schema: "employees", Name: "salaries"},
        {Schema: "AdventureWorks.Person", Name: "Person"},
        {Schema: "AdventureWorks.Person", Name: "Password"},
        {Schema: "AdventureWorks.Sales", Name: "SalesOrderDetail"},
        {Schema: "AdventureWorks.Sales", Name: "SalesOrderHeader"},
        {Schema: "AdventureWorks.Production", Name: "WorkOrder"},
        {Schema: "AdventureWorks.Production", Name: "WorkOrderRouting"},
        {Schema: "AdventureWorks.Production", Name: "ProductPhoto"},
        {Schema: "AdventureWorks.Production", Name: "TransactionHistory"},
        {Schema: "AdventureWorks.Production", Name: "TransactionHistoryArchive"},
    }

    for _, table := range tables {
        fmt.Printf("%5v: %v\n", f.MatchTable(table.Schema, table.Name), table)
    }
}
```

Try to run with `./main -f 'employee.*' -f '*.WorkOrder'` and see the result.

## Syntax

### Allowlist

The input to the `filter.Parse()` function is a list of table filter rules.
Each rule specifies what the fully-qualified name of the table to be accepted.

```
db1.tbl1
db2.tbl2
db3.tbl3
```

A plain name must only consist of valid [identifier characters]
`[0-9a-zA-Z$_\U00000080-\U0010ffff]+`. All other ASCII characters are reserved.
Some punctuations have special meanings, described below.

### Wildcards

Each part of the name can be a wildcard symbol as in [fnmatch(3)]:
* `*` — matches zero or more characters
* `?` — matches one character
* `[a-z]` — matches one character between “a” and “z” inclusive
* `[!a-z]` — matches one character except “a” to “z”.

```
db[0-9].tbl[0-9][0-9]
data.*
*.backup_*
```

“Character” here means a Unicode code point, so e.g.
* U+00E9 (é) is 1 character.
* U+0065 U+0301 (é) are 2 characters.
* U+1F926 U+1F3FF U+200D U+2640 U+FE0F (🤦🏿‍♀️) are 5 characters.

### File import

Include an `@` at the beginning of the string to specify a file name, which
`filter.Parse()` reads every line as filter rules.

For example, if a file `config/filter.txt` has content:

```
employees.*
*.WorkOrder
```

the following two invocations would be equivalent:

```sh
./main -f '@config/filter.txt'
./main -f 'employees.*' -f '*.WorkOrder'
```

A filter file cannot further import another file.

### Comments and blank lines

Leading and trailing white-spaces of every line are trimmed.

Blank lines (empty strings) are ignored.

A leading `#` marks a comment and is ignored.
`#` not at start of line may be considered syntax error.

### Blocklist

An `!` at the beginning of the line means the pattern after it is used to
exclude tables from being processed. This effectively turns the filter into a
blocklist.

```ini
*.*
#^ note: must add the *.* to include all tables first
!*.Password
!employees.salaries
```

### Escape character

Precede any special character by a `\` to turn it into an identifier character.

```
AdventureWorks\.*.*
```

For simplicity and future compatibility, the following sequences are prohibited:
* `\` at the end of the line after trimming whitespaces (use “`[ ]`” to match a literal whitespace at the end).
* `\` followed by any ASCII alphanumeric character (`[0-9a-zA-Z]`). In particular, C-like escape sequences like `\0`, `\r`, `\n` and `\t` currently are meaningless.

### Quoted identifier

Besides `\`, special characters can also be escaped by quoting using `"` or `` ` ``.

```
"AdventureWorks.Person".Person
`AdventureWorks.Person`.Password
```

Quoted identifier cannot span multiple lines.

It is invalid to partially quote an identifier.

```
"this is "invalid*.*
```

### Regular expression

Use `/` to delimit regular expressions:

```
/^db\d{2,}$/./^tbl\d{2,}$/
```

These regular expressions use the [Go dialect]. The pattern is matched if the
identifier contains a substring matching the regular expression. For instance,
`/b/` matches `db01`.

(Note: every `/` in the regex must be escaped as `\/`, including inside `[`…`]`.
You cannot place an unescaped `/` between `\Q`…`\E`.)

[identifier characters]: https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
[fnmatch(3)]: https://pubs.opengroup.org/onlinepubs/9699919799/utilities/V3_chap02.html#tag_18_13
[Go dialect]: https://pkg.go.dev/regexp/syntax?tab=doc

## Algorithm

### Default behavior

When a table name matches none of the rules in the filter list, the default
behavior is to ignore such unmatched tables.

To build a blocklist, an explicit `*.*` must be used as the first rule,
otherwise all tables will be excluded.

```sh
# every table will be filtered out
./main -f '!*.Password'

# only the "Password" table is filtered out, the rest are included.
./main -f '*.*' -f '!*.Password'
```

### Precedence

In a filter list, if a table name matches multiple patterns, the last match
decides the outcome. For instance, given

```ini
# rule 1
employees.*
# rule 2
!*.dep*
# rule 3
*.departments
```

We get:

| Table name            | Rule 1 | Rule 2 | Rule 3 | Outcome          |
|-----------------------|--------|--------|--------|------------------|
| irrelevant.table      |        |        |        | Default (reject) |
| employees.employees   | ✓      |        |        | Rule 1 (accept)  |
| employees.dept_emp    | ✓      | ✓      |        | Rule 2 (reject)  |
| employees.departments | ✓      | ✓      | ✓      | Rule 3 (accept)  |
| else.departments      |        | ✓      | ✓      | Rule 3 (accept)  |
