# diff
## introduction
diff is a library to provide a function to compare tables:
- support comapre table's data and struct
- can generate sql for target table to fix data
- support compare tables with different name
- support compare one target table with multipe source tables


To comapre tables, you should construct a TableDiff struct:
```go
// TableDiff saves config for diff table
type TableDiff struct {
	// source tables
	SourceTables []*TableInstance
	// target table
	TargetTable *TableInstance

	// columns be ignored, will not check this column's data
	IgnoreColumns []string

	// field should be the primary key, unique key or field with index
	Field string

	// select range, for example: "age > 10 AND age < 20"
	Range string

	// for example, the whole data is [1...100]
	// we can split these data to [1...10], [11...20], ..., [91...100]
	// the [1...10] is a chunk, and it's chunk size is 10
	// size of the split chunk
	ChunkSize int

	// sampling check percent, for example 10 means only check 10% data
	Sample int

	// how many goroutines are created to check data
	CheckThreadCount int

	// set false if want to comapre the data directly
    UseChecksum bool
    
    // collation config in mysql/tidb, should corresponding to charset.
	Collation string

	// ignore check table's struct
	IgnoreStructCheck bool

	// ignore check table's data
	IgnoreDataCheck bool
}
```

Then call TableDiff's function Equal to get the compare result. The Equal function define as:
```go
func (t *TableDiff) Equal(ctx context.Context, writeFixSQL func(string) error) (structEqual bool, dataEqual bool, err error)
```
