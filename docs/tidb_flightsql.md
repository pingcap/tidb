# Apache Arrow Flight SQL Support

TiDB supports [Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html), a high-performance protocol for interacting with SQL databases using Apache Arrow's columnar format. This is an optional feature that must be explicitly enabled at build time.

## Building TiDB with Flight SQL Support

Flight SQL support is enabled via a build tag. To build TiDB with Flight SQL:

```bash
make server-flightsql
```

This will produce a `bin/tidb-server` binary with Flight SQL capabilities.

To build without Flight SQL (default):

```bash
make server
```

## Configuration

When built with Flight SQL support, TiDB listens on an additional port for Flight SQL connections.

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `flight-sql-port` | 4001 | Port for Flight SQL connections. Set to 0 to disable. |

Example `tidb.toml` configuration:

```toml
[server]
flight-sql-port = 4001
```

## Connecting to TiDB via Flight SQL

### Using the Go FlightSQL Driver

```go
import (
    "database/sql"
    _ "github.com/apache/arrow-go/v18/arrow/flight/flightsql/driver"
)

func main() {
    // Connect with username and password
    db, err := sql.Open("flightsql", "flightsql://user:password@localhost:4001")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Execute queries as normal
    rows, err := db.Query("SELECT * FROM test.users")
    // ...
}
```

### Connection String Format

```
flightsql://[user[:password]@]host:port[?param=value]
```

Examples:
- `flightsql://root@localhost:4001` - Connect as root with no password
- `flightsql://admin:secret@tidb.example.com:4001` - Connect with credentials
- `flightsql://root@localhost:4001?timeout=30s` - With connection timeout

## Authentication

Flight SQL uses the same authentication as TiDB's MySQL protocol:

1. **Basic Authentication**: Username and password in the connection string
2. **Session Tokens**: After initial authentication, the server issues session tokens for subsequent requests

## Supported Features

### Currently Supported

- Basic SQL queries (SELECT)
- DML statements (INSERT, UPDATE, DELETE)
- Prepared statements with parameters
- Authentication via username/password
- Session token-based authentication

### Data Type Mappings

| MySQL Type | Arrow Type |
|------------|------------|
| TINYINT, SMALLINT, INT, BIGINT | Int64 |
| FLOAT | Float32 |
| DOUBLE, DECIMAL | Float64 |
| VARCHAR, TEXT, BLOB | String |
| DATE, DATETIME, TIMESTAMP | Date64 |
| JSON | String (serialized) |
| BIT | Int64 |

### Not Yet Implemented

- Transaction support (BEGIN/COMMIT/ROLLBACK)
- Catalog/schema introspection (GetTables, GetSchemas, etc.)
- TLS/SSL connections
- Substrait query plans

## Performance Considerations

Flight SQL uses Apache Arrow's columnar format for data transfer, which can provide significant performance benefits for:

- Analytical queries returning large result sets
- Integration with data processing frameworks (Apache Spark, DuckDB, etc.)
- Columnar data pipelines

For OLTP workloads with small result sets, the traditional MySQL protocol may be more efficient due to lower overhead.

## Troubleshooting

### Connection Refused

Ensure Flight SQL support is enabled:
1. Check that TiDB was built with `make server-flightsql`
2. Verify `flight-sql-port` is configured and not 0
3. Check firewall rules allow connections to the Flight SQL port

### Authentication Errors

- Verify the user exists and has appropriate privileges
- For session token signing, ensure `security.session-token-signing-cert` and `security.session-token-signing-key` are configured

## Dependencies

When built with Flight SQL support, TiDB includes additional dependencies:

- `github.com/apache/arrow-go/v18` - Apache Arrow Go implementation
- gRPC libraries for the Flight protocol

These dependencies are not included when building without the `flightsql` tag.
