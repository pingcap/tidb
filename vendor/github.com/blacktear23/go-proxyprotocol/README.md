# go-proxyprotocol

PROXY protocol implementation in Go.

## Usage

import

```go
import (
	proxyprotocol "github.com/blacktear23/go-proxyprotocol"
)
```

basic usage

```go
// Create listener
l, err := net.Listen("tcp", "...")

// Wrap listener as PROXY protocol listener
ppl, err := proxyprotocol.NewListener(l, "*", 5)

for {
    conn, err := ppl.Accept()
    if err != nil {
        // PROXY protocol related errors can be output by log and
        // continue accept next one.
        if proxyprotocol.IsProxyProtocolError(err) {
            log.Errorf("PROXY protocol error: %s", err.Error())
            continue
        }
        panic(err)
    }
    go processConn(conn)
}
```
