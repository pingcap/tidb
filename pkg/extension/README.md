# Extension Framework Usage

<!-- TOC -->
* [Extension Framework Usage](#extension-framework-usage)
    * [Introduction](#introduction)
    * [Usage Guidelines](#usage-guidelines)
        * [A Simple Example](#a-simple-example)
        * [Further Explanation about Session Handler](#further-explanation-about-session-handler)
        * [Registering Extensions with Options](#registering-extensions-with-options)
        * [Key Extension Options](#key-extension-options)
<!-- TOC -->

## Introduction

The Extension Framework facilitates the seamless integration of standalone code with TiDB. This framework is useful when:

- You aim to extend TiDB functionality but prefer to encapsulate new code within a standalone package, minimizing deep integration.
- Considering confidentiality, you wish to extend TiDB without exposing the code publicly.

## Usage Guidelines

### A Simple Example

The subsequent example demonstrates how to log information when a new connection is established:

```go
// pkg/extension/example/example.go

package example

import (
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func createSessionHandler() *extension.SessionHandler {
	return &extension.SessionHandler{
		OnConnectionEvent: func(tp extension.ConnEventTp, info *extension.ConnEventInfo) {
			if tp == extension.ConnConnected {
				logutil.BgLogger().Info("new connection connected", zap.String("client IP", info.ClientIP))
			}
		},
	}
}

func init() {
	err := extension.Register(
		"example",
		extension.WithSessionHandlerFactory(createSessionHandler),
	)
	terror.MustNil(err)
}
```

```go
// pkg/extension/_import/import_example.go

package extensionimport

import (
	_ "github.com/pingcap/tidb/pkg/extension/example"
)
```

Incorporate these files into your TiDB file structure, rebuild, and run the TiDB server. Observe the enhanced logging when a new connection attempts to connect to TiDB:

```
[2023/11/14 16:22:26.854 +08:00] [INFO] [example.go:28] ["new connection connected"] ["client IP"=127.0.0.1]
```

Let's delve into a step-by-step explanation of how this works:

In the primary file, `example.go`, a new function named `createSessionHandler` is introduced. This function acts as a factory and is intended to be invoked when a new connection is established. It returns an object of type `extension.SessionHandler` to guide the framework on handling the connection. The `SessionHandler` type provides various customizable fields. In the provided example, we utilize the `OnConnectionEvent` field to manage the `extension.ConnConnected` event for connections, logging the client's IP address.

The custom code is registered using `extension.Register`. In the `init` function of the `example.go` file, the first parameter passed to `extension.Register` is the extension name. Any string can be used as the name, with the only constraint being its uniqueness across all extensions. Following the extension name, an arbitrary number of options can be passed to `extension.Register`. In our example, we employ the `extension.WithSessionHandlerFactory` option to convey the `createSessionHandler` function to the previously defined framework.

It's essential to note that `example.go` resides in a newly created package. If this package is not imported by other packages, the `init` function will not be called. To address this, a new file, `example_import.go`, is created within an existing package, specifically `pkg/extension/_import`. This file imports `pkg/extension/example`, ensuring that the extension is registered when TiDB starts.

### Further Explanation about Session Handler

The `SessionHandler` is designed to manage session events and is declared as follows:

```go
// SessionHandler is used to listen session events
type SessionHandler struct {
	OnConnectionEvent func(ConnEventTp, *ConnEventInfo)
	OnStmtEvent       func(StmtEventTp, StmtEventInfo)
}
```

`SessionHandler` offers several customizable fields. These fields default to empty, implying that if left unset, the framework will not perform any actions when corresponding events occur.

**OnConnectionEvent**

The `OnConnectionEvent` function is used to observe all connection events. The types of connection events are defined by `ConnEventTp`, with key types including:

- `ConnConnected`: the creation of a new connection that is not yet authenticated.
- `ConnHandshakeAccepted`: successful authentication of a connection.
- `ConnHandshakeRejected`: rejection due to authentication failure.
- `ConnClose`: the close of a connection.

It is important to note that `OnConnectionEvent` does not return errors; it is designed as a listener, and any encountered errors in custom code must be managed before the function concludes. In essence, **`OnConnectionEvent`** does not disrupt the connection flow but merely observes.

**OnStmtEvent**

`OnStmtEvent` serves to monitor events related to statements. Similar to `OnConnectionEvent`, it functions as a listener without influencing the statement flow. Statement event types, defined by `StmtEventTp`, include:

- `StmtError`: Denoting the completion of a statement with an error.
- `StmtSuccess`: Indicating the successful completion of a statement.

### **Registering Extensions with Options**

In addition to `extension.WithSessionHandlerFactory`, various options are available for extension customization. All options can be registered using the `extension.Register` function. For instance:

```go
	err := extension.Register(
		"example",
		extension.WithSessionHandlerFactory(createSessionHandler),
		extension.WithBootstrapSQL("CREATE TABLE IF NOT EXISTS mysql.example(a int)"),
		// ...
	)
	terror.MustNil(err)
```

Alternatively, use `extension.RegisterFactory` for dynamic registration of extension options. This proves beneficial when global configurations influence the selection of options. For example:

```go
	err := extension.RegisterFactory("example", func() ([]extension.Option, error) {
		cfg := config.GetGlobalConfig()
		var factory func() *extension.SessionHandler
		if cfg.SomeFlag {
			factory = createSessionHandler1
		} else {
			factory = createSessionHandler2
		}

		return []extension.Option{
			extension.WithSessionHandlerFactory(factory),
		}, nil
	})
	terror.MustNil(err)
```

### **Key Extension Options**

Several important extension options are outlined below:

**WithCustomSysVariables**

The `WithCustomSysVariables` option registers custom system variables, accepting a slice of `variable.SysVar` for addition.

**WithCustomDynPrivs**

Use `WithCustomDynPrivs` to register custom dynamic privileges.

**WithCustomFunctions**

The `WithCustomFunctions` option registers custom functions.

**AccessCheckFunc**

The `AccessCheckFunc` option customizes the access check logic, enabling additional checks for table access.

**WithSessionHandlerFactory**

This option is instrumental in handling session events.

**WithBootstrap**

`WithBootstrap` customizes the bootstrap logic, receiving a function invoked during TiDB bootstrap. Note that `WithBootstrapSQL` and `WithBootstrap` are mutually exclusive, allowing the use of only one.

**WithBootstrapSQL**

`WithBootstrapSQL` customizes the bootstrap logic with a string containing SQL statements for bootstrapping. Note that `WithBootstrapSQL` and `WithBootstrap` are mutually exclusive, allowing the use of only one.
