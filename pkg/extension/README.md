# Extension Framework Usage

<!-- TOC -->
* [Extension Framework Usage](#extension-framework-usage)
  * [Introduction](#introduction)
  * [How to use](#how-to-use)
    * [A simple example](#a-simple-example)
    * [Further explanation about session handler](#further-explanation-about-session-handler)
    * [Register Extension with Options](#register-extension-with-options)
    * [Some Important Extension Options](#some-important-extension-options)
<!-- TOC -->

## Introduction

Extension framework is used to integrate some standalone codes to TiDB. You may need it for some reason:

- You want to write some code to extend TiDB, but you want to put the new code in some standalone package without a deep integration with TiDB.
- You want to write some code to extend TiDB, but you don't want to expose the code to the public. 

## How to use

### A simple example

The below example shows how to log some information when a new connection is created:

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

You can add these two files to your local TiDB file tree, rebuild, and then run tidb server. You can see a new log when any new connection trys to connect the TiDB.

```
[2023/11/14 16:22:26.854 +08:00] [INFO] [example.go:28] ["new connection connected"] ["client IP"=127.0.0.1]
```

Let's explain how it works step-by-step.

Firstly, in the main file, `example.go`, a new function `createSessionHandler` is added. `createSessionHandler` is a factory function which is expected to be called when a new connection comes. It returns an object with type `extension.SessionHandler` to tell the framework how to handle the connection. `SessionHandler` has many fields to custom, in the above example, we only use the field `OnConnectionEvent` to handle the `extension.ConnConnected` event of connections and log the client IP then.

Then we should register our custom code using `extension.Register`. You can see the `init` function of file `example.go`. The first parameter passed to `extension.Register` is the extension name; you can use any string you like, the only limitation is that it should be unique across all extensions. After extension name, we can pass arbitrary count of options to `extension.Register`. In the example, we use the option `extension.WithSessionHandlerFactory` to pass the `createSessionHandler` to the framework we write before.

Please notice that `example.go` is a file in a new created package, and if it is not imported by other packages, the `init` will not be called, so we created a new file `example_import.go` in an existing package `pkg/extension/_import` and it imports `pkg/extension/example` to make sure the extension will be registered when TiDB starts.

### Further explanation about session handler

```SessionHandler``` is used to handle session events, and it is declared as below:

```go
// SessionHandler is used to listen session events
type SessionHandler struct {
	OnConnectionEvent func(ConnEventTp, *ConnEventInfo)
	OnStmtEvent       func(StmtEventTp, StmtEventInfo)
}
```

You can see that `SessionHandler` provides several fields to custom. These fields are empty by default, which means if you don't set them, the framework will do nothing when the corresponding event happens.

**OnConnectionEvent**

You can use `OnConnectionEvent` to listen to all connection events. The "connection events" here do not contain some events related to statements. The types of all connection events is defined by `ConnEventTp`, some important types are listed below:

- `ConnConnected`: A new connection is created, but not authenticated yet.
- `ConnHandshakeAccepted`: A connection is authenticated successfully.
- `ConnHandshakeRejected`: A connection is rejected because of authentication failure.
- `ConnClose`: A connection is closed.

Please notice that `OnConnectionEvent` does not return any error that means it is always expected to success, if any error occurs in your custom code, you should handle it by your self before the function returns. In the other words, `OnConnectionEvent` does not affect the connection flow; it is just a "listener."

**OnStmtEvent**

`OnStmtEvent` is used to listen to events related to statements. Like `OnConnectionEvent`, it is a listener and does not affect the statement flow. The types of all statement events is defined by `StmtEventTp`, some important types are listed below:

- `StmtError`: A statement is finished with an error.
- `StmtSuccess`: A statement is finished successfully.

### Register Extension with Options

Except for `extension.WithSessionHandlerFactory`, we also have other options to custom the extension. All of them can be registered using `extension.Register` function, for example:

```go
	err := extension.Register(
		"example",
		extension.WithSessionHandlerFactory(createSessionHandler),
		extension.WithBootstrapSQL("CREATE TABLE IF NOT EXISTS mysql.example(a int)"),
		// ...
	)
	terror.MustNil(err)
```

You can also use `extension.RegisterFactory` to return the extension options dynamically. This is useful when you want to use some global configurations to decide which options to use. For example:

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

### Some Important Extension Options

Some important extension options are listed below:

**WithCustomSysVariables**

`WithCustomSysVariables` is used to register custom system variables. It receives a slice of `variable.SysVar` which need to be added.

**WithCustomDynPrivs**

`WithCustomDynPrivs` is used to register custom dynamic privileges. 

**WithCustomFunctions**

`WithCustomFunctions` is used to register custom functions. 

**AccessCheckFunc**

`AccessCheckFunc` is used to custom the access check logic. It can add some extra checks for table access.

**WithSessionHandlerFactory**

`WithSessionHandlerFactory` is used to handle session events.

**WithBootstrap**

`WithBootstrap` is used to custom the bootstrap logic. It receives a function which will be called when TiDB bootstraps. `WithBootstrapSQL` and `WithBootstrap` are exclusive, you can only use one of them.

**WithBootstrapSQL**

`WithBootstrapSQL` is used to custom the bootstrap logic. It receives a string which contains the SQLs to bootstrap. `WithBootstrapSQL` and `WithBootstrap` are exclusive, you can only use one of them.
