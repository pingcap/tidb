# gofail

[![Build Status](https://travis-ci.org/coreos/gofail.svg?branch=master)](https://travis-ci.org/coreos/gofail)

An implementation of [failpoints][failpoint] for golang.

[failpoint]: http://www.freebsd.org/cgi/man.cgi?query=fail

## Add a failpoint

Failpoints are special comments that include a failpoint variable declaration and some trigger code,

```go
func someFunc() string {
	// gofail: var SomeFuncString string
	// // this is called when the failpoint is triggered
	// return SomeFuncString
	return "default"
}
```

## Build with failpoints

Building with failpoints will translate gofail comments in place to code that accesses the gofail runtime.

Call gofail in the directory with failpoints to generate gofail runtime bindings, then build as usual,

```sh
gofail enable
go build cmd/
```

The translated code looks something like,

```go
func someFunc() string {
        if vSomeFuncString, __fpErr := __fp_SomeFuncString.Acquire(); __fpErr == nil { defer __fp_SomeFuncString.Release(); SomeFuncString, __fpTypeOK := vSomeFuncString.(string); if !__fpTypeOK { goto __badTypeSomeFuncString}
		// this is called when the failpoint is triggered
		return SomeFuncString; __badTypeSomeFuncString: __fp_SomeFuncString.BadType(vSomeFuncString, "string"); };
        return "default"
}
```

To disable failpoints and revert to the original code,

```sh
gofail disable
```

## Triggering a failpoint

After building with failpoints enabled, the program's failpoints can be activated so they may trigger when evaluated.

### Command line

From the command line, trigger the failpoint to set SomeFuncString to `hello`,

```sh
GOFAIL_FAILPOINTS='my/package/path/SomeFuncString=return("hello")' ./cmd
```

Multiple failpoints are set by using ';' for a delimiter,

```sh
GOFAIL_FAILPOINTS='failpoint1=return("hello");failpoint2=sleep(10)' ./cmd
```

### HTTP endpoint

First, enable the HTTP server from the command line,

```sh
GOFAIL_HTTP="127.0.0.1:1234" ./cmd
```


Activate a failpoint with curl,

```sh
$ curl http://127.0.0.1:1234/my/package/path/SomeFuncString -XPUT -d'return("hello")'
```

List the failpoints,

```sh
$ curl http://127.0.0.1:1234/
my/package/path/SomeFuncString=return("hello")
```

Deactivate a failpoint,

```sh
$ curl http://127.0.0.1:1234/my/package/path/SomeFuncString -XDELETE
```

### Unit tests

From a unit test,

```go
import (
	"testing"

	gofail "github.com/coreos/gofail/runtime"
)

func TestWhatever(t *testing.T) {
	gofail.Enable("my/package/path/SomeFuncString", `return("hello")`)
	defer gofail.Disable("my/package/path/SomeFuncString")
	...
}
```

