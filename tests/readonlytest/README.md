# E2E test for TiDB restricted read only

The test is not yet automated, so you need to set up an environment manually to test it.

Create a TiDB cluster with 2 TiDB servers on localhost. One server listen on port 4001 and another on 4002, then execute `go test` in this folder to run the test.

You are expected to see 2 tests passed.

```
$ go test
OK: 2 passed
PASS
ok      github.com/pingcap/tidb/tests/readonlytest      2.150s
```

