# GlobalKillTest

GlobalKillTest is a test command tool for TiDB __"Global Kill"__ feature.

_(About __"Global Kill"__, see [design doc](https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-01-global-kill.md) for detail.)_

```
Usage: ./run-tests.sh [options]

    -h: Print this help message.

    -L <info|warn|error>: Log level of testing. Defaults to "info".

    --server_log_level <info|warn|error>: Log level of TiDB server. Defaults to "info".
    
    --tmp <temporary path>: Temporary files path. Defaults to "/tmp/tidb_globalkilltest".

    -s <tidb-server-path>: Use tidb-server in <tidb-server-path> for testing.
                           Defaults to "bin/globalkilltest_tidb-server".

    --tidb_start_port <port>: First TiDB server listening port. port ~ port+2 will be used.
                              Defaults to "5000".

    --tidb_status_port <port>: First TiDB server status listening port. port ~ port+2 will be used.
                               Defaults to "8000".

    --pd <pd-client-path>: PD client path, ip:port list seperated by comma.
                           Defaults to "127.0.0.1:2379".

    --pd_proxy_port <port>: PD proxy port. PD proxy is used to simulate lost connection between TiDB and PD.
                            Defaults to "3379".

    --conn_lost <timeout in seconds>: Lost connection to PD timeout,
                                      should be the same as TiDB ldflag <ldflagLostConnectionToPDTimeout>.
                                      See tidb/Makefile for detail.
                                      Defaults to "5".

    --conn_restored <timeout in seconds>: Time to check PD connection restored,
                                          should be the same as TiDB ldflag 
                                          <ldflagServerIDTimeToCheckPDConnectionRestored>.
                                          See tidb/Makefile for detail.
                                          Defaults to "1".

```


## Prerequisite
1. Build TiDB binary for test. See [Makefile](https://github.com/pingcap/tidb/blob/master/tests/globalkilltest/Makefile) for detail.

2. Establish a cluster with PD & TiKV, and provide PD client path by `--pd=ip:port[,ip:port]`.


## Test Scenarios

1. A TiDB without PD, killed by Ctrl+C, and killed by KILL.

2. One TiDB with PD, killed by Ctrl+C, and killed by KILL.

3. Multiple TiDB nodes, killed {local,remote} by {Ctrl-C,KILL}.

4. TiDB with PD, existing connections are killed after PD lost connection for long time.

5. TiDB with PD, new connections are not accepted after PD lost connection for long time.

6. TiDB with PD, new connections are accepted after PD lost connection for long time and then recovered.

7. TiDB with PD, connections can be killed (see 3) after PD lost connection for long time and then recovered.


## How it works

* TiDB is built by [Makefile](https://github.com/pingcap/tidb/blob/master/tests/globalkilltest/Makefile), to hack some timeout variables, as the default value of these variables are too long _(several hours)_ for automated testing.

* Execute `SELECT SLEEP(x)` as payload, and kill the query before `x` expired. If the query had no error and elapsed less than `x`, the test is PASSED.


## Usage

### Regression Execute in Integration Test

In Integration Test after commit and before merge, run these commands under TiDB `tests/globalkilltest` folder.

```sh
cd tests/globalkilltest
make
./run-tests.sh --pd=<pd client path>
```

Again, before testing, establish a cluster with PD & TiKV and provide `pd client path` by `--pd=<pd client path>`.

### Manual Test

Run a single test manually (take `TestMultipleTiDB` as example):

```sh
cd tests/globalkilltest
make
go test -check.f TestMultipleTiDB -args --pd=<pd client path>
```
