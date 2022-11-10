# MySQL Tester

This is a golang implementation of [MySQL Test Framework](https://github.com/mysql/mysql-server/tree/8.0/mysql-test).

## Requirements

- All the tests should be put in [`t`](./t), take [t/example.test](./t/example.test) as an example.
- All the expected test results should be put in [`r`](./r). Result file has the same file name with the corresponding test file, but with a `.result` file suffix, take [r/example.result](./r/example.result) as an examle.

## How to use

Build the `mysql-tester` binary:
```sh
make
```

Basic usage:
```
Usage of ./mysql-tester:
  -all
        run all tests
  -host string
        The host of the TiDB/MySQL server. (default "127.0.0.1")
  -log-level string
        The log level of mysql-tester: info, warn, error, debug. (default "error")
  -params string
        Additional params pass as DSN(e.g. session variable)
  -passwd string
        The password for the user.
  -port string
        The listen port of TiDB/MySQL server. (default "4000")
  -record
        Whether to record the test output to the result file.
  -reserve-schema
        Reserve schema after each test
  -retry-connection-count int
        The max number to retry to connect to the database. (default 120)
  -user string
        The user for connecting to the database. (default "root")
  -xunitfile string
        The xml file path to record testing results.
```

By default, it connects to the TiDB/MySQL server at `127.0.0.1:4000` with `root` and no passward:
```sh
./mysql-tester # run all the tests
./mysql-tester example # run a specified test
./mysql-tester example1 example2   example3 # seperate different tests with one or more spaces
```

For more details about how to run and write test cases, see the [Wiki](https://github.com/pingcap/mysql-tester/wiki) page.

## Contributing

Contributions are welcomed and greatly appreciated. You can help by:

- writing user document about how to use this framework
- triaging issues
- submitting new test cases
- fixing bugs of this test framework
- adding features that mysql test has but this implementation does not
- ...

In case you have any problem, discuss with us in the [tidbcommunity](https://join.slack.com/t/tidbcommunity/shared_invite/enQtNzc0MzI4ODExMDc4LWYwYmIzMjZkYzJiNDUxMmZlN2FiMGJkZjAyMzQ5NGU0NGY0NzI3NTYwMjAyNGQ1N2I2ZjAxNzc1OGUwYWM0NzE) slack workspace.

See [CONTRIBUTING.md](./CONTRIBUTING.md) for details.

## License

MySQL Tester is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

