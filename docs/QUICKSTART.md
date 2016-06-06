# Quick Start

#### Run TiDB with Docker

You can quickly test TiDB with Docker, the source repository contains the Dockerfile which contains local tidb-server.

Or you can pull TiDB Docker image contains HBase standalone and then run TiDB as a distributed database in a Docker container.

To install Docker on your system, you can read the document on https://docs.docker.com/

```
docker pull pingcap/tidb:latest
docker run --name tidb-server -d -p 4000:4000 pingcap/tidb:latest
```

`docker pull` may take a while to download images ~560M.

Then you can use official mysql client to connect to TiDB.

```
mysql -h 127.0.0.1 -P 4000 -u root -D test
```

Notice: OS X user may use `docker-machine ip` to connect it.

#### __Run TiDB on HBase__

Read this [doc](https://github.com/pingcap/tidb/blob/master/docs/HBASE_QUICKSTART.md).

#### __Pre-requirement__

Go environment. Currently a 64-bit version of go >= 1.5 is required.
```
git clone https://github.com/pingcap/tidb.git $GOPATH/src/github.com/pingcap/tidb
cd $GOPATH/src/github.com/pingcap/tidb
make
```

#### __Run command line interpreter__

Interpreter is an interactive command line TiDB client.
You can just enter some SQL statements and get the result.
```
make interpreter
cd interpreter && ./interpreter
```
Press `Ctrl+C` to quit.

#### __Run as MySQL protocol server__

```
make server
cd tidb-server && ./tidb-server
```

In case you want to compile a specific location:

```
make server TARGET=$GOPATH/bin/tidb-server
```

The default server port is `4000` and can be changed by flag `-P <port>`.

Run `./tidb-server -h` to see more flag options.

After you started tidb-server, you can use official `mysql` client to connect to TiDB.

```
mysql -h 127.0.0.1 -P 4000 -u root -D test
```
