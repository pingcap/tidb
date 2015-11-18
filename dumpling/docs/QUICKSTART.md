# Quick Start

#### Run tidb with docker

You can quickly test tidb with docker, the source repository contains the Dockerfile,
You can build TiDB docker image and then run TiDB in a docker container.

To install docker on your system, you can read the document on https://docs.docker.com/

```
git clone https://github.com/pingcap/tidb.git
cd tidb
docker build --rm -t tidb-server .
docker images
docker run -d --net=host --name tidb-server tidb-server
```

Then you can use official mysql client to connect to TiDB.

 ```
 mysql -h 127.0.0.1 -P 4000 -u root -D test
 ```

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

#### __Run as go library__

See [USAGE.md](./USAGE.md) for detailed instructions to use TiDB as library in Go code.

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

After you started tidb-server, you can use official mysql client to connect to TiDB.

```
mysql -h 127.0.0.1 -P 4000 -u root -D test
```

#### __Run as MySQL protocol server with distributed transactional KV storage engine__

Comming soon.
