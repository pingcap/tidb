# PD 

[![TravisCI Build Status](https://travis-ci.org/pingcap/pd.svg?branch=master)](https://travis-ci.org/pingcap/pd)
![Project Status](https://img.shields.io/badge/version-1.0-green.svg)
[![CircleCI Build Status](https://circleci.com/gh/pingcap/pd.svg?style=shield)](https://circleci.com/gh/pingcap/pd)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/pd)](https://goreportcard.com/report/github.com/pingcap/pd)
[![Coverage Status](https://coveralls.io/repos/github/pingcap/pd/badge.svg?branch=master)](https://coveralls.io/github/pingcap/pd?branch=master)

PD is the abbreviation for Placement Driver. It is used to manage and schedule the [TiKV](https://github.com/pingcap/tikv) cluster. 

PD supports distribution and fault-tolerance by embedding [etcd](https://github.com/coreos/etcd). 

## Build

1. Make sure [​*Go*​](https://golang.org/) (version 1.8+) is installed.
2. Use `make` to install PD. PD is installed in the `bin` directory.

## Usage

### Command flags

See [configuration](https://github.com/pingcap/docs/blob/master/op-guide/configuration.md#placement-driver-pd).

### Single Node with default ports

You can run `pd-server` directly on your local machine, if you want to connect to PD from outside, 
you can let PD listen on the host IP.

```bash
# Set correct HostIP here. 
export HostIP="192.168.199.105"

pd-server --name="pd" \
          --data-dir="pd" \
          --client-urls="http://${HostIP}:2379" \
          --peer-urls="http://${HostIP}:2380" \
          --log-file=pd.log
```

Using `curl` to see PD member:

```bash
curl ${HostIP}:2379/v2/members

{"members":[{"id":"f62e88a6e81c149","name":"default","peerURLs":["http://192.168.199.105:2380"],"clientURLs":["http://192.168.199.105:2379"]}]}
```

A better tool [httpie](https://github.com/jkbrzt/httpie) is recommended:

```bash
http ${HostIP}:2379/v2/members
HTTP/1.1 200 OK
Content-Length: 144
Content-Type: application/json
Date: Thu, 21 Jul 2016 09:37:12 GMT
X-Etcd-Cluster-Id: 33dc747581249309

{
    "members": [
        {
            "clientURLs": [
                "http://192.168.199.105:2379"
            ], 
            "id": "f62e88a6e81c149", 
            "name": "default", 
            "peerURLs": [
                "http://192.168.199.105:2380"
            ]
        }
    ]
}
```

### Docker

You can use the following command to build a PD image directly:

```
docker build -t pingcap/pd .
```

Or you can also use following command to get PD from Docker hub:

```
docker pull pingcap/pd
```

Run a single node with Docker: 

```bash
# Set correct HostIP here. 
export HostIP="192.168.199.105"

docker run -d -p 2379:2379 -p 2380:2380 --name pd pingcap/pd \
          --name="pd" \
          --data-dir="pd" \
          --client-urls="http://0.0.0.0:2379" \
          --advertise-client-urls="http://${HostIP}:2379" \
          --peer-urls="http://0.0.0.0:2380" \
          --advertise-peer-urls="http://${HostIP}:2380" \
          --log-file=pd.log
```

### Cluster

PD is a component in TiDB project, you must run it with TiDB and TiKV together, see 
[binary deployment](https://github.com/pingcap/docs/blob/master/op-guide/binary-deployment.md) to learn 
how to set up the cluster and run them.

You can also use [Docker](https://github.com/pingcap/docs/blob/master/op-guide/docker-deployment.md) to 
run the cluster.
