# Run etcd clusters inside containers

The following guide shows how to run etcd with rkt and Docker using the [static bootstrap process](clustering.md#static).

## rkt

### Running a single node etcd

The following rkt run command will expose the etcd client API on port 2379 and expose the peer API on port 2380.

Use the host IP address when configuring etcd.

```
export NODE1=192.168.1.21
```

Trust the CoreOS [App Signing Key](https://coreos.com/security/app-signing-key/).

```
sudo rkt trust --prefix coreos.com/etcd
# gpg key fingerprint is: 18AD 5014 C99E F7E3 BA5F  6CE9 50BD D3E0 FC8A 365E
```

Run the `v3.1.2` version of etcd or specify another release version.

```
sudo rkt run --net=default:IP=${NODE1} coreos.com/etcd:v3.1.2 -- -name=node1 -advertise-client-urls=http://${NODE1}:2379 -initial-advertise-peer-urls=http://${NODE1}:2380 -listen-client-urls=http://0.0.0.0:2379 -listen-peer-urls=http://${NODE1}:2380 -initial-cluster=node1=http://${NODE1}:2380
```

List the cluster member.

```
etcdctl --endpoints=http://192.168.1.21:2379 member list
```

### Running a 3 node etcd cluster

Setup a 3 node cluster with rkt locally, using the `-initial-cluster` flag.

```sh
export NODE1=172.16.28.21
export NODE2=172.16.28.22
export NODE3=172.16.28.23
```

```
# node 1
sudo rkt run --net=default:IP=${NODE1} coreos.com/etcd:v3.1.2 -- -name=node1 -advertise-client-urls=http://${NODE1}:2379 -initial-advertise-peer-urls=http://${NODE1}:2380 -listen-client-urls=http://0.0.0.0:2379 -listen-peer-urls=http://${NODE1}:2380 -initial-cluster=node1=http://${NODE1}:2380,node2=http://${NODE2}:2380,node3=http://${NODE3}:2380

# node 2
sudo rkt run --net=default:IP=${NODE2} coreos.com/etcd:v3.1.2 -- -name=node2 -advertise-client-urls=http://${NODE2}:2379 -initial-advertise-peer-urls=http://${NODE2}:2380 -listen-client-urls=http://0.0.0.0:2379 -listen-peer-urls=http://${NODE2}:2380 -initial-cluster=node1=http://${NODE1}:2380,node2=http://${NODE2}:2380,node3=http://${NODE3}:2380

# node 3
sudo rkt run --net=default:IP=${NODE3} coreos.com/etcd:v3.1.2 -- -name=node3 -advertise-client-urls=http://${NODE3}:2379 -initial-advertise-peer-urls=http://${NODE3}:2380 -listen-client-urls=http://0.0.0.0:2379 -listen-peer-urls=http://${NODE3}:2380 -initial-cluster=node1=http://${NODE1}:2380,node2=http://${NODE2}:2380,node3=http://${NODE3}:2380
```

Verify the cluster is healthy and can be reached.

```
ETCDCTL_API=3 etcdctl --endpoints=http://172.16.28.21:2379,http://172.16.28.22:2379,http://172.16.28.23:2379 endpoint health
```

### DNS

Production clusters which refer to peers by DNS name known to the local resolver must mount the [host's DNS configuration](https://coreos.com/kubernetes/docs/latest/kubelet-wrapper.html#customizing-rkt-options).

## Docker

In order to expose the etcd API to clients outside of Docker host, use the host IP address of the container. Please see [`docker inspect`](https://docs.docker.com/engine/reference/commandline/inspect) for more detail on how to get the IP address. Alternatively, specify `--net=host` flag to `docker run` command to skip placing the container inside of a separate network stack.

### Running a single node etcd

Use the host IP address when configuring etcd:

```
export NODE1=192.168.1.21
```

Run the latest version of etcd:

```
docker run \
  -p 2379:2379 \
  -p 2380:2380 \
  --volume=${DATA_DIR}:/etcd-data \
  --name etcd quay.io/coreos/etcd:latest \
  /usr/local/bin/etcd \
  --data-dir=/etcd-data --name node1 \
  --initial-advertise-peer-urls http://${NODE1}:2380 --listen-peer-urls http://${NODE1}:2380 \
  --advertise-client-urls http://${NODE1}:2379 --listen-client-urls http://${NODE1}:2379 \
  --initial-cluster node1=http://${NODE1}:2380
```

List the cluster member:

```
etcdctl --endpoints=http://${NODE1}:2379 member list
```

### Running a 3 node etcd cluster

```
# For each machine
ETCD_VERSION=latest
TOKEN=my-etcd-token
CLUSTER_STATE=new
NAME_1=etcd-node-0
NAME_2=etcd-node-1
NAME_3=etcd-node-2
HOST_1=10.20.30.1
HOST_2=10.20.30.2
HOST_3=10.20.30.3
CLUSTER=${NAME_1}=http://${HOST_1}:2380,${NAME_2}=http://${HOST_2}:2380,${NAME_3}=http://${HOST_3}:2380
DATA_DIR=/var/lib/etcd

# For node 1
THIS_NAME=${NAME_1}
THIS_IP=${HOST_1}
docker run \
  -p 2379:2379 \
  -p 2380:2380 \
  --volume=${DATA_DIR}:/etcd-data \
  --name etcd quay.io/coreos/etcd:${ETCD_VERSION} \
  /usr/local/bin/etcd \
  --data-dir=/etcd-data --name ${THIS_NAME} \
  --initial-advertise-peer-urls http://${THIS_IP}:2380 --listen-peer-urls http://${THIS_IP}:2380 \
  --advertise-client-urls http://${THIS_IP}:2379 --listen-client-urls http://${THIS_IP}:2379 \
  --initial-cluster ${CLUSTER} \
  --initial-cluster-state ${CLUSTER_STATE} --initial-cluster-token ${TOKEN}

# For node 2
THIS_NAME=${NAME_2}
THIS_IP=${HOST_2}
docker run \
  -p 2379:2379 \
  -p 2380:2380 \
  --volume=${DATA_DIR}:/etcd-data \
  --name etcd quay.io/coreos/etcd:${ETCD_VERSION} \
  /usr/local/bin/etcd \
  --data-dir=/etcd-data --name ${THIS_NAME} \
  --initial-advertise-peer-urls http://${THIS_IP}:2380 --listen-peer-urls http://${THIS_IP}:2380 \
  --advertise-client-urls http://${THIS_IP}:2379 --listen-client-urls http://${THIS_IP}:2379 \
  --initial-cluster ${CLUSTER} \
  --initial-cluster-state ${CLUSTER_STATE} --initial-cluster-token ${TOKEN}

# For node 3
THIS_NAME=${NAME_3}
THIS_IP=${HOST_3}
docker run \
  -p 2379:2379 \
  -p 2380:2380 \
  --volume=${DATA_DIR}:/etcd-data \
  --name etcd quay.io/coreos/etcd:${ETCD_VERSION} \
  /usr/local/bin/etcd \
  --data-dir=/etcd-data --name ${THIS_NAME} \
  --initial-advertise-peer-urls http://${THIS_IP}:2380 --listen-peer-urls http://${THIS_IP}:2380 \
  --advertise-client-urls http://${THIS_IP}:2379 --listen-client-urls http://${THIS_IP}:2379 \
  --initial-cluster ${CLUSTER} \
  --initial-cluster-state ${CLUSTER_STATE} --initial-cluster-token ${TOKEN}
```

To run `etcdctl` using API version 3:

```
docker exec etcd /bin/sh -c "export ETCDCTL_API=3 && /usr/local/bin/etcdctl put foo bar"
```

## Bare Metal

To provision a 3 node etcd cluster on bare-metal, the examples in the [baremetal repo](https://github.com/coreos/coreos-baremetal/tree/master/examples) may be useful.

## Mounting a certificate volume

The etcd release container does not include default root certificates. To use HTTPS with certificates trusted by a root authority (e.g., for discovery), mount a certificate directory into the etcd container:

```
rkt run \
  --volume etcd-ssl-certs-bundle,kind=host,source=/etc/ssl/certs/ca-certificates.crt \
  --mount volume=etcd-ssl-certs-bundle,target=/etc/ssl/certs/ca-certificates.crt \
  quay.io/coreos/etcd:latest -- --name my-name \
  --initial-advertise-peer-urls http://localhost:2380 --listen-peer-urls http://localhost:2380 \
  --advertise-client-urls http://localhost:2379 --listen-client-urls http://localhost:2379 \
  --discovery https://discovery.etcd.io/c11fbcdc16972e45253491a24fcf45e1
```

```
docker run \
  -p 2379:2379 \
  -p 2380:2380 \
  --volume=/etc/ssl/certs/ca-certificates.crt:/etc/ssl/certs/ca-certificates.crt \
  quay.io/coreos/etcd:latest \
  /usr/local/bin/etcd --name my-name \
  --initial-advertise-peer-urls http://localhost:2380 --listen-peer-urls http://localhost:2380 \
  --advertise-client-urls http://localhost:2379 --listen-client-urls http://localhost:2379 \
  --discovery https://discovery.etcd.io/86a9ff6c8cb8b4c4544c1a2f88f8b801
```
