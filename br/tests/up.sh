#!/usr/bin/env bash

set -eo pipefail

IMAGE_TAG="nightly"
while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
        --pull-images)
            PULL_DOCKER_IMAGES=1
            shift
            ;;
        --tag)
            IMAGE_TAG=$2
            shift
            shift
            ;;
        --cleanup-docker)
            CLEANUP_DOCKER=1
            shift
            ;;
        --cleanup-data)
            CLEANUP_DATA=1
            shift
            ;;
        --cleanup-all)
            CLEANUP_ALL=1
            shift
            ;;
        --bind-bin)
            BIND_BIN=1
            shift
            ;;
        --help)
            HELP=1
            shift
            ;;
        *)
            HELP=1
            break
            ;;
    esac
done

if [ "$HELP" ]; then
    echo "Usage: $0 [OPTIONS]"
    echo "OPTIONS:"
    echo "  --help                 Display this message"
    echo "  --pull-images          Update docker images used in br tests"
    echo "  --tag (TAG)            Specify images tag used in br tests"
    echo "  --cleanup-docker       Clean up br tests Docker containers"
    echo "  --cleanup-data         Clean up persistent data"
    echo "  --cleanup-all          Clean up all data inlcuding Docker images, containers and persistent data"
    echo "  --bind-bin             Bind br/bin directory"
    exit 0
fi

host_tmp=/tmp/br_tests
host_bash_history=$host_tmp/bash_history

# Persist tests data and bash history
mkdir -p $host_tmp
touch $host_bash_history || true
function cleanup_data() {
    rm -rf $host_tmp || { echo try "sudo rm -rf $host_tmp"? ; exit 1; }
}
if [ "$CLEANUP_DATA" ]; then
    cleanup_data
    exit 0
fi

# Clean up docker images and containers.
docker_repo=br_tests
function cleanup_docker_containers() {
    containers=$(docker container ps --all --filter="ancestor=$docker_repo:$IMAGE_TAG" -q)
    if [ "$containers" ]; then
        docker stop $containers
        docker rm $containers
    fi
}
function cleanup_docker_images() {
    images=$(docker images --filter="reference=$docker_repo:$IMAGE_TAG" -q)
    if [ "$images" ]; then
        docker rmi $images
    fi
}
if [ "$CLEANUP_DOCKER" ]; then
    cleanup_docker_containers
    exit 0
fi

if [ "$CLEANUP_ALL" ]; then
    cleanup_data
    cleanup_docker_containers
    cleanup_docker_images
    exit 0
fi

if [ "$PULL_DOCKER_IMAGES" ]; then
    for image in "pingcap/tidb" "pingcap/tikv" "pingcap/pd" "pingcap/ticdc" "pingcap/tiflash" "pingcap/tidb-lightning"; do
        docker pull $image:$IMAGE_TAG
        docker tag $image:$IMAGE_TAG $image:$IMAGE_TAG.$docker_repo
    done
fi

docker build -t $docker_repo:$IMAGE_TAG - << EOF
FROM pingcap/tidb:$IMAGE_TAG.$docker_repo           AS tidb-builder
FROM pingcap/tikv:$IMAGE_TAG.$docker_repo           AS tikv-builder
FROM pingcap/pd:$IMAGE_TAG.$docker_repo             AS pd-builder
FROM pingcap/ticdc:$IMAGE_TAG.$docker_repo          AS ticdc-builder
FROM pingcap/tiflash:$IMAGE_TAG.$docker_repo        AS tiflash-builder
FROM pingcap/tidb-lightning:$IMAGE_TAG.$docker_repo AS lightning-builder
FROM pingcap/br:v4.0.8                              AS br408-builder
FROM minio/minio                                    AS minio-builder
FROM minio/mc                                       AS mc-builder
FROM fsouza/fake-gcs-server                         AS gcs-builder

FROM golang:1.16.4-buster as ycsb-builder
WORKDIR /go/src/github.com/pingcap/
RUN git clone https://github.com/pingcap/go-ycsb.git && \
    cd go-ycsb && \
    make && \
    cp bin/go-ycsb /go-ycsb

FROM golang:1.16.4-buster

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    wget \
    openssl \
    lsof \
    psmisc \
    vim \
    less \
    jq \
    default-mysql-client

RUN mkdir -p /br/bin
COPY --from=tidb-builder      /tidb-server                   /br/bin/tidb-server
COPY --from=tikv-builder      /tikv-server                   /br/bin/tikv-server
COPY --from=pd-builder        /pd-server                     /br/bin/pd-server
COPY --from=pd-builder        /pd-ctl                        /br/bin/pd-ctl
COPY --from=ticdc-builder     /cdc                           /br/bin/cdc
COPY --from=br408-builder     /br                            /br/bin/brv4.0.8
COPY --from=ycsb-builder      /go-ycsb                       /br/bin/go-ycsb
COPY --from=tiflash-builder   /tiflash/tiflash               /br/bin/tiflash
COPY --from=tiflash-builder   /tiflash/libtiflash_proxy.so   /br/bin/libtiflash_proxy.so
COPY --from=tiflash-builder   /tiflash/flash_cluster_manager /br/bin/flash_cluster_manager
COPY --from=minio-builder     /usr/bin/minio                 /br/bin/minio
COPY --from=mc-builder        /usr/bin/mc                    /br/bin/mc
COPY --from=gcs-builder       /bin/fake-gcs-server           /br/bin/fake-gcs-server

WORKDIR /br

# Required by tiflash
ENV LD_LIBRARY_PATH=/br/bin

ENTRYPOINT ["/bin/bash"]
EOF

# Start an existing container or create and run a new container.
exist_container=$(docker container ps --all -q --filter="ancestor=$docker_repo:$IMAGE_TAG" --filter="status=exited" | head -n 1)
if [ "$exist_container" ]; then
    docker start $exist_container
    echo "Attach exsiting container: $exist_container"
    exec docker attach $exist_container
else
    volume_args=
    for f in `ls -a`; do
        if [ $f = "." ] || [ $f = ".." ]; then
            continue
        fi
        if [ $f = "bin" ] && [ ! "$BIND_BIN" ]; then
            continue
        fi
        volume_args="$volume_args -v `pwd`/$f:/br/$f"
    done
    echo "Run a new container"
    exec docker run -it \
        -v $host_tmp:/tmp/br/tests \
        -v $host_bash_history:/root/.bash_history \
        $volume_args \
        $docker_repo:$IMAGE_TAG
fi
