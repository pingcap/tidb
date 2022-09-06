#!/usr/bin/env bash

repo=385595570414.dkr.ecr.us-east-1.amazonaws.com/dbaas/jujiajia/tidb-lightning
tag=test2

make build_lightning
cp bin/tidb-lightning lightning-docker/
pushd lightning-docker/
docker buildx build --platform linux/arm64 \
	--tag $repo:$tag \
	--push \
	-f Dockerfile.lightning .
popd
