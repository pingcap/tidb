#!/usr/bin/env bash

make build_lightning
cp bin/tidb-lightning lightning-docker/
pushd lightning-docker/
docker build --platform linux/amd64 \
	--tag 385595570414.dkr.ecr.us-east-1.amazonaws.com/dbaas/jujiajia/tidb-lightning:test \
	-f Dockerfile.lightning .
popd

echo "----"
echo "- push image use: docker push 385595570414.dkr.ecr.us-east-1.amazonaws.com/dbaas/jujiajia/tidb-lightning:test"
echo "----"
