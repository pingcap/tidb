#!/usr/bin/env bash

COLOR=""
mkdir -p bin
echo $TERM
if [ "$TERM" = "" ]; then
	COLOR="--color=yes"
fi
bazel $1 build $2 \
	//cmd/importer:importer //tidb-server:tidb-server //tidb-server:tidb-server-check --//build:with_nogo_flag=true
if [ "$TERM" = "" ]; then
	export TERM="xterm-256color"
else
	export TERM=""
fi
echo $TERM
if [ "$TERM" = "" ]; then
	COLOR="--color=yes"
fi
bazel $1 build $2 \
	//cmd/importer:importer //tidb-server:tidb-server //tidb-server:tidb-server-check --//build:with_nogo_flag=true
