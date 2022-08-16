#!/usr/bin/env bash


if [ -t 0 ] ; then
	echo "is a tty"
else
	echo "not a tty"
fi

COLOR=""
mkdir -p bin
echo $TERM
if [ "$TERM" = "" ]; then
	COLOR="--color=yes"
fi
bazel $1 build $2 $COLOR \
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
bazel $1 build $2 $COLOR \
	//cmd/importer:importer //tidb-server:tidb-server //tidb-server:tidb-server-check --//build:with_nogo_flag=true
