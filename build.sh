#!/usr/bin/env bash


if [ -t 0 ] ; then
	echo "stdin is a tty"
else
	echo "stdin not a tty"
fi

if [ -t 1 ] ; then
	echo "stdout is a tty"
else
	echo "stdout not a tty"
fi

if [ -t 2 ] ; then
	echo "stderr is a tty"
else
	echo "stderr not a tty"
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
