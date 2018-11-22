#!/usr/bin/env bash

set -ex

check_protoc_version() {
    version=$(protoc --version)
    major=$(echo ${version} | sed -n -e 's/.*\([0-9]\{1,\}\)\.[0-9]\{1,\}\.[0-9]\{1,\}.*/\1/p')
    minor=$(echo ${version} | sed -n -e 's/.*[0-9]\{1,\}\.\([0-9]\{1,\}\)\.[0-9]\{1,\}.*/\1/p')
    if [ "$major" -gt 3 ]; then
        return 0
    fi
    if [ "$major" -eq 3 ] && [ "$minor" -ge 1 ]; then
        return 0
    fi
    echo "protoc version not match, version 3.1.x is needed, current version: ${version}"
    return 1
}

push () {
    pushd $1 >/dev/null 2>&1
}

pop () {
    popd $1 >/dev/null 2>&1
}

cmd_exists () {
    which "$1" 1>/dev/null 2>&1
}

cargo_install() {
    if ! cargo install --list|grep "$1 v$2"; then
        echo "missing $1, trying to download/install it"
        cargo install $1 --vers "$2" -f
    fi
}
