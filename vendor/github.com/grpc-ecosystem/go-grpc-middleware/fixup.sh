#!/bin/bash
# Script that checks the code for errors.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
GOBIN=${GOBIN:="$GOPATH/bin"}

function print_real_go_files {
    grep --files-without-match 'DO NOT EDIT!' $(find . -iname '*.go')
}

function generate_markdown {
    echo "Generating Github markdown"
    oldpwd=$(pwd)
    for i in $(find . -iname 'doc.go'); do
        dir=${i%/*}
        realdir=$(realpath $dir)
        package=${realdir##${GOPATH}/src/}
        echo "$package"
        cd ${dir}
        ${GOBIN}/godoc2ghmd -ex -file DOC.md ${package}
        ln -s DOC.md README.md 2> /dev/null # can fail
        cd ${oldpwd}
    done;
}

function goimports_all {
    echo "Running goimports"
    goimports -l -w $(print_real_go_files)
    return $?
}

go get github.com/davecheney/godoc2md

generate_markdown
goimports_all
echo "returning $?"
