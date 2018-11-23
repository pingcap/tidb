#!/bin/bash

. ./common.sh

if ! check_protoc_version; then
	exit 1
fi

cargo_install protobuf-codegen 2.0.4
cargo_install grpcio-compiler 0.3.0

echo "generate rust code..."
KVPROTO_ROOT=`pwd`
push proto
GOGO_ROOT=${KVPROTO_ROOT}/_vendor/src/github.com/gogo/protobuf

protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf:../include --rust_out ../src *.proto || exit $?
protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf:../include --grpc_out ../src --plugin=protoc-gen-grpc=`which grpc_rust_plugin` *.proto || exit $?
pop

push src
LIB_RS=`mktemp`
rm -f lib.rs
cat <<EOF > ${LIB_RS}
extern crate futures;
extern crate grpcio;
extern crate protobuf;
extern crate raft;

use raft::eraftpb;

EOF
for file in `ls *.rs`
    do
    base_name=$(basename $file ".rs")
    echo "pub mod $base_name;" >> ${LIB_RS}
done
mv ${LIB_RS} lib.rs
pop

# Use the old way to read protobuf enums.
# TODO: Remove this once stepancheg/rust-protobuf#233 is resolved.
for f in src/*; do
python <<EOF
import re
with open("$f") as reader:
    src = reader.read()

res = re.sub('::protobuf::rt::read_proto3_enum_with_unknown_fields_into\(([^,]+), ([^,]+), &mut ([^,]+), [^\)]+\)\?', 'if \\\\1 == ::protobuf::wire_format::WireTypeVarint {\\\\3 = \\\\2.read_enum()?;} else { return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type)); }', src)

with open("$f", "w") as writer:
    writer.write(res)
EOF
done
