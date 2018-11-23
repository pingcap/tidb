#!/bin/bash

cd proto

echo "generate rust code..."
ret=0
GOGO_ROOT=${GOPATH}/src/github.com/gogo/protobuf
protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --rust_out ../src *.proto || ret=$?


echo "extern crate protobuf;" > ../src/lib.rs
for file in `ls *.proto`
    do
    base_name=$(basename $file ".proto")
    echo "#[cfg_attr(rustfmt, rustfmt_skip)]" >> ../src/lib.rs
    echo "pub mod $base_name;" >> ../src/lib.rs
done
if [[ $ret -ne 0 ]]; then
    exit $ret
fi
cd ..

# Use the old way to read protobuf enums.
# TODO: Remove this once stepancheg/rust-protobuf#233 is resolved.
for f in src/*; do
python <<EOF
import re
with open("$f") as reader:
    src = reader.read()

res = re.sub('::protobuf::rt::read_proto2_enum_with_unknown_fields_into\(([^,]+), ([^,]+), &mut ([^,]+), [^\)]+\)\?', 'if \\\\1 == ::protobuf::wire_format::WireTypeVarint {\\\\3 = ::std::option::Option::Some(\\\\2.read_enum()?);} else { return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type)); }', src)

with open("$f", "w") as writer:
    writer.write(res)
EOF
done

cargo build
