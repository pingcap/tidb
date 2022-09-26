### consume and parse kafka binlog message for java demo

#### Env：
tidb: v3.0.0 <br/>
drainer: v3.0.0 <br/>
kafka: kafka_2.12 1.0.0 <br/>
local windows environment protobuf version：protoc-3.9.1-win64 <br/>
[binlog.proto](https://github.com/pingcap/tidb-tools/blob/master/tidb-binlog/slave_binlog_proto/proto/binlog.proto) use official point file。

#### Execute protoc command to generate java file：
protoc  --java_out=src/main/java src/main/resources/proto/descriptor.proto    --proto_path=src/main/resources/proto/  <br/>
protoc  --java_out=src/main/java src/main/resources/proto/gogo.proto    --proto_path=src/main/resources/proto/  <br/>
protoc  --java_out=src/main/java src/main/resources/proto/binlog.proto  --proto_path=src/main/resources/proto/  <br/>

#### How to run:
in intel idea ide， run Booter.java main method。<br/>
point Booter.topic、Booter.serever、Booter.offset and run it。
