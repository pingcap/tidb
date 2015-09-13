go get github.com/qiuyesuifeng/goyacc
go get github.com/qiuyesuifeng/golex
type nul >>temp.XXXXXX & copy temp.XXXXXX +,,
goyacc -o nul -xegen "temp.XXXXXX" parser/parser.y
goyacc -o parser/parser.go -xe "temp.XXXXXX" parser/parser.y
DEL /F /A /Q temp.XXXXXX
DEL /F /A /Q y.output
