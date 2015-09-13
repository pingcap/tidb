go get github.com/qiuyesuifeng/goyacc
go get github.com/qiuyesuifeng/golex
touch temp.XXXXXX
goyacc -o /dev/null -xegen "temp.XXXXXX" parser/parser.y
goyacc -o parser/parser.go -xe "temp.XXXXXX" parser/parser.y
rm -f "temp.XXXXXX"
rm -f y.output
