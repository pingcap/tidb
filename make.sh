#go build option
LDFLAGS="-X github.com/pingcap/tidb/util/printer.TiDBBuildTS=`date -u '+%Y-%m-%d %I:%M:%S'` -X github.com/pingcap/tidb/util/printer.TiDBGitHash=`git rev-parse HEAD`"
ARCH="`uname -s`"
LINUX="Linux"
MAC="Darwin"

# godep
go get github.com/tools/godep

echo [Parser]
go get github.com/qiuyesuifeng/goyacc
go get github.com/qiuyesuifeng/golex
touch temp.XXXXXX
goyacc -o /dev/null -xegen "temp.XXXXXX" parser/parser.y
goyacc -o parser/parser.go -xe "temp.XXXXXX" parser/parser.y
rm -f "temp.XXXXXX"
rm -f y.output

if [ $ARCH = $LINUX ];
then
	sed -i -e 's|//line.*||' -e 's/yyEofCode/yyEOFCode/' parser/parser.go;
elif [ $ARCH = $MAC ];
then
	sed -i "" 's|//line.*||' parser/parser.go;
	sed -i "" 's/yyEofCode/yyEOFCode/' parser/parser.go;
fi

golex -o parser/scanner.go parser/scanner.l


echo [Build]
godep go build -ldflags '$LDFLAGS'

echo [Install] 
godep go install ./...


echo [Test]
godep go test -cover ./...

#done
echo [Done]
