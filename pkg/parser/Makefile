.PHONY: all parser clean

all: fmt parser generate

test: fmt parser
	sh test.sh

parser: parser.go hintparser.go

genkeyword: generate_keyword/genkeyword.go
	go build -C generate_keyword -o ../genkeyword

generate: genkeyword parser.y
	go generate

%arser.go: prefix = $(@:parser.go=)
%arser.go: %arser.y bin/goyacc
	@echo "bin/goyacc -o $@ -p yy$(prefix) -t $(prefix)Parser $<"
	@bin/goyacc -o $@ -p yy$(prefix) -t $(prefix)Parser $< || ( rm -f $@ && echo 'Please check y.output for more information' && exit 1 )
	@rm -f y.output

%arser_golden.y: %arser.y
	@bin/goyacc -fmt -fmtout $@ $<
	@(git diff --no-index --exit-code $< $@ && rm $@) || (mv $@ $< && >&2 echo "formatted $<" && exit 1)

bin/goyacc: goyacc/main.go goyacc/format_yacc.go
	GO111MODULE=on go build -o bin/goyacc goyacc/main.go goyacc/format_yacc.go

fmt: bin/goyacc parser_golden.y hintparser_golden.y
	@echo "gofmt (simplify)"
	@gofmt -s -l -w . 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

clean:
	go clean -i ./...
	rm -rf *.out
	rm -f parser.go hintparser.go
