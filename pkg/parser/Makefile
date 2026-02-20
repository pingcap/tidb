.PHONY: all clean test generate

all: fmt generate

test: fmt
	sh test.sh

genkeyword: generate_keyword/genkeyword.go
	go build -C generate_keyword -o ../genkeyword

generate: genkeyword
	go generate

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w . 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

clean:
	go clean -i ./...
	rm -rf *.out
