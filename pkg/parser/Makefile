.PHONY: all clean test fmt generate

all: fmt

test: fmt
	sh test.sh

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w . 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

generate:
	@echo "Hand-written parser: no code generation needed"

clean:
	go clean -i ./...
	rm -rf *.out
