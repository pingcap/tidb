.PHONY: all clean test fmt

all: fmt

test: fmt
	sh test.sh

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w . 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

clean:
	go clean -i ./...
	rm -rf *.out
