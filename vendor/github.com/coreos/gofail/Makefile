SRCS=$(filter-out %_test.go, $(wildcard *.go */*.go))

.PHONY: all
all: gofail

.PHONY: clean
clean:
	rm -f gofail

.PHONY: test
test:
	(gofmt -l -s -d $(SRCS) | wc -l | grep "^0$$") || (echo "run make fmt"; false)
	go tool vet -all -shadow  $(SRCS)
	go test -v --race -cpu=1,2,4 ./code/ ./runtime/

.PHONY: fmt
fmt:
	gofmt -w -l -s $(SRCS)

gofail: $(SRCS)
	go build -v
