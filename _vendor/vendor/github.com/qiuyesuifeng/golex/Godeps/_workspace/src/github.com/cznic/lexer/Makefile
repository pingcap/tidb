.PHONY: all clean editor later nuke todo

grep=--include=*.go

all: editor
	make todo

clean:
	go clean
	rm -f *~

editor:
	go fmt
	go test -i
	go test
	go build

later:
	@grep -n $(grep) LATER * || true
	@grep -n $(grep) MAYBE * || true

nuke: clean
	go clean -i

todo:
	@grep -nr $(grep) ^[[:space:]]*_[[:space:]]*=[[:space:]][[:alpha:]][[:alnum:]]* * || true
	@grep -nr $(grep) TODO * || true
	@grep -nr $(grep) BUG * || true
	@grep -nr $(grep) println * || true
