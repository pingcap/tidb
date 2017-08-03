package prometheus

import (
	"bytes"
	"os"
	"regexp"
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/procfs"
)

func TestProcessCollector(t *testing.T) {
	if _, err := procfs.Self(); err != nil {
		t.Skipf("skipping TestProcessCollector, procfs not available: %s", err)
	}

	registry := NewRegistry()
	if err := registry.Register(NewProcessCollector(os.Getpid(), "")); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(NewProcessCollectorPIDFn(
		func() (int, error) { return os.Getpid(), nil }, "foobar"),
	); err != nil {
		t.Fatal(err)
	}

	mfs, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	for _, mf := range mfs {
		if _, err := expfmt.MetricFamilyToText(&buf, mf); err != nil {
			t.Fatal(err)
		}
	}

	for _, re := range []*regexp.Regexp{
		regexp.MustCompile("process_cpu_seconds_total [0-9]"),
		regexp.MustCompile("process_max_fds [1-9]"),
		regexp.MustCompile("process_open_fds [1-9]"),
		regexp.MustCompile("process_virtual_memory_bytes [1-9]"),
		regexp.MustCompile("process_resident_memory_bytes [1-9]"),
		regexp.MustCompile("process_start_time_seconds [0-9.]{10,}"),
		regexp.MustCompile("foobar_process_cpu_seconds_total [0-9]"),
		regexp.MustCompile("foobar_process_max_fds [1-9]"),
		regexp.MustCompile("foobar_process_open_fds [1-9]"),
		regexp.MustCompile("foobar_process_virtual_memory_bytes [1-9]"),
		regexp.MustCompile("foobar_process_resident_memory_bytes [1-9]"),
		regexp.MustCompile("foobar_process_start_time_seconds [0-9.]{10,}"),
	} {
		if !re.Match(buf.Bytes()) {
			t.Errorf("want body to match %s\n%s", re, buf.String())
		}
	}
}
