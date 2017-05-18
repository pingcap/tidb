## BenchFileSort

BenchFileSort is a command line tool to test the performance of util/filesort.

### Quick Start (Examples)

Step 1 - Generate the synthetic data

```
./benchfilesort gen -keySize 8 -valSize 16 -scale 1000
```

Expected output:

```
Generating...
Done!
Data placed in: /path/to/data.out
Time used: xxxx ms
=================================
```

Step 2 - Load the data and run the benchmark

```
./benchfilesort run -bufSize 50 -nWorkers 1 -inputRatio 100 -outputRatio 50
```

Expected output:

```
Loading...
	number of rows = 1000, key size = 8, value size = 16
	load 1000 rows
Done!
Loaded 1000 rows
Time used: xxxx ms
=================================
Inputing...
Done!
Input 1000 rows
Time used: xxxx s
=================================
Outputing...
Done!
Output 500 rows
Time used: xxxx ms
=================================
Closing...
Done!
Time used: xxxx ms
=================================
```

For performance tuning purpose, `Input` time and `Output` time are two KPIs you should focus on.
`Close` time reflects the GC performance, which might be noteworthy sometimes.

### Commands and Arguments

#### `gen` command

The `gen` command generate the synthetic data for the benchmark.

You can specify how many rows you want to generate, the key size
and value size for each row.

The generated data is located in `$dir/data.out` (`$dir` is specified
by the `dir` argument).

The `gen` command supports the following arguments:

* `dir` (default: current working directory)
  Specify the home directory of generated data

* `keySize` (default: 8)
  Specify the key size for generated rows

* `valSize` (default: 8)
  Specify the value size for generated rows

* `scale` (default: 100)
  Specify how many rows to generate

* `cpuprofile` (default: "")
  Turn on the CPU profile

#### `run` command

The `run` command load the synthetic data and run the benchmark.

You can specify the home directory of the synthetic data.

The benchmark will use predefined amount of memory, which is controlled
by the `bufSize` argument, to run the test.

You can control how many rows to input into and output from, which are
defined by the `inputRatio` and `outputRatio` arguments.

The `run` command supports the following arguments:

* `dir` (default: current working directory)
  Specify the home directory of synthetic data

* `bufSize` (default: 500000)
  Specify the amount of memory used by the benchmark

* `nWorkers` (default: 1)
  Specify the number of workers used in async sorting

* `inputRatio` (default: 100)
  Specify the percentage of rows to input:

  `# of rows to input = # of total rows * inputRatio / 100`

* `outputRatio` (default: 100)
  Specify the percentage of rows to output:

  `# of rows to output = # of rows to input * outputRatio / 100`

* `cpuprofile` (default: "")
  Turn on the CPU profile
