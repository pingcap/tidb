# Auto-Analyze Priority Calculator Test

This test suite evaluates the effectiveness of the auto-analyze priority calculator for database statistics. The calculator prioritizes which tables should be analyzed based on factors such as table size, data changes, and time since last analysis.

## Core Principles

1. **Data Generation**: The test generates diverse scenarios using combinations of:
   - Table sizes (ranging from 1K to 100M rows)
   - Changes in data
   - Time since last analysis (ranging from 10 seconds to 3 days)

2. **Priority Calculation**: For each scenario, the priority calculator computes a weight based on:
   - Change percentage (changes / table size)
   - Table size
   - Time since last analysis

3. **Realistic Constraints**: The test data is generated with realistic constraints:
   - Change rate calculation based on table size
   - Changes capped at 300% of table size
   - Minimum meaningful change of 1000 rows

## How It Works

1. `generateTestData()` creates test scenarios directly in memory.
2. `TestPriorityCalculatorWithGeneratedData()` is the main test function that:
   - Generates test data using `generateTestData()`
   - Calculates new priorities using `PriorityCalculator`
   - Sorts the priorities in descending order
   - Compares the results with `calculated_priorities.golden.csv`

## Key Files

- `calculator_analysis_test.go`: Contains the main test logic and data generation
- `calculated_priorities.golden.csv`: Expected output with calculated priorities for all scenarios

## Running the Test

To run the test:
```shell
go test -v ./pkg/statistics/handle/autoanalyze/priorityqueue/calculatoranalysis
```
If you want to update the golden file, run the test with `-update` flag:
```shell
go test -v ./pkg/statistics/handle/autoanalyze/priorityqueue/calculatoranalysis -update
```

## Output

The test compares the generated priorities against the golden file `calculated_priorities.golden.csv` with
- ID
- CalculatedPriority
- TableSize
- Changes
- TimeSinceLastAnalyze
- ChangeRatio

This output allows for analysis of how the priority calculator behaves across different scenarios.

This test helps ensure that the priority calculator provides consistent and predictable behavior in the auto-analyze feature across a wide range of realistic scenarios.
