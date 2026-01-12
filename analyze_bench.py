#!/usr/bin/env python3
"""
Benchmark analysis tool for Go benchmarks
Usage: python3 analyze_bench.py baseline.txt new.txt
"""

import sys
import re
from collections import defaultdict

def parse_benchmark_line(line):
    """Parse a benchmark output line and extract metrics."""
    # Format: BenchmarkName-8   100    12345678 ns/op    1234567 B/op    12345 allocs/op
    parts = line.split()
    if len(parts) < 3:
        return None
    
    name = parts[0]
    iterations = parts[1]
    
    # Extract ns/op
    ns_per_op = None
    for i, part in enumerate(parts):
        if 'ns/op' in part:
            ns_per_op = int(parts[i].replace('ns/op', '').replace(',', ''))
        break
        elif 'µs/op' in part:
            # Convert microseconds to nanoseconds
            ns_per_op = int(float(parts[i].replace('µs/op', '').replace(',', '')) * 1000)
            break
        elif 'ms/op' in part:
            # Convert milliseconds to nanoseconds
            ns_per_op = int(float(parts[i].replace('ms/op', '').replace(',', '')) * 1000000)
            break
    
    # Extract B/op
    b_per_op = None
    for i, part in enumerate(parts):
        if 'B/op' in part:
            b_per_op = int(parts[i].replace('B/op', '').replace(',', ''))
            break
    
    # Extract allocs/op
    allocs_per_op = None
    for i, part in enumerate(parts):
        if 'allocs/op' in part:
            allocs_per_op = int(parts[i].replace('allocs/op', '').replace(',', ''))
            break
    
    return {
        'name': name,
        'iterations': int(iterations),
        'ns_per_op': ns_per_op,
        'b_per_op': b_per_op,
        'allocs_per_op': allocs_per_op
    }

def parse_benchmark_file(filename):
    """Parse a benchmark output file and return a dict of benchmarks."""
    benchmarks = {}
    try:
        with open(filename, 'r') as f:
            for line in f:
                if line.startswith('Benchmark'):
                    bench = parse_benchmark_line(line)
                    if bench:
                        benchmarks[bench['name']] = bench
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found")
        sys.exit(1)
    return benchmarks

def format_ns(ns):
    """Format nanoseconds in a human-readable way."""
    if ns is None:
        return "N/A"
    if ns < 1000:
        return f"{ns}ns"
    elif ns < 1000000:
        return f"{ns/1000:.2f}µs"
    elif ns < 1000000000:
        return f"{ns/1000000:.2f}ms"
    else:
        return f"{ns/1000000000:.2f}s"

def format_bytes(b):
    """Format bytes in a human-readable way."""
    if b is None:
        return "N/A"
    if b < 1024:
        return f"{b}B"
    elif b < 1024 * 1024:
        return f"{b/1024:.2f}KB"
    else:
        return f"{b/(1024*1024):.2f}MB"

def compare_benchmarks(baseline_file, new_file):
    """Compare two benchmark files and print a report."""
    baseline = parse_benchmark_file(baseline_file)
    new = parse_benchmark_file(new_file)
    
    print("=" * 80)
    print("Benchmark Comparison Report")
    print("=" * 80)
    print(f"Baseline: {baseline_file}")
    print(f"New:      {new_file}")
    print()
    
    # Find common benchmarks
    common = set(baseline.keys()) & set(new.keys())
    only_baseline = set(baseline.keys()) - set(new.keys())
    only_new = set(new.keys()) - set(baseline.keys())
    
    if only_baseline:
        print(f"⚠️  Benchmarks only in baseline: {', '.join(only_baseline)}")
    if only_new:
        print(f"⚠️  Benchmarks only in new: {', '.join(only_new)}")
    print()
    
    # Print comparison table
    print(f"{'Benchmark Name':<50} {'Baseline':<15} {'New':<15} {'Change':<10} {'Speedup':<10}")
    print("-" * 100)
    
    improvements = []
    regressions = []
    
    for name in sorted(common):
        b = baseline[name]
        n = new[name]
        
        b_ns = b['ns_per_op']
        n_ns = n['ns_per_op']
        
        if b_ns and n_ns:
            change_pct = ((n_ns - b_ns) / b_ns) * 100
            speedup = b_ns / n_ns if n_ns > 0 else 0
            
            baseline_str = format_ns(b_ns)
            new_str = format_ns(n_ns)
            change_str = f"{change_pct:+.1f}%"
            speedup_str = f"{speedup:.2f}x"
            
            if change_pct < -5:  # More than 5% faster
                improvements.append((name, change_pct, speedup))
                change_str = f"✅ {change_str}"
            elif change_pct > 5:  # More than 5% slower
                regressions.append((name, change_pct, speedup))
                change_str = f"❌ {change_str}"
            
            print(f"{name:<50} {baseline_str:<15} {new_str:<15} {change_str:<10} {speedup_str:<10}")
    
    print()
    print("=" * 80)
    print("Summary")
    print("=" * 80)
    
    if improvements:
        print(f"\n✅ Improvements ({len(improvements)} benchmarks):")
        for name, change, speedup in improvements:
            print(f"  - {name}: {change:.1f}% faster ({speedup:.2f}x speedup)")
    
    if regressions:
        print(f"\n❌ Regressions ({len(regressions)} benchmarks):")
        for name, change, speedup in regressions:
            print(f"  - {name}: {change:.1f}% slower ({speedup:.2f}x slowdown)")
    
    if not improvements and not regressions:
        print("\n✅ No significant changes detected (within 5% threshold)")
    
    # Memory comparison
    print("\n" + "=" * 80)
    print("Memory Comparison")
    print("=" * 80)
    print(f"{'Benchmark Name':<50} {'Baseline (B/op)':<20} {'New (B/op)':<20} {'Change':<10}")
    print("-" * 100)
    
    for name in sorted(common):
        b = baseline[name]
        n = new[name]
        
        b_mem = b['b_per_op']
        n_mem = n['b_per_op']
        
        if b_mem and n_mem:
            change_pct = ((n_mem - b_mem) / b_mem) * 100
            baseline_str = format_bytes(b_mem)
            new_str = format_bytes(n_mem)
            change_str = f"{change_pct:+.1f}%"
            
            if change_pct < -5:
                change_str = f"✅ {change_str}"
            elif change_pct > 5:
                change_str = f"❌ {change_str}"
            
            print(f"{name:<50} {baseline_str:<20} {new_str:<20} {change_str:<10}")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python3 analyze_bench.py baseline.txt new.txt")
        sys.exit(1)
    
    compare_benchmarks(sys.argv[1], sys.argv[2])
