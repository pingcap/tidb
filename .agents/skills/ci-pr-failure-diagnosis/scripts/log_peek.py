#!/usr/bin/env python3
import argparse
import re
import sys
from collections import deque


def parse_range(value: str):
    try:
        start_s, end_s = value.split(":", 1)
        start = int(start_s)
        end = int(end_s)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("range must be START:END") from exc
    if start < 1 or end < start:
        raise argparse.ArgumentTypeError("range must be START:END with 1-based line numbers")
    return start, end


def iter_lines(path: str):
    with open(path, "r", errors="replace") as handle:
        for idx, line in enumerate(handle, 1):
            yield idx, line.rstrip("\n")


def print_line(lineno: int, line: str):
    sys.stdout.write(f"{lineno}: {line}\n")


def head(path: str, count: int):
    for lineno, line in iter_lines(path):
        if lineno > count:
            break
        print_line(lineno, line)


def tail(path: str, count: int):
    buf = deque(maxlen=count)
    for lineno, line in iter_lines(path):
        buf.append((lineno, line))
    for lineno, line in buf:
        print_line(lineno, line)


def slice_range(path: str, start: int, end: int):
    for lineno, line in iter_lines(path):
        if lineno < start:
            continue
        if lineno > end:
            break
        print_line(lineno, line)


def grep(path: str, pattern: str, context: int, ignore_case: bool):
    flags = re.IGNORECASE if ignore_case else 0
    try:
        regex = re.compile(pattern, flags)
    except re.error as exc:
        sys.stderr.write(f"Invalid regex: {exc}\n")
        sys.exit(2)

    before = deque(maxlen=context)
    after = 0
    last_printed = 0

    for lineno, line in iter_lines(path):
        if regex.search(line):
            for b_lineno, b_line in before:
                if b_lineno > last_printed:
                    if last_printed and b_lineno > last_printed + 1:
                        sys.stdout.write("--\n")
                    print_line(b_lineno, b_line)
                    last_printed = b_lineno
            if lineno > last_printed:
                if last_printed and lineno > last_printed + 1:
                    sys.stdout.write("--\n")
                print_line(lineno, line)
                last_printed = lineno
            after = context
            before.clear()
        elif after > 0:
            if lineno > last_printed:
                if last_printed and lineno > last_printed + 1:
                    sys.stdout.write("--\n")
                print_line(lineno, line)
                last_printed = lineno
            after -= 1
        else:
            before.append((lineno, line))


def main():
    parser = argparse.ArgumentParser(
        description="Print log slices without loading the entire log into context."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--head", type=int, help="show first N lines")
    group.add_argument("--tail", type=int, help="show last N lines")
    group.add_argument("--range", type=parse_range, help="show line range START:END")
    group.add_argument("--grep", help="regex pattern to search")
    parser.add_argument(
        "--context",
        type=int,
        default=2,
        help="lines of context for --grep (default: 2)",
    )
    parser.add_argument(
        "--ignore-case", action="store_true", help="case-insensitive --grep"
    )
    parser.add_argument("path", help="path to the log file")
    args = parser.parse_args()

    if args.context < 0:
        parser.error("--context must be >= 0")

    if args.head is None and args.tail is None and args.range is None and args.grep is None:
        args.tail = 200

    try:
        if args.head is not None:
            head(args.path, args.head)
            return
        if args.tail is not None:
            tail(args.path, args.tail)
            return
        if args.range is not None:
            start, end = args.range
            slice_range(args.path, start, end)
            return
        grep(args.path, args.grep, args.context, args.ignore_case)
    except FileNotFoundError:
        sys.stderr.write(f"Error: File not found: {args.path}\n")
        sys.exit(1)
    except BrokenPipeError:
        sys.stderr.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
