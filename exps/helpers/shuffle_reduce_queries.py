#!/usr/bin/env python3

import sys
import random
from pathlib import Path


def shuffle_file(input_file, percentage=100, keep_header=False):
    """
    Shuffle all lines in a file randomly and optionally reduce to a percentage.
    
    Args:
        input_file: Path to the input file
        percentage: Percentage of lines to keep (1-100). Default is 100 (keep all lines).
        keep_header: If True, preserves the first line as a header. Default is False.
    """
    # Validate percentage
    if not 1 <= percentage <= 100:
        raise ValueError("Percentage must be between 1 and 100")
    
    # Read all lines from input file
    with open(input_file, 'r') as f:
        lines = f.readlines()
    
    total_lines = len(lines)
    print(f"Total lines in {input_file}: {total_lines}")
    
    # Ensure all lines end with newline
    lines = [line if line.endswith('\n') else line + '\n' for line in lines]
    
    # Separate header if needed
    header = None
    if keep_header and len(lines) > 0:
        header = lines[0]
        lines = lines[1:]
        print(f"Keeping header row separate")
    
    # Shuffle the lines (excluding header)
    random.shuffle(lines)
    
    # Reduce to specified percentage (excluding header from count)
    data_lines = len(lines)
    if percentage < 100:
        num_lines_to_keep = max(1, int(data_lines * percentage / 100))
        lines = lines[:num_lines_to_keep]
        print(f"Reducing to {percentage}% of data lines: {num_lines_to_keep} lines")
    
    # Generate output filename
    input_path = Path(input_file)
    stem = input_path.stem
    suffix = input_path.suffix
    parent = input_path.parent
    
    if percentage < 100:
        output_file = parent / f"{stem}.shuffled.{percentage}pct{suffix}"
    else:
        output_file = parent / f"{stem}.shuffled{suffix}"
    
    # Write shuffled lines (with header if applicable)
    with open(output_file, 'w') as f:
        if header is not None:
            f.write(header)
        f.writelines(lines)
    
    total_output_lines = len(lines) + (1 if header is not None else 0)
    print(f"Created {output_file} with {total_output_lines} lines")


if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 4:
        print("Usage: python shuffle_reduce_queries.py <input_file> [percentage] [--keep-header]")
        print("  input_file: Path to the file to shuffle")
        print("  percentage: Optional. Percentage of lines to keep (1-100). Default: 100")
        print("  --keep-header: Optional. If specified, preserves the first line as a header")
        print("\nExamples:")
        print("  python shuffle_reduce_queries.py queries.txt 15")
        print("  python shuffle_reduce_queries.py queries.txt 15 --keep-header")
        print("  python shuffle_reduce_queries.py queries.txt 100 --keep-header")
        sys.exit(1)
    
    input_file = sys.argv[1]
    percentage = 100
    keep_header = False
    
    # Parse optional arguments
    for i in range(2, len(sys.argv)):
        if sys.argv[i] == "--keep-header":
            keep_header = True
        else:
            percentage = int(sys.argv[i])
    
    shuffle_file(input_file, percentage, keep_header)
