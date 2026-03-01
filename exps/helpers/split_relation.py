#!/usr/bin/env python3

from pathlib import Path


def split_file(input_file, split_ratio):
    """
    Split a file into two parts based on a percentage ratio.
    
    Args:
        input_file: Path to the input file
        split_ratio: Percentage for the top file (0-100)
    """
    split_ratio = float(split_ratio)
    bottom_ratio = 100 - split_ratio

    # Generate output filenames
    input_path = Path(input_file)
    stem = input_path.stem
    parent = input_path.parent

    # Format ratios: remove .0 if it's a whole number
    top_ratio_str = f"{split_ratio:.10g}"  # Remove trailing zeros
    bottom_ratio_str = f"{bottom_ratio:.10g}"  # Remove trailing zeros

    top_file = parent / f"{stem}.part1-{top_ratio_str}percent.dat"
    bottom_file = parent / f"{stem}.part2-{bottom_ratio_str}percent.dat"

    # Skip if output files already exist
    if top_file.exists() and bottom_file.exists():
        print(f"Output files already exist, skipping: {top_file}, {bottom_file}")
        return

    # Read all lines from input file
    with open(input_file, 'r') as f:
        lines = f.readlines()

    total_lines = len(lines)
    print(f"Total lines in {input_file}: {total_lines}")

    # Calculate split point
    split_point = int(total_lines * split_ratio / 100)

    # Write top portion
    with open(top_file, 'w') as f:
        f.writelines(lines[:split_point])
    
    # Write bottom portion
    with open(bottom_file, 'w') as f:
        f.writelines(lines[split_point:])
    
    print(f"Created {top_file} with {split_point} lines")
    print(f"Created {bottom_file} with {total_lines - split_point} lines")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Split a file into two parts based on one or more percentage ratios"
    )
    parser.add_argument("input_file", help="Path to the input file")
    parser.add_argument(
        "split_ratios", type=float, nargs="+", help="One or more split ratios (percentages for the top part, 0-100)"
    )
    args = parser.parse_args()

    for ratio in args.split_ratios:
        split_file(args.input_file, ratio)