import argparse
import random
import os

#!/usr/bin/env python3
"""Extract random relation IDs based on a percentage of total lines."""


def process(input_file, percentage):
    # Generate output filename
    base_name = os.path.splitext(input_file)[0]
    ext = os.path.splitext(input_file)[1]
    output_file = f"{base_name}.randomIDs-{int(percentage)}percent{ext}"

    # Skip if output file already exists
    if os.path.exists(output_file):
        print(f"Output file already exists, skipping: {output_file}")
        return

    # Count lines in file
    with open(input_file, "r") as f:
        num_lines = sum(1 for _ in f)

    # Calculate number of IDs to generate
    num_ids = int(num_lines * percentage / 100)

    # Generate random IDs (0-indexed) - using set to avoid duplicates
    random_ids = set()
    while len(random_ids) < num_ids:
        random_ids.add(random.randint(0, num_lines - 1))

    # Shuffle IDs
    shuffled_ids = list(random_ids)
    random.shuffle(shuffled_ids)

    # Write IDs to file
    with open(output_file, "w") as f:
        for id_ in shuffled_ids:
            f.write(f"{id_}\n")

    print(f"Wrote {len(shuffled_ids)} IDs to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate random IDs based on a percentage of lines in a file"
    )
    parser.add_argument("file", help="Path to the input file")
    parser.add_argument(
        "percentages", type=float, nargs="+", help="One or more percentages of IDs to generate (0-100)"
    )
    args = parser.parse_args()

    for pct in args.percentages:
        process(args.file, pct)


if __name__ == "__main__":
    main()