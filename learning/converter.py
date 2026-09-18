#!/usr/bin/env python3
# -*- coding: utf-8 -*-
################################################################################
# Project:  SynthDex
# Purpose:  Adaptive Ensemble Indexing for Temporal Information Retrieval via Learned Cost Models
# Author:   Christian Rauch
################################################################################
# Copyright (c) 2025 - 2026
#
# All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
################################################################################

import sys
import os
import json
import math
import random
import re
import pandas as pd


def main():
    if len(sys.argv) < 3:
        print("Usage: converter.py <config_file> <out_dir>", file=sys.stderr)
        sys.exit(1)

    config_file = sys.argv[1]
    out_dir     = sys.argv[2]
    with open(config_file) as f:
        lines = [l for l in f if not l.lstrip().startswith('#')]
        text = re.sub(r',\s*([}\]])', r'\1', ''.join(lines))
        cfg = json.loads(text)
    sample_ratio   = cfg["conversion"]["sample-ratio"]
    dist_ratios    = cfg["conversion"]["distribution-ratios"]
    delete_after   = cfg["conversion"]["delete-OQIPstats-afterwards"]

    oqip_dir = os.path.join(out_dir, "stats", "OQIP")
    if not os.path.isdir(oqip_dir):
        print(f"OQIP directory not found: {oqip_dir}", file=sys.stderr)
        sys.exit(1)

    csv_files = sorted([
        os.path.join(oqip_dir, f)
        for f in os.listdir(oqip_dir)
        if f.endswith('.OQIP.csv')
    ])

    if not csv_files:
        print(f"No .OQIP.csv files found in {oqip_dir}")
        return

    print(f"OQIP files found\t= {len(csv_files)}")
    print(f"Sample ratio\t= {sample_ratio * 100:.1f}%")
    print(f"Distribution ratios\t= training:{dist_ratios['training']} " +
          f"validation:{dist_ratios['validation']} " +
          f"test:{dist_ratios['test']}")

    # Shuffle files before distributing
    random.seed(42)
    random.shuffle(csv_files)

    n = len(csv_files)
    ratio_train = dist_ratios.get("training", 0.0)
    ratio_val   = dist_ratios.get("validation", 0.0)
    ratio_test  = dist_ratios.get("test", 0.0)

    n_train = math.floor(ratio_train * n)
    n_val   = math.floor(ratio_val   * n)
    n_test  = n - n_train - n_val      # remainder to avoid off-by-one

    # If test ratio is 0, keep remainder in training
    if ratio_test == 0.0:
        n_train += n_test
        n_test   = 0

    splits = (
        [("training",   csv_files[:n_train])]
        + [("validation", csv_files[n_train:n_train + n_val])]
        + ([("test",      csv_files[n_train + n_val:])] if n_test > 0 else [])
    )

    print(f"Split\t= training:{n_train} validation:{n_val} test:{n_test}")

    for split_name, files in splits:
        if not files:
            continue
        split_dir = os.path.join(out_dir, "train", split_name)
        os.makedirs(split_dir, exist_ok=True)

        for csv_path in files:
            basename = os.path.basename(csv_path)

            df = pd.read_csv(csv_path, sep='\t')
            total_rows = len(df)
            sample_size = max(1, math.floor(total_rows * sample_ratio))

            df = df.sample(n=sample_size, random_state=42).reset_index(drop=True)

            parquet_name = basename.replace('.OQIP.csv', f'.{sample_size}rows.OQIP.parquet')
            parquet_path = os.path.join(split_dir, parquet_name)

            df.to_parquet(parquet_path, index=False)

            print(f"Converted\t= {basename} -> {split_name}/{parquet_name} "
                  f"({sample_size}/{total_rows} rows, {sample_ratio*100:.1f}%)")

            if delete_after:
                os.remove(csv_path)
                print(f"Deleted\t= {basename}")


if __name__ == "__main__":
    main()
