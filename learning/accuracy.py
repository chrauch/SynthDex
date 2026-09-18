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

"""
Parquet-based accuracy check. Loads previously recorded execution data from
parquet files, runs model predictions on the features, and compares against
the recorded actual performance values.

Usage: accuracy.py <config_file> <output_dir>
"""

import sys
import os
import torch
import pandas as pd
import numpy as np
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

from learningbase import LearningBase


class ParquetAccuracy(LearningBase):

    def __init__(self, config_file, output_dir):
        super().__init__(config_file, output_dir)
        self.model = self.load_model(self.cfg["out"]["machine-prefix"])
        if self.model is None:
            raise ValueError(
                f"No trained model found matching "
                f"'{self.cfg['out']['machine-prefix']}' in "
                f"{os.path.join(self.output_dir, 'lcms')}. Run training first.")

    def run(self):
        test_dir = os.path.join(self.output_dir, "train", "test")
        targets = self.cfg["learning"]["targets"]
        features_dim = self.cfg["learning"]["features-dim"]
        aggregate = self.cfg["accuracy"].get(
            "from-parquet-aggregate-OI", True)

        # Load all parquet files from test directory
        if not os.path.exists(test_dir):
            raise FileNotFoundError(f"Test directory not found: {test_dir}")

        parquet_files = sorted(
            f for f in os.listdir(test_dir) if f.endswith('.parquet'))
        if not parquet_files:
            raise FileNotFoundError(
                f"No parquet files found in: {test_dir}")

        print(f"\tTest directory = {test_dir}")
        print(f"\tParquet files = {len(parquet_files)}")
        print(f"\tTargets = {targets}")
        print(f"\tAggregate O+I = {aggregate}")

        # Load all data
        dfs = []
        for pf in parquet_files:
            df = pd.read_parquet(os.path.join(test_dir, pf))
            dfs.append(df)
        data = pd.concat(dfs, ignore_index=True)
        print(f"\tTotal rows = {len(data)}")

        # Extract actual target values
        for t in targets:
            if t not in data.columns:
                raise ValueError(f"Target column '{t}' not found in parquet data")

        # Extract features (numeric columns excluding targets)
        feature_cols = [c for c in data.columns if c not in targets]
        features_df = data[feature_cols].select_dtypes(include='number')

        if features_df.shape[1] != features_dim:
            raise ValueError(
                f"Feature dimension mismatch: data has {features_df.shape[1]}, "
                f"model expects {features_dim}")

        print(f"\tFeatures = {features_df.shape[1]}")

        # Predict
        features_tensor = torch.tensor(
            features_df.to_numpy(dtype='float32'), dtype=torch.float32)

        with torch.no_grad():
            predictions = self.model(features_tensor).numpy()

        # Add predictions to dataframe
        for i, t in enumerate(targets):
            data[f"predicted_{t}"] = predictions[:, i]

        if aggregate:
            self._write_aggregate(data, targets, parquet_files)
        else:
            self._write_per_query(data, targets, parquet_files)

    def _group_key_cols(self, data):
        """Return columns that identify a unique O+I combination."""
        i_enc_cols = [c for c in data.columns if c.startswith('i_enc-')]
        return ['O_name'] + sorted(i_enc_cols)

    def _write_aggregate(self, data, targets, parquet_files):
        """Write accuracy CSV aggregated by O+I (one row per index)."""
        key_cols = self._group_key_cols(data)
        groups = data.groupby(key_cols, sort=False)
        n_groups = len(groups)
        print(f"\tO+I groups = {n_groups}")

        acc_dir = os.path.join(self.output_dir, "accuracy")
        os.makedirs(acc_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        out_path = os.path.join(acc_dir, f"accuracy-parquet-{ts}.csv")

        sum_abs = {t: 0.0 for t in targets}
        sum_rel = {t: 0.0 for t in targets}

        with open(out_path, 'w') as f:
            cols = ["group", "queries", "O_name", "i_schema"]
            for t in targets:
                short = t.replace("p_", "").replace("_log", "")
                cols += [
                    f"actual_{short}_log",
                    f"predicted_{short}_log",
                    f"disc_{short}_log",
                    f"actual_{short}",
                    f"predicted_{short}",
                    f"disc_{short}_rel",
                ]
            f.write("\t".join(cols) + "\n")

            for gi, (key, grp) in enumerate(groups):
                o_name = key[0] if isinstance(key, tuple) else key
                i_enc_vals = key[1:] if isinstance(key, tuple) else []
                i_schema = ";".join(f"{v}" for v in i_enc_vals)
                n_queries = len(grp)

                vals = [str(gi), str(n_queries), str(o_name), i_schema]

                for t in targets:
                    actual_log_vals = grp[t].values
                    pred_log_vals = grp[f"predicted_{t}"].values

                    actual_linear = 10.0 ** actual_log_vals
                    pred_linear = 10.0 ** pred_log_vals

                    avg_actual = float(np.mean(actual_linear))
                    avg_pred = float(np.mean(pred_linear))

                    actual_log = np.log10(avg_actual) if avg_actual > 0 else -10
                    pred_log = np.log10(avg_pred) if avg_pred > 0 else -10
                    disc_log = pred_log - actual_log
                    disc_rel = abs(avg_pred - avg_actual) / avg_actual if avg_actual > 0 else 0

                    sum_abs[t] += abs(disc_log)
                    sum_rel[t] += disc_rel

                    vals += [
                        f"{actual_log:.8f}",
                        f"{pred_log:.8f}",
                        f"{disc_log:.8f}",
                        f"{avg_actual:.8f}",
                        f"{avg_pred:.8f}",
                        f"{disc_rel:.8f}",
                    ]
                f.write("\t".join(vals) + "\n")

            f.write(f"\n# Summary ({n_groups} O+I groups, "
                    f"{len(data)} rows from {len(parquet_files)} files)\n")
            for t in targets:
                short = t.replace("p_", "").replace("_log", "")
                f.write(f"# MAE {short} (log):  {sum_abs[t] / n_groups:.6f}\n")
                f.write(f"# MRE {short}:        {sum_rel[t] / n_groups:.6f}\n")

        print(f"\n\tAccuracy CSV = {out_path}")
        print(f"\tEvaluations = {n_groups} O+I groups")
        for t in targets:
            short = t.replace("p_", "").replace("_log", "")
            print(f"\tMAE {short} (log) = {sum_abs[t] / n_groups:.6f}")
            print(f"\tMRE {short} = {sum_rel[t] / n_groups:.6f}")

    def _write_per_query(self, data, targets, parquet_files):
        """Write accuracy CSV with one row per query."""
        acc_dir = os.path.join(self.output_dir, "accuracy")
        os.makedirs(acc_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        out_path = os.path.join(acc_dir, f"accuracy-parquet-{ts}.csv")

        n = len(data)
        sum_abs = {t: 0.0 for t in targets}
        sum_rel = {t: 0.0 for t in targets}

        with open(out_path, 'w') as f:
            cols = ["row"]
            for t in targets:
                short = t.replace("p_", "").replace("_log", "")
                cols += [
                    f"actual_{short}_log",
                    f"predicted_{short}_log",
                    f"disc_{short}_log",
                    f"disc_{short}_rel",
                ]
            f.write("\t".join(cols) + "\n")

            for row in range(n):
                vals = [str(row)]
                for t in targets:
                    a = float(data[t].iloc[row])
                    p = float(data[f"predicted_{t}"].iloc[row])
                    disc_log = p - a
                    a_lin = 10.0 ** a
                    p_lin = 10.0 ** p
                    disc_rel = abs(p_lin - a_lin) / a_lin if a_lin > 0 else 0
                    sum_abs[t] += abs(disc_log)
                    sum_rel[t] += disc_rel
                    vals += [
                        f"{a:.8f}",
                        f"{p:.8f}",
                        f"{disc_log:.8f}",
                        f"{disc_rel:.8f}",
                    ]
                f.write("\t".join(vals) + "\n")

            f.write(f"\n# Summary ({n} rows from {len(parquet_files)} files)\n")
            for t in targets:
                short = t.replace("p_", "").replace("_log", "")
                f.write(f"# MAE {short} (log):  {sum_abs[t] / n:.6f}\n")
                f.write(f"# MRE {short}:        {sum_rel[t] / n:.6f}\n")

        print(f"\n\tAccuracy CSV = {out_path}")
        print(f"\tEvaluations = {n} rows from {len(parquet_files)} files")
        for t in targets:
            short = t.replace("p_", "").replace("_log", "")
            print(f"\tMAE {short} (log) = {sum_abs[t] / n:.6f}")
            print(f"\tMRE {short} = {sum_rel[t] / n:.6f}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Arguments: <config_file> <output_dir>")
        sys.exit(1)

    config_file = sys.argv[1]
    output_dir = sys.argv[2]

    ParquetAccuracy(config_file, output_dir).run()
