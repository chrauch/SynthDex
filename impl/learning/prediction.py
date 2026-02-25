#!/usr/bin/env python3
# -*- coding: utf-8 -*-
################################################################################
# Project:  synthdex
# Purpose:  Adaptive TIR indexing
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

from math import log10
import sys
import os
import torch
import pandas as pd
import torch.nn as nn
import re

sys.path.insert(0, os.path.dirname(__file__))

from learnedcostmodel import LCM
from learningbase import LearningBase


class Prediction(LearningBase):

    def __init__(self, config_dir):
        super().__init__(config_dir)
        self.model = self.load_model(self.cfg["out"]["machine-prefix"])


    def predict(self, input_file: str):
        """
        Make predictions on data from a CSV file using the trained model.
        
        Args:
            input_file: Path to CSV file containing input features
        """

        print("Prediction")

        data, original_df = self.load_input_data(input_file)
        
        predictions = self.make_predictions(self.model, data)
        
        self.save_predictions(predictions, input_file, original_df)
        

    def load_input_data(self, input_file: str) -> tuple[torch.Tensor, pd.DataFrame]:
        """Load and preprocess input data from CSV file.
        
        Returns:
            Tuple of (features_tensor, original_dataframe)
        """
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        print(f"  Input  = {input_file}")

        # Load CSV/TSV data (TSV in our pipeline)
        df = pd.read_csv(input_file, sep='\t')

        # Remove target columns if present (for prediction, we only need features)
        feature_columns = [col for col in df.columns if col not in self.cfg["learning"]["targets"]]
        print(f"  Shape  = {len(df)} rows and {len(df.columns)} columns ({len(feature_columns)} features)")
        features_df = df[feature_columns]
                
        # Convert to tensor (and validate dimensions)
        features_tensor = torch.tensor(features_df.values, dtype=torch.float32)
        if features_tensor.shape[1] != self.cfg["learning"]["features-dim"]:
            raise ValueError(f"Input data has {features_tensor.shape[1]} features, "
                           f"but model expects {self.cfg["learning"]["features-dim"]}")
        
        return features_tensor, df
    

    def make_predictions(self, model: nn.Module, data: torch.Tensor) -> torch.Tensor:
        """Make predictions using the trained model."""

        with torch.no_grad(): predictions = model(data)

        return predictions


    def save_predictions(self, predictions: torch.Tensor, input_file: str, original_df: pd.DataFrame):
        """Save predictions to a CSV file along with original input data.
        
        Args:
            predictions: Model predictions tensor
            input_file: Original input file path
            original_df: Original input DataFrame containing all columns
        
        Note:
            Aggregation converts log predictions to linear space then sums:
            - Throughput: sum(10^log_pred) gives total time; N/total_time gives overall q/s
            - Size: sum(10^log_pred) gives total size
        """
        # Convert predictions to numpy and then to DataFrame
        pred_numpy = predictions.numpy()
        
        # Create DataFrame with target column names
        target_columns = [f"predicted_{target}" for target in self.cfg["learning"]["targets"]]
        pred_df = pd.DataFrame(pred_numpy, columns=target_columns)
        
        # Combine original data with predictions
        # Original data comes first, then predictions
        combined_df = pd.concat([original_df.reset_index(drop=True), pred_df], axis=1)
        
        # Generate output filename
        input_name = os.path.splitext(os.path.basename(input_file))[0]
        stats_dir = os.path.join(self.output_dir, "stats", "OQIP-predictions")
        os.makedirs(stats_dir, exist_ok=True)
        output_file = os.path.join(stats_dir, f"{input_name}P-predictions.csv")
        
        # Save combined data with predictions (using tab separator to match input format)
        combined_df.to_csv(output_file, sep='\t', index=False)
        print(f"  Output = {output_file}")
        
        # Print predictions
        sum = {}
        for col in target_columns:
            sum[col] = 0
        if self.cfg["prediction"]["top-k"] > 0:
            print(f"  Sample predictions (first {self.cfg["prediction"]["top-k"]}):")
        for i in range(len(pred_df)):
            for col in target_columns:
                value = pred_df.iloc[i][col]
                valuex = 10**value
                sum[col] += valuex
                if i < self.cfg["prediction"]["top-k"]:
                    print(f"    Sample {i+1} {col}: {10**value:.10f} (log: {value:.8f})")

        def format_col_name(col_name: str) -> str:
            col_name = re.sub(r'_log$', '    ', col_name)
            return col_name

        # Print aggregate values
        # For throughput (time per query), we sum linear values to get total time,
        # then compute overall throughput. For size, we sum linear values to get total size.
        print(f"\n  Aggregate predictions over {len(pred_df)} queries:")
        
        for col in target_columns:
            # Get log predictions for this target
            log_predictions = pred_df[col].values
            
            # Convert to linear space and sum
            linear_values = 10 ** log_predictions
            total_linear = linear_values.sum()
            
            # For throughput: sum gives total time for all queries
            if col.find("throughput") != -1:
                # Total time for all queries (sum of s/q values)
                total_time_seconds = total_linear
                # Overall throughput: queries / total_time
                overall_throughput_qps = len(pred_df) / total_time_seconds if total_time_seconds > 0 else 0
                # Average time per query
                avg_time_per_query = total_time_seconds / len(pred_df)
                
                print(f"    {format_col_name(col)} avg [s/q]:  {avg_time_per_query:.10f}")
                print(f"    {format_col_name(col)} overall [q/s]: {overall_throughput_qps:.10f}")
            
            # For size: sum gives total size
            elif col.find("size") != -1:
                total_size_bytes = total_linear
                avg_size_bytes = total_size_bytes / len(pred_df)
                
                print(f"    {format_col_name(col)} avg:   {avg_size_bytes:.2f} bytes ({(avg_size_bytes / (1024*1024)):.3f} MB)")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Arguments missing: <config_dir> <input_file1> [<input_file2> ...]")
        sys.exit(1)
    
    config_dir = sys.argv[1]
    input_files = sys.argv[2:]
    
    predictor = Prediction(config_dir)

    exit_code = 0
    for input_file in input_files:
        try:
            predictor.predict(input_file)
        except Exception as e:
            print(f"ERROR while processing {input_file}: {e}")
            exit_code = 1
    sys.exit(exit_code)
