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
Base class for synthesis implementations.

Provides common functionality for all index parameter optimization strategies:
  - Model loading and initialization
  - OQ/I file loading
  - Results saving to CSV
  - Main entry point logic

Subclasses must implement:
  - get_variable_features(i_enc_min, i_enc_max) -> list
  - infer(oq_file) -> None (optimize and populate self.synthesized_i)
"""
import sys
import os
import pandas as pd
import numpy as np

# Add parent directory to path to enable imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from learning.learningbase import LearningBase
from skyline import compute_skyline_minimize, skyline_stats


class SynthesisBase(LearningBase):
    """
    Abstract base class for synthesis optimization strategies.
    """

    def __init__(self, config_file, output_dir):
        """
        Initialize synthesis with configuration file path.
        
        Args:
            config_file: Path to configuration file
            output_dir: Path to output directory
        """
        super().__init__(config_file, output_dir)
        self.model = self.load_model(self.cfg["out"]["machine-prefix"])
        self.synthesized_i = {}
        self.synth_id = 0  # Incremented with each stored synthesis result
        self.evaluated_configs_count = 0  # Incremented with each evaluation (stored or not)
        self.synthesis_start_time = None  # Track when synthesis started
        self.total_io_time = 0.0  # Track time spent on I/O operations


    def load_oq(self, oq_file):
        """
        Load query workload (OQ) statistics from CSV file.
        
        Args:
            oq_file: Path to OQ statistics CSV file (tab-separated)
        """
        import time
        io_start = time.time()
        print(f"OQ statistics file = {oq_file}")
        self.OQ = pd.read_csv(oq_file, sep='\t')
        self.OQ = self.OQ.drop(columns=[c for c in ['O_name'] if c in self.OQ.columns])
        print(f"\tShape = {len(self.OQ)} rows and {len(self.OQ.columns)} columns")
        print(f"\tQueries received: {len(self.OQ)}")
        self.total_io_time += time.time() - io_start


    def load_i(self, i_file):
        """
        Load index configuration (I) statistics from CSV file.
        
        Args:
            i_file: Path to I statistics CSV file (tab-separated)
        """
        import time
        io_start = time.time()
        print(f"I statistics file = {i_file}")
        self.I = pd.read_csv(i_file, sep='\t')
        print(f"\tShape = {len(self.I)} rows and {len(self.I.columns)} columns")
        self.total_io_time += time.time() - io_start


    def save(self):
        """
        Save optimized index configurations to CSV files.
        
        For each OQ file processed, creates a corresponding I-synthesis CSV file
        with optimized parameters and predicted throughput/size for all index patterns.
        Applies skyline algorithm to filter out dominated configurations before saving.
        
        A configuration is dominated if another configuration has:
        - Better or equal throughput (lower log10(s/q)) AND
        - Better or equal size (lower log10(bytes)) AND
        - At least one strictly better
        
        If the file already exists, appends the new results without writing the header.
        """
        # Output overall throughput metrics (before any file I/O)
        if self.synthesis_start_time is not None:
            import time
            elapsed_time = time.time() - self.synthesis_start_time
            synthesis_time = elapsed_time - self.total_io_time
            if synthesis_time > 0:
                configs_per_second = self.evaluated_configs_count / synthesis_time
                # Calculate queries per config (sample size from last OQ file)
                sample_size = min(len(self.OQ), self.cfg["synthesis"]["sample-size"]) if hasattr(self, 'OQ') else 0
                print(f"Evaluated configurations: {self.evaluated_configs_count}")
                print(f"Stored configurations:    {self.synth_id}")
                print(f"Queries per evaluation:   {sample_size}")
                print(f"Total time elapsed [s]:   {elapsed_time:.2f}")
                print(f"I/O time (excluded) [s]:  {self.total_io_time:.2f}")
                print(f"Synthesis time net [s]:   {synthesis_time:.2f}")
                print(f"Query evaluations [q/s]:  {(configs_per_second * sample_size):.2f}")
                print(f"Index evaluations [i/s]:  {configs_per_second:.2f}")
                self.save_run_metrics(elapsed_time, synthesis_time, sample_size)
        else :
            print("No synthesis evaluations were performed.")
            self.save_run_metrics(0.0, 0.0, 0)
        
        
        # Group results by OQ file
        oq_file_results = {}
        for key in self.synthesized_i.keys():
            variant, oq_file, template_id, synth_id = key
            if oq_file not in oq_file_results:
                oq_file_results[oq_file] = []
            oq_file_results[oq_file].append(self.synthesized_i[key])
        
        # Save each OQ file's results
        for oq_file, results_list in oq_file_results.items():
            output_file = oq_file.replace(".OQ.csv", ".I-synthesis.csv") \
                .replace("/OQ/", "/I-synthesis/")

            # Create DataFrame with appropriate columns (variant added as first column)
            columns = ["variant", "OQ_file", "temporal_band", "template_id", "synth_id", "predicted_throughput", "predicted_size"] + list(self.I.columns)
            synthesized_i_df = pd.DataFrame(results_list, columns=columns)
            
            # Ensure numeric columns are properly typed (in case they're stored as strings)
            synthesized_i_df['predicted_throughput'] = pd.to_numeric(synthesized_i_df['predicted_throughput'], errors='coerce')
            synthesized_i_df['predicted_size'] = pd.to_numeric(synthesized_i_df['predicted_size'], errors='coerce')
            
            # Check if file exists - if so, load existing results and combine
            file_exists = os.path.isfile(output_file)
            if file_exists:
                existing_df = pd.read_csv(output_file, sep='\t')
                # Ensure numeric columns are properly typed
                existing_df['predicted_throughput'] = pd.to_numeric(existing_df['predicted_throughput'], errors='coerce')
                existing_df['predicted_size'] = pd.to_numeric(existing_df['predicted_size'], errors='coerce')
                # Combine existing and new results
                synthesized_i_df = pd.concat([existing_df, synthesized_i_df], ignore_index=True)
                print(f"\tCombined {len(existing_df)} existing + {len(results_list)} new = {len(synthesized_i_df)} total configurations")
            
            # Ensure we have at least one configuration
            if len(synthesized_i_df) == 0:
                raise RuntimeError(f"No configurations to save for {oq_file}. This should not happen - check store() calls.")
            
            # Apply skyline algorithm to filter dominated configurations
            if self.cfg["synthesis"].get("skyline", False):
                # Extract throughput and size columns (indices 5 and 6)
                objectives = synthesized_i_df.iloc[:, [5, 6]].values  # [predicted_throughput, predicted_size]
                
                if self.cfg["synthesis"]["verbose"]:
                    print(f"\tObjectives before skyline (total: {len(objectives)}, first 5):")
                    for idx in range(min(5, len(objectives))):
                        print(f"\t  Row {idx}: throughput={objectives[idx, 0]:.6f}, size={objectives[idx, 1]:.6f}")
                
                # Compute skyline mask (both objectives should be minimized)
                skyline_mask = compute_skyline_minimize(objectives)
                
                # Get statistics
                stats = skyline_stats(objectives)
                print(f"\tSkyline filtering: {stats['skyline']}/{stats['total']} configurations retained "
                      f"({stats['dominated']} dominated configurations removed)")
                
                if self.cfg["synthesis"]["verbose"]:
                    print(f"\tSkyline mask: {skyline_mask[:min(10, len(skyline_mask))]}")
                
                # Filter to keep only skyline configurations
                synthesized_i_df = synthesized_i_df[skyline_mask].reset_index(drop=True)
                
                # Post-process: remove near-duplicates from skyline using tolerance
                skyline_tolerance = self.cfg["synthesis"].get("skyline-tolerance", 0.005)
                if skyline_tolerance > 0 and len(synthesized_i_df) > 1:
                    # Extract objectives from skyline points
                    skyline_objectives = synthesized_i_df.iloc[:, [5, 6]].values
                    
                    if self.cfg["synthesis"]["verbose"]:
                        print(f"\tApplying tolerance pruning (±{skyline_tolerance*100:.2f}%)...")
                    
                    # Sort by throughput (primary) and size (secondary) - both ascending for minimization
                    sort_indices = np.lexsort((skyline_objectives[:, 1], skyline_objectives[:, 0]))
                    sorted_objectives = skyline_objectives[sort_indices]
                    
                    # Build mask of which skyline points to keep after tolerance pruning
                    keep_mask = np.zeros(len(sorted_objectives), dtype=bool)
                    keep_mask[0] = True  # Always keep the first (best) one
                    last_kept_idx = 0
                    
                    for i in range(1, len(sorted_objectives)):
                        current = sorted_objectives[i]
                        last_kept = sorted_objectives[last_kept_idx]
                        
                        # Check if current should be pruned based on tolerance
                        # Convert from log space to linear space and check percentage point difference
                        # Prune if: current doesn't improve over last_kept by more than tolerance in ANY dimension
                        
                        differences_pct_points = np.abs(np.power(10, current) / np.power(10, last_kept) - 1.0)
                        
                        # Check if current is better (lower) than last_kept in each dimension
                        is_better = current < last_kept
                        is_significantly_better = (differences_pct_points > skyline_tolerance) & is_better
                        
                        # Prune if current is not significantly better in ANY dimension
                        should_prune = not np.any(is_significantly_better)
                        
                        if self.cfg["synthesis"]["verbose"]:
                            current_linear = np.power(10, current)
                            last_kept_linear = np.power(10, last_kept)
                            print(f"\t  Checking sorted_idx={i}: throughput={current[0]:.6f}, size={current[1]:.6f}")
                            print(f"\t    vs last_kept (idx={last_kept_idx}): throughput={last_kept[0]:.6f}, size={last_kept[1]:.6f}")
                            print(f"\t    Linear values: current=[{current_linear[0]:.6e}, {current_linear[1]:.6e}], "
                                  f"last_kept=[{last_kept_linear[0]:.6e}, {last_kept_linear[1]:.6e}]")
                            print(f"\t    Differences (percentage points): throughput={differences_pct_points[0]*100:.2f}%, size={differences_pct_points[1]*100:.2f}%")
                            print(f"\t    Is better: throughput={is_better[0]}, size={is_better[1]}")
                            print(f"\t    Is significantly better (>{skyline_tolerance*100:.2f}pp): throughput={is_significantly_better[0]}, size={is_significantly_better[1]}")
                            print(f"\t    Tolerance: {skyline_tolerance*100:.2f} percentage points")
                            print(f"\t    Should prune: {should_prune}")
                        
                        if should_prune:
                            # Not significantly better in any dimension, prune
                            if self.cfg["synthesis"]["verbose"]:
                                print(f"\t    -> PRUNING (not significantly better in any dimension)")
                            keep_mask[i] = False
                        else:
                            # Significantly better in at least one dimension, keep it
                            if self.cfg["synthesis"]["verbose"]:
                                print(f"\t    -> KEEPING (significantly better in at least one dimension)")
                            keep_mask[i] = True
                            last_kept_idx = i
                    
                    # Apply the keep mask (need to unsort first)
                    unsort_indices = np.argsort(sort_indices)
                    keep_mask_original_order = keep_mask[unsort_indices]
                    
                    n_before_pruning = len(synthesized_i_df)
                    synthesized_i_df = synthesized_i_df.iloc[keep_mask_original_order].reset_index(drop=True)
                    n_after_pruning = len(synthesized_i_df)
                    
                    print(f"\tTolerance pruning (±{skyline_tolerance*100:.2f}%): "
                          f"{n_after_pruning}/{n_before_pruning} configurations retained "
                          f"({n_before_pruning - n_after_pruning} near-duplicates removed)")
                else:
                    if skyline_tolerance <= 0:
                        print(f"\tTolerance pruning disabled (skyline-tolerance={skyline_tolerance})")
                    elif len(synthesized_i_df) <= 1:
                        print(f"\tTolerance pruning skipped (only {len(synthesized_i_df)} configuration)")
            else:
                print(f"\tSkyline filtering disabled - keeping all {len(synthesized_i_df)} configurations")
            
            # Write complete set (overwrite file)
            synthesized_i_df.to_csv(
                output_file, sep='\t',
                mode='w',  # Always overwrite with complete set
                header=True,
                index=False)
            
            print(f"Optimal I statistics file = {output_file}")


    def save_run_metrics(self, elapsed_time, synthesis_time, sample_size):
        """
        Emit machine-readable metrics for the synthesis run just completed.

        Overwritten on every run; consumed by the C++ tuning runner immediately
        after the process exits to attribute probe counts and timings to the
        hyper-parameter setting under test.
        """
        metrics_dir = os.path.join(self.output_dir, "synthesis")
        os.makedirs(metrics_dir, exist_ok=True)
        path = os.path.join(metrics_dir, "run-metrics.tsv")

        with open(path, "w") as f:
            f.write("variant\tevaluated_configs\tstored_configs\tqueries_per_eval"
                    "\ttotal_time_s\tio_time_s\tsynthesis_time_s\n")
            f.write(f"{self.variant}\t{self.evaluated_configs_count}\t{self.synth_id}"
                    f"\t{sample_size}\t{elapsed_time:.6f}\t{self.total_io_time:.6f}"
                    f"\t{synthesis_time:.6f}\n")

        print(f"Run metrics file = {path}")


    def get_variable_features(self, i_enc_min: dict, i_enc_max: dict) -> list:
        """
        Identify which index parameters vary between min/max configurations.
        
        The encoding structure (8 values total):
        - i_enc-0 to i_enc-3: method one-hot (tif_basic, tif_slicing, tif_sharding, tif_hint)
        - i_enc-4: alpha (dynbase parameter)
        - i_enc-5: beta (dynratio parameter)
        - i_enc-6: dict_boundary (partition boundary)
        - i_enc-7: irhint_levels
        
        Args:
            i_enc_min: Dictionary of minimum index configuration values
            i_enc_max: Dictionary of maximum index configuration values
            
        Returns:
            List of feature names that have different min/max values
        """
        features = [col for col in i_enc_min.keys()
                     if col.startswith("i_enc-") and i_enc_min[col] != i_enc_max[col]]
        return features


    def get_oq_subset(self):
        """
        Select a subset of OQ rows for optimization, limiting batch size if needed.
        
        This helper provides consistent row selection logic across all synthesis implementations:
        - Uses all rows if dataset is small enough
        - Takes random sample if dataset exceeds max_batch_size
        - Prints informative messages about sampling decisions
        
        Args:
            max_batch_size: Maximum number of queries to use in optimization batch
            seed: Random seed for reproducible sampling
            
        Returns:
            tuple: (all_oq_features, actual_batch_size) where all_oq_features is numpy array
        """
        
        sample_size = min(len(self.OQ), self.cfg["synthesis"]["sample-size"])

        if len(self.OQ) > sample_size:
            print(f"\tUsing random sample of {sample_size} queries (out of {len(self.OQ)})")
            np.random.seed(17)
            random_indices = np.random.choice(len(self.OQ), size=sample_size, replace=False)
            all_oq_features = self.OQ.iloc[random_indices].values
        else:
            all_oq_features = self.OQ.values
            
        return all_oq_features, sample_size


    def store(
        self, 
        template_id: int,
        optimized_params,
        prediction_throughput: float,
        prediction_size: float
    ):
        """
        Store optimized parameters and predictions in standard format.
        
        This helper handles the common post-optimization logic:
        - Reshapes parameters and prediction
        - Creates optimal i_features by replacing optimized values
        - Stores result in self.synthesized_i for later saving
        
        Uses instance attributes:
        - self.current_oq_file: Path to OQ file being optimized
        - self.feature_optimize_names: List of parameter names that were optimized
        - self.feature_optimize_ranges: List of (min, max) tuples for each optimizable feature
        - self.current_i_features: Original i_features array (before optimization)
        
        Args:
            template_id: ID of the design space template (which min/max pair from I file)
            optimized_params: Array of optimized parameter values
            prediction_throughput: Predicted log10(s/q) throughput value
            prediction_size: Predicted log10(bytes) index size value
        """
        import numpy as np
        
        self.synth_id += 1
        
        # Use the optimized parameters
        final_optimized_params = optimized_params.reshape(1, -1)
        final_prediction = np.array([[prediction_throughput, prediction_size]])

        # Generate features description from instance attributes
        features_desc_opt = []
        for i, feature_name in enumerate(self.feature_optimize_names):
            min_val, max_val = self.feature_optimize_ranges[i]
            optimized_val = final_optimized_params.flatten()[i]
            features_desc_opt.append(f"{feature_name} [{min_val:.4f}..{max_val:.4f}] -> {optimized_val:.4f}")

        # Convert predictions to actual values for display
        actual_sq = np.power(10, prediction_throughput)
        actual_qs = 1.0 / actual_sq if actual_sq > 0 else 0
        actual_size_bytes = np.power(10, prediction_size)

        if self.cfg["synthesis"]["verbose"]:
            print(f"\tSynthesis #{self.synth_id}: throughput={actual_qs:.2f} q/s (log={prediction_throughput:.4f}), size={actual_size_bytes:.0f} bytes (log={prediction_size:.4f})")
            print("\tParameters = " + "; ".join(features_desc_opt))

        # Create optimal i_features by replacing optimized values
        i_features_optimal = self.current_i_features.copy()
        for i, feature_name in enumerate(self.feature_optimize_names):
            feature_idx = list(self.I.columns).index(feature_name)
            i_features_optimal[feature_idx] = final_optimized_params.flatten()[i]
        
        # Add variant, OQ file, temporal band, pattern index, synthesis ID, and predictions as first columns
        i_features_optimal_with_metadata = np.concatenate([
            [self.variant],  # First column: variant
            [self.current_oq_file],  # Second column: OQ file
            [self.current_temporal_band],  # Third column: temporal band
            [template_id],  # Fourth column: template ID
            [self.synth_id],  # Fifth column: synthesis ID
            final_prediction.flatten(),  # Sixth & seventh columns: throughput, size predictions
            i_features_optimal  # Remaining columns: optimized features
        ])

        key = (self.variant, self.current_oq_file, template_id, self.synth_id)
        self.synthesized_i[key] = i_features_optimal_with_metadata
        if self.cfg["synthesis"]["verbose"]:
            print("\tEncoding = " + " ".join(map(str, i_features_optimal_with_metadata)))


    def optimize_features(self, **kwargs):
        """
        Optimize index parameters for a batch of queries.
        
        Must be implemented by subclasses with their specific optimization strategy.
        
        Subclasses should access optimization inputs from instance attributes:
            - self.model: Trained neural network model
            - self.fixed_features_batch: Tensor of fixed feature values for each row
            - self.feature_optimize_names: List of feature names to optimize
            - self.feature_optimize_ranges: List of (min, max) tuples for each optimizable feature
            - self.feature_names: List of all feature column names in order
            
        Args:
            **kwargs: Optimization-specific parameters (max_steps, lr, etc.)
            
        Returns:
            Tuple of (predictions, optimized_parameters)
        """
        raise NotImplementedError("Subclasses must implement optimize_features")


    def count_evaluation(self, num_configs=1):
        """
        Increment the counter for evaluated configurations.
        
        This should be called whenever configurations are evaluated,
        regardless of whether they are stored or not.
        
        Args:
            num_configs: Number of configurations evaluated (default: 1)
        """
        self.evaluated_configs_count += num_configs


    def evaluate_configs_batch(self, candidate_configs):
        """
        Efficiently evaluate multiple configurations in batched mode.
        
        Instead of evaluating each configuration separately (num_configs model calls),
        this method batches all evaluations together (1 model call) for efficiency.
        
        Algorithm:
            1. For each config, expand to all queries: (num_configs, num_queries, num_features)
            2. Flatten to (num_configs * num_queries, num_features)
            3. Run model once on entire batch
            4. Reshape output to (num_configs, num_queries, num_targets)
        
        Args:
            candidate_configs: List/array of configurations to evaluate
                              Each config is a 1D array of variable parameter values
        
        Returns:
            numpy array of predictions with shape (num_configs, num_queries, num_targets)
        """
        import torch
        import numpy as np
        
        if len(candidate_configs) == 0:
            return np.array([])
        
        # Convert to numpy array for consistent handling
        configs_array = np.array(candidate_configs)  # Shape: (num_configs, num_params)
        num_configs = len(configs_array)
        num_queries = self.fixed_features_batch.shape[0]
        
        # Get feature indices for variable parameters
        feature_indices = [self.feature_names.index(n) for n in self.feature_optimize_names]
        
        all_predictions = []
        
        with torch.no_grad():
            for start_idx in range(0, num_configs, self.batch_evals_size):
                end_idx = min(start_idx + self.batch_evals_size, num_configs)
                batch_configs = configs_array[start_idx:end_idx]
                current_batch_size = len(batch_configs)
                
                # Create expanded feature matrix
                # Shape: (current_batch_size * num_queries, num_features)
                x_batch = self.fixed_features_batch.repeat(current_batch_size, 1)
                
                # Insert parameter values for each configuration
                for config_idx, param_values in enumerate(batch_configs):
                    row_start = config_idx * num_queries
                    row_end = (config_idx + 1) * num_queries
                    
                    param_tensor = torch.tensor(param_values, dtype=torch.float32)
                    for feat_idx, pv in zip(feature_indices, param_tensor):
                        x_batch[row_start:row_end, feat_idx] = pv
                
                # Get predictions from model
                predictions = self.model(x_batch).cpu().numpy()
                
                # Reshape to (current_batch_size, num_queries, num_targets)
                predictions_reshaped = predictions.reshape(current_batch_size, num_queries, -1)
                all_predictions.append(predictions_reshaped)
        
        # Concatenate all batches
        return np.concatenate(all_predictions, axis=0)


    def infer(self, oq_file):
        """
        Optimize index parameters for a given query workload.
        
        Common inference logic that:
        1. Loads OQ file
        2. Loops through index configuration pairs in I file
        3. Identifies variable parameters
        4. Creates batched features combining OQ and I
        5. Calls subclass-specific optimize_features()
        6. Stores optimized results
        
        Args:
            oq_file: Path to OQ statistics CSV file
        """
        import torch
        import numpy as np
        import time
        
        self.load_oq(oq_file)
        self.current_oq_file = oq_file

        # Extract temporal band from OQ filename (pattern: .sliceN[start-end))
        import re
        band_match = re.search(r'\.slice\d+\[[^\]]*\)', os.path.basename(oq_file))
        self.current_temporal_band = band_match.group(0).lstrip('.') if band_match else ""
        if self.current_temporal_band:
            print(f"Temporal band = {self.current_temporal_band}")

        print("Optimization = " + self.variant)

        self.feature_names = list(self.OQ.columns) + list(self.I.columns)
        print(f"\tFeature names = {self.feature_names}")

        # Start timing after loading OQ file (exclude I/O from timing)
        if self.synthesis_start_time is None:
            self.synthesis_start_time = time.time()
        
        # Load batch size configuration once for all evaluations
        self.batch_evals_size = self.cfg["synthesis"]["batch-evals-size"]

        for index in range(0, len(self.I), 2):
            if index + 1 >= len(self.I): break

            min_row = self.I.iloc[index]
            max_row = self.I.iloc[index + 1]
            
            # Store template_id for access during optimization
            self.current_template_id = index
            
            self.feature_optimize_names = self.get_variable_features(
                min_row.to_dict(), max_row.to_dict())

            self.feature_optimize_ranges = []
            for f in self.feature_optimize_names:
                min_val = min_row[f]
                max_val = max_row[f]
                self.feature_optimize_ranges.append((min_val, max_val))

            # Get subset of OQ rows for optimization
            all_oq_features, actual_batch_size = self.get_oq_subset()
            
            # Prepare batched features: each row combines one OQ query with the I features
            i_features = min_row.values
            self.current_i_features = i_features  # For use in store
            i_features_batch = np.tile(i_features, (actual_batch_size, 1))
            combined_batch = np.concatenate([all_oq_features, i_features_batch], axis=1)
            self.fixed_features_batch = torch.tensor(combined_batch, dtype=torch.float32)
            
            print(f"\tBatch shape: {self.fixed_features_batch.shape}")

            # Call subclass-specific optimization
            batch_results = self.optimize_features()
            
            # Process results - batch_results[0] contains predictions for all targets (batch_size x 2)
            # Column 0: log10(s/q) throughput, Column 1: log10(bytes) size
            all_predictions = batch_results[0]  # Shape: (batch_size, 2)
            optimized_params = batch_results[1][0]
            
            # Convert log predictions to linear space and sum (same as prediction.py)
            # For throughput: sum of time per query values gives total time
            throughput_predictions = all_predictions[:, 0]
            throughput_linear = np.power(10, throughput_predictions)  # Convert to s/q
            total_time_seconds = throughput_linear.sum()  # Sum to get total time
            
            # Overall throughput: queries / total_time
            overall_throughput_qps = actual_batch_size / total_time_seconds if total_time_seconds > 0 else 0
            avg_time_per_query = total_time_seconds / actual_batch_size
            
            # Convert back to log space for storage
            aggregated_throughput_log = np.log10(avg_time_per_query)
            
            # For size: sum of individual sizes gives total size
            size_predictions = all_predictions[:, 1]
            size_linear = np.power(10, size_predictions)  # Convert to bytes
            total_size_bytes = size_linear.sum()
            avg_size_bytes = total_size_bytes / actual_batch_size
            
            # Convert back to log space for storage
            aggregated_size_log = np.log10(avg_size_bytes)
            
            print(f"\tAggregate throughput: {aggregated_throughput_log:.6f} (log10 s/q) = {overall_throughput_qps:.2f} q/s")
            print(f"\tAggregate size: {aggregated_size_log:.6f} (log10 bytes) = {avg_size_bytes:.0f} bytes")
            print(f"\tThroughput range: {throughput_predictions.min():.6f} to {throughput_predictions.max():.6f}")

            # Store optimized result (skip if store_all_evaluations is enabled to avoid duplicates)
            store_all = self.cfg["synthesis"].get("store-all-evaluations", False)
            if not store_all:
                self.store(
                    template_id=index,
                    optimized_params=optimized_params,
                    prediction_throughput=aggregated_throughput_log,
                    prediction_size=aggregated_size_log
                )


    @staticmethod
    def main(synthesis_class):
        """
        Main entry point for synthesis optimization.
        
        Args:
            synthesis_class: The Synthesis class to instantiate (subclass of SynthesisBase)
        """
        if len(sys.argv) < 5:
            print("Usage: <config_file> <out_dir> <i_file> <oq_files>")
            print("  config_file: Path to configuration file")
            print("  out_dir: Path to output directory")
            print("  i_file: Path to I file")
            print("  oq_files: Path to OQ files (one or more)")
            sys.exit(1)

        config_file = sys.argv[1]
        output_dir  = sys.argv[2]
        i_file = sys.argv[3]
        oq_files = sys.argv[4:]

        s = synthesis_class(config_file, output_dir)
        s.load_i(i_file)

        for oq_file in oq_files: s.infer(oq_file)

        s.save()
