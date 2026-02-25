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

"""
Grid-search based synthesis implementation.

Systematically samples configurations at regular intervals across each parameter dimension,
providing deterministic and exhaustive coverage of the design space.
"""
import sys
import json
import os
import torch
import numpy as np
import pandas as pd
from itertools import product

# Add parent directory to path to enable imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from synthesisbase import SynthesisBase


class Synthesis(SynthesisBase):
    """
    Grid-search based index parameter optimization.
    
    Systematically evaluates configurations at regular intervals across the parameter space.
    For each parameter, divides the range into N equal intervals and evaluates all combinations.
    """

    def __init__(self, config_dir):
        """
        Initialize grid-search synthesis.
        
        Args:
            config_dir: Path to configuration directory
        """
        super().__init__(config_dir)
        self.variant = "Grid Search"


    def optimize_features(self) -> tuple:
        """
        Multi-step grid-search optimization for shared index parameters over a workload.
        
        Algorithm:
          1. Initial grid: For each parameter, generate N evenly-spaced points in [min, max],
             where N is the resolution from step 1 in step-resolution-and-top-k
          2. Create Cartesian product of all parameter grids and evaluate all combinations
          3. Select top-K configurations (K from step 1)
          4. For each subsequent step:
             a. Create local grids around each of the top-K configurations from previous step
             b. Local grid resolution is specified by the current step's resolution value
             c. Evaluate all new configurations in the local grids
             d. Select top-K configurations from ALL evaluated configs (K from current step)
          5. Return the best configuration overall
        
        Example configuration:
          "step-resolution-and-top-k": [[6, 10], [3, 5], [2, 2]]
          - Step 1: 6^n initial grid, keep top 10
          - Step 2: Around each of the 10, create 3^n local grids, keep top 5
          - Step 3: Around each of the 5, create 2^n local grids, keep top 2
        
        Returns:
            Tuple of (predictions, optimized_parameters)
              - predictions: numpy array of log-scale predictions for each query
              - optimized_parameters: numpy array of best parameter values (1 x n_params)
        """
        # Load grid search hyperparameters
        verbose = self.cfg["synthesis"]["verbose"]
        store_all_evaluations = self.cfg["synthesis"]["store-all-evaluations"]
        grid_cfg = self.cfg["synthesis"].get("grid", {})
        candidates_limit = grid_cfg.get("candidates-limit", 1000)
        step_resolution_and_top_k = grid_cfg.get("step-resolution-and-top-k", [[5, 10]])
        # NOTE: objective and weight-temperature are deprecated (kept for config compatibility)
        # Aggregation now always uses sum of linear values (consistent with prediction.py)
        objective = grid_cfg.get("objective", "weighted_log")  # Deprecated
        weight_temperature = grid_cfg.get("weight-temperature", 1.0)  # Deprecated
        
        # Validate inputs
        if len(self.feature_optimize_names) != len(self.feature_optimize_ranges):
            raise ValueError("Mismatch between feature names and ranges")
        if self.fixed_features_batch.shape[0] == 0:
            raise ValueError("Empty batch")
        
        # Ensure there are parameters to optimize
        if len(self.feature_optimize_names) == 0:
            raise ValueError("No variable features to optimize - min and max rows are identical")
        
        # Validate parameter ranges
        for (lo, hi), name in zip(self.feature_optimize_ranges, self.feature_optimize_names):
            if not lo < hi:
                raise ValueError(f"Invalid range for {name}: {lo} >= {hi}")
        
        self.model.eval()
        batch_size = self.fixed_features_batch.shape[0]
        
        # Validate step-resolution-and-top-k format
        if not isinstance(step_resolution_and_top_k, list) or len(step_resolution_and_top_k) == 0:
            raise ValueError("step-resolution-and-top-k must be a non-empty list of [resolution, top-k] pairs")
        for step_idx, step_config in enumerate(step_resolution_and_top_k):
            if not isinstance(step_config, list) or len(step_config) != 2:
                raise ValueError(f"Step {step_idx}: each step must be [resolution, top-k], got {step_config}")
            resolution, top_k = step_config
            if not isinstance(resolution, int) or resolution < 1:
                raise ValueError(f"Step {step_idx}: resolution must be positive integer, got {resolution}")
            if not isinstance(top_k, int) or top_k < 1:
                raise ValueError(f"Step {step_idx}: top-k must be positive integer, got {top_k}")
        
        if verbose:
            print(f"\n\tMulti-step grid search with {len(step_resolution_and_top_k)} steps:")
            
            # Calculate upper bound on number of configurations
            n_params = len(self.feature_optimize_names)
            upper_bound = 0
            upper_bound_details = []
            
            # Step 1: initial grid
            step1_configs = step_resolution_and_top_k[0][0] ** n_params
            upper_bound += step1_configs
            upper_bound_details.append(f"Step 1: {step_resolution_and_top_k[0][0]}^{n_params} = {step1_configs}")
            
            # Subsequent steps: top-k from previous * resolution^n_params
            for step_idx in range(1, len(step_resolution_and_top_k)):
                resolution = step_resolution_and_top_k[step_idx][0]
                prev_top_k = step_resolution_and_top_k[step_idx - 1][1]
                step_configs = prev_top_k * (resolution ** n_params)
                upper_bound += step_configs
                upper_bound_details.append(
                    f"Step {step_idx + 1}: {prev_top_k} × {resolution}^{n_params} = {step_configs}"
                )
            
            print(f"\t  Parameters: {n_params}")
            print(f"\t  Upper bound (no deduplication): {upper_bound:,} configurations")
            for detail in upper_bound_details:
                print(f"\t    {detail}")
            print()
            
            for step_idx, (resolution, top_k) in enumerate(step_resolution_and_top_k):
                print(f"\t  Step {step_idx + 1}: resolution={resolution}, top-k={top_k}")
        
        # Generate grid points for each parameter (initial step)
        initial_resolution = step_resolution_and_top_k[0][0]
        grid_points_per_param = []
        for (lo, hi), name in zip(self.feature_optimize_ranges, self.feature_optimize_names):
            # Use linspace to create evenly spaced points including boundaries
            grid_points = np.linspace(lo, hi, initial_resolution)
            grid_points_per_param.append(grid_points)
            if verbose:
                print(f"\t{name}: [{lo:.4f}, {hi:.4f}] with {initial_resolution} samples")
        
        # Create all combinations (Cartesian product) for initial grid
        grid_configurations = list(product(*grid_points_per_param))
        total_configs = len(grid_configurations)
        
        if verbose:
            print(f"\tInitial grid: {len(self.feature_optimize_names)} parameters, "
                  f"{initial_resolution} samples/dim = {total_configs} total configurations")
        
        # Shuffle and limit initial grid to avoid exponential blowup
        np.random.shuffle(grid_configurations)
        if len(grid_configurations) > candidates_limit:
            if verbose:
                print(f"\tLimiting initial grid from {len(grid_configurations)} to {candidates_limit} configurations")
            grid_configurations = grid_configurations[:candidates_limit]
        
        # Track all evaluated configurations to avoid re-evaluation
        # Round values to avoid floating-point precision issues
        def config_key(params):
            """Create hashable key from parameter values, rounding to avoid FP issues."""
            return tuple(np.round(params, decimals=10))
        
        evaluated_configs = {}  # key: tuple(param_values), value: (loss, predictions)
        
        # Define objective function (same as gradient-based approach)
        def compute_objective(predictions_log: np.ndarray):
            """
            Compute objective value from log-scale predictions.
            
            Uses sum of linear values (same as storage and prediction.py):
            - Convert log predictions to linear space
            - Sum to get total time
            - Compute average and convert back to log
            
            Args:
                predictions_log: numpy array of log10-scale predictions (batch_size,)
            
            Returns:
                Scalar objective value (log10 s/q - lower is better)
            """
            # Convert to linear space and sum (consistent with prediction.py)
            linear_values = np.power(10, predictions_log)
            total_time = linear_values.sum()
            avg_time = total_time / len(predictions_log)
            # Return log for optimization (lower is better)
            return np.log10(avg_time)
        
        def evaluate_configurations(configs_to_eval, round_name="initial"):
            """Evaluate a list of configurations in batched mode and update evaluated_configs."""
            nonlocal evaluated_configs
            
            # Filter out already-evaluated configs
            new_configs = []
            for param_values in configs_to_eval:
                key = config_key(param_values)
                if key not in evaluated_configs:
                    new_configs.append(param_values)
            
            if len(new_configs) == 0:
                if verbose:
                    print(f"\t{round_name}: All {len(configs_to_eval)} configurations already evaluated")
                return
            
            if verbose:
                print(f"\t{round_name}: Evaluating {len(new_configs)} new configurations "
                      f"(out of {len(configs_to_eval)} total)")
            
            # Batch evaluate all new configurations at once
            all_predictions = self.evaluate_configs_batch(new_configs)
            
            # Count all evaluations
            self.count_evaluation(len(new_configs))
            
            # Process results for each configuration
            for config_idx, (param_values, predictions) in enumerate(zip(new_configs, all_predictions)):
                key = config_key(param_values)
                
                # Extract only the first target (throughput) - shape: (batch_size,)
                predictions_throughput = predictions[:, 0]
                
                # Compute objective
                loss = compute_objective(predictions_throughput)
                
                # Store in evaluated configs
                evaluated_configs[key] = (loss, predictions)
                
                # Store evaluation if enabled
                if store_all_evaluations:
                    # Convert log predictions to linear space and sum (same as prediction.py)
                    throughput_predictions = predictions[:, 0]
                    throughput_linear = np.power(10, throughput_predictions)
                    total_time = throughput_linear.sum()
                    avg_time = total_time / len(throughput_predictions)
                    throughput_pred = np.log10(avg_time)
                    
                    size_predictions = predictions[:, 1]
                    size_linear = np.power(10, size_predictions)
                    total_size = size_linear.sum()
                    avg_size = total_size / len(size_predictions)
                    size_pred = np.log10(avg_size)
                    
                    self.store(
                        template_id=self.current_template_id,
                        optimized_params=np.array(param_values).reshape(1, -1),
                        prediction_throughput=throughput_pred,
                        prediction_size=size_pred
                    )
                
                # Progress reporting
                if verbose and (config_idx < 5 or (config_idx + 1) % max(1, len(new_configs) // 10) == 0 
                               or config_idx == len(new_configs) - 1):
                    total_evals = len(evaluated_configs)
                    best_loss_so_far = min(loss for loss, _ in evaluated_configs.values())
                    print(f"\t  {round_name}: {config_idx + 1}/{len(new_configs)} configs, "
                          f"current_loss={loss:.6f}, best_loss={best_loss_so_far:.6f}, "
                          f"total_evaluated={total_evals}")
        
        # Initial grid search (Step 1)
        evaluate_configurations(grid_configurations, "Step 1 (initial grid)")
        
        # Track current cell size for each parameter (initialized from Step 1)
        current_cell_sizes = []
        for (lo, hi) in self.feature_optimize_ranges:
            initial_cell_size = (hi - lo) / max(1, initial_resolution - 1) if initial_resolution > 1 else (hi - lo)
            current_cell_sizes.append(initial_cell_size)
        
        # Multi-step refinement
        for step_idx in range(1, len(step_resolution_and_top_k)):
            resolution, top_k = step_resolution_and_top_k[step_idx]
            prev_top_k = step_resolution_and_top_k[step_idx - 1][1]
            
            # Get top-k configurations from previous step's selection
            sorted_configs = sorted(evaluated_configs.items(), key=lambda x: x[1][0])
            # Use the previous step's top-k as the centers for refinement
            step_centers = [np.array(config) for config, _ in sorted_configs[:prev_top_k]]
            
            if verbose:
                print(f"\n\tStep {step_idx + 1}: resolution={resolution}, selecting top-{top_k}")
                print(f"\t  Using top-{prev_top_k} configurations from Step {step_idx} as refinement centers:")
                for i, cfg in enumerate(step_centers):
                    loss = evaluated_configs[config_key(cfg)][0]
                    print(f"\t    #{i+1}: loss={loss:.6f}, params={cfg}")
            
            # Calculate step size for this refinement level
            # Each refinement step creates local grids around selected centers:
            # - Local grid radius (step_size) = half the previous cell size
            # - This makes the local grid span one full cell from the previous step
            # - New cell size = grid spacing within the local grid
            step_sizes = []
            new_cell_sizes = []
            for param_idx, (current_cell_size, (lo, hi), name) in enumerate(
                zip(current_cell_sizes, self.feature_optimize_ranges, self.feature_optimize_names)):
                
                # The local grid radius is half the current cell size
                # This creates a local range of width = current_cell_size
                step_size = current_cell_size / 2.0
                step_sizes.append(step_size)
                
                # Update cell size for next iteration: divide by (resolution - 1)
                # This represents the grid spacing within the local grid
                new_cell_size = current_cell_size / max(1, resolution - 1) if resolution > 1 else current_cell_size
                new_cell_sizes.append(new_cell_size)
                
                if verbose and param_idx == 0:  # Print once for first parameter as example
                    local_range_width = 2 * step_size
                    print(f"\t  Refinement details (using {name} as example):")
                    print(f"\t    Previous cell size: {current_cell_size:.6f}")
                    print(f"\t    Local grid radius: ±{step_size:.6f} (range width: {local_range_width:.6f})")
                    print(f"\t    Resolution: {resolution} points -> new cell size: {new_cell_size:.6f}")
            
            # Update current cell sizes for next step
            current_cell_sizes = new_cell_sizes
            
            # Generate refinement candidates around each center
            refinement_candidates = []
            for center_config in step_centers:
                # For each center, create a local grid
                local_grid_per_param = []
                for param_idx, (center_val, step_size, (lo, hi)) in enumerate(
                    zip(center_config, step_sizes, self.feature_optimize_ranges)):
                    
                    # Calculate range for this parameter
                    # The local grid spans from center - step_size to center + step_size
                    local_lo = max(lo, center_val - step_size)
                    local_hi = min(hi, center_val + step_size)
                    
                    # Generate grid points within this range
                    if resolution == 1:
                        local_points = [center_val]
                    else:
                        local_points = np.linspace(local_lo, local_hi, resolution)
                    
                    local_grid_per_param.append(local_points)
                
                # Create all combinations for this center's local grid
                local_configs = list(product(*local_grid_per_param))
                refinement_candidates.extend(local_configs)
            
            # Remove duplicates while preserving order
            unique_candidates = []
            seen = set()
            for cfg in refinement_candidates:
                key = config_key(cfg)
                if key not in seen:
                    seen.add(key)
                    unique_candidates.append(cfg)
            
            if len(unique_candidates) == 0:
                if verbose:
                    print(f"\t  No new candidates generated, stopping refinement early")
                break
            
            # Shuffle candidates to avoid bias
            np.random.shuffle(unique_candidates)
            
            # Limit number of candidates per step
            if len(unique_candidates) > candidates_limit:
                if verbose:
                    print(f"\t  Limiting candidates from {len(unique_candidates)} to {candidates_limit}")
                unique_candidates = unique_candidates[:candidates_limit]
            
            # Evaluate refinement candidates
            evaluate_configurations(unique_candidates, f"Step {step_idx + 1} (refinement)")
        
        # Find best configuration from all evaluations
        # For the final step, we select the top-k specified in the last step
        final_top_k = step_resolution_and_top_k[-1][1]
        sorted_configs = sorted(evaluated_configs.items(), key=lambda x: x[1][0])
        
        # Use the best configuration (top-1 from final step)
        best_config_key, (best_loss, best_predictions_full) = sorted_configs[0]
        best_params = np.array(best_config_key)
        
        if verbose:
            print(f"\n\tMulti-step grid search complete after {len(step_resolution_and_top_k)} steps")
            print(f"\tTotal configurations evaluated: {len(evaluated_configs)}")
            print(f"\n\tFinal top-{final_top_k} configurations:")
            for i in range(min(final_top_k, len(sorted_configs))):
                cfg_key, (loss, _) = sorted_configs[i]
                cfg_params = np.array(cfg_key)
                print(f"\t  #{i+1}: loss={loss:.6f}, params={cfg_params}")
            
            print(f"\n\tBest objective = {best_loss:.6f}")
            print(f"\tBest parameters:")
            for name, val, (lo, hi) in zip(self.feature_optimize_names, 
                                           best_params, 
                                           self.feature_optimize_ranges):
                print(f"\t  {name}: {val:.4f} in [{lo:.4f}, {hi:.4f}]")
            
            # Statistics on predictions
            throughput_preds = best_predictions_full[:, 0]
            print(f"\tThroughput prediction range: {throughput_preds.min():.6f} to {throughput_preds.max():.6f}")
            print(f"\tThroughput prediction mean: {throughput_preds.mean():.6f}")
        
        # Return predictions and parameters in expected format
        params_optimized = best_params.reshape(1, -1)
        return best_predictions_full, params_optimized


if __name__ == "__main__":
    SynthesisBase.main(Synthesis)
