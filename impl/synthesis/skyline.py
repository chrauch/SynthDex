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
Skyline computation for multi-objective optimization.

Implements skyline (Pareto frontier) algorithm to filter dominated solutions
in multi-dimensional optimization spaces. A point dominates another if it is
better or equal in all dimensions and strictly better in at least one.
"""
import numpy as np
from typing import List, Tuple


def compute_skyline_minimize(points: np.ndarray) -> np.ndarray:
    """
    Compute skyline for points where ALL dimensions should be minimized.
    
    A point P dominates point Q if:
    - P[i] <= Q[i] for all dimensions i, AND
    - P[j] < Q[j] for at least one dimension j
    
    Args:
        points: numpy array of shape (n_points, n_dimensions)
                Each row is a point, each column is a dimension to minimize
    
    Returns:
        Boolean mask of shape (n_points,) where True indicates skyline point
        
    Example:
        points = np.array([
            [-6.1, 5.0],  # Good throughput, medium size
            [-6.0, 4.5],  # Worse throughput, smaller size  
            [-5.0, 6.0],  # Bad throughput, large size (dominated)
        ])
        mask = compute_skyline_minimize(points)
        # Returns [True, True, False] - third point is dominated by first
    """
    n_points = points.shape[0]
    
    if n_points == 0:
        return np.array([], dtype=bool)
    
    if n_points == 1:
        return np.array([True])
    
    # Track which points are in the skyline (not dominated)
    is_skyline = np.ones(n_points, dtype=bool)
    
    # For each point, check if it's dominated by any other point
    for i in range(n_points):
        if not is_skyline[i]:
            # Already marked as dominated, skip
            continue
            
        for j in range(n_points):
            if i == j or not is_skyline[j]:
                continue
            
            # Check if point j dominates point i
            # j dominates i if: j[k] <= i[k] for all k, and j[k] < i[k] for at least one k
            better_or_equal = np.all(points[j] <= points[i])
            strictly_better = np.any(points[j] < points[i])
            
            if better_or_equal and strictly_better:
                # Point i is dominated by point j
                is_skyline[i] = False
                break
    
    return is_skyline


def compute_skyline_indices(points: np.ndarray) -> List[int]:
    """
    Compute skyline and return indices of skyline points.
    
    Args:
        points: numpy array of shape (n_points, n_dimensions)
    
    Returns:
        List of indices (row numbers) of points in the skyline
    """
    mask = compute_skyline_minimize(points)
    return np.where(mask)[0].tolist()


def filter_skyline_with_metadata(
    points: np.ndarray,
    metadata: List[Tuple]
) -> Tuple[np.ndarray, List[Tuple]]:
    """
    Compute skyline and filter both points and associated metadata.
    
    Args:
        points: numpy array of shape (n_points, n_dimensions)
        metadata: List of metadata tuples, one per point
    
    Returns:
        Tuple of (skyline_points, skyline_metadata)
        - skyline_points: filtered points array
        - skyline_metadata: filtered metadata list
    """
    if len(metadata) != points.shape[0]:
        raise ValueError(f"Metadata length {len(metadata)} must match points {points.shape[0]}")
    
    skyline_mask = compute_skyline_minimize(points)
    skyline_indices = np.where(skyline_mask)[0]
    
    skyline_points = points[skyline_mask]
    skyline_metadata = [metadata[i] for i in skyline_indices]
    
    return skyline_points, skyline_metadata


def skyline_stats(points: np.ndarray) -> dict:
    """
    Compute statistics about skyline vs dominated points.
    
    Args:
        points: numpy array of shape (n_points, n_dimensions)
    
    Returns:
        Dictionary with statistics:
        - total: total number of points
        - skyline: number of skyline points
        - dominated: number of dominated points
        - skyline_ratio: fraction of points in skyline
    """
    mask = compute_skyline_minimize(points)
    n_skyline = np.sum(mask)
    n_total = len(mask)
    n_dominated = n_total - n_skyline
    
    return {
        'total': n_total,
        'skyline': n_skyline,
        'dominated': n_dominated,
        'skyline_ratio': n_skyline / n_total if n_total > 0 else 0.0
    }
