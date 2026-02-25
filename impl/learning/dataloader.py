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

import os
import pandas as pd
import torch
from torch.utils.data import Dataset, DataLoader, random_split, Subset, ConcatDataset
import psutil
from collections import OrderedDict
import random

import pyarrow.parquet as pq


class ParquetDataset(Dataset):
    """Unified dataset for loading parquet files from a directory."""
    
    def __init__(self, parquet_dir, targets, prefix=None, csv_dir=None):
        """
        Args:
            parquet_dir: Directory containing parquet files
            targets: List of target column names
            prefix: Optional prefix filter for parquet files
            csv_dir: Optional directory with CSV files to convert to parquet first
        """
        self.parquet_dir = parquet_dir
        self.targets = targets
        self.prefix = prefix
        self._file_cache = OrderedDict()
        
        # Convert CSV files if source directory provided
        if csv_dir:
            self._ensure_parquet_files(csv_dir)
        
        # Load parquet files
        self._load_parquet_files()
    
    def _ensure_parquet_files(self, csv_dir):
        """Convert CSV files to parquet files if needed."""
        os.makedirs(self.parquet_dir, exist_ok=True)
        
        if not os.path.exists(csv_dir):
            return
        
        csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.OQIP.csv')]
        if not csv_files:
            return
        
        converted_count = 0
        for csv_file in csv_files:
            csv_path = os.path.join(csv_dir, csv_file)
            parquet_file = csv_file.replace('.OQIP.csv', '.OQIP.parquet')
            parquet_path = os.path.join(self.parquet_dir, parquet_file)
            
            if (not os.path.exists(parquet_path) or 
                os.path.getmtime(csv_path) > os.path.getmtime(parquet_path)):
                print(f"Converting {csv_file} to Parquet file {parquet_file}...")
                df = pd.read_csv(csv_path, sep='\t')
                df.to_parquet(parquet_path, index=False)
                converted_count += 1
        
        if converted_count > 0:
            print(f"Converted {converted_count} CSV files to Parquet")
    
    def _load_parquet_files(self):
        """Load all parquet files and create unified index."""
        if not os.path.exists(self.parquet_dir):
            self.file_info = []
            self.length = 0
            self.cache_size = 0
            return
        
        parquet_files = [f for f in os.listdir(self.parquet_dir) if f.endswith('.parquet')]
        
        # Apply prefix filter if specified
        if self.prefix:
            parquet_files = [f for f in parquet_files if f.startswith(self.prefix)]
        
        if not parquet_files:
            self.file_info = []
            self.length = 0
            self.cache_size = 0
            return
        
        self.file_info = []
        total_rows = 0
        
        for pfile in sorted(parquet_files):
            ppath = os.path.join(self.parquet_dir, pfile)
            table = pq.read_table(ppath)
            num_rows = table.num_rows
            
            self.file_info.append({
                'path': ppath,
                'start_idx': total_rows,
                'end_idx': total_rows + num_rows,
                'num_rows': num_rows
            })
            total_rows += num_rows
        
        self.length = total_rows
        self.cache_size = len(parquet_files)
        
        if self.length > 0:
            print(f"Loading {len(parquet_files)} Parquet files = {parquet_files}")
            print(f"Dataset = {self.length} rows from {len(parquet_files)} files (cache size={self.cache_size})")

    def __len__(self):
        return self.length
    
    def __getitem__(self, idx):
        if self.length == 0:
            raise IndexError("Empty dataset")
        
        # Find which file contains this index
        for file_info in self.file_info:
            if file_info['start_idx'] <= idx < file_info['end_idx']:
                file_path = file_info['path']
                local_idx = idx - file_info['start_idx']
                
                # Use cached file or load it
                if file_path in self._file_cache:
                    # Move to end (mark as recently used)
                    self._file_cache.move_to_end(file_path)
                    cached_df = self._file_cache[file_path]
                else:
                    # Manage cache size with LRU eviction
                    if len(self._file_cache) >= self.cache_size:
                        # Remove least recently used entry (first item)
                        lru_key = next(iter(self._file_cache))
                        if len(self._file_cache) == self.cache_size:
                            print(f"Cache evicting {os.path.basename(lru_key)} for {os.path.basename(file_path)}")
                        del self._file_cache[lru_key]
                    
                    # Load and cache the file
                    self._file_cache[file_path] = pq.read_table(file_path).to_pandas()
                    cached_df = self._file_cache[file_path]
                
                # Get row from cached data
                row = cached_df.iloc[local_idx]
                
                # Separate features and targets
                features = row.drop(self.targets).values
                targets = row[self.targets].values
                
                features = torch.tensor(features, dtype=torch.float32)
                targets = torch.tensor(targets, dtype=torch.float32)
                
                return features, targets
        
        raise IndexError(f"Index {idx} out of range")


def create_dataloader(dir, prefix, targets, batch_size, split_ratios, single_file_per_o=False, sample_ratio=1.0):
    """
    Create a DataLoader for the OQIP dataset with train/validation/test splitting
    
    Args:
        dir: Data directory
        targets: Target columns for training
        batch_size: Batch size for DataLoader
        single_file_per_o: If True, split by files instead of random rows
        sample_ratio: Fraction of data to use (0.0 to 1.0), default 1.0 uses all data
    
    Returns:
        DataLoader for specified split
    """
    
    # Load main OQIP dataset (with CSV conversion)
    dataset = ParquetDataset(
        parquet_dir=os.path.join(dir, "train"),
        targets=targets,
        prefix=prefix,
        csv_dir=os.path.join(dir, "stats", "OQIP")
    )
    
    if abs(sum(split_ratios) - 1.0) > 1e-6:
        raise ValueError(f"Split ratios must sum to 1.0, got {sum(split_ratios)}")
    
    # Check if file-level splitting is enabled (only if main dataset has files)
    
    if dataset.length > 0 and single_file_per_o:
        # File-level splitting: each file goes entirely into one set
        num_files = len(dataset.file_info)
        
        # Calculate number of files for each set based on ratios
        train_files = max(1, int(split_ratios[0] * num_files)) if split_ratios[0] > 0 else 0
        val_files = max(1, int(split_ratios[1] * num_files)) if split_ratios[1] > 0 else 0
        test_files = max(1, int(split_ratios[2] * num_files)) if split_ratios[2] > 0 else 0
        
        # Ensure we have at least enough files for non-zero ratios
        min_files_needed = sum([1 for r in split_ratios[:3] if r > 0])
        if num_files < min_files_needed:
            raise ValueError(f"File-level splitting requires at least {min_files_needed} files, but only {num_files} available")
        
        # Adjust to account for any rounding discrepancy - prioritize train set
        total_assigned = train_files + val_files + test_files
        if total_assigned != num_files:
            train_files += (num_files - total_assigned)
        
        print(f"File-level splitting = Training: {train_files} files; Validation: {val_files} files; Test: {test_files} files", flush=True)
        
        # Assign files to sets
        file_assignments = (
            ['train'] * train_files + 
            ['val'] * val_files + 
            ['test'] * test_files
        )
        
        # Create subset datasets based on file assignments
        train_indices = []
        val_indices = []
        test_indices = []
        train_files_list = []
        val_files_list = []
        test_files_list = []
        
        for file_idx, (file_info, assignment) in enumerate(zip(dataset.file_info, file_assignments)):
            indices = list(range(file_info['start_idx'], file_info['end_idx']))
            file_name = os.path.basename(file_info['path'])
            if assignment == 'train':
                train_indices.extend(indices)
                train_files_list.append(file_name)
            elif assignment == 'val':
                val_indices.extend(indices)
                val_files_list.append(file_name)
            elif assignment == 'test':
                test_indices.extend(indices)
                test_files_list.append(file_name)
        
        train_dataset = Subset(dataset, train_indices)
        val_dataset = Subset(dataset, val_indices)
        test_dataset = Subset(dataset, test_indices) if test_indices else None
        
        print(f"File-level split sizes = Training: {len(train_indices)} rows; Validation: {len(val_indices)} rows; Test: {len(test_indices)} rows", flush=True)
        print(f"Training files: {';'.join(train_files_list)}", flush=True)
        print(f"Validation files: {';'.join(val_files_list)}", flush=True)
        if test_files_list:
            print(f"Test files: {';'.join(test_files_list)}", flush=True)
    elif dataset.length > 0:
        # Random row-level splitting
        total_size = len(dataset)
        train_size = int(split_ratios[0] * total_size)
        val_size = int(split_ratios[1] * total_size)
        test_size = total_size - train_size - val_size  # Remainder goes to test
        
        print(f"Random splitting ratios = Training: {split_ratios[0]} ({train_size}); Validation: {split_ratios[1]} ({val_size}); Test: {split_ratios[2]} ({test_size})", flush=True)
        
        if test_size > 0:
            train_dataset, val_dataset, test_dataset = random_split(
                dataset, [train_size, val_size, test_size],
                generator=torch.Generator().manual_seed(42)  # For reproducible splits
            )
        else:
            train_dataset, val_dataset = random_split(
                dataset, [train_size, val_size],
                generator=torch.Generator().manual_seed(42)  # For reproducible splits
            )
            test_dataset = None
    else:
        # No main dataset files - will use only additional datasets from subdirectories
        print("No main dataset files found, using only additional datasets from subdirectories", flush=True)
        train_dataset = Subset(dataset, [])
        val_dataset = Subset(dataset, [])
        test_dataset = None

    # Load additional parquet files from subdirectories
    additional_train = ParquetDataset(os.path.join(dir, "train", "training"), targets)
    additional_val = ParquetDataset(os.path.join(dir, "train", "validation"), targets)
    additional_test = ParquetDataset(os.path.join(dir, "train", "test"), targets)
    
    # Merge additional data with split datasets
    if additional_train.length > 0:
        train_dataset = ConcatDataset([train_dataset, additional_train])
        filenames = ';'.join([os.path.basename(f['path']) for f in additional_train.file_info])
        print(f"  Added {additional_train.length} rows to training set from: {filenames}")
    
    if additional_val.length > 0:
        val_dataset = ConcatDataset([val_dataset, additional_val])
        filenames = ';'.join([os.path.basename(f['path']) for f in additional_val.file_info])
        print(f"  Added {additional_val.length} rows to validation set from: {filenames}")
    
    if additional_test.length > 0:
        if test_dataset is None:
            test_dataset = additional_test
        else:
            test_dataset = ConcatDataset([test_dataset, additional_test])
        filenames = ';'.join([os.path.basename(f['path']) for f in additional_test.file_info])
        print(f"  Added {additional_test.length} rows to test set from: {filenames}")
    
    # Validate that we have at least some data in train and val sets
    if len(train_dataset) == 0:
        raise ValueError(f"No training data found for prefix '{prefix}' in main directory or training subdirectory")
    if len(val_dataset) == 0:
        raise ValueError(f"No validation data found for prefix '{prefix}' in main directory or validation subdirectory")

    # Apply random sampling if sample_ratio < 1.0
    # This reduces the training/validation/test sets proportionally
    if sample_ratio < 1.0:
        if sample_ratio <= 0.0:
            raise ValueError(f"sample_ratio must be > 0.0, got {sample_ratio}")
        
        random.seed(42)  # For reproducibility
        
        def sample_dataset(ds, ratio):
            if ds is None:
                return None
            total_size = len(ds)
            if total_size == 0:
                return ds  # Return empty dataset as-is
            sample_size = max(1, int(total_size * ratio))
            all_indices = list(range(total_size))
            sampled_indices = sorted(random.sample(all_indices, sample_size))
            return Subset(ds, sampled_indices)
        
        orig_train_size = len(train_dataset)
        orig_val_size = len(val_dataset)
        orig_test_size = len(test_dataset) if test_dataset else 0
        
        train_dataset = sample_dataset(train_dataset, sample_ratio)
        val_dataset = sample_dataset(val_dataset, sample_ratio)
        test_dataset = sample_dataset(test_dataset, sample_ratio)
        
        print(f"Sampling {sample_ratio*100:.1f}% of data:", flush=True)
        print(f"Training: {len(train_dataset)}/{orig_train_size} rows", flush=True)
        print(f"Validation: {len(val_dataset)}/{orig_val_size} rows", flush=True)
        if orig_test_size > 0:
            print(f"Test: {len(test_dataset)}/{orig_test_size} rows", flush=True)

    has_cuda = torch.cuda.is_available()
    num_workers = 2 # os.cpu_count() if has_cuda else max(1, os.cpu_count() // 2)
    # If we have sufficient memory
    # - Keep workers alive between epochs
    # - Increase number of batches to prefetch per worker
    big_memory = psutil.virtual_memory().total / (1024**3) > 16
    
    # Create dataloaders
    train_dataloader = DataLoader(
        train_dataset,
        batch_size=batch_size,
        num_workers=num_workers,
        shuffle=True,
        pin_memory=has_cuda,
        prefetch_factor=4 if big_memory else 2,
        persistent_workers=big_memory
    )
    val_dataloader = DataLoader(
        val_dataset,
        batch_size=batch_size,
        num_workers=max(1, num_workers//3),
        shuffle=False,
        pin_memory=has_cuda,
        prefetch_factor=2 if big_memory else 1,
        persistent_workers=big_memory
    )
    
    result = {
        'train': train_dataloader,
        'val': val_dataloader,
    }
    
    if test_dataset is not None:
        test_dataloader = DataLoader(
            test_dataset,
            batch_size=batch_size,
            num_workers=1,
            shuffle=False,
            pin_memory=has_cuda
        )
        result['test'] = test_dataloader
    
    return result


def inspect_data(dir, prefix, targets, limit, batch_size=32, split_ratios=(0.8, 0.1, 0.1, 0.0), single_file_per_o=False):
    """Inspect the OQIP dataset"""
    dataloaders = create_dataloader(dir, prefix, targets, batch_size, split_ratios, single_file_per_o)
    
    for batch_idx, (features, targets) in enumerate(dataloaders['train']):
        print(f"Batch {batch_idx}, Features {features.shape}, Targets {targets.shape}")
        if batch_idx >= limit:
            break