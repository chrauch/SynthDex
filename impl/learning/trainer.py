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

import sys
import os
import torch
import pandas as pd
import torch.nn as nn
from datetime import datetime, timedelta
import time

sys.path.insert(0, os.path.dirname(__file__))
from learnedcostmodel import LCM
from dataloader import create_dataloader
from learningbase import LearningBase


class Trainer(LearningBase):

    def __init__(self, config_dir):
        super().__init__(config_dir)
        self.pretrained_lcm_regex = self.cfg["out"]["machine-prefix"]
        if self.pretrained_lcm_regex == "": self.pretrained_lcm_regex = None


    def train(self) -> nn.Module:
        print("Starting training")

        # Create dataloaders for training and validation
        dataloaders = create_dataloader(
            self.output_dir,
            self.cfg["out"]["machine-prefix"],
            self.cfg["learning"]["targets"],
            batch_size=self.cfg["learning"]["batch-size"],
            split_ratios=(
                self.cfg["learning"]["split"][0],
                self.cfg["learning"]["split"][1],
                self.cfg["learning"]["split"][2]),
            single_file_per_o=self.cfg["out"]["single-OQIP-file-per-O"],
            sample_ratio=self.cfg["learning"]["sample-ratio"]
        )
        
        train_dataloader = dataloaders['train']
        val_dataloader = dataloaders['val']
        test_dataloader = dataloaders.get('test', None)
        
        # Get input and output dimensions from the first batch
        first_batch_features, first_batch_targets = next(iter(train_dataloader))
        input_dim = first_batch_features.shape[1]
        output_dim = first_batch_targets.shape[1]
        
        print(f"Input dimension: {input_dim}; Output dimension: {output_dim}")

        # Initialize model - either new or load pre-trained
        if self.pretrained_lcm_regex:
            print(f"Regex for pre-trained model = {self.pretrained_lcm_regex}")
            model = self.load_model(self.pretrained_lcm_regex)
        else:
            hidden_layers = self.cfg["learning"]["neural-network"]["hidden-layers"]
            model = LCM(input_dim, output_dim, hidden_layers=hidden_layers)
            print("Created new model")
        
        print(f"Model = {model}")
        
        # Move model to GPU if available
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        model = model.to(device)
        print(f"Using device = {device}")
        print(f"PyTorch threads = {torch.get_num_threads()}")
        print(f"OpenMP threads = {torch.get_num_interop_threads()}")
        
        # Load penalty weights for different metrics
        penalty_weights = torch.tensor(self.cfg["learning"]["penalty-weights"], dtype=torch.float32, device=device)
        print(f"Penalty weights = {penalty_weights.tolist()}")
        
        # Define weighted MSE loss function
        def weighted_mse_loss(pred, target, weights):
            squared_diff = (pred - target) ** 2
            weighted_squared_diff = squared_diff * weights
            return weighted_squared_diff.mean()
        
        criterion = lambda pred, target: weighted_mse_loss(pred, target, penalty_weights)
        print(f"Criterion = Weighted MSE Loss with penalty weights {penalty_weights.tolist()}")
        
        # Use AdamW optimizer
        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=self.cfg["learning"]["optimizer"]["lr"],
            weight_decay=self.cfg["learning"]["optimizer"]["weight-decay"],
            eps=self.cfg["learning"]["optimizer"]["eps"]
        )
        print(f"Optimizer = {optimizer}")
        
        # Add learning rate scheduler
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer,
            mode='min',
            factor=self.cfg["learning"]["scheduler"]["factor"],
            patience=self.cfg["learning"]["scheduler"]["patience"],
            min_lr=self.cfg["learning"]["scheduler"]["min-lr"]
        )
        print(f"Scheduler = {scheduler} (factor={scheduler.factor}, patience={scheduler.patience}, min_lr={scheduler.min_lrs[0]})")

        # Initialize mixed precision training if GPU available
        use_amp = torch.cuda.is_available()
        scaler = torch.cuda.amp.GradScaler() if use_amp else None
        if use_amp:
            print("Using Automatic Mixed Precision (AMP) for faster training")

        best_val_loss = float('inf')
        patience_counter = 0
        
        # Calculate progress reporting interval
        progress_steps = self.cfg["learning"]["progress-steps"]
        progress_interval = max(1, len(train_dataloader) // progress_steps)
        
        # Training start time
        training_start_time = time.time()

        for epoch in range(self.cfg["learning"]["epochs"]):
            print(f"Epoch {epoch+1}/{self.cfg["learning"]["epochs"]}")
            epoch_start_time = time.time()
            
            # Training phase
            model.train()
            total_train_loss = 0
            train_batch_count = 0
            
            for batch_idx, (xb, yb) in enumerate(train_dataloader):
                # Move data to device
                xb, yb = xb.to(device), yb.to(device)
                
                optimizer.zero_grad()
                
                # Use mixed precision if available
                if use_amp:
                    with torch.cuda.amp.autocast():
                        pred = model(xb)
                        loss = criterion(pred, yb)
                    
                    # Scale loss and backward
                    scaler.scale(loss).backward()
                    
                    # Gradient clipping with scaler
                    scaler.unscale_(optimizer)
                    torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
                    
                    # Optimizer step with scaler
                    scaler.step(optimizer)
                    scaler.update()
                else:
                    # Standard precision training
                    pred = model(xb)
                    loss = criterion(pred, yb)
                    loss.backward()
                    
                    # Gradient clipping
                    torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
                    
                    optimizer.step()
                
                total_train_loss += loss.item()
                train_batch_count += 1
                
                # Progress reporting
                if (batch_idx + 1) % progress_interval == 0 or batch_idx == 0:
                    current_avg_loss = total_train_loss / train_batch_count
                    progress_pct = (batch_idx + 1) / len(train_dataloader) * 100
                    
                    # Calculate ETA for current epoch
                    elapsed_time = time.time() - epoch_start_time
                    if batch_idx > 0:
                        avg_time_per_batch = elapsed_time / (batch_idx + 1)
                        remaining_batches = len(train_dataloader) - (batch_idx + 1)
                        eta_seconds = remaining_batches * avg_time_per_batch
                        eta_str = str(timedelta(seconds=int(eta_seconds)))
                        
                        print(f"  Batch {batch_idx + 1}/{len(train_dataloader)} ({progress_pct:.1f}%) - "
                              f"Loss: {current_avg_loss:.8f} - Epoch {epoch+1} ETA: {eta_str}", flush=True)
                    else:
                        print(f"  Batch {batch_idx + 1}/{len(train_dataloader)} ({progress_pct:.1f}%) - "
                              f"Loss: {current_avg_loss:.8f}", flush=True)
            
            train_loss = total_train_loss / train_batch_count
            
            # Validation
            print(f"  Running validation...", flush=True)
            model.eval()
            total_val_loss = 0
            val_batch_count = 0
            
            with torch.no_grad():
                for batch_idx, (xb, yb) in enumerate(val_dataloader):
                    # Move data to device
                    xb, yb = xb.to(device), yb.to(device)
                    pred = model(xb)
                    loss = criterion(pred, yb)
                    total_val_loss += loss.item()
                    val_batch_count += 1
            
            val_loss = total_val_loss / val_batch_count
            
            # Calculate epoch time and overall ETA
            epoch_time = time.time() - epoch_start_time
            
            if epoch > 0:  # Calculate ETA after first epoch
                avg_epoch_time = (time.time() - training_start_time) / (epoch + 1)
                remaining_epochs = self.cfg["learning"]["epochs"] - (epoch + 1)
                eta_seconds = remaining_epochs * avg_epoch_time
                eta_str = str(timedelta(seconds=int(eta_seconds)))
                
                print(f"Train Loss: {train_loss:.8f}, Val Loss: {val_loss:.8f}, LR: {optimizer.param_groups[0]['lr']:.2e}")
                print(f"Epoch time: {timedelta(seconds=int(epoch_time))}, Overall ETA: {eta_str}")
            else:
                print(f"Train Loss: {train_loss:.8f}, Val Loss: {val_loss:.8f}, LR: {optimizer.param_groups[0]['lr']:.2e}")
                print(f"Epoch time: {timedelta(seconds=int(epoch_time))}")
            
            # Step the scheduler
            scheduler.step(val_loss)
            
            # Early stopping based on validation loss
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                patience_counter = 0
                if val_loss < self.cfg["learning"]["persistence-threshold"][0]:
                    print(f"Validation loss < {self.cfg["learning"]["persistence-threshold"]}. Saving model.")
                    self.save(model)
            else:
                patience_counter += 1
                
            if val_loss < self.cfg["learning"]["stopping-threshold"][0]:
                print(f"Validation loss < {self.cfg["learning"]["stopping-threshold"]}. Stopping.")
                break
                
            if patience_counter >= self.cfg["learning"]["patience"]:
                print(f"No improvement for {self.cfg["learning"]["patience"]} epochs. Stopping.")
                break

        print("Training complete.")

        # Final evaluation on test set (if it exists)
        if test_dataloader is not None:
            print("Evaluating on test set...")
            model.eval()
            total_test_loss = 0
            test_batch_count = 0
            
            with torch.no_grad():
                for xb, yb in test_dataloader:
                    # Move data to device
                    xb, yb = xb.to(device), yb.to(device)
                    pred = model(xb)
                    loss = criterion(pred, yb)
                    total_test_loss += loss.item()
                    test_batch_count += 1
            
            avg_test_loss = total_test_loss / test_batch_count
            print(f"Final Test Loss: {avg_test_loss:.8f}")

            # Some sample predictions
            print("Sample predictions from test set:")
            model.eval()
            targets = self.cfg["learning"]["targets"]
            with torch.no_grad():
                test_batch = next(iter(test_dataloader))
                xb, yb = test_batch
                # Move data to device
                xb, yb = xb.to(device), yb.to(device)
                preds = model(xb)
                count = 0
                for actual, pred in zip(yb, preds):
                    # Handle multiple targets
                    actual_str = ""
                    pred_str = ""
                    for i, target_name in enumerate(targets):
                        actual_val = 10**actual[i].item()
                        pred_val = 10**pred[i].item()
                        actual_str += f"{target_name}: {actual_val:.16f}"
                        pred_str += f"{target_name}: {pred_val:.16f}"
                        if i < len(targets) - 1:
                            actual_str += ", "
                            pred_str += ", "
                    print(f"Actual:    {actual_str}\nPredicted: {pred_str}")
                    count += 1
                    if count >= 10: break
        else:
            print("Skipping test set evaluation (no test data)")

        return model


    def save(self, model):
        lcms_dir = os.path.join(self.output_dir, "lcms")
        os.makedirs(lcms_dir, exist_ok=True)
        id = self.cfg["out"]["machine-prefix"]
        #host = socket.gethostname()
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        model_path = os.path.join(lcms_dir, f"{id}.{timestamp}.lcm.pth")
        torch.save(model.state_dict(), model_path)
        print(f"LCM file saved = {model_path}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Arguments missing: <config_dir>")
        sys.exit(1)
    
    config_dir = sys.argv[1]
    
    t = Trainer(config_dir)
    model = t.train()
