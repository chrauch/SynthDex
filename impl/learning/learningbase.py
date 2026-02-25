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
import json
import importlib.metadata
import os
import psutil
import torch
import torch.nn as nn
import re

sys.path.insert(0, os.path.dirname(__file__))
from learnedcostmodel import LCM


class LearningBase:
    """
    Base class for learning-related components with shared initialization logic.
    """
    
    def __init__(self, config_dir: str):
        self._load_config(config_dir)
        self._print_environment_info()
    
    def _load_config(self, config_dir):
        """Load settings."""
        cfg_file = config_dir + "/config.json"
        print(f"\tConfiguration = {cfg_file}")
        
        with open(cfg_file) as p:
            lines = [l for l in p if not l.lstrip().startswith('#')]
            self.cfg = json.loads(''.join(lines))
        
        self.output_dir = self.cfg["out"]["dir"]
        print(f"\tOutput directory = {self.output_dir}")

    def _print_environment_info(self):
        print(f"\tSystem resources = {(psutil.virtual_memory().total / (1024**3)):.1f}GB RAM, {os.cpu_count()} CPUs, CUDA:{torch.cuda.is_available()}")

        print(f"\tPython version = {sys.version}; Virtual environment = {sys.prefix}")
        
        # Get installed packages
        installed = list(importlib.metadata.distributions())
        installed_unique = {p.metadata['Name'].lower(): p for p in installed}
        installed_unique_str = '; '.join(
            [f"{p.metadata['Name']}=={p.version}" for p in sorted(
            installed_unique.values(), key=lambda x: x.metadata['Name'].lower())])
        print(f"\tInstalled Packages = [{installed_unique_str}]")

    def load_model(self, regex: str) -> nn.Module:
        """
        Load a trained neural network model.
        
        Args:
            regex: Regex pattern to filter model files.
        
        Returns:
            Loaded LCM model ready for inference
        """
        lcms_dir = os.path.join(self.output_dir, "lcms")
        if not os.path.exists(lcms_dir):
            raise ValueError(f"LCMs directory not found: {lcms_dir}")
        
        # Get all .pth files
        pth_files = [f for f in os.listdir(lcms_dir) if f.endswith('.pth')]
        if not pth_files:
            raise ValueError(f"No LCM files found in {lcms_dir}")
        
        # Apply filter
        pth_files = [f for f in pth_files if re.search(regex, f)]
        if not pth_files:
            raise ValueError(f"No LCM files matching regex '{regex}' found in {lcms_dir}")
        pth_files.sort(reverse=True)
        
        model_filename = pth_files[0]
        model_path = os.path.join(lcms_dir, model_filename)
        
        print(f"Model = {model_path}")
        
        # Initialize model with correct dimensions
        output_dim = len(self.cfg["learning"]["targets"])
        input_dim = self.cfg["learning"]["features-dim"]
        hidden_layers = self.cfg["learning"]["neural-network"]["hidden-layers"]
        model = LCM(input_dim, output_dim, hidden_layers=hidden_layers)

        # Load the trained weights
        model.load_state_dict(torch.load(model_path, map_location='cpu'))
        model.eval()
        
        return model
