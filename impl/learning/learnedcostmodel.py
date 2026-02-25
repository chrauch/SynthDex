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

import torch.nn as nn

class LCM(nn.Module):
    """
    Learned Cost Model: Feedforward neural network for regression.
    No batch input normalization and no dropout, as the data is normalized and standardized.
    """
    def __init__(self, input_dim, output_dim, hidden_layers):
        super().__init__()
        
        # Build network layers dynamically
        layers = []
        prev_dim = input_dim
        
        for hidden_dim in hidden_layers:
            layers.append(nn.Linear(prev_dim, hidden_dim))
            layers.append(nn.ReLU())
            prev_dim = hidden_dim

        #self.input_norm = nn.BatchNorm1d(input_dim) 
        #nn.Dropout(0.1)
        
        # Output layer
        layers.append(nn.Linear(prev_dim, output_dim))
        
        self.net = nn.Sequential(*layers)


    def forward(self, x):
        return self.net(x)
