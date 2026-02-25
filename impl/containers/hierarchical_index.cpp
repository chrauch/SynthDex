/******************************************************************************
 * Project:  synthdex
 * Purpose:  Adaptive TIR indexing
 * Author:   Christian Rauch
 ******************************************************************************
 * Copyright (c) 2025 - 2026
 *
 *
 * Extending
 *
 * Project:  irhint
 * Purpose:  Fast indexing for termporal information retrieval
 * Author:   Panagiotis Bouros, pbour@github.io
 * Author:   Christian Rauch
 ******************************************************************************
 * Copyright (c) 2023 - 2024
 *
 *
 * Extending
 *
 * Project:  hint
 * Purpose:  Indexing interval data
 * Author:   Panagiotis Bouros, pbour@github.io
 ******************************************************************************
 * Copyright (c) 2020 - 2022
 *
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

#include "hierarchical_index.h"


// HierarchicalIndex class
HierarchicalIndex::HierarchicalIndex(const Relation &R, const unsigned int numBits = 0)
{
    // Set bits and height
    this->gstart = R.gstart;
    this->gend = R.gend;
    this->numBits            = numBits;
    this->maxBits            = int(log2(this->gend-this->gstart))+1;
    this->height             = this->numBits+1;
    this->numBits_cutoff     = this->numBits;

    // Initialize statistics
    this->numPartitions      = 0;
    this->numEmptyPartitions = 0;
    this->avgPartitionSize   = 0;
    this->numOriginals       = 0;
    this->numReplicas        = 0;
    this->numOriginalsIn     = 0;
    this->numOriginalsAft    = 0;
    this->numReplicasIn      = 0;
    this->numReplicasAft     = 0;
}


HierarchicalIndex::HierarchicalIndex(const Relation &R, const Relation &U, const unsigned int numBits = 0)
{
    // Set bits and height
    this->gstart             = min(R.gstart, U.gstart);
    this->gend               = max(R.gend, U.gend);
    this->numBits            = numBits;
    this->maxBits            = int(log2(this->gend-this->gstart))+1;
    this->height             = this->numBits+1;
    this->numBits_cutoff     = this->numBits;

    // Initialize statistics
    this->numPartitions      = 0;
    this->numEmptyPartitions = 0;
    this->avgPartitionSize   = 0;
    this->numOriginals       = 0;
    this->numReplicas        = 0;
    this->numOriginalsIn     = 0;
    this->numOriginalsAft    = 0;
    this->numReplicasIn      = 0;
    this->numReplicasAft     = 0;
}



// HierarchicalIRIndex class
HierarchicalIRIndex::HierarchicalIRIndex(const IRelation &R, const unsigned int numBits = 0)
{
    // Set indexed relation
    this->R = &R;

    // Set bits and height
    this->numBits            = numBits;
    this->maxBits            = int(log2(R.gend-R.gstart))+1;
    this->height             = this->numBits+1;

    // Initialize statistics
    this->numPartitions      = 0;
    this->numEmptyPartitions = 0;
    this->avgPartitionSize   = 0;
    this->numOriginals       = 0;
    this->numReplicas        = 0;
    this->numOriginalsIn     = 0;
    this->numOriginalsAft    = 0;
    this->numReplicasIn      = 0;
    this->numReplicasAft     = 0;
}

