/******************************************************************************
 * Project:  SynthDex
 * Purpose:  Adaptive Ensemble Indexing for Temporal Information Retrieval via Learned Cost Models
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

#ifndef _tIFSLICINGc_H_
#define _tIFSLICINGc_H_

#include <vector>
#include <queue>
#include "../structure/framework.h"
#include "../containers/relations.h"
#include "../containers/inverted_file.h"
#include "../containers/1dgrid.h"
#include "../containers/hint_m.h"
#include "../utils/global.h"



/**
 * tIF + Temporal Slicing, variant c  (dense vector + pre-sorted ID arrays)
 *
 * Builds on variant b (dense vector of posting lists, O(1) lookup via
 * offset subtraction), but replaces plain OneDimensionalGrid with
 * OneDimensionalGrid_Classed. The "Classed" container augments each temporal
 * partition with two pre-sorted ID arrays:
 *   - pAllIds:  sorted IDs of all records in the partition (used for the
 *               first / move_out pass, enables binary-search or merge-join).
 *   - pOrgIds:  sorted IDs of original (non-updated) records only (used for
 *               subsequent refine passes over a shrinking candidate set).
 *
 * The sorted arrays allow the refine step to run a merge-join instead of a
 * nested-loop scan, cutting the refinement cost from O(|candidates| * |part|)
 * to O(|candidates| + |part|). This matters most when candidate sets are large
 * (e.g. high-rank / rare elements that produce many initial hits).
 *
 * move_out:  same O(1) vector lookup as variant b, then scan pAllIds into the
 *            result set.
 * refine:    merge-join the current candidate set against pOrgIds (sorted),
 *            producing a filtered result in linear time.
 *
 * Trade-off: higher memory usage and longer build time than b (pre-sorting
 * pAllIds / pOrgIds at construction), but faster at query time for large
 * result sets. Preferred when the workload contains many refinement steps.
 * 
 * It seems to be slower than b for most of the tested workloads.
 */
class tIFSLICINGc final : public Refinement_ElemFreq
{
private:
    uint partitions;
    double dynratio;
    double dynbase;
    Timestamp time_start;
    ElementId elem_min;
    ElementId elem_max;
    // Posting lists (vector indexed by elem - lists_offset for O(1) access)
    vector<OneDimensionalGrid_Classed*> lists;
    ElementId lists_offset;
    
public:
    // Constructing
    tIFSLICINGc();
    tIFSLICINGc(
        const Boundaries &boundaries,
        const uint &partitions,
        const double &dynbase,
        const double &dynratio,
        const IRelation &R);
    void getStats();
    size_t getSize();
    void print(char c);
    ~tIFSLICINGc();

    // Updating
    void update(const IRelation &R);

    // Deleting
    void remove(const vector<bool> &idsToDelete);

    // Soft-deleting (tombstone: replace id with -1)
    void softdelete(const vector<bool> &idsToDelete);

    // Querying
    bool moveOut_checkBoth(const RangeIRQuery &q, RelationId &candidates);
    bool intersectx(const RangeIRQuery &q, const unsigned int elemoff, RelationId &candidates);
    bool intersectAndOutput(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result);
    
    // Querying
    void move_out(
        const RangeIRQuery &q,
        ElementId &elem_off,
        RelationId &candidates);

    void refine(
        const RangeIRQuery &q,
        ElementId &elem_off,
        RelationId &candidates);

    string str(const int level) const;
};


#endif
