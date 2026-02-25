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
 * Author:   Nikos Mamoulis
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

#ifndef _irHINTa_H_
#define _irHINTa_H_

#include "../structure/framework.h"
#include "../containers/inverted_file.h"
#include "../containers/temporal_inverted_file.h"
#include "../containers/offsets.h"



// Base variant; every hint partition is stored as an inverted file
class irHINTa : public Refinement_ElemFreq
{
private:
    ElementId elem_min;
    ElementId elem_max;
    Timestamp time_start;
    Timestamp time_end;
    unsigned int maxBits;
    unsigned int numBits;
    unsigned int height;
    
    TemporalInvertedFile **pOrgsIn;
    TemporalInvertedFile **pOrgsAft;
    TemporalInvertedFile **pRepsIn;
    TemporalInvertedFile **pRepsAft;
    
    // Temporary counter
    unordered_map<ElementId, ElementId> **pOrgsIn_lsizes;
    unordered_map<ElementId, ElementId> **pOrgsAft_lsizes;
    unordered_map<ElementId, ElementId> **pRepsIn_lsizes;
    unordered_map<ElementId, ElementId> **pRepsAft_lsizes;
    
    // Construction
    inline void updateCounters(const IRecord &r);
    inline void updatePartitions(const IRecord &r);
    void extractRecords(IRelation &R) const;
    
    // Querying
    inline void scanPartitionContainment_CheckBoth(TemporalInvertedFile &IF, const RangeIRQuery &q, RelationId &result);
    inline void scanPartitionContainment_CheckStart(TemporalInvertedFile &IF, const RangeIRQuery &q, RelationId &result);
    inline void scanPartitionContainment_CheckEnd(TemporalInvertedFile &IF, const RangeIRQuery &q, RelationId &result);
    inline void scanPartitionContainment_NoChecks(TemporalInvertedFile &IF, const RangeIRQuery &q, RelationId &result);

    inline void scanPartition_intersect(TemporalInvertedFile &IF, const RangeIRQuery &q, RelationId &candidates, RelationId &results);

public:
    // Construction
    irHINTa(
        const Boundaries &boundaries,
        const int &numBits,
        const IRelation &R);
    void getStats();
    size_t getSize();
    ~irHINTa();

    // Updating
    void update(const IRelation &R);

    // Deleting
    void remove(const RelationId &ids);

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


#endif // _irHINTa_H_
