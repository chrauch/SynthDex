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

#ifndef _TEMPORAL_INVERTED_FILES_H_
#define _TEMPORAL_INVERTED_FILES_H_

#include <unordered_map>
#include <queue>
#include "../containers/relations.h"
#include "../containers/inverted_file.h"
#include "../containers/1dgrid.h"
#include "../containers/hint_m.h"
#include "../utils/global.h"



class TemporalInvertedFile : public InvertedFileTemplate
{
private:
    // Indexed relation
    const IRelation *R;
        
    
    // Querying
    
public:
    // Posting lists
    unordered_map<ElementId, Relation> lists;

    // Constructing
    TemporalInvertedFile();
    TemporalInvertedFile(const IRelation &R);
    void index(const IRecord &r);
    void index(const IRecord &r, const ElementId &min, const ElementId &max);
    void extractRecords(unordered_map<RecordId, IRecord> &recordMap, const Timestamp time_offset) const;
    void getStats();
    size_t getSize();
    void print(char c);
    ~TemporalInvertedFile();

    // Querying
    bool moveOut_NoChecks(const RangeIRQuery &q, RelationId &candidates);
    bool moveOut_CheckBoth(const RangeIRQuery &q, RelationId &candidates);
    bool moveOut_CheckStart(const RangeIRQuery &q, RelationId &candidates);
    bool moveOut_CheckEnd(const RangeIRQuery &q, RelationId &candidates);
    bool intersect(const RangeIRQuery &q, const unsigned int elemoff, RelationId &candidates);
    bool intersectAndOutput(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result);
    bool intersectAndOutputAndErase(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result);

    void query(const RangeIRQuery &q, RelationId &result);

    // Updating
    void update(const IRelation &R) override;

    // Deleting
    void remove(const vector<bool> &idsToDelete) override;

    // Soft-deleting (tombstone: replace id with -1)
    void softdelete(const vector<bool> &idsToDelete) override;
};


#endif
