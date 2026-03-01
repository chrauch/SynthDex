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

#ifndef _HIERARCHICAL_INDEX_H_
#define _HIERARCHICAL_INDEX_H_

#include "../utils/global.h"
#include "../containers/relations.h"



// Frameworks
class IRIndex
{

public:

    virtual void print(const char c) {};
    virtual void getStats() {};
    virtual size_t getSize() { return 1; };
    
    // Querying
    virtual void query(const RangeIRQuery &q, RelationId &result) = 0;

    // Updating
    virtual void update(const IRelation &R) = 0;

    // Deleting
    virtual void remove(const vector<bool> &idsToDelete) = 0;

    // Soft-deleting (tombstone: replace id with -1)
    virtual void softdelete(const vector<bool> &idsToDelete) = 0;

    virtual ~IRIndex() {};

    virtual string str() const { return "Index"; };

};


class HierarchicalIndex
{
protected:

    // Index parameters
    unsigned int maxBits;
    unsigned int height;
    unsigned int numBits_cutoff;
    
    // Construction
    virtual inline void updateCounters(const Record &r) {};
    virtual inline void updatePartitions(const Record &r) {};

public:
    // Domain
    Timestamp gstart;
    Timestamp gend;
    
    // Index parameters
    unsigned int numBits;

    // Statistics
    size_t numPartitions;
    size_t numEmptyPartitions;
    float avgPartitionSize;
    size_t numOriginals, numReplicas;
    size_t numOriginalsIn, numOriginalsAft, numReplicasIn, numReplicasAft;

    // Construction
    HierarchicalIndex(const Relation &R, const unsigned numBits);
    HierarchicalIndex(const Relation &R, const Relation &U, const unsigned numBits);    // For updates only
    virtual void print(const char c) {};
    virtual void getStats() {};
    virtual size_t getSize() {};
    virtual ~HierarchicalIndex() {};
    
    // Querying
    virtual size_t executeBottomUp(RangeQuery Q) {return 0;};
};



class HierarchicalIRIndex : public IRIndex
{
protected:
    // Indexed relation
    const IRelation *R;

    // Index parameters
    Timestamp gstart;
    Timestamp gend;
    unsigned int maxBits;
    unsigned int height;
    
    // Construction
    virtual inline void updateCounters(const IRecord &r) {};
    virtual inline void updatePartitions(const IRecord &r) {};

public:
    // Index parameters
    unsigned int numBits;

    // Statistics
    size_t numPartitions;
    size_t numEmptyPartitions;
    float avgPartitionSize;
    size_t numOriginals, numReplicas;
    size_t numOriginalsIn, numOriginalsAft, numReplicasIn, numReplicasAft;

    // Construction
    HierarchicalIRIndex(const IRelation &R, const unsigned int numBits);
    virtual void print(const char c) {};
    virtual void getStats() {};
    virtual size_t getSize() {};
    virtual ~HierarchicalIRIndex() {};    

    // Querying
    virtual void query(const RangeIRQuery &q, RelationId &result) { };
};
#endif // _HIERARCHICAL_INDEX_H_
