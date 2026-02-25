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
 * Author:   George Christodoulou
 * Author:   Nikos Mamoulis
 ******************************************************************************
 * Copyright (c) 2020 - 2024
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

#ifndef _HINT_M_H_
#define _HINT_M_H_

#include "../utils/global.h"
#include "../containers/relations.h"
#include "../containers/offsets.h"
#include "../containers/hierarchical_index.h"



class HINT_M_SubsSortByRecordId_CM : public HierarchicalIndex
{
private:
    Relation      **pOrgsInTmp;
    Relation      **pOrgsAftTmp;
    Relation      **pRepsInTmp;
    Relation      **pRepsAftTmp;
    
    RelationId    **pOrgsInIds;
    RelationId    **pOrgsAftIds;
    RelationId    **pRepsInIds;
    RelationId    **pRepsAftIds;
    vector<pair<Timestamp, Timestamp> > **pOrgsInTimestamps;
    vector<pair<Timestamp, Timestamp> > **pOrgsAftTimestamps;
    vector<pair<Timestamp, Timestamp> > **pRepsInTimestamps;
    vector<pair<Timestamp, Timestamp> > **pRepsAftTimestamps;
    
    RecordId      **pOrgsIn_sizes, **pOrgsAft_sizes;
    size_t        **pRepsIn_sizes, **pRepsAft_sizes;
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
    // Querying
    // Auxiliary functions to scan a partition.
    inline void scanPartition_CheckBoth_moveOut(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, const RangeQuery &Q, RelationId &result);
    inline void scanPartition_CheckEnd_moveOut(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, const RangeQuery &Q, RelationId &result);
    inline void scanPartition_CheckStart_moveOut(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, const RangeQuery &Q, RelationId &result);
    inline void scanPartition_NoChecks_moveOut(unsigned int level, Timestamp t, RelationId **ids, RelationId &result);
    inline void scanPartitions_NoChecks_moveOut(unsigned int level, Timestamp ts, Timestamp te, RelationId **ids, RelationId &result);
    inline void scanPartition_NoChecks_intersect(unsigned int level, Timestamp t, RelationId **ids, RelationId &candidates, RelationId &result);
    //    inline void scanPartitions_NoChecks_intersect(unsigned int level, Timestamp ts, Timestamp te, RelationId **ids, RelationId &candidates, RelationId &result);
    
    inline void scanPartition_CheckBoth_moveOut(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, const RangeQuery &q, vector<RelationId> &vec_result);
    inline void scanPartition_CheckEnd_moveOut(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, const RangeQuery &q, vector<RelationId> &vec_result);
    inline void scanPartition_CheckStart_moveOut(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, const RangeQuery &q, vector<RelationId> &vec_result);
    inline void scanPartition_NoChecks_moveOut(unsigned int level, Timestamp t, RelationId **ids, vector<RelationId> &vec_result);
    inline void scanPartitions_NoChecks_moveOut(unsigned int level, Timestamp ts, Timestamp te, RelationId **ids, vector<RelationId> &vec_result);
    inline void scanPartition_NoChecks_intersect(unsigned int level, Timestamp t, RelationId **ids, RelationId &candidates, vector<RelationId> &vec_result);
//    inline void scanPartition_CheckBoth_intersect(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, const RangeQuery &Q, RelationId &candidates, RelationId &result);
//    inline void scanPartition_CheckEnd_intersect(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, const RangeQuery &Q, RelationId &candidates, RelationId &result);
//    inline void scanPartition_CheckStart_intersect(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, const RangeQuery &Q, RelationId &candidates, RelationId &result);
//    inline void scanPartition_NoChecks_intersect(unsigned int level, Timestamp t, RelationId **ids, RelationId &candidates, RelationId &result);
//    inline void scanPartitions_NoChecks_intersect(unsigned int level, Timestamp ts, Timestamp te, RelationId **ids, RelationId &candidates, RelationId &result);
    
public:
    // Construction
    HINT_M_SubsSortByRecordId_CM(const Relation &R, const unsigned int numBits);
    void getStats();
    size_t getSize();
    ~HINT_M_SubsSortByRecordId_CM();
    
    // Data extraction for updates
    void extractRecords(Relation &R) const;

    // Updating
    void insert(const Record &r);

    // Querying
    void moveOut(const RangeQuery &q, RelationId &candidates);
    void intersect(const RangeQuery &q, RelationId &candidates);
    void intersectAndOutput(const RangeQuery &q, RelationId &candidates, RelationId &result);
    
    void moveOut(const RangeQuery &q, vector<RelationId> &candidates);
    void moveOut_NoChecks(const RangeQuery &q, vector<RelationId> &candidates);
    void intersect(const RangeQuery &q, RelationId &candidates, vector<RelationId> &vec_candidates);
};



// HINT^m with subs+sort, skewness & sparsity optimizations and cash misses activated, from VLDB Journal
class HINT_M_SubsSort_SS_CM : public HierarchicalIndex
{
private:
    // Temporary structures
    Relation      *pOrgsInTmp;
    Relation      *pOrgsAftTmp;
    Relation      *pRepsInTmp;
    Relation      *pRepsAftTmp;
    
    // Index structure
    RelationId    *pOrgsInIds;
    vector<pair<Timestamp, Timestamp> > *pOrgsInTimestamps;
    RelationId    *pOrgsAftIds;
    vector<pair<Timestamp, Timestamp> > *pOrgsAftTimestamps;
    RelationId    *pRepsInIds;
    vector<pair<Timestamp, Timestamp> > *pRepsInTimestamps;
    RelationId    *pRepsAftIds;
    vector<pair<Timestamp, Timestamp> > *pRepsAftTimestamps;
    
    RecordId      **pOrgsIn_sizes, **pOrgsAft_sizes;
    size_t        **pRepsIn_sizes, **pRepsAft_sizes;
    RecordId      **pOrgsIn_offsets, **pOrgsAft_offsets;
    size_t        **pRepsIn_offsets, **pRepsAft_offsets;
    Offsets_SS_CM *pOrgsIn_ioffsets;
    Offsets_SS_CM *pOrgsAft_ioffsets;
    Offsets_SS_CM *pRepsIn_ioffsets;
    Offsets_SS_CM *pRepsAft_ioffsets;
    
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
    // Querying
    // Auxiliary functions to determine exactly how to scan a partition.
    inline bool getBounds(unsigned int level, Timestamp t, PartitionId &next_from, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, vector<pair<Timestamp, Timestamp> >::iterator &iterStart, vector<pair<Timestamp, Timestamp> >::iterator &iterEnd, RelationIdIterator &iterI);
    inline bool getBounds(unsigned int level, Timestamp t, PartitionId &next_from, Offsets_SS_CM *ioffsets, RelationId *ids, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd);
    inline bool getBounds(unsigned int level, Timestamp ts, Timestamp te, PartitionId &next_from, PartitionId &next_to, Offsets_SS_CM *ioffsets, RelationId *ids, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd);
    
    // Auxiliary functions to scan a partition.
    inline void scanPartition_CheckBoth_moveOut(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), const RangeQuery &q, PartitionId &next_from, RelationId &result);
    inline void scanPartition_CheckEnd_moveOut(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, const RangeQuery &q, PartitionId &next_from, RelationId &result);
    inline void scanPartition_CheckEnd_moveOut(const unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), const RangeQuery &q, PartitionId &next_from, RelationId &result);
    inline void scanPartition_CheckStart_moveOut(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), const RangeQuery &q, PartitionId &next_from, RelationId &result);
    inline void scanPartition_NoChecks_moveOut(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, RelationId *ids, PartitionId &next_from, RelationId &result);
    inline void scanPartitions_NoChecks_moveOut(const unsigned int level, const Timestamp ts, Timestamp te, Offsets_SS_CM *ioffsets, RelationId *ids, PartitionId &next_from, PartitionId &next_to, RelationId &result);

    inline void scanPartition_CheckBoth_intersect(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), const RangeQuery &q, PartitionId &next_from, RelationId &candidates, RelationId &result);
    inline void scanPartition_CheckEnd_intersect(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, const RangeQuery &q, PartitionId &next_from, RelationId &candidates, RelationId &result);
    inline void scanPartition_CheckEnd_intersect(const unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), const RangeQuery &q, PartitionId &next_from, RelationId &candidates, RelationId &result);
    inline void scanPartition_CheckStart_intersect(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), const RangeQuery &q, PartitionId &next_from, RelationId &candidates, RelationId &result);
    inline void scanPartition_NoChecks_intersect(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, RelationId *ids, PartitionId &next_from, RelationId &candidates, RelationId &result);
    inline void scanPartitions_NoChecks_intersect(const unsigned int level, const Timestamp ts, Timestamp te, Offsets_SS_CM *ioffsets, RelationId *ids, PartitionId &next_from, PartitionId &next_to, RelationId &candidates, RelationId &result);

public:
    // Construction
    HINT_M_SubsSort_SS_CM(const Relation &R, const unsigned int numBits);
    void getStats();
    size_t getSize();
    ~HINT_M_SubsSort_SS_CM();
    
    // Querying
    void moveOut(const RangeQuery &q, RelationId &candidates);
    void intersect(const RangeQuery &q, RelationId &candidates);
    void intersectAndOutput(const RangeQuery &q, RelationId &candidates, RelationId &result);
};



// Comparators
inline bool CompareTimestampPairsByStart(const pair<Timestamp, Timestamp> &lhs, const pair<Timestamp, Timestamp> &rhs)
{
    return (lhs.first < rhs.first);
}

inline bool CompareTimestampPairsByEnd(const pair<Timestamp, Timestamp> &lhs, const pair<Timestamp, Timestamp> &rhs)
{
    return (lhs.second < rhs.second);
}
#endif // _HINT_M_H_
