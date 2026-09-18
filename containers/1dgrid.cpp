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
 * Project:  hint
 * Purpose:  Indexing interval data
 * Author:   Panagiotis Bouros, pbour@github.io
 * Author:   George Christodoulou
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

#include "1dgrid.h"
#include <unordered_set>



inline void OneDimensionalGrid::updateCounters(const Record &r)
{
    auto s_pid = (r.start == this->gend)? this->numPartitionsMinus1: (max(r.start,gstart)-gstart)/this->partitionExtent;
    auto e_pid = (r.end   == this->gend)? this->numPartitionsMinus1: (min(r.end,gend)-gstart)/this->partitionExtent;
    
    this->pRecs_sizes[s_pid]++;
    while (s_pid != e_pid)
    {
        s_pid++;
        this->pRecs_sizes[s_pid]++;
    }
}


void OneDimensionalGrid::updatePartitions(const Record &r)
{
    auto s_pid = (r.start == this->gend)? this->numPartitionsMinus1: (max(r.start,gstart)-gstart)/this->partitionExtent;
    auto e_pid = (r.end   == this->gend)? this->numPartitionsMinus1: (min(r.end,gend)-gstart)/this->partitionExtent;

    this->pRecs[s_pid].push_back(r);
    while (s_pid != e_pid)
    {
        s_pid++;
        this->pRecs[s_pid].push_back(r);
    }
}


OneDimensionalGrid::OneDimensionalGrid()
{
    
}


OneDimensionalGrid::OneDimensionalGrid(const Relation &R, const PartitionId numPartitions)
{
    // Initialize.
    this->numPartitions       = numPartitions;
    this->numPartitionsMinus1 = this->numPartitions-1;
    this->gstart              = R.gstart;
    this->gend                = R.gend;
    this->partitionExtent     = (Timestamp)ceil((double)(this->gend-this->gstart)/this->numPartitions);
    

    // Step 1: one pass to count the contents inside each partition.
    this->pRecs_sizes = (size_t*)calloc(this->numPartitions, sizeof(size_t));

    for (const Record &r : R)
        this->updateCounters(r);

    // Step 2: allocate necessary memory.
    this->pRecs = new Relation[this->numPartitions];
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        this->pRecs[pId].gstart = this->gstart            + pId*this->partitionExtent;
        this->pRecs[pId].gend   = this->pRecs[pId].gstart + this->partitionExtent;
        this->pRecs[pId].reserve(this->pRecs_sizes[pId]);
    }

    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);

    // Free auxiliary memory.
    free(pRecs_sizes);
}


void OneDimensionalGrid::print(char c)
{
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        Relation &p = this->pRecs[pId];
        
        cout << "Partition " << pId << " [" << p.gstart << ".." << p.gend << "] (" << p.size() << "):";
        for (size_t i = 0; i < p.size(); i++)
            cout << " r" << p[i].id;
        cout << endl;
    }
}


OneDimensionalGrid::~OneDimensionalGrid()
{
    delete[] this->pRecs;
}


void OneDimensionalGrid::getStats()
{
}


size_t OneDimensionalGrid::getSize()
{
    size_t size = 0;
    
    // Size of the object's member variables
    size += sizeof(numPartitions);
    size += sizeof(numPartitionsMinus1);
    size += sizeof(gstart);
    size += sizeof(gend);
    size += sizeof(partitionExtent);
    
    // Size of the arrays of partitions
    size += sizeof(Relation*); // pRecs pointer
    size += sizeof(size_t*);   // pRecs_sizes pointer
    
    // Size of each partition and its metadata
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        // Size of the Relation structure itself
        size += sizeof(Relation);
        
        // Size of the records in this partition
        size += this->pRecs[pId].size() * sizeof(Record);
        
        // Size of the size counter
        size += sizeof(size_t);
    }
    
    return size;
}


void OneDimensionalGrid::extractRecords(Relation &R) const
{
    // Extract all unique records from the grid
    // Use a set to deduplicate since records can appear in multiple partitions
    unordered_map<RecordId, Record> uniqueRecs;
    
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        for (const auto &rec : this->pRecs[pId])
        {
            uniqueRecs[rec.id] = rec;
        }
    }
    
    // Transfer to output relation
    R.clear();
    R.reserve(uniqueRecs.size());
    for (const auto &pair : uniqueRecs)
    {
        R.push_back(pair.second);
    }
    
    // Sort by ID to maintain expected ordering (map iteration is unordered)
    sort(R.begin(), R.end(), compareRecordsById);
    
    // Update domain
    R.gstart = this->gstart;
    R.gend = this->gend;
}


void OneDimensionalGrid::softdelete(const vector<bool> &idsToDelete)
{
    // Replace matching record IDs with tombstone (-1) in all partitions
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        for (auto &rec : this->pRecs[pId])
        {
            if (inDeleteSet(rec.id, idsToDelete))
                rec.id = -1;
        }
    }
}


// Querying
void OneDimensionalGrid::moveOut_checkBoth(const RangeQuery &q, RelationId &candidates)
{
    if (q.start > this->gend || q.end < this->gstart) return;

    RelationIterator iter, iterBegin, iterEnd;
    auto s_pid = (max(q.start,gstart) == this->gend)? this->numPartitionsMinus1: (max(q.start,gstart)-gstart)/this->partitionExtent;
    auto e_pid = (min(q.end,gend)     == this->gend)? this->numPartitionsMinus1: (min(q.end,gend)-gstart)/this->partitionExtent;
    
    // Handle the first partition.
    iterBegin = this->pRecs[s_pid].begin();
    iterEnd   = this->pRecs[s_pid].end();

    for (RelationIterator iter = iterBegin; iter != iterEnd; iter++)
    {
        if ((iter->start <= q.end) && (q.start <= iter->end))
            candidates.push_back(iter->id);
    }

    // Handle partitions completely contained inside the query range.
    for (auto pid = s_pid+1; pid < e_pid; pid++)
    {
        Relation &p = this->pRecs[pid];
        iterBegin = p.begin();
        iterEnd = p.end();
        for (iter = p.begin(); iter != iterEnd; iter++)
        {
            // Perform de-duplication test.
            if (iter->start >= p.gstart)
                candidates.push_back(iter->id);
        }
    }

    // Handle the last partition.
    if (e_pid != s_pid)
    {
        iterBegin = this->pRecs[e_pid].begin();
        iterEnd = this->pRecs[e_pid].end();
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if ((iter->start >= this->pRecs[e_pid].gstart) && (iter->start <= q.end) && (q.start <= iter->end))
                candidates.push_back(iter->id);
        }
    }
}


inline void mergeSort_withoutDeduplication(Relation &partition, RelationId &candidates, RelationId &result)
{
    auto iterP    = partition.begin();
    auto iterPEnd = partition.end();
    auto iterC    = candidates.begin();
    auto iterCEnd = candidates.end();

//    tmp.reserve(candidates.size());
    while ((iterP != iterPEnd) && (iterC != iterCEnd))
    {
        if (iterP->id < *iterC)
            iterP++;
        else if (iterP->id > *iterC)
            iterC++;
        else
        {
            result.push_back(iterP->id);
            iterP++;
            
            iterC++;
        }
    }
//    candidates.swap(tmp);
}

inline void mergeSort_withDeduplication(Relation &partition, RelationId &candidates, RelationId &result)
{
    auto iterP    = partition.begin();
    auto iterPEnd = partition.end();
    auto iterC    = candidates.begin();
    auto iterCEnd = candidates.end();

    while ((iterP != iterPEnd) && (iterC != iterCEnd))
    {
        if (iterP->id < *iterC)
            iterP++;
        else if (iterP->id > *iterC)
            iterC++;
        else
        {
            if (iterP->start >= partition.gstart)
                result.push_back(iterP->id);
            iterP++;
            iterC++;
        }
    }
//    candidates.swap(tmp);
}


void OneDimensionalGrid::interesect(const RangeQuery &q, RelationId &candidates)
{
    if (q.start > this->gend || q.end < this->gstart)
    {
        candidates.clear();
        return;
    }

    RelationId result;
    auto s_pid = (max(q.start,gstart) == this->gend)? this->numPartitionsMinus1: (max(q.start,gstart)-gstart)/this->partitionExtent;
    auto e_pid = (min(q.end,gend)     == this->gend)? this->numPartitionsMinus1: (min(q.end,gend)-gstart)/this->partitionExtent;

    mergeSort_withoutDeduplication(this->pRecs[s_pid], candidates, result);

    for (auto pid = s_pid+1; pid <= e_pid; pid++)
    {
        mergeSort_withDeduplication(this->pRecs[pid], candidates, result);
    }

    candidates.swap(result);
}


void OneDimensionalGrid::interesectAndOutput(const RangeQuery &q, RelationId &candidates, RelationId &result)
{
    if (q.start > this->gend || q.end < this->gstart) return;

    auto s_pid = (max(q.start,gstart) == this->gend)? this->numPartitionsMinus1: (max(q.start,gstart)-gstart)/this->partitionExtent;
    auto e_pid = (min(q.end,gend)     == this->gend)? this->numPartitionsMinus1: (min(q.end,gend)-gstart)/this->partitionExtent;

    mergeSort_withoutDeduplication(this->pRecs[s_pid], candidates, result);

    for (auto pid = s_pid+1; pid <= e_pid; pid++)
    {
        mergeSort_withDeduplication(this->pRecs[pid], candidates, result);
    }
}



// ===== OneDimensionalGrid_Classed =====

inline void OneDimensionalGrid_Classed::updateCounters(const Record &r)
{
    auto s_pid = (r.start == this->gend)? this->numPartitionsMinus1: (max(r.start,gstart)-gstart)/this->partitionExtent;
    auto e_pid = (r.end   == this->gend)? this->numPartitionsMinus1: (min(r.end,gend)-gstart)/this->partitionExtent;
    
    this->pRecs_sizes[s_pid]++;
    while (s_pid != e_pid)
    {
        s_pid++;
        this->pRecs_sizes[s_pid]++;
    }
}


void OneDimensionalGrid_Classed::updatePartitions(const Record &r)
{
    auto s_pid = (r.start == this->gend)? this->numPartitionsMinus1: (max(r.start,gstart)-gstart)/this->partitionExtent;
    auto e_pid = (r.end   == this->gend)? this->numPartitionsMinus1: (min(r.end,gend)-gstart)/this->partitionExtent;

    this->pRecs[s_pid].push_back(r);
    // Maintain sorted ID arrays for intersect.
    auto &allIds = this->pAllIds[s_pid];
    allIds.insert(lower_bound(allIds.begin(), allIds.end(), r.id), r.id);
    auto &orgIds = this->pOrgIds[s_pid];
    orgIds.insert(lower_bound(orgIds.begin(), orgIds.end(), r.id), r.id);

    while (s_pid != e_pid)
    {
        s_pid++;
        this->pRecs[s_pid].push_back(r);
        auto &allIds2 = this->pAllIds[s_pid];
        allIds2.insert(lower_bound(allIds2.begin(), allIds2.end(), r.id), r.id);
    }
}


OneDimensionalGrid_Classed::OneDimensionalGrid_Classed()
{
}


OneDimensionalGrid_Classed::OneDimensionalGrid_Classed(const Relation &R, const PartitionId numPartitions)
{
    this->numPartitions       = numPartitions;
    this->numPartitionsMinus1 = this->numPartitions-1;
    this->gstart              = R.gstart;
    this->gend                = R.gend;
    this->partitionExtent     = (Timestamp)ceil((double)(this->gend-this->gstart)/this->numPartitions);

    // Step 1: count records per partition.
    this->pRecs_sizes = (size_t*)calloc(this->numPartitions, sizeof(size_t));

    for (const Record &r : R)
        this->updateCounters(r);

    // Step 2: allocate flat record arrays.
    this->pRecs = new Relation[this->numPartitions];
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        this->pRecs[pId].gstart = this->gstart            + pId*this->partitionExtent;
        this->pRecs[pId].gend   = this->pRecs[pId].gstart + this->partitionExtent;
        this->pRecs[pId].reserve(this->pRecs_sizes[pId]);
    }

    // Step 3: fill partitions (flat, no classification).
    for (const Record &r : R)
    {
        auto s_pid = (r.start == this->gend)? this->numPartitionsMinus1: (max(r.start,gstart)-gstart)/this->partitionExtent;
        auto e_pid = (r.end   == this->gend)? this->numPartitionsMinus1: (min(r.end,gend)-gstart)/this->partitionExtent;

        this->pRecs[s_pid].push_back(r);
        auto pid = s_pid;
        while (pid != e_pid)
        {
            pid++;
            this->pRecs[pid].push_back(r);
        }
    }

    free(pRecs_sizes);

    // Step 4: build sorted ID arrays for cache-optimal intersect.
    this->pAllIds = new RelationId[this->numPartitions];
    this->pOrgIds = new RelationId[this->numPartitions];
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        Relation &p = this->pRecs[pId];

        // pAllIds: all records in this partition, sorted by ID
        auto &allIds = this->pAllIds[pId];
        allIds.reserve(p.size());
        for (const auto &rec : p)
            allIds.push_back(rec.id);
        sort(allIds.begin(), allIds.end());

        // pOrgIds: originals only (start >= gstart), sorted by ID
        auto &orgIds = this->pOrgIds[pId];
        for (const auto &rec : p)
        {
            if (rec.start >= p.gstart)
                orgIds.push_back(rec.id);
        }
        sort(orgIds.begin(), orgIds.end());
    }
}


void OneDimensionalGrid_Classed::print(char c)
{
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        Relation &p = this->pRecs[pId];
        
        cout << "Partition " << pId << " [" << p.gstart << ".." << p.gend << "] (" << p.size() << "):";
        for (size_t i = 0; i < p.size(); i++)
            cout << " r" << p[i].id;
        cout << endl;
    }
}


OneDimensionalGrid_Classed::~OneDimensionalGrid_Classed()
{
    delete[] this->pRecs;
    delete[] this->pAllIds;
    delete[] this->pOrgIds;
}


void OneDimensionalGrid_Classed::getStats()
{
}


size_t OneDimensionalGrid_Classed::getSize()
{
    size_t size = 0;
    
    size += sizeof(numPartitions);
    size += sizeof(numPartitionsMinus1);
    size += sizeof(gstart);
    size += sizeof(gend);
    size += sizeof(partitionExtent);
    
    size += sizeof(Relation*);
    size += sizeof(size_t*);
    size += 2 * sizeof(RelationId*);
    
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        size += sizeof(Relation);
        size += this->pRecs[pId].size() * sizeof(Record);
        size += 2 * sizeof(RelationId);
        size += this->pAllIds[pId].size() * sizeof(RecordId);
        size += this->pOrgIds[pId].size() * sizeof(RecordId);
    }
    
    return size;
}


void OneDimensionalGrid_Classed::extractRecords(Relation &R) const
{
    unordered_map<RecordId, Record> uniqueRecs;
    
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        for (const auto &rec : this->pRecs[pId])
            uniqueRecs[rec.id] = rec;
    }
    
    R.clear();
    R.reserve(uniqueRecs.size());
    for (const auto &pair : uniqueRecs)
        R.push_back(pair.second);
    
    sort(R.begin(), R.end(), compareRecordsById);
    R.gstart = this->gstart;
    R.gend = this->gend;
}


void OneDimensionalGrid_Classed::softdelete(const vector<bool> &idsToDelete)
{
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        for (auto &rec : this->pRecs[pId])
        {
            if (inDeleteSet(rec.id, idsToDelete))
                rec.id = -1;
        }
    }
}


// Querying
void OneDimensionalGrid_Classed::moveOut_checkBoth(const RangeQuery &q, RelationId &candidates)
{
    if (q.start > this->gend || q.end < this->gstart) return;

    RelationIterator iter, iterBegin, iterEnd;
    auto s_pid = (max(q.start,gstart) == this->gend)? this->numPartitionsMinus1: (max(q.start,gstart)-gstart)/this->partitionExtent;
    auto e_pid = (min(q.end,gend)     == this->gend)? this->numPartitionsMinus1: (min(q.end,gend)-gstart)/this->partitionExtent;
    
    // First partition: check both start and end.
    iterBegin = this->pRecs[s_pid].begin();
    iterEnd   = this->pRecs[s_pid].end();
    for (iter = iterBegin; iter != iterEnd; iter++)
    {
        if ((iter->start <= q.end) && (q.start <= iter->end))
            candidates.push_back(iter->id);
    }

    // Middle partitions: originals only via pre-built ID array (4 bytes/entry).
    for (auto pid = s_pid+1; pid < e_pid; pid++)
        candidates.insert(candidates.end(), this->pOrgIds[pid].begin(), this->pOrgIds[pid].end());

    // Last partition (if different from first): dedup + check both.
    if (e_pid != s_pid)
    {
        iterBegin = this->pRecs[e_pid].begin();
        iterEnd   = this->pRecs[e_pid].end();
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if ((iter->start >= this->pRecs[e_pid].gstart) && (iter->start <= q.end) && (q.start <= iter->end))
                candidates.push_back(iter->id);
        }
    }
}


// Single merge-join of two sorted ID arrays (cache-optimal: 4 bytes per entry).
inline void mergeJoin_ids(RelationId &partIds, RelationId &candidates, RelationId &result)
{
    auto iterP    = partIds.begin();
    auto iterPEnd = partIds.end();
    auto iterC    = candidates.begin();
    auto iterCEnd = candidates.end();

    while ((iterP != iterPEnd) && (iterC != iterCEnd))
    {
        if (*iterP < *iterC)
            iterP++;
        else if (*iterP > *iterC)
            iterC++;
        else
        {
            result.push_back(*iterP);
            iterP++;
            iterC++;
        }
    }
}


void OneDimensionalGrid_Classed::interesect(const RangeQuery &q, RelationId &candidates)
{
    if (q.start > this->gend || q.end < this->gstart)
    {
        candidates.clear();
        return;
    }

    RelationId result;
    auto s_pid = (max(q.start,gstart) == this->gend)? this->numPartitionsMinus1: (max(q.start,gstart)-gstart)/this->partitionExtent;
    auto e_pid = (min(q.end,gend)     == this->gend)? this->numPartitionsMinus1: (min(q.end,gend)-gstart)/this->partitionExtent;

    // First partition: all records via pre-built sorted ID array
    mergeJoin_ids(this->pAllIds[s_pid], candidates, result);

    // Subsequent partitions: originals only via pre-built sorted ID array
    for (auto pid = s_pid+1; pid <= e_pid; pid++)
        mergeJoin_ids(this->pOrgIds[pid], candidates, result);

    candidates.swap(result);
}


void OneDimensionalGrid_Classed::interesectAndOutput(const RangeQuery &q, RelationId &candidates, RelationId &result)
{
    if (q.start > this->gend || q.end < this->gstart) return;

    auto s_pid = (max(q.start,gstart) == this->gend)? this->numPartitionsMinus1: (max(q.start,gstart)-gstart)/this->partitionExtent;
    auto e_pid = (min(q.end,gend)     == this->gend)? this->numPartitionsMinus1: (min(q.end,gend)-gstart)/this->partitionExtent;

    // First partition: all records via pre-built sorted ID array
    mergeJoin_ids(this->pAllIds[s_pid], candidates, result);

    // Subsequent partitions: originals only via pre-built sorted ID array
    for (auto pid = s_pid+1; pid <= e_pid; pid++)
        mergeJoin_ids(this->pOrgIds[pid], candidates, result);
}
