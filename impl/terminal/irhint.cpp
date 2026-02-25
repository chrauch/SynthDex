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
 * Author:   Christian Rauch
 * Author:   Panagiotis Bouros, pbour@github.io
 ******************************************************************************
 * Copyright (c) 2023 - 2025
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

#include "irhint.h"
#include <unordered_set>

//#define CONSTRUCTION_TWO_PASSES slower


string irHINTa::str(const int l) const
{
    return "irHINT-alpha m=" + to_string(this->numBits);
}


void irHINTa::updateCounters(const IRecord &r)
{
    int level = 0;
    Timestamp a = r.start >> (this->maxBits-this->numBits);
    Timestamp b = r.end   >> (this->maxBits-this->numBits);
    Timestamp prevb;
    int firstfound = 0, lastfound = 0;
    

    while (level < this->height && a <= b)
    {
        if (a%2)
        { //last bit of a is 1
            if (firstfound)
            {
                if ((a == b) && (!lastfound))
                {
                    for (auto &tid : r.elements)
                    {
                        if (tid > this->elem_max) continue;
                        if (tid < this->elem_min) break;
                        this->pRepsIn_lsizes[level][a][tid]++;
                    }
                    lastfound = 1;
                }
                else
                {
                    for (auto &tid : r.elements)
                    {
                        if (tid > this->elem_max) continue;
                        if (tid < this->elem_min) break;
                        this->pRepsAft_lsizes[level][a][tid]++;
                    }
                }
            }
            else
            {
                if ((a == b) && (!lastfound))
                {
                    for (auto &tid : r.elements)
                    {
                        if (tid > this->elem_max) continue;
                        if (tid < this->elem_min) break;
                        this->pOrgsIn_lsizes[level][a][tid]++;
                    }
                }
                else
                {
                    for (auto &tid : r.elements)
                    {
                        if (tid > this->elem_max) continue;
                        if (tid < this->elem_min) break;
                        this->pOrgsAft_lsizes[level][a][tid]++;
                    }
                }
                firstfound = 1;
            }
            a++;
        }
        if (!(b%2))
        { //last bit of b is 0
            prevb = b;
            b--;
            if ((!firstfound) && b < a)
            {
                if (!lastfound)
                {
                    for (auto &tid : r.elements)
                    {
                        if (tid > this->elem_max) continue;
                        if (tid < this->elem_min) break;
                        this->pOrgsIn_lsizes[level][prevb][tid]++;
                    }
                }
                else
                {
                    for (auto &tid : r.elements)
                    {
                        if (tid > this->elem_max) continue;
                        if (tid < this->elem_min) break;
                        this->pOrgsAft_lsizes[level][prevb][tid]++;
                    }
                }
            }
            else
            {
                if (!lastfound)
                {
                    for (auto &tid : r.elements)
                    {
                        if (tid > this->elem_max) continue;
                        if (tid < this->elem_min) break;
                        this->pRepsIn_lsizes[level][prevb][tid]++;
                    }
                    lastfound = 1;
                }
                else
                {
                    for (auto &tid : r.elements)
                    {
                        if (tid > this->elem_max) continue;
                        if (tid < this->elem_min) break;
                        this->pRepsAft_lsizes[level][prevb][tid]++;
                    }
                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


void irHINTa::updatePartitions(const IRecord &r_org)
{
    // TODO, no copy
    IRecord r(r_org.id, r_org.start - this->time_start, r_org.end - this->time_start);
    r.elements = r_org.elements;

    int level = 0;
    Timestamp a = r.start >> (this->maxBits-this->numBits);
    Timestamp b = r.end   >> (this->maxBits-this->numBits);
    Timestamp prevb;
    int firstfound = 0, lastfound = 0;

    while (level < this->height && a <= b)
    {
        if (a%2)
        { //last bit of a is 1
            if (firstfound)
            {
                if ((a == b) && (!lastfound))
                {
                    this->pRepsIn[level][a].index(
                        r, this->elem_min, this->elem_max);
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAft[level][a].index(
                        r, this->elem_min, this->elem_max);
                }
            }
            else
            {
                if ((a == b) && (!lastfound))
                {
                    this->pOrgsIn[level][a].index(
                        r, this->elem_min, this->elem_max);
                }
                else
                {
                    this->pOrgsAft[level][a].index(
                        r, this->elem_min, this->elem_max);
                }
                firstfound = 1;
            }
            a++;
        }
        if (!(b%2))
        { //last bit of b is 0
            prevb = b;
            b--;
            if ((!firstfound) && b < a)
            {
                if (!lastfound)
                {
                    this->pOrgsIn[level][prevb].index(
                        r, this->elem_min, this->elem_max);
                }
                else
                {
                    this->pOrgsAft[level][prevb].index(
                        r, this->elem_min, this->elem_max);
                }
            }
            else
            {
                if (!lastfound)
                {
                    this->pRepsIn[level][prevb].index(
                        r, this->elem_min, this->elem_max);
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAft[level][prevb].index(
                        r, this->elem_min, this->elem_max);
                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


irHINTa::irHINTa(
    const Boundaries &boundaries,
    const int &numBits,
    const IRelation &R)
{
    this->elem_min           = boundaries.elem_min;
    this->elem_max           = boundaries.elem_max;
    this->time_start         = boundaries.time_start;
    this->time_end           = boundaries.time_end;

    if (this->elem_min != 0) 
        throw runtime_error("elem_min != 0: " + boundaries.str());

    //this->start              = R.gstart;
    //this->end                = R.gend - R.gstart;
    this->numBits            = numBits;
    this->maxBits            = int(log2(this->time_end - this->time_start)) + 1;
    this->height             = this->numBits + 1;

#ifdef CONSTRUCTION_TWO_PASSES
    // Step 1: one pass to count the contents inside each partition.
    this->pOrgsIn_lsizes  = new unordered_map<ElementId, ElementId>*[this->height];
    this->pOrgsAft_lsizes = new unordered_map<ElementId, ElementId>*[this->height];
    this->pRepsIn_lsizes  = new unordered_map<ElementId, ElementId>*[this->height];
    this->pRepsAft_lsizes = new unordered_map<ElementId, ElementId>*[this->height];

    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgsIn_lsizes[l]  = new unordered_map<ElementId, ElementId>[cnt];
        this->pOrgsAft_lsizes[l] = new unordered_map<ElementId, ElementId>[cnt];
        this->pRepsIn_lsizes[l]  = new unordered_map<ElementId, ElementId>[cnt];
        this->pRepsAft_lsizes[l] = new unordered_map<ElementId, ElementId>[cnt];
    }

    for (const IRecord &r : R)
        this->updateCounters(r);
#endif

    // Step 2: allocate necessary memory
    this->pOrgsIn  = new TemporalInvertedFile*[this->height];
    this->pOrgsAft = new TemporalInvertedFile*[this->height];
    this->pRepsIn  = new TemporalInvertedFile*[this->height];
    this->pRepsAft = new TemporalInvertedFile*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgsIn[l]  = new TemporalInvertedFile[cnt];
        this->pOrgsAft[l] = new TemporalInvertedFile[cnt];
        this->pRepsIn[l]  = new TemporalInvertedFile[cnt];
        this->pRepsAft[l] = new TemporalInvertedFile[cnt];

#ifdef CONSTRUCTION_TWO_PASSES
        // Note: TemporalInvertedFile uses unordered_map internally which grows dynamically
        // The size information in pOrgsIn_lsizes etc. is available but not currently used
        // Could be used in future if TemporalInvertedFile adds a reserve() method
#endif
    }
    
    // Step 3: fill partitions.
    for (const IRecord &r : R)
        this->updatePartitions(r);
    

#ifdef CONSTRUCTION_TWO_PASSES
    // Free auxiliary memory.
    for (auto l = 0; l < this->height; l++)
    {
        delete[] this->pOrgsIn_lsizes[l];
        delete[] this->pOrgsAft_lsizes[l];
        delete[] this->pRepsIn_lsizes[l];
        delete[] this->pRepsAft_lsizes[l];
    }
    delete[] this->pOrgsIn_lsizes;
    delete[] this->pOrgsAft_lsizes;
    delete[] this->pRepsIn_lsizes;
    delete[] this->pRepsAft_lsizes;
#endif
}


void irHINTa::getStats()
{

}


size_t irHINTa::getSize()
{
    size_t size = 0;
    
    // Member variables
    size += sizeof(ElementId);  // elem_min
    size += sizeof(ElementId);  // elem_max
    size += sizeof(Timestamp);  // time_start
    size += sizeof(Timestamp);  // time_end
    size += sizeof(unsigned int);  // maxBits
    size += sizeof(unsigned int);  // numBits
    size += sizeof(unsigned int);  // height
    
    // Four pointer arrays (each holds 'height' pointers)
    size += sizeof(TemporalInvertedFile**) * 4;  // pOrgsIn, pOrgsAft, pRepsIn, pRepsAft
    size += sizeof(TemporalInvertedFile*) * this->height * 4;  // Pointers at each level
    
    // TemporalInvertedFile arrays at each level and their contents
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        
        // Size of the TemporalInvertedFile arrays themselves
        size += sizeof(TemporalInvertedFile) * cnt * 4;  // Four arrays per level
        
        // Contents of each TemporalInvertedFile
        for (int pid = 0; pid < cnt; pid++)
        {
            size += this->pOrgsIn[l][pid].getSize();
            size += this->pOrgsAft[l][pid].getSize();
            size += this->pRepsIn[l][pid].getSize();
            size += this->pRepsAft[l][pid].getSize();
        }
    }
    
    return size;
}


irHINTa::~irHINTa()
{
    for (auto l = 0; l < this->height; l++)
    {
        delete[] this->pOrgsIn[l];
        delete[] this->pOrgsAft[l];
        delete[] this->pRepsIn[l];
        delete[] this->pRepsAft[l];
    }

    delete[] this->pOrgsIn;
    delete[] this->pOrgsAft;
    delete[] this->pRepsIn;
    delete[] this->pRepsAft;
}


void irHINTa::extractRecords(IRelation &R) const
{
    // Extract all records from all partitions
    R.clear();
    
    unordered_map<RecordId, IRecord> recordMap;
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits - l);
        
        for (int pid = 0; pid < cnt; pid++)
        {
            // Extract from all four partition types
            this->pOrgsIn[l][pid].extractRecords(recordMap, this->time_start);
            this->pOrgsAft[l][pid].extractRecords(recordMap, this->time_start);
            this->pRepsIn[l][pid].extractRecords(recordMap, this->time_start);
            this->pRepsAft[l][pid].extractRecords(recordMap, this->time_start);
        }
    }
    
    // Convert map to vector
    R.reserve(recordMap.size());
    for (auto& [id, record] : recordMap)
    {
        R.push_back(move(record));
    }
    
    // Sort by record ID (should already be sorted due to monotonic assumption, but ensure it)
    sort(R.begin(), R.end(), [](const IRecord &lhs, const IRecord &rhs)
        { return lhs.id < rhs.id; });
}


// Updating
void irHINTa::update(const IRelation &R)
{
    if (R.empty()) return;
    
    // Check if any records fall outside current temporal domain
    Timestamp new_min_time = this->time_start;
    Timestamp new_max_time = this->time_end;
    bool domain_changed = false;
    
    for (const auto& r : R)
    {
        if (r.start < new_min_time)
        {
            new_min_time = r.start;
            domain_changed = true;
        }
        if (r.end > new_max_time)
        {
            new_max_time = r.end;
            domain_changed = true;
        }
    }
    
    if (!domain_changed)
    {
        // Simple case: all new records within existing domain
        // Assumption: Update records have monotonically increasing IDs (higher than existing records)
        // The index() method appends records, maintaining sorted order with this assumption
        for (const IRecord &r : R)
            this->updatePartitions(r);
    }
    else
    {
        // Complex case: temporal domain expansion requires rebuild
        
        // Extract all existing records
        IRelation existingRecords;
        this->extractRecords(existingRecords);
        
        // Merge with new records (monotonic ID assumption: just append)
        IRelation mergedRecords;
        mergedRecords.reserve(existingRecords.size() + R.size());
        mergedRecords.insert(mergedRecords.end(), existingRecords.begin(), existingRecords.end());
        mergedRecords.insert(mergedRecords.end(), R.begin(), R.end());
        
        // No sorting needed: extractRecords returns sorted IDs, new IDs are monotonically increasing
        
        // Free old partitions
        for (auto l = 0; l < this->height; l++)
        {
            delete[] this->pOrgsIn[l];
            delete[] this->pOrgsAft[l];
            delete[] this->pRepsIn[l];
            delete[] this->pRepsAft[l];
        }
        delete[] this->pOrgsIn;
        delete[] this->pOrgsAft;
        delete[] this->pRepsIn;
        delete[] this->pRepsAft;
        
        // Update domain boundaries
        this->time_start = new_min_time;
        this->time_end = new_max_time;
        this->maxBits = int(log2(this->time_end - this->time_start)) + 1;
        this->height = this->numBits + 1;
        
        // Reallocate partitions with new domain
        this->pOrgsIn  = new TemporalInvertedFile*[this->height];
        this->pOrgsAft = new TemporalInvertedFile*[this->height];
        this->pRepsIn  = new TemporalInvertedFile*[this->height];
        this->pRepsAft = new TemporalInvertedFile*[this->height];
        
        for (auto l = 0; l < this->height; l++)
        {
            auto cnt = (int)(pow(2, this->numBits - l));
            
            this->pOrgsIn[l]  = new TemporalInvertedFile[cnt];
            this->pOrgsAft[l] = new TemporalInvertedFile[cnt];
            this->pRepsIn[l]  = new TemporalInvertedFile[cnt];
            this->pRepsAft[l] = new TemporalInvertedFile[cnt];
        }
        
        // Rebuild with merged records
        for (const IRecord &r : mergedRecords)
            this->updatePartitions(r);
    }
}


void irHINTa::remove(const RelationId &ids)
{
    // Build set of IDs to delete
    unordered_set<RecordId> idsToDelete(ids.begin(), ids.end());
    
    // Extract all existing records
    IRelation existingRecords;
    this->extractRecords(existingRecords);
    
    // Filter out records to delete
    existingRecords.erase(
        remove_if(existingRecords.begin(), existingRecords.end(),
            [&idsToDelete](const IRecord &rec) { return idsToDelete.count(rec.id) > 0; }),
        existingRecords.end());
    
    // If no records left, nothing to rebuild
    if (existingRecords.empty())
        return;
    
    // Recompute temporal domain
    Timestamp new_min_time = numeric_limits<Timestamp>::max();
    Timestamp new_max_time = numeric_limits<Timestamp>::min();
    for (const auto& r : existingRecords)
    {
        new_min_time = min(new_min_time, r.start);
        new_max_time = max(new_max_time, r.end);
    }
    
    // Free old partitions
    for (auto l = 0; l < this->height; l++)
    {
        delete[] this->pOrgsIn[l];
        delete[] this->pOrgsAft[l];
        delete[] this->pRepsIn[l];
        delete[] this->pRepsAft[l];
    }
    
    delete[] this->pOrgsIn;
    delete[] this->pOrgsAft;
    delete[] this->pRepsIn;
    delete[] this->pRepsAft;
    
    // Update temporal bounds
    this->time_start = new_min_time;
    this->time_end = new_max_time;
    
    // Reallocate partitions
    this->pOrgsIn  = new TemporalInvertedFile*[this->height];
    this->pOrgsAft = new TemporalInvertedFile*[this->height];
    this->pRepsIn  = new TemporalInvertedFile*[this->height];
    this->pRepsAft = new TemporalInvertedFile*[this->height];
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits - l));
        
        this->pOrgsIn[l]  = new TemporalInvertedFile[cnt];
        this->pOrgsAft[l] = new TemporalInvertedFile[cnt];
        this->pRepsIn[l]  = new TemporalInvertedFile[cnt];
        this->pRepsAft[l] = new TemporalInvertedFile[cnt];
    }
    
    // Rebuild with filtered records
    for (const IRecord &r : existingRecords)
        this->updatePartitions(r);
}


// Querying
inline void irHINTa::scanPartitionContainment_CheckBoth(TemporalInvertedFile &IF, const RangeIRQuery &q, RelationId &result)
{
    auto numQueryElements = q.elems.size();
    
    if (numQueryElements == 1)
    {
        IF.moveOut_CheckBoth(q, result);
    }
    else
    {
        RelationId candidates;
        candidates.reserve(128);
        
        if (!IF.moveOut_CheckBoth(q, candidates))
            return;
        
        for (auto i = 1; i < numQueryElements-1; i++)
        {
            if (!IF.intersect(q, i, candidates))
                return;
        }
        
        IF.intersectAndOutput(q, numQueryElements-1, candidates, result);
    }
}

inline void irHINTa::scanPartitionContainment_CheckStart(TemporalInvertedFile &IF, const RangeIRQuery &q, RelationId &result)
{
    auto numQueryElements = q.elems.size();
    
    if (numQueryElements == 1)
    {
        IF.moveOut_CheckStart(q, result);
    }
    else
    {
        RelationId candidates;
        candidates.reserve(128);
        
        if (!IF.moveOut_CheckStart(q, candidates))
            return;
        
        for (auto i = 1; i < numQueryElements-1; i++)
        {
            if (!IF.intersect(q, i, candidates))
                return;
        }
        
        IF.intersectAndOutput(q, numQueryElements-1, candidates, result);
    }
}

inline void irHINTa::scanPartitionContainment_CheckEnd(TemporalInvertedFile &IF, const RangeIRQuery &q, RelationId &result)
{
    auto numQueryElements = q.elems.size();
   
    if (numQueryElements == 1)
    {
        IF.moveOut_CheckEnd(q, result);
    }
    else
    {
        RelationId candidates;
        candidates.reserve(128);
        
        if (!IF.moveOut_CheckEnd(q, candidates))
            return;
        
        for (auto i = 1; i < numQueryElements-1; i++)
        {
            if (!IF.intersect(q, i, candidates))
                return;
        }
        
        IF.intersectAndOutput(q, numQueryElements-1, candidates, result);
    }
}

inline void irHINTa::scanPartitionContainment_NoChecks(TemporalInvertedFile &IF, const RangeIRQuery &q, RelationId &result)
{
    auto numQueryElements = q.elems.size();
   
    if (numQueryElements == 1)
    {
        IF.moveOut_NoChecks(q, result);
    }
    else
    {
        RelationId candidates;
        candidates.reserve(128);
        
        if (!IF.moveOut_NoChecks(q, candidates))
            return;
        
        for (auto i = 1; i < numQueryElements-1; i++)
        {
            if (!IF.intersect(q, i, candidates))
                return;
        }
        
        IF.intersectAndOutput(q, numQueryElements-1, candidates, result);
    }
}


void irHINTa::move_out(
    const RangeIRQuery &qo,
    ElementId &elem_off,
    RelationId &result)
{
    RangeIRQuery q(qo.id, qo.start, qo.end);

    if (elem_off == 0)
    {
        q.elems = qo.elems; // Share the same vector
    }
    else
    {
        q.elems.reserve(qo.elems.size() - elem_off);
        q.elems.insert(q.elems.end(), qo.elems.begin() + elem_off, qo.elems.end());
    }

    Timestamp a = q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = q.end   >> (this->maxBits-this->numBits); // prefix
    bool foundzero = false;
    bool foundone = false;

    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            this->scanPartitionContainment_NoChecks(this->pRepsIn[l][a], q, result);
            this->scanPartitionContainment_NoChecks(this->pRepsAft[l][a], q, result);

            // Handle rest: consider only originals
            for (auto i = a; i <= b; i++)
            {
                this->scanPartitionContainment_NoChecks(this->pOrgsIn[l][i], q, result);
                this->scanPartitionContainment_NoChecks(this->pOrgsAft[l][i], q, result);
            }
        }
        else
        {
            // Comparisons needed
            
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                this->scanPartitionContainment_CheckBoth(this->pOrgsIn[l][a], q, result);
                this->scanPartitionContainment_CheckStart(this->pOrgsAft[l][a], q, result);
            }

            else
            {
                // Lemma 1
                this->scanPartitionContainment_CheckEnd(this->pOrgsIn[l][a], q, result);
                this->scanPartitionContainment_NoChecks(this->pOrgsAft[l][a], q, result);
            }

            // Lemma 1, 3
            this->scanPartitionContainment_CheckEnd(this->pRepsIn[l][a], q, result);
            this->scanPartitionContainment_NoChecks(this->pRepsAft[l][a], q, result);

            if (a < b)
            {
                // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                for (auto i = a+1; i < b; i++)
                {
                    this->scanPartitionContainment_NoChecks(this->pOrgsIn[l][i], q, result);
                    this->scanPartitionContainment_NoChecks(this->pOrgsAft[l][i], q, result);
                }

                // Handle the partition that contains b: consider only originals, comparisons needed
                this->scanPartitionContainment_CheckStart(this->pOrgsIn[l][b], q, result);
                this->scanPartitionContainment_CheckStart(this->pOrgsAft[l][b], q, result);
            }

            if (b%2) //last bit of b is 1
                foundone = 1;
            if (!(a%2)) //last bit of a is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    
    // Handle root.
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        this->scanPartitionContainment_NoChecks(this->pOrgsIn[this->numBits][0], q, result);
    }
    else
    {
        // Comparisons needed
        this->scanPartitionContainment_CheckBoth(this->pOrgsIn[this->numBits][0], q, result);
    }

    elem_off = qo.elems.size();
}


inline void irHINTa::scanPartition_intersect(TemporalInvertedFile &IF, const RangeIRQuery &q, RelationId &candidates, RelationId &results)
{
    if (q.elems.size() == 1)
    {
        IF.intersectAndOutput(q, 0, candidates, results);

        return;
    }

    RelationId tmp;
    tmp.reserve(candidates.size());

    if (!IF.intersectAndOutput(q, 0, candidates, tmp)) return;


    for (auto i = 1; i < q.elems.size() - 1; i++)
    {
        if (!IF.intersect(q, i, tmp)) 
        {
            return;
        }
    }

    IF.intersectAndOutput(q, q.elems.size() - 1, tmp, results);
}


void irHINTa::refine(
    const RangeIRQuery &qo,
    ElementId &elem_off,
    RelationId &candidates)
{
    sort(candidates.begin(), candidates.end());

    RangeIRQuery q(qo.id, qo.start, qo.end);
    
    if (elem_off == 0)
    {
        q.elems = qo.elems;
    }
    else
    {
        q.elems.reserve(qo.elems.size() - elem_off);
        q.elems.insert(q.elems.end(), qo.elems.begin() + elem_off, qo.elems.end());
    }

    RelationId results;
    results.reserve(candidates.size() / 4); // Estimate result size

    Timestamp a = q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = q.end   >> (this->maxBits-this->numBits); // prefix
    bool foundzero = false;
    bool foundone = false;

    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            this->scanPartition_intersect(this->pRepsIn[l][a], q, candidates, results);
            this->scanPartition_intersect(this->pRepsAft[l][a], q, candidates, results);

            // Handle rest: consider only originals
            for (auto i = a; i <= b; i++)
            {
                this->scanPartition_intersect(this->pOrgsIn[l][i], q, candidates, results);
                this->scanPartition_intersect(this->pOrgsAft[l][i], q, candidates, results);
            }
        }
        else
        {
            // Comparisons needed
            
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed

            this->scanPartition_intersect(this->pOrgsIn[l][a], q, candidates, results);
            this->scanPartition_intersect(this->pOrgsAft[l][a], q, candidates, results);
            

            // Lemma 1, 3
            this->scanPartition_intersect(this->pRepsIn[l][a], q, candidates, results);
            this->scanPartition_intersect(this->pRepsAft[l][a], q, candidates, results);

            if (a < b)
            {
                // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                for (auto i = a+1; i < b; i++)
                {
                    this->scanPartition_intersect(this->pOrgsIn[l][i], q, candidates, results);
                    this->scanPartition_intersect(this->pOrgsAft[l][i], q, candidates, results);
                }

                // Handle the partition that contains b: consider only originals, comparisons needed
                this->scanPartition_intersect(this->pOrgsIn[l][b], q, candidates, results);
                this->scanPartition_intersect(this->pOrgsAft[l][b], q, candidates, results);
            }

            if (b%2) //last bit of b is 1
                foundone = 1;
            if (!(a%2)) //last bit of a is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root.
    this->scanPartition_intersect(this->pOrgsIn[this->numBits][0], q, candidates, results);

    candidates.swap(results);

    // this sub-index is always the last one
    elem_off = qo.elems.size();
}