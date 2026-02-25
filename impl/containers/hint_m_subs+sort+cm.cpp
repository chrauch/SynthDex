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

#include "hint_m.h"



inline void mergeSort(RelationIdIterator iterPBegin, RelationIdIterator iterPEnd, RelationId &candidates, RelationId &result)
{
    auto iterP = iterPBegin;
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


inline void mergeSort(RelationIdIterator iterPBegin, RelationIdIterator iterPEnd, RelationId &candidates, vector<RelationId> &vec_result)
{
    auto iterP = iterPBegin;
    auto iterC    = candidates.begin();
    auto iterCEnd = candidates.end();
    RelationId tmp;

    while ((iterP != iterPEnd) && (iterC != iterCEnd))
    {
        if (*iterP < *iterC)
            iterP++;
        else if (*iterP > *iterC)
            iterC++;
        else
        {
            tmp.push_back(*iterP);
            iterP++;
            
            iterC++;
        }
    }
    
    if (!tmp.empty())
        vec_result.push_back(tmp);
}



inline void HINT_M_SubsSortByRecordId_CM::updateCounters(const Record &r)
{
    int level = 0;
    Timestamp a = (max(r.start,this->gstart)-this->gstart) >> (this->maxBits-this->numBits);
    Timestamp b = (min(r.end,this->gend)-this->gstart)     >> (this->maxBits-this->numBits);
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
                    this->pRepsIn_sizes[level][a]++;
                    lastfound = 1;
                }
                else
                    this->pRepsAft_sizes[level][a]++;
            }
            else
            {
                if ((a == b) && (!lastfound))
                    this->pOrgsIn_sizes[level][a]++;
                else
                    this->pOrgsAft_sizes[level][a]++;
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
                    this->pOrgsIn_sizes[level][prevb]++;
                else
                    this->pOrgsAft_sizes[level][prevb]++;
            }
            else
            {
                if (!lastfound)
                {
                    this->pRepsIn_sizes[level][prevb]++;
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAft_sizes[level][prevb]++;
                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


inline void HINT_M_SubsSortByRecordId_CM::updatePartitions(const Record &r)
{
    int level = 0;
    Timestamp a = (max(r.start,this->gstart)-this->gstart) >> (this->maxBits-this->numBits);
    Timestamp b = (min(r.end,this->gend)-this->gstart)     >> (this->maxBits-this->numBits);
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
                    this->pRepsInTmp[level][a].push_back(r);
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAftTmp[level][a].push_back(r);
                }
            }
            else
            {
                if ((a == b) && (!lastfound))
                {
                    this->pOrgsInTmp[level][a].push_back(r);
                }
                else
                {
                    this->pOrgsAftTmp[level][a].push_back(r);
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
                    this->pOrgsInTmp[level][prevb].push_back(r);
                }
                else
                {
                    this->pOrgsAftTmp[level][prevb].push_back(r);
                }
            }
            else
            {
                if (!lastfound)
                {
                    this->pRepsInTmp[level][prevb].push_back(r);
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAftTmp[level][prevb].push_back(r);
                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


HINT_M_SubsSortByRecordId_CM::HINT_M_SubsSortByRecordId_CM(const Relation &R, const unsigned int numBits = 0)  : HierarchicalIndex(R, numBits)
{
    // Step 1: one pass to count the contents inside each partition.
    this->pOrgsIn_sizes  = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pOrgsAft_sizes = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pRepsIn_sizes  = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pRepsAft_sizes = (size_t **)malloc(this->height*sizeof(size_t *));
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        //calloc allocates memory and sets each counter to 0
        this->pOrgsIn_sizes[l]  = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pOrgsAft_sizes[l] = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pRepsIn_sizes[l]  = (size_t *)calloc(cnt, sizeof(size_t));
        this->pRepsAft_sizes[l] = (size_t *)calloc(cnt, sizeof(size_t));
    }
    
    for (const Record &r : R)
        this->updateCounters(r);
    
    // Step 2: allocate necessary memory.
    this->pOrgsInTmp  = new Relation*[this->height];
    this->pOrgsAftTmp = new Relation*[this->height];
    this->pRepsInTmp  = new Relation*[this->height];
    this->pRepsAftTmp = new Relation*[this->height];
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgsInTmp[l]  = new Relation[cnt];
        this->pOrgsAftTmp[l] = new Relation[cnt];
        this->pRepsInTmp[l]  = new Relation[cnt];
        this->pRepsAftTmp[l] = new Relation[cnt];
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            this->pOrgsInTmp[l][pId].reserve(this->pOrgsIn_sizes[l][pId]);
            this->pOrgsAftTmp[l][pId].reserve(this->pOrgsAft_sizes[l][pId]);
            this->pRepsInTmp[l][pId].reserve(this->pRepsIn_sizes[l][pId]);
            this->pRepsAftTmp[l][pId].reserve(this->pRepsAft_sizes[l][pId]);
        }
    }
    
    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);
    
    // Free auxiliary memory.
    for (auto l = 0; l < this->height; l++)
    {
        free(this->pOrgsIn_sizes[l]);
        free(this->pOrgsAft_sizes[l]);
        free(this->pRepsIn_sizes[l]);
        free(this->pRepsAft_sizes[l]);
    }
    free(this->pOrgsIn_sizes);
    free(this->pOrgsAft_sizes);
    free(this->pRepsIn_sizes);
    free(this->pRepsAft_sizes);
    
    // Copy and free auxiliary memory.
    this->pOrgsInIds  = new RelationId*[this->height];
    this->pOrgsAftIds = new RelationId*[this->height];
    this->pRepsInIds  = new RelationId*[this->height];
    this->pRepsAftIds = new RelationId*[this->height];
    this->pOrgsInTimestamps  = new vector<pair<Timestamp, Timestamp> >*[this->height];
    this->pOrgsAftTimestamps = new vector<pair<Timestamp, Timestamp> >*[this->height];
    this->pRepsInTimestamps  = new vector<pair<Timestamp, Timestamp> >*[this->height];
    this->pRepsAftTimestamps = new vector<pair<Timestamp, Timestamp> >*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgsInIds[l]  = new RelationId[cnt];
        this->pOrgsAftIds[l] = new RelationId[cnt];
        this->pRepsInIds[l]  = new RelationId[cnt];
        this->pRepsAftIds[l] = new RelationId[cnt];
        this->pOrgsInTimestamps[l]  = new vector<pair<Timestamp, Timestamp> >[cnt];
        this->pOrgsAftTimestamps[l] = new vector<pair<Timestamp, Timestamp> >[cnt];
        this->pRepsInTimestamps[l]  = new vector<pair<Timestamp, Timestamp> >[cnt];
        this->pRepsAftTimestamps[l] = new vector<pair<Timestamp, Timestamp> >[cnt];
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            auto cnt = this->pOrgsInTmp[l][pId].size();
            this->pOrgsInIds[l][pId].resize(cnt);
            this->pOrgsInTimestamps[l][pId].resize(cnt);
            for (auto j = 0; j < cnt; j++)
            {
                this->pOrgsInIds[l][pId][j] = this->pOrgsInTmp[l][pId][j].id;
                this->pOrgsInTimestamps[l][pId][j].first = this->pOrgsInTmp[l][pId][j].start;
                this->pOrgsInTimestamps[l][pId][j].second = this->pOrgsInTmp[l][pId][j].end;
            }
            
            cnt = this->pOrgsAftTmp[l][pId].size();
            this->pOrgsAftIds[l][pId].resize(cnt);
            this->pOrgsAftTimestamps[l][pId].resize(cnt);
            for (auto j = 0; j < cnt; j++)
            {
                this->pOrgsAftIds[l][pId][j] = this->pOrgsAftTmp[l][pId][j].id;
                this->pOrgsAftTimestamps[l][pId][j].first = this->pOrgsAftTmp[l][pId][j].start;
                this->pOrgsAftTimestamps[l][pId][j].second = this->pOrgsAftTmp[l][pId][j].end;
            }
            
            cnt = this->pRepsInTmp[l][pId].size();
            this->pRepsInIds[l][pId].resize(cnt);
            this->pRepsInTimestamps[l][pId].resize(cnt);
            for (auto j = 0; j < cnt; j++)
            {
                this->pRepsInIds[l][pId][j] = this->pRepsInTmp[l][pId][j].id;
                this->pRepsInTimestamps[l][pId][j].first = this->pRepsInTmp[l][pId][j].start;
                this->pRepsInTimestamps[l][pId][j].second = this->pRepsInTmp[l][pId][j].end;
            }

            cnt = this->pRepsAftTmp[l][pId].size();
            this->pRepsAftIds[l][pId].resize(cnt);
            this->pRepsAftTimestamps[l][pId].resize(cnt);
            for (auto j = 0; j < cnt; j++)
            {
                this->pRepsAftIds[l][pId][j] = this->pRepsAftTmp[l][pId][j].id;
                this->pRepsAftTimestamps[l][pId][j].first = this->pRepsAftTmp[l][pId][j].start;
                this->pRepsAftTimestamps[l][pId][j].second = this->pRepsAftTmp[l][pId][j].end;
            }
        }
        
        delete[] this->pOrgsInTmp[l];
        delete[] this->pOrgsAftTmp[l];
        delete[] this->pRepsInTmp[l];
        delete[] this->pRepsAftTmp[l];
    }
    delete[] this->pOrgsInTmp;
    delete[] this->pOrgsAftTmp;
    delete[] this->pRepsInTmp;
    delete[] this->pRepsAftTmp;
}


void HINT_M_SubsSortByRecordId_CM::getStats()
{
//    size_t sum = 0;
//    for (auto l = 0; l < this->height; l++)
//    {
//        auto cnt = pow(2, this->numBits-l);
//
//        this->numPartitions += cnt;
//        for (int pid = 0; pid < cnt; pid++)
//        {
//            this->numOriginalsIn  += this->pOrgsInIds[l][pid].size();
//            this->numOriginalsAft += this->pOrgsAftIds[l][pid].size();
//            this->numReplicasIn   += this->pRepsInIds[l][pid].size();
//            this->numReplicasAft  += this->pRepsAftIds[l][pid].size();
//            if ((this->pOrgsInIds[l][pid].empty()) && (this->pOrgsAftIds[l][pid].empty()) && (this->pRepsInIds[l][pid].empty()) && (this->pRepsAftIds[l][pid].empty()))
//                this->numEmptyPartitions++;
//        }
//    }
//
//    this->avgPartitionSize = (float)(this->numIndexedRecords+this->numReplicasIn+this->numReplicasAft)/(this->numPartitions-numEmptyPartitions);
}


size_t HINT_M_SubsSortByRecordId_CM::getSize()
{
    size_t size = 0;
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        
        for (int pid = 0; pid < cnt; pid++)
        {
            size += this->pOrgsInIds[l][pid].size()  * (sizeof(RecordId) + 2*sizeof(Timestamp));
            size += this->pOrgsAftIds[l][pid].size() * (sizeof(RecordId) + 2*sizeof(Timestamp));
            size += this->pRepsInIds[l][pid].size()  * (sizeof(RecordId) + 2*sizeof(Timestamp));
            size += this->pRepsAftIds[l][pid].size() * (sizeof(RecordId) + 2*sizeof(Timestamp));
        }
    }
    
    return size;
}


HINT_M_SubsSortByRecordId_CM::~HINT_M_SubsSortByRecordId_CM()
{
    for (auto l = 0; l < this->height; l++)
    {
        delete[] this->pOrgsInIds[l];
        delete[] this->pOrgsInTimestamps[l];
        delete[] this->pOrgsAftIds[l];
        delete[] this->pOrgsAftTimestamps[l];
        delete[] this->pRepsInIds[l];
        delete[] this->pRepsInTimestamps[l];
        delete[] this->pRepsAftIds[l];
        delete[] this->pRepsAftTimestamps[l];
    }
    
    delete[] this->pOrgsInIds;
    delete[] this->pOrgsInTimestamps;
    delete[] this->pOrgsAftIds;
    delete[] this->pOrgsAftTimestamps;
    delete[] this->pRepsInIds;
    delete[] this->pRepsInTimestamps;
    delete[] this->pRepsAftIds;
    delete[] this->pRepsAftTimestamps;
}


void HINT_M_SubsSortByRecordId_CM::extractRecords(Relation &R) const
{
    // Extract all unique records from the HINT structure
    // Use a map to deduplicate since records can appear in multiple partitions
    unordered_map<RecordId, pair<Timestamp, Timestamp>> uniqueRecs;
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        for (int pid = 0; pid < cnt; pid++)
        {
            // Extract from OrgsIn
            for (size_t i = 0; i < this->pOrgsInIds[l][pid].size(); i++)
            {
                uniqueRecs[this->pOrgsInIds[l][pid][i]] = this->pOrgsInTimestamps[l][pid][i];
            }
            // Extract from OrgsAft
            for (size_t i = 0; i < this->pOrgsAftIds[l][pid].size(); i++)
            {
                uniqueRecs[this->pOrgsAftIds[l][pid][i]] = this->pOrgsAftTimestamps[l][pid][i];
            }
            // Extract from RepsIn
            for (size_t i = 0; i < this->pRepsInIds[l][pid].size(); i++)
            {
                uniqueRecs[this->pRepsInIds[l][pid][i]] = this->pRepsInTimestamps[l][pid][i];
            }
            // Extract from RepsAft
            for (size_t i = 0; i < this->pRepsAftIds[l][pid].size(); i++)
            {
                uniqueRecs[this->pRepsAftIds[l][pid][i]] = this->pRepsAftTimestamps[l][pid][i];
            }
        }
    }
    
    // Transfer to output relation
    R.clear();
    R.reserve(uniqueRecs.size());
    for (const auto &pair : uniqueRecs)
    {
        R.emplace_back(pair.first, pair.second.first, pair.second.second);
    }
    
    // Deduplicate and sort by record ID (map iteration is unordered)
    sort(R.begin(), R.end(), compareRecordsById);
    
    // Update domain
    R.gstart = this->gstart;
    R.gend = this->gend;
}


// Updating
void HINT_M_SubsSortByRecordId_CM::insert(const Record &r)
{
    int level = 0;
    Timestamp a = (max(r.start,this->gstart)-this->gstart) >> (this->maxBits-this->numBits);
    Timestamp b = (min(r.end,this->gend)-this->gstart)     >> (this->maxBits-this->numBits);
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
                    this->pRepsInIds[level][a].push_back(r.id);
                    this->pRepsInTimestamps[level][a].push_back(make_pair(r.start, r.end));
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAftIds[level][a].push_back(r.id);
                    this->pRepsAftTimestamps[level][a].push_back(make_pair(r.start, r.end));
                }
            }
            else
            {
                if ((a == b) && (!lastfound))
                {
                    this->pOrgsInIds[level][a].push_back(r.id);
                    this->pOrgsInTimestamps[level][a].push_back(make_pair(r.start, r.end));
                }
                else
                {
                    this->pOrgsAftIds[level][a].push_back(r.id);
                    this->pOrgsAftTimestamps[level][a].push_back(make_pair(r.start, r.end));
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
                    this->pOrgsInIds[level][prevb].push_back(r.id);
                    this->pOrgsInTimestamps[level][prevb].push_back(make_pair(r.start, r.end));

                }
                else
                {
                    this->pOrgsAftIds[level][prevb].push_back(r.id);
                    this->pOrgsAftTimestamps[level][prevb].push_back(make_pair(r.start, r.end));
                }
            }
            else
            {
                if (!lastfound)
                {
                    this->pRepsInIds[level][prevb].push_back(r.id);
                    this->pRepsInTimestamps[level][prevb].push_back(make_pair(r.start, r.end));
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAftIds[level][prevb].push_back(r.id);
                    this->pRepsAftTimestamps[level][prevb].push_back(make_pair(r.start, r.end));
                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}



// Querying
inline void HINT_M_SubsSortByRecordId_CM::scanPartition_CheckBoth_moveOut(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, const RangeQuery &q, RelationId &result)
{
    auto iterI = ids[level][t].begin();
    auto iterBegin = timestamps[level][t].begin();
    auto iterEnd = timestamps[level][t].end();
    
    for (auto iter = iterBegin; iter != iterEnd; iter++)
    {
        if ((iter->first <= q.end) && (q.start <= iter->second))
            result.push_back(*iterI);
        iterI++;
    }
}


inline void HINT_M_SubsSortByRecordId_CM::scanPartition_CheckStart_moveOut(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, const RangeQuery &q, RelationId &result)
{
    auto iterI = ids[level][t].begin();
    auto iterBegin = timestamps[level][t].begin();
    auto iterEnd = timestamps[level][t].end();
    
    for (auto iter = iterBegin; iter != iterEnd; iter++)
    {
        if (iter->first <= q.end)
            result.push_back(*iterI);
        iterI++;
    }
}


inline void HINT_M_SubsSortByRecordId_CM::scanPartition_CheckEnd_moveOut(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, const RangeQuery &q, RelationId &result)
{
    auto iterI = ids[level][t].begin();
    auto iterBegin = timestamps[level][t].begin();
    auto iterEnd = timestamps[level][t].end();
    
    for (auto iter = iterBegin; iter != iterEnd; iter++)
    {
        if (q.start <= iter->second)
            result.push_back(*iterI);
        iterI++;
    }
}


inline void HINT_M_SubsSortByRecordId_CM::scanPartition_NoChecks_moveOut(unsigned int level, Timestamp t, RelationId **ids, RelationId &result)
{
    auto iterIBegin = ids[level][t].begin();
    auto iterIEnd = ids[level][t].end();

    result.insert(result.end(), iterIBegin, iterIEnd);
}


inline void HINT_M_SubsSortByRecordId_CM::scanPartitions_NoChecks_moveOut(unsigned int level, Timestamp ts, Timestamp te, RelationId **ids, RelationId &result)
{
    for (auto j = ts; j <= te; j++)
    {
        auto iterIBegin = ids[level][j].begin();
        auto iterIEnd = ids[level][j].end();

        result.insert(result.end(), iterIBegin, iterIEnd);
    }
}


void HINT_M_SubsSortByRecordId_CM::moveOut(const RangeQuery &q, RelationId &candidates)
{
    if (q.start > this->gend || q.end < this->gstart)
        return;

    Timestamp a = (max(q.start,this->gstart)-this->gstart) >> (this->maxBits-this->numBits);
    Timestamp b = (min(q.end,this->gend)-this->gstart)     >> (this->maxBits-this->numBits);
    bool foundzero = false;
    bool foundone = false;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            this->scanPartition_NoChecks_moveOut(l, a, this->pRepsInIds, candidates);
            this->scanPartition_NoChecks_moveOut(l, a, this->pRepsAftIds, candidates);
            
            // Handle rest: consider only originals
            this->scanPartitions_NoChecks_moveOut(l, a, b, this->pOrgsInIds, candidates);
            this->scanPartitions_NoChecks_moveOut(l, a, b, this->pOrgsAftIds, candidates);
        }
        else
        {
            // Comparisons needed
            
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                this->scanPartition_CheckBoth_moveOut(l, a, this->pOrgsInIds, this->pOrgsInTimestamps, q, candidates);
                this->scanPartition_CheckStart_moveOut(l, a, this->pOrgsAftIds, this->pOrgsAftTimestamps, q, candidates);
            }
            else
            {
                // Lemma 1
                this->scanPartition_CheckEnd_moveOut(l, a, this->pOrgsInIds, this->pOrgsInTimestamps, q, candidates);
                this->scanPartition_NoChecks_moveOut(l, a, this->pOrgsAftIds, candidates);
            }

            // Lemma 1, 3
            this->scanPartition_CheckEnd_moveOut(l, a, this->pRepsInIds, this->pRepsInTimestamps, q, candidates);
            this->scanPartition_NoChecks_moveOut(l, a, this->pRepsAftIds, candidates);

            if (a < b)
            {
                // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                this->scanPartitions_NoChecks_moveOut(l, a+1, b-1, this->pOrgsInIds, candidates);
                this->scanPartitions_NoChecks_moveOut(l, a+1, b-1, this->pOrgsAftIds, candidates);

                // Handle the partition that contains b: consider only originals, comparisons needed
                this->scanPartition_CheckStart_moveOut(l, b, this->pOrgsInIds, this->pOrgsInTimestamps, q, candidates);
                this->scanPartition_CheckStart_moveOut(l, b, this->pOrgsAftIds, this->pOrgsAftTimestamps, q, candidates);
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
        auto iterIBegin = this->pOrgsInIds[this->numBits][0].begin();
        auto iterIEnd = this->pOrgsInIds[this->numBits][0].end();
        
//        for (auto iterI = iterIBegin; iterI != iterIEnd; iterI++)
//            candidates.push_back(*iterI);
        candidates.insert(candidates.end(), iterIBegin, iterIEnd);
    }
    else
    {
        // Comparisons needed
        auto iterI = this->pOrgsInIds[this->numBits][0].begin();
        auto iterBegin = this->pOrgsInTimestamps[this->numBits][0].begin();
        auto iterEnd = this->pOrgsInTimestamps[this->numBits][0].end();
        for (auto iter = iterBegin; iter != iterEnd; iter++)
        {
            if ((iter->first <= q.end) && (q.start <= iter->second))
                candidates.push_back(*iterI);
            iterI++;
        }
    }
}

inline void HINT_M_SubsSortByRecordId_CM::scanPartition_NoChecks_intersect(unsigned int level, Timestamp t, RelationId **ids, RelationId &candidates, RelationId &result)
{
    auto iterIBegin = ids[level][t].begin();
    auto iterIEnd = ids[level][t].end();

    if (iterIBegin != iterIEnd)
        mergeSort(iterIBegin, iterIEnd, candidates, result);
}


void HINT_M_SubsSortByRecordId_CM::intersect(const RangeQuery &q, RelationId &candidates)
{
    if (q.start > this->gend || q.end < this->gstart)
    {
        candidates.clear();
        return;
    }

    RelationId result;
    RelationIdIterator iterIBegin, iterIEnd;
    Timestamp a = (max(q.start,this->gstart)-this->gstart) >> (this->maxBits-this->numBits);
    Timestamp b = (min(q.end,this->gend)-this->gstart)     >> (this->maxBits-this->numBits);
    bool foundzero = false;
    bool foundone = false;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        // Handle the partition that contains a: consider both originals and replicas
        this->scanPartition_NoChecks_intersect(l, a, this->pRepsInIds, candidates, result);
        this->scanPartition_NoChecks_intersect(l, a, this->pRepsAftIds, candidates, result);

        // Handle rest: consider only originals
        for (auto i = a; i <= b; i++)
        {
            this->scanPartition_NoChecks_intersect(l, i, this->pOrgsInIds, candidates, result);
            this->scanPartition_NoChecks_intersect(l, i, this->pOrgsAftIds, candidates, result);
        }

        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root.
    iterIBegin = this->pOrgsInIds[this->numBits][0].begin();
    iterIEnd = this->pOrgsInIds[this->numBits][0].end();
    if (iterIBegin != iterIEnd)
        mergeSort(iterIBegin, iterIEnd, candidates, result);
    
    candidates.swap(result);
}


void HINT_M_SubsSortByRecordId_CM::intersectAndOutput(const RangeQuery &q, RelationId &candidates, RelationId &result)
{
    if (q.start > this->gend || q.end < this->gstart)
    {
        result.clear();
        return;
    }

    RelationIdIterator iterIBegin, iterIEnd;
    Timestamp a = (max(q.start,this->gstart)-this->gstart) >> (this->maxBits-this->numBits);
    Timestamp b = (min(q.end,this->gend)-this->gstart)     >> (this->maxBits-this->numBits);
    bool foundzero = false;
    bool foundone = false;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        // Handle the partition that contains a: consider both originals and replicas
        this->scanPartition_NoChecks_intersect(l, a, this->pRepsInIds, candidates, result);
        this->scanPartition_NoChecks_intersect(l, a, this->pRepsAftIds, candidates, result);
        
        // Handle rest: consider only originals
        for (auto i = a; i <= b; i++)
        {
            this->scanPartition_NoChecks_intersect(l, i, this->pOrgsInIds, candidates, result);
            this->scanPartition_NoChecks_intersect(l, i, this->pOrgsAftIds, candidates, result);
        }
        
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root.
    iterIBegin = this->pOrgsInIds[this->numBits][0].begin();
    iterIEnd = this->pOrgsInIds[this->numBits][0].end();
    if (iterIBegin != iterIEnd)
        mergeSort(iterIBegin, iterIEnd, candidates, result);
}



// M-way
inline void HINT_M_SubsSortByRecordId_CM::scanPartition_CheckBoth_moveOut(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, const RangeQuery &q, vector<RelationId> &vec_result)
{
    auto iterI = ids[level][t].begin();
    auto iterBegin = timestamps[level][t].begin();
    auto iterEnd = timestamps[level][t].end();
    RelationId tmp;
    
    for (auto iter = iterBegin; iter != iterEnd; iter++)
    {
        if ((iter->first <= q.end) && (q.start <= iter->second))
            tmp.push_back(*iterI);
        iterI++;
    }
    
    if (!tmp.empty())
        vec_result.push_back(tmp);
}


inline void HINT_M_SubsSortByRecordId_CM::scanPartition_CheckEnd_moveOut(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, const RangeQuery &q, vector<RelationId> &vec_result)
{
    auto iterI = ids[level][t].begin();
    auto iterBegin = timestamps[level][t].begin();
    auto iterEnd = timestamps[level][t].end();
    RelationId tmp;

    for (auto iter = iterBegin; iter != iterEnd; iter++)
    {
        if (q.start <= iter->second)
            tmp.push_back(*iterI);
        iterI++;
    }
    
    if (!tmp.empty())
        vec_result.push_back(tmp);
}


inline void HINT_M_SubsSortByRecordId_CM::scanPartition_CheckStart_moveOut(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, const RangeQuery &q, vector<RelationId> &vec_result)
{
    auto iterI = ids[level][t].begin();
    auto iterBegin = timestamps[level][t].begin();
    auto iterEnd = timestamps[level][t].end();
    RelationId tmp;
    
    for (auto iter = iterBegin; iter != iterEnd; iter++)
    {
        if (iter->first <= q.end)
            tmp.push_back(*iterI);
        iterI++;
    }
    
    if (!tmp.empty())
        vec_result.push_back(tmp);
}


inline void HINT_M_SubsSortByRecordId_CM::scanPartition_NoChecks_moveOut(unsigned int level, Timestamp t, RelationId **ids, vector<RelationId> &vec_result)
{
    auto iterIBegin = ids[level][t].begin();
    auto iterIEnd = ids[level][t].end();

    if (iterIBegin != iterIEnd)
        vec_result.push_back(RelationId(iterIBegin, iterIEnd));
}


inline void HINT_M_SubsSortByRecordId_CM::scanPartitions_NoChecks_moveOut(unsigned int level, Timestamp ts, Timestamp te, RelationId **ids, vector<RelationId> &vec_result)
{
    for (auto j = ts; j <= te; j++)
    {
        auto iterIBegin = ids[level][j].begin();
        auto iterIEnd = ids[level][j].end();

        if (iterIBegin != iterIEnd)
            vec_result.push_back(RelationId(iterIBegin, iterIEnd));
    }
}


void HINT_M_SubsSortByRecordId_CM::moveOut(const RangeQuery &q, vector<RelationId> &vec_candidates)
{
    if (q.start > this->gend || q.end < this->gstart)
        return;

    RelationIdIterator iterI, iterIBegin, iterIEnd;
    Timestamp a = (max(q.start,this->gstart)-this->gstart) >> (this->maxBits-this->numBits);
    Timestamp b = (min(q.end,this->gend)-this->gstart)     >> (this->maxBits-this->numBits);
    bool foundzero = false;
    bool foundone = false;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            this->scanPartition_NoChecks_moveOut(l, a, this->pRepsInIds, vec_candidates);
            this->scanPartition_NoChecks_moveOut(l, a, this->pRepsAftIds, vec_candidates);
            
            // Handle rest: consider only originals
            this->scanPartitions_NoChecks_moveOut(l, a, b, this->pOrgsInIds, vec_candidates);
            this->scanPartitions_NoChecks_moveOut(l, a, b, this->pOrgsAftIds, vec_candidates);
        }
        else
        {
            // Comparisons needed
            
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                this->scanPartition_CheckBoth_moveOut(l, a, this->pOrgsInIds, this->pOrgsInTimestamps, q, vec_candidates);
                this->scanPartition_CheckStart_moveOut(l, a, this->pOrgsAftIds, this->pOrgsAftTimestamps, q, vec_candidates);
            }

            else
            {
                // Lemma 1
                this->scanPartition_CheckEnd_moveOut(l, a, this->pOrgsInIds, this->pOrgsInTimestamps, q, vec_candidates);
                this->scanPartition_NoChecks_moveOut(l, a, this->pOrgsAftIds, vec_candidates);
            }

            // Lemma 1, 3
            this->scanPartition_CheckEnd_moveOut(l, a, this->pRepsInIds, this->pRepsInTimestamps, q, vec_candidates);
            this->scanPartition_NoChecks_moveOut(l, a, this->pRepsAftIds, vec_candidates);

            if (a < b)
            {
                // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                this->scanPartitions_NoChecks_moveOut(l, a+1, b-1, this->pOrgsInIds, vec_candidates);
                this->scanPartitions_NoChecks_moveOut(l, a+1, b-1, this->pOrgsAftIds, vec_candidates);

                // Handle the partition that contains b: consider only originals, comparisons needed
                this->scanPartition_CheckStart_moveOut(l, b, this->pOrgsInIds, this->pOrgsInTimestamps, q, vec_candidates);
                this->scanPartition_CheckStart_moveOut(l, b, this->pOrgsAftIds, this->pOrgsAftTimestamps, q, vec_candidates);
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
        iterIBegin = this->pOrgsInIds[this->numBits][0].begin();
        iterIEnd = this->pOrgsInIds[this->numBits][0].end();
        if (iterIBegin != iterIEnd)
            vec_candidates.push_back(RelationId(iterIBegin, iterIEnd));
    }
    else
    {
        // Comparisons needed
        RelationId tmp;
        auto iterI = this->pOrgsInIds[this->numBits][0].begin();
        auto iterBegin = this->pOrgsInTimestamps[this->numBits][0].begin();
        auto iterEnd = this->pOrgsInTimestamps[this->numBits][0].end();
        for (auto iter = iterBegin; iter != iterEnd; iter++)
        {
            if ((iter->first <= q.end) && (q.start <= iter->second))
                tmp.push_back(*iterI);
            iterI++;
        }

        if (!tmp.empty())
            vec_candidates.push_back(tmp);
    }
}


void HINT_M_SubsSortByRecordId_CM::moveOut_NoChecks(const RangeQuery &q, vector<RelationId> &vec_candidates)
{
    if (q.start > this->gend || q.end < this->gstart) return;

    RelationIdIterator iterI, iterIBegin, iterIEnd;
    Timestamp a = (max(q.start,this->gstart)-this->gstart) >> (this->maxBits-this->numBits);
    Timestamp b = (min(q.end,this->gend)-this->gstart)     >> (this->maxBits-this->numBits);
    bool foundzero = false;
    bool foundone = false;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        // Handle the partition that contains a: consider both originals and replicas
        this->scanPartition_NoChecks_moveOut(l, a, this->pRepsInIds, vec_candidates);
        this->scanPartition_NoChecks_moveOut(l, a, this->pRepsAftIds, vec_candidates);
        
        // Handle rest: consider only originals
        this->scanPartitions_NoChecks_moveOut(l, a, b, this->pOrgsInIds, vec_candidates);
        this->scanPartitions_NoChecks_moveOut(l, a, b, this->pOrgsAftIds, vec_candidates);

        
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root.
    iterIBegin = this->pOrgsInIds[this->numBits][0].begin();
    iterIEnd = this->pOrgsInIds[this->numBits][0].end();
    if (iterIBegin != iterIEnd)
        vec_candidates.push_back(RelationId(iterIBegin, iterIEnd));
}


inline void HINT_M_SubsSortByRecordId_CM::scanPartition_NoChecks_intersect(unsigned int level, Timestamp t, RelationId **ids, RelationId &candidates, vector<RelationId> &vec_result)
{
    auto iterIBegin = ids[level][t].begin();
    auto iterIEnd = ids[level][t].end();

    if (iterIBegin != iterIEnd)
        mergeSort(iterIBegin, iterIEnd, candidates, vec_result);
}


void HINT_M_SubsSortByRecordId_CM::intersect(const RangeQuery &q, RelationId &candidates, vector<RelationId> &vec_candidates)
{
    if (q.start > this->gend || q.end < this->gstart)
    {
        candidates.clear();
        return;
    }

    RelationId result;
    RelationIdIterator iterIBegin, iterIEnd;
    Timestamp a = (max(q.start,this->gstart)-this->gstart) >> (this->maxBits-this->numBits);
    Timestamp b = (min(q.end,this->gend)-this->gstart)     >> (this->maxBits-this->numBits);
    bool foundzero = false;
    bool foundone = false;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        // Handle the partition that contains a: consider both originals and replicas
        this->scanPartition_NoChecks_intersect(l, a, this->pRepsInIds, candidates, vec_candidates);
        this->scanPartition_NoChecks_intersect(l, a, this->pRepsAftIds, candidates, vec_candidates);

        // Handle rest: consider only originals
        for (auto i = a; i <= b; i++)
        {
            this->scanPartition_NoChecks_intersect(l, i, this->pOrgsInIds, candidates, vec_candidates);
            this->scanPartition_NoChecks_intersect(l, i, this->pOrgsAftIds, candidates, vec_candidates);
        }

        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root.
    iterIBegin = this->pOrgsInIds[this->numBits][0].begin();
    iterIEnd = this->pOrgsInIds[this->numBits][0].end();
    if (iterIBegin != iterIEnd)
        mergeSort(iterIBegin, iterIEnd, candidates, vec_candidates);
}
