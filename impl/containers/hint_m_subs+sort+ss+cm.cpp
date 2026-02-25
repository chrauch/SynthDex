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


#include <queue>
class compareQueueElements
{
public:
  bool operator() (const pair<RelationIdIterator,RelationIdIterator> &lhs, const pair<RelationIdIterator,RelationIdIterator> &rhs) const
  {
      return (*(lhs.first) > *(rhs.first));
  }
};
inline void mergeSort(RelationIdIterator &iterPBegin, RelationIdIterator &iterPEnd, vector<RelationId> &vec_candidates, vector<RelationId> &vec_result)
{
    priority_queue<pair<RelationIdIterator,RelationIdIterator>, vector<pair<RelationIdIterator,RelationIdIterator>>, compareQueueElements> Q;
    auto iterP = iterPBegin;
    RelationId tmp;

    for (RelationId &cands: vec_candidates)
        Q.push(make_pair(cands.begin(), cands.end()));

    while ((iterP != iterPEnd) && (!Q.empty()))
    {
        auto curr = Q.top();

        if (*iterP < *curr.first)
            iterP++;
        else if (*iterP > *curr.first)
        {
            Q.pop();
            while ((curr.first != curr.second) && (*iterP > *curr.first))
                curr.first++;
            
            if (curr.first != curr.second)
                Q.push(curr);
        }
        else
        {
            tmp.push_back(*iterP);
            iterP++;
            
            Q.pop();
            curr.first++;
            if (curr.first != curr.second)
                Q.push(curr);
        }
    }
    
    if (!tmp.empty())
        vec_result.push_back(tmp);
}







inline void HINT_M_SubsSort_SS_CM::updateCounters(const Record &r)
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


inline void HINT_M_SubsSort_SS_CM::updatePartitions(const Record &r)
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
                    this->pRepsInTmp[level][this->pRepsIn_offsets[level][a]++] = Record(r.id, r.start, r.end);
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAftTmp[level][this->pRepsAft_offsets[level][a]++] = Record(r.id, r.start, r.end);
                }
            }
            else
            {
                if ((a == b) && (!lastfound))
                {
                    this->pOrgsInTmp[level][this->pOrgsIn_offsets[level][a]++] = Record(r.id, r.start, r.end);
                }
                else
                {
                    this->pOrgsAftTmp[level][this->pOrgsAft_offsets[level][a]++] = Record(r.id, r.start, r.end);
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
                    this->pOrgsInTmp[level][this->pOrgsIn_offsets[level][prevb]++] = Record(r.id, r.start, r.end);
                }
                else
                {
                    this->pOrgsAftTmp[level][this->pOrgsAft_offsets[level][prevb]++] = Record(r.id, r.start, r.end);
                }
            }
            else
            {
                if (!lastfound)
                {
                    this->pRepsInTmp[level][this->pRepsIn_offsets[level][prevb]++] = Record(r.id, r.start, r.end);
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAftTmp[level][this->pRepsAft_offsets[level][prevb]++] = Record(r.id, r.start, r.end);
                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


HINT_M_SubsSort_SS_CM::HINT_M_SubsSort_SS_CM(const Relation &R, const unsigned int numBits = 0) : HierarchicalIndex(R, numBits)
{
    OffsetEntry_SS_CM dummySE;
    Offsets_SS_CM_Iterator iterSEO, iterSEOBegin, iterSEOEnd;
    PartitionId tmp = -1;


    // Step 1: one pass to count the contents inside each partition.
    this->pOrgsIn_sizes  = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pOrgsAft_sizes = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pRepsIn_sizes  = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pRepsAft_sizes = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pOrgsIn_offsets  = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pOrgsAft_offsets = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pRepsIn_offsets  = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pRepsAft_offsets = (size_t **)malloc(this->height*sizeof(size_t *));
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        //calloc allocates memory and sets each counter to 0
        this->pOrgsIn_sizes[l]  = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pOrgsAft_sizes[l] = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pRepsIn_sizes[l]  = (size_t *)calloc(cnt, sizeof(size_t));
        this->pRepsAft_sizes[l] = (size_t *)calloc(cnt, sizeof(size_t));
        this->pOrgsIn_offsets[l]  = (RecordId *)calloc(cnt+1, sizeof(RecordId));
        this->pOrgsAft_offsets[l] = (RecordId *)calloc(cnt+1, sizeof(RecordId));
        this->pRepsIn_offsets[l]  = (size_t *)calloc(cnt+1, sizeof(size_t));
        this->pRepsAft_offsets[l] = (size_t *)calloc(cnt+1, sizeof(size_t));
    }
    
    for (const Record &r : R)
        this->updateCounters(r);
    
    
    // Step 2: allocate necessary memory.
    this->pOrgsInTmp  = new Relation[this->height];
    this->pOrgsAftTmp = new Relation[this->height];
    this->pRepsInTmp  = new Relation[this->height];
    this->pRepsAftTmp = new Relation[this->height];
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        size_t sumOin = 0, sumOaft = 0, sumRin = 0, sumRaft = 0;
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            this->pOrgsIn_offsets[l][pId]  = sumOin;
            this->pOrgsAft_offsets[l][pId] = sumOaft;
            this->pRepsIn_offsets[l][pId]  = sumRin;
            this->pRepsAft_offsets[l][pId] = sumRaft;
            sumOin  += this->pOrgsIn_sizes[l][pId];
            sumOaft += this->pOrgsAft_sizes[l][pId];
            sumRin  += this->pRepsIn_sizes[l][pId];
            sumRaft += this->pRepsAft_sizes[l][pId];
        }
        this->pOrgsIn_offsets[l][cnt]  = sumOin;
        this->pOrgsAft_offsets[l][cnt] = sumOaft;
        this->pRepsIn_offsets[l][cnt]  = sumRin;
        this->pRepsAft_offsets[l][cnt] = sumRaft;
        
        this->pOrgsInTmp[l].resize(sumOin);
        this->pOrgsAftTmp[l].resize(sumOaft);
        this->pRepsInTmp[l].resize(sumRin);
        this->pRepsAftTmp[l].resize(sumRaft);
    }
    
    
    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);
    
    
    // Step 4: sort partition contents; first need to reset the offsets
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        size_t sumOin = 0, sumOaft = 0, sumRin = 0, sumRaft = 0;
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            this->pOrgsIn_offsets[l][pId]  = sumOin;
            this->pOrgsAft_offsets[l][pId] = sumOaft;
            this->pRepsIn_offsets[l][pId]  = sumRin;
            this->pRepsAft_offsets[l][pId] = sumRaft;
            sumOin  += this->pOrgsIn_sizes[l][pId];
            sumOaft += this->pOrgsAft_sizes[l][pId];
            sumRin  += this->pRepsIn_sizes[l][pId];
            sumRaft += this->pRepsAft_sizes[l][pId];
        }
        this->pOrgsIn_offsets[l][cnt]  = sumOin;
        this->pOrgsAft_offsets[l][cnt] = sumOaft;
        this->pRepsIn_offsets[l][cnt]  = sumRin;
        this->pRepsAft_offsets[l][cnt] = sumRaft;
    }
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        for (auto pId = 0; pId < cnt; pId++)
        {
            sort(this->pOrgsInTmp[l].begin()+this->pOrgsIn_offsets[l][pId], this->pOrgsInTmp[l].begin()+this->pOrgsIn_offsets[l][pId+1]);
            sort(this->pOrgsAftTmp[l].begin()+this->pOrgsAft_offsets[l][pId], this->pOrgsAftTmp[l].begin()+this->pOrgsAft_offsets[l][pId+1]);
            sort(this->pRepsInTmp[l].begin()+this->pRepsIn_offsets[l][pId], this->pRepsInTmp[l].begin()+this->pRepsIn_offsets[l][pId+1], compareRecordsByEnd);
            sort(this->pRepsAftTmp[l].begin()+this->pRepsAft_offsets[l][pId], this->pRepsAftTmp[l].begin()+this->pRepsAft_offsets[l][pId+1], compareRecordsByEnd);
        }
    }
    
    
    // Step 5: break-down data to create id- and timestamp-dedicated arrays; free auxiliary memory.
    this->pOrgsInIds  = new RelationId[this->height];
    this->pOrgsAftIds = new RelationId[this->height];
    this->pRepsInIds  = new RelationId[this->height];
    this->pRepsAftIds = new RelationId[this->height];
    this->pOrgsInTimestamps  = new vector<pair<Timestamp, Timestamp> >[this->height];
    this->pOrgsAftTimestamps = new vector<pair<Timestamp, Timestamp> >[this->height];
    this->pRepsInTimestamps  = new vector<pair<Timestamp, Timestamp> >[this->height];
    this->pRepsAftTimestamps = new vector<pair<Timestamp, Timestamp> >[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pOrgsInTmp[l].size();
        
        this->pOrgsInIds[l].resize(cnt);
        this->pOrgsInTimestamps[l].resize(cnt);
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsInIds[l][j] = this->pOrgsInTmp[l][j].id;
            this->pOrgsInTimestamps[l][j].first = this->pOrgsInTmp[l][j].start;
            this->pOrgsInTimestamps[l][j].second = this->pOrgsInTmp[l][j].end;
        }
        
        cnt = pOrgsAftTmp[l].size();
        this->pOrgsAftIds[l].resize(cnt);
        this->pOrgsAftTimestamps[l].resize(cnt);
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsAftIds[l][j] = this->pOrgsAftTmp[l][j].id;
            this->pOrgsAftTimestamps[l][j].first = this->pOrgsAftTmp[l][j].start;
            this->pOrgsAftTimestamps[l][j].second = this->pOrgsAftTmp[l][j].end;
        }
        
        cnt = pRepsInTmp[l].size();
        this->pRepsInIds[l].resize(cnt);
        this->pRepsInTimestamps[l].resize(cnt);
        for (auto j = 0; j < cnt; j++)
        {
            this->pRepsInIds[l][j] = this->pRepsInTmp[l][j].id;
            this->pRepsInTimestamps[l][j].first = this->pRepsInTmp[l][j].start;
            this->pRepsInTimestamps[l][j].second = this->pRepsInTmp[l][j].end;
        }

        cnt = pRepsAftTmp[l].size();
        this->pRepsAftIds[l].resize(cnt);
        this->pRepsAftTimestamps[l].resize(cnt);
        for (auto j = 0; j < cnt; j++)
        {
            this->pRepsAftIds[l][j] = this->pRepsAftTmp[l][j].id;
            this->pRepsAftTimestamps[l][j].first = this->pRepsAftTmp[l][j].start;
            this->pRepsAftTimestamps[l][j].second = this->pRepsAftTmp[l][j].end;
        }
    }
    
    
    // Free auxiliary memory
    for (auto l = 0; l < this->height; l++)
    {
        free(this->pOrgsIn_offsets[l]);
        free(this->pOrgsAft_offsets[l]);
        free(this->pRepsIn_offsets[l]);
        free(this->pRepsAft_offsets[l]);
    }
    free(this->pOrgsIn_offsets);
    free(this->pOrgsAft_offsets);
    free(this->pRepsIn_offsets);
    free(this->pRepsAft_offsets);
    
    delete[] this->pOrgsInTmp;
    delete[] this->pOrgsAftTmp;
    delete[] this->pRepsInTmp;
    delete[] this->pRepsAftTmp;
    

    // Step 4: create offset pointers
    this->pOrgsIn_ioffsets  = new Offsets_SS_CM[this->height];
    this->pOrgsAft_ioffsets = new Offsets_SS_CM[this->height];
    this->pRepsIn_ioffsets  = new Offsets_SS_CM[this->height];
    this->pRepsAft_ioffsets = new Offsets_SS_CM[this->height];
    for (int l = this->height-1; l > -1; l--)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        size_t sumOin = 0, sumOaft = 0, sumRin = 0, sumRaft = 0;
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            bool isEmpty = true;
            
            dummySE.tstamp = pId >> 1;//((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pOrgsIn_sizes[l][pId] > 0)
            {
                isEmpty = false;
                tmp = -1;
                if (l < this->height-1)
                {
                    iterSEOBegin = this->pOrgsIn_ioffsets[l+1].begin();
                    iterSEOEnd = this->pOrgsIn_ioffsets[l+1].end();
                    iterSEO = lower_bound(iterSEOBegin, iterSEOEnd, dummySE);//, CompareOffsetsByTimestamp);
                    tmp = (iterSEO != iterSEOEnd)? (iterSEO-iterSEOBegin): -1;
                }
                this->pOrgsIn_ioffsets[l].push_back(OffsetEntry_SS_CM(pId, this->pOrgsInIds[l].begin()+sumOin, this->pOrgsInTimestamps[l].begin()+sumOin, tmp));
            }
            
            dummySE.tstamp = pId >> 1;//((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pOrgsAft_sizes[l][pId] > 0)
            {
                isEmpty = false;
                tmp = -1;
                if (l < this->height-1)
                {
                    iterSEOBegin = this->pOrgsAft_ioffsets[l+1].begin();
                    iterSEOEnd = this->pOrgsAft_ioffsets[l+1].end();
                    iterSEO = lower_bound(iterSEOBegin, iterSEOEnd, dummySE);//, CompareOffsetsByTimestamp);
                    tmp = (iterSEO != iterSEOEnd)? (iterSEO-iterSEOBegin): -1;
                }
                this->pOrgsAft_ioffsets[l].push_back(OffsetEntry_SS_CM(pId, this->pOrgsAftIds[l].begin()+sumOaft, this->pOrgsAftTimestamps[l].begin()+sumOaft, tmp));
            }
            
            dummySE.tstamp = pId >> 1;//((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pRepsIn_sizes[l][pId] > 0)
            {
                isEmpty = false;
                tmp = -1;
                if (l < this->height-1)
                {
                    iterSEOBegin = this->pRepsIn_ioffsets[l+1].begin();
                    iterSEOEnd = this->pRepsIn_ioffsets[l+1].end();
                    iterSEO = lower_bound(iterSEOBegin, iterSEOEnd, dummySE);//, CompareOffsetsByTimestamp);
                    tmp = (iterSEO != iterSEOEnd)? (iterSEO-iterSEOBegin): -1;
                }
                this->pRepsIn_ioffsets[l].push_back(OffsetEntry_SS_CM(pId, this->pRepsInIds[l].begin()+sumRin, this->pRepsInTimestamps[l].begin()+sumRin, tmp));
            }
            
            dummySE.tstamp = pId >> 1;//((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pRepsAft_sizes[l][pId] > 0)
            {
                isEmpty = false;
                tmp = -1;
                if (l < this->height-1)
                {
                    iterSEOBegin = this->pRepsAft_ioffsets[l+1].begin();
                    iterSEOEnd = this->pRepsAft_ioffsets[l+1].end();
                    iterSEO = lower_bound(iterSEOBegin, iterSEOEnd, dummySE);//, CompareOffsetsByTimestamp);
                    tmp = (iterSEO != iterSEOEnd)? (iterSEO-iterSEOBegin): -1;
                }
                this->pRepsAft_ioffsets[l].push_back(OffsetEntry_SS_CM(pId, this->pRepsAftIds[l].begin()+sumRaft, this->pRepsAftTimestamps[l].begin()+sumRaft, tmp));
            }
            
            sumOin += this->pOrgsIn_sizes[l][pId];
            sumOaft += this->pOrgsAft_sizes[l][pId];
            sumRin += this->pRepsIn_sizes[l][pId];
            sumRaft += this->pRepsAft_sizes[l][pId];
            
            if (isEmpty)
                this->numEmptyPartitions++;
        }
    }
    
    
    // Free auxliary memory
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
}


void HINT_M_SubsSort_SS_CM::getStats()
{
//    size_t sum = 0;
//    for (auto l = 0; l < this->height; l++)
//    {
//        auto cnt = pow(2, this->numBits-l);
//
//        this->numPartitions += cnt;
//
//        this->numOriginalsIn  += this->pOrgsInIds[l].size();
//        this->numOriginalsAft += this->pOrgsAftIds[l].size();
//        this->numReplicasIn   += this->pRepsInIds[l].size();
//        this->numReplicasAft  += this->pRepsAftIds[l].size();
//    }
//
//    this->avgPartitionSize = (float)(this->numIndexedRecords+this->numReplicasIn+this->numReplicasAft)/(this->numPartitions-numEmptyPartitions);
}


size_t HINT_M_SubsSort_SS_CM::getSize()
{
    auto size = 0;

    for (auto l = 0; l < this->height; l++)
    {
        size += this->pOrgsInIds[l].size() * sizeof(RecordId);
        size += this->pOrgsAftIds[l].size() * sizeof(RecordId);
        size += this->pRepsInIds[l].size() * sizeof(RecordId);
        size += this->pRepsAftIds[l].size() * sizeof(RecordId);
        
        size += this->pOrgsInTimestamps[l].size() * 2 * sizeof(Timestamp);
        size += this->pOrgsAftTimestamps[l].size() * 2 * sizeof(Timestamp);
        size += this->pRepsInTimestamps[l].size() * 2 * sizeof(Timestamp);
        size += this->pRepsAftTimestamps[l].size() * 2 * sizeof(Timestamp);
    }
    return size;
}


HINT_M_SubsSort_SS_CM::~HINT_M_SubsSort_SS_CM()
{
    delete[] this->pOrgsIn_ioffsets;
    delete[] this->pOrgsAft_ioffsets;
    delete[] this->pRepsIn_ioffsets;
    delete[] this->pRepsAft_ioffsets;
    
    delete[] this->pOrgsInIds;
    delete[] this->pOrgsInTimestamps;
    delete[] this->pOrgsAftIds;
    delete[] this->pOrgsAftTimestamps;
    delete[] this->pRepsInIds;
    delete[] this->pRepsInTimestamps;
    delete[] this->pRepsAftIds;
    delete[] this->pRepsAftTimestamps;
}


// Auxiliary functions to determine exactly how to scan a partition.
inline bool HINT_M_SubsSort_SS_CM::getBounds(unsigned int level, Timestamp t, PartitionId &next_from, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, vector<pair<Timestamp, Timestamp> >::iterator &iterBegin, vector<pair<Timestamp, Timestamp> >::iterator &iterEnd, RelationIdIterator &iterI)
{
    OffsetEntry_SS_CM qdummy;
    Offsets_SS_CM_Iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = ioffsets[level].size();
    PartitionId from = next_from;

    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from >= cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = ioffsets[level].begin();
            iterIOEnd = ioffsets[level].end();

            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareOffsetsByTimestamp);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp == t))
            {
                iterI = iterIO->iterI;
                iterBegin = iterIO->iterT;
                iterEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterT : timestamps[level].end());

                next_from =iterIO->pid;
                
                return true;
            }
            else
                return false;
        }
        else
        {
            Timestamp tmp = (ioffsets[level][from]).tstamp;
            if (tmp < t)
            {
                while ((from < cnt) && ((ioffsets[level][from]).tstamp < t))
                    from++;
            }
            else if (tmp > t)
            {
                while ((from > -1) && ((ioffsets[level][from]).tstamp > t))
                    from--;
                if ((from == -1) || ((ioffsets[level][from]).tstamp != t))
                    from++;
            }

            if ((from != cnt) && ((ioffsets[level][from]).tstamp == t))
            {
                iterI = (ioffsets[level][from]).iterI;
                iterBegin = (ioffsets[level][from]).iterT;
                iterEnd = ((from+1 != cnt) ? (ioffsets[level][from+1]).iterT : timestamps[level].end());

                next_from = (ioffsets[level][from]).pid;
                
                return true;
            }
            else
            {
                next_from = -1;

                return false;
            }
        }
    }
    else
    {
        next_from = -1;
        
        return false;
    }
}


inline bool HINT_M_SubsSort_SS_CM::getBounds(unsigned int level, Timestamp t, PartitionId &next_from, Offsets_SS_CM *ioffsets, RelationId *ids, RelationIdIterator &iterIBegin, RelationIdIterator &iterIEnd)
{
    OffsetEntry_SS_CM qdummy;
    Offsets_SS_CM_Iterator iterIO, iterIO2, iterIOBegin, iterIOEnd;
    size_t cnt = ioffsets[level].size();
    PartitionId from = next_from;


    if (cnt > 0)
    {
        if ((from == -1) || (from >= cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = ioffsets[level].begin();
            iterIOEnd = ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareOffsetsByTimestamp);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp == t))
            {
                iterIBegin = iterIO->iterI;
                iterIEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterI : ids[level].end());

                next_from =iterIO->pid;
                
                return true;
            }
            else
                return false;
        }
        else
        {
            Timestamp tmp = (ioffsets[level][from]).tstamp;
            if (tmp < t)
            {
                while ((from < cnt) && ((ioffsets[level][from]).tstamp < t))
                    from++;
            }
            else if (tmp > t)
            {
                while ((from > -1) && ((ioffsets[level][from]).tstamp > t))
                    from--;
                if ((from == -1) || ((ioffsets[level][from]).tstamp != t))
                    from++;
            }

            if ((from != cnt) && ((ioffsets[level][from]).tstamp == t))
            {
                iterIBegin = (ioffsets[level][from]).iterI;
                iterIEnd = ((from+1 != cnt) ? (ioffsets[level][from+1]).iterI : ids[level].end());

                next_from = (ioffsets[level][from]).pid;
                
                return true;
            }
            else
            {
                next_from = -1;
                
                return false;
            }
        }
    }
    else
    {
        next_from = -1;
        
        return false;
    }
}


inline bool HINT_M_SubsSort_SS_CM::getBounds(unsigned int level, Timestamp ts, Timestamp te, PartitionId &next_from, PartitionId &next_to, Offsets_SS_CM *ioffsets, RelationId *ids, RelationIdIterator &iterIBegin, RelationIdIterator &iterIEnd)
{
    OffsetEntry_SS_CM qdummyTS, qdummyTE;
    Offsets_SS_CM_Iterator iterIO, iterIO2, iterIOBegin, iterIOEnd;
    size_t cnt = ioffsets[level].size();
    PartitionId from = next_from, to = next_to;


    if (cnt > 0)
    {
        from = next_from;
        to = next_to;

        // Do binary search or follow vertical pointers.
        if ((from == -1) || (to == -1))
        {
            qdummyTS.tstamp = ts;
            iterIOBegin = ioffsets[level].begin();
            iterIOEnd = ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummyTS);//, CompareOffsetsByTimestamp);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp <= te))
            {
                next_from =iterIO->pid;

                qdummyTE.tstamp = te;
                iterIBegin = iterIO->iterI;

                iterIO2 = upper_bound(iterIO, iterIOEnd, qdummyTE);//, CompareOffsetsByTimestamp);

                iterIEnd = ((iterIO2 != iterIOEnd) ? iterIEnd = iterIO2->iterI: ids[level].end());

                if (iterIO2 != iterIOEnd)
                    next_to = iterIO2->pid;
                else
                    next_to = -1;
                
                return true;
            }
            else
            {
                next_from = -1;
                
                return false;
            }
        }
        else
        {
            Timestamp tmp = (ioffsets[level][from]).tstamp;
            if (tmp < ts)
            {
                while ((from < cnt) && ((ioffsets[level][from]).tstamp < ts))
                    from++;
            }
            else if (tmp > ts)
            {
                while ((from > -1) && ((ioffsets[level][from]).tstamp > ts))
                    from--;
                if ((from == -1) || ((ioffsets[level][from]).tstamp != ts))
                    from++;
            }

            tmp = (ioffsets[level][to]).tstamp;
            if (tmp > te)
            {
                while ((to > -1) && ((ioffsets[level][to]).tstamp > te))
                    to--;
                to++;
            }
            else if (tmp == te)
            {
                while ((to < cnt) && ((ioffsets[level][to]).tstamp <= te))
                    to++;
            }

            if ((from != cnt) && (from != -1) && (from < to))
            {
                iterIBegin = (ioffsets[level][from]).iterI;
                iterIEnd   = (to != cnt)? (ioffsets[level][to]).iterI: ids[level].end();

                next_from = (ioffsets[level][from]).pid;
                next_to   = (to != cnt) ? (ioffsets[level][to]).pid  : -1;
                
                return true;
            }
            else
            {
                next_from = next_to = -1;
                
                return false;
            }
        }
    }
    else
    {
        next_from = -1;
        next_to = -1;
        
        return false;
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckBoth_moveOut(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), const RangeQuery &q, PartitionId &next_from, RelationId &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;

    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(q.end+1, q.end+1), compare);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if (q.start <= iter->second)
                result.push_back(*iterI);
            iterI++;
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckEnd_moveOut(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, const RangeQuery &q, PartitionId &next_from, RelationId &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;

    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (q.start <= iter->second)
                result.push_back(*iterI);
            iterI++;
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckEnd_moveOut(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), const RangeQuery &q, PartitionId &next_from, RelationId &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;

    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(q.start, q.start), compare);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
            result.push_back(*iterI);
            iter++;
            iterI++;
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckStart_moveOut(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), const RangeQuery &q, PartitionId &next_from, RelationId &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;

    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(q.end+1, q.end+1), compare);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            result.push_back(*iterI);
            iterI++;
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_NoChecks_moveOut(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, RelationId *ids, PartitionId &next_from, RelationId &result)
{
    RelationIdIterator iterI, iterIBegin, iterIEnd;

    if (this->getBounds(level, t, next_from, ioffsets, ids, iterIBegin, iterIEnd))
    {
        result.insert(result.end(), iterIBegin, iterIEnd);
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartitions_NoChecks_moveOut(const unsigned int level, const Timestamp ts, const Timestamp te, Offsets_SS_CM *ioffsets, RelationId *ids, PartitionId &next_from, PartitionId &next_to, RelationId &result)
{
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    OffsetEntry_SS_CM qdummyTS, qdummyTE;

    if (this->getBounds(level, ts, te, next_from, next_to, ioffsets, ids, iterIBegin, iterIEnd))
    {
        result.insert(result.end(), iterIBegin, iterIEnd);
    }
}


void HINT_M_SubsSort_SS_CM::moveOut(const RangeQuery &q, RelationId &candidates)
{
    if (q.start > this->gend || q.end < this->gstart) 
        return;

    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    Timestamp a = (max(q.start,this->gstart)-this->gstart) >> (this->maxBits-this->numBits);
    Timestamp b = (min(q.end,this->gend)-this->gstart)     >> (this->maxBits-this->numBits);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromRinA = -1, next_fromRaftA = -1, next_fromOinB = -1, next_fromOaftB = -1, next_fromOinAB = -1, next_toOinAB = -1, next_fromOaftAB = -1, next_toOaftAB = -1, next_fromR = -1, next_fromO = -1, next_toO = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results

            // Handle the partition that contains a: consider both originals and replicas
            this->scanPartition_NoChecks_moveOut(l, a, this->pRepsIn_ioffsets, this->pRepsInIds, next_fromRinA, candidates);
            this->scanPartition_NoChecks_moveOut(l, a, this->pRepsAft_ioffsets, this->pRepsAftIds, next_fromRaftA, candidates);

            this->scanPartitions_NoChecks_moveOut(l, a, b, this->pOrgsIn_ioffsets, this->pOrgsInIds, next_fromOinAB, next_toOinAB, candidates);
            this->scanPartitions_NoChecks_moveOut(l, a, b, this->pOrgsAft_ioffsets, this->pOrgsAftIds, next_fromOaftAB, next_toOaftAB, candidates);
        }
        else
        {
            // Comparisons needed

            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                this->scanPartition_CheckBoth_moveOut(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, q, next_fromOinA, candidates);
                this->scanPartition_CheckStart_moveOut(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, q, next_fromOaftA, candidates);
            }
            else
            {
                // Lemma 1
                this->scanPartition_CheckEnd_moveOut(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, q, next_fromOinA, candidates);
                this->scanPartition_NoChecks_moveOut(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftIds, next_fromOaftA, candidates);
            }

            // Lemma 1, 3
            this->scanPartition_CheckEnd_moveOut(l, a, this->pRepsIn_ioffsets, this->pRepsInTimestamps, CompareTimestampPairsByEnd, q, next_fromRinA, candidates);
            this->scanPartition_NoChecks_moveOut(l, a, this->pRepsAft_ioffsets, this->pRepsAftIds, next_fromRaftA, candidates);

            if (a < b)
            {
                // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                this->scanPartitions_NoChecks_moveOut(l, a+1, b-1, this->pOrgsIn_ioffsets, this->pOrgsInIds, next_fromOinAB, next_toOinAB, candidates);
                this->scanPartitions_NoChecks_moveOut(l, a+1, b-1, this->pOrgsAft_ioffsets, this->pOrgsAftIds, next_fromOaftAB, next_toOaftAB, candidates);

                // Handle the partition that contains b: consider only originals, comparisons needed
                this->scanPartition_CheckStart_moveOut(l, b, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, q, next_fromOinB, candidates);
                this->scanPartition_CheckStart_moveOut(l, b, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, q, next_fromOaftB, candidates);
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
        iterIBegin = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
            candidates.push_back(*iterI);
    }
    else
    {
        // Comparisons needed
        iterI = this->pOrgsInIds[this->numBits].begin();
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = lower_bound(iterBegin, this->pOrgsInTimestamps[this->numBits].end(), make_pair<Timestamp, Timestamp>(q.end+1, q.end+1), CompareTimestampPairsByStart);
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (q.start <= iter->second)
                candidates.push_back(*iterI);
            iterI++;
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckBoth_intersect(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), const RangeQuery &q, PartitionId &next_from, RelationId &candidates, RelationId &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;

    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(q.end+1, q.end+1), compare);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if ((q.start <= iter->second) && (binary_search(candidates.begin(), candidates.end(), *iterI)))
                result.push_back(*iterI);
            iterI++;
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckEnd_intersect(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, const RangeQuery &q, PartitionId &next_from, RelationId &candidates, RelationId &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;

    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if ((q.start <= iter->second) && (binary_search(candidates.begin(), candidates.end(), *iterI)))
                result.push_back(*iterI);
            iterI++;
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckEnd_intersect(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), const RangeQuery &q, PartitionId &next_from, RelationId &candidates, RelationId &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;

    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(q.start, q.start), compare);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
            if (binary_search(candidates.begin(), candidates.end(), *iterI))
                result.push_back(*iterI);
            iter++;
            iterI++;
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckStart_intersect(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), const RangeQuery &q, PartitionId &next_from, RelationId &candidates, RelationId &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;

    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(q.end+1, q.end+1), compare);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if (binary_search(candidates.begin(), candidates.end(), *iterI))
                result.push_back(*iterI);
            iterI++;
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_NoChecks_intersect(const unsigned int level, const Timestamp t, Offsets_SS_CM *ioffsets, RelationId *ids, PartitionId &next_from, RelationId &candidates, RelationId &result)
{
    RelationIdIterator iterI, iterIBegin, iterIEnd;

    if (this->getBounds(level, t, next_from, ioffsets, ids, iterIBegin, iterIEnd))
    {
        for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
        {
            if (binary_search(candidates.begin(), candidates.end(), *iterI))
                result.push_back(*iterI);
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartitions_NoChecks_intersect(const unsigned int level, const Timestamp ts, const Timestamp te, Offsets_SS_CM *ioffsets, RelationId *ids, PartitionId &next_from, PartitionId &next_to, RelationId &candidates, RelationId &result)
{
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    OffsetEntry_SS_CM qdummyTS, qdummyTE;

    if (this->getBounds(level, ts, te, next_from, next_to, ioffsets, ids, iterIBegin, iterIEnd))
    {
        for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
        {
            if (binary_search(candidates.begin(), candidates.end(), *iterI))
                result.push_back(*iterI);
        }
    }
}


void HINT_M_SubsSort_SS_CM::intersect(const RangeQuery &q, RelationId &candidates)
{
    if (q.start > this->gend || q.end < this->gstart)
    {
        candidates.clear();
        return;
    }

    RelationId result;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    Timestamp a = (max(q.start,this->gstart)-this->gstart) >> (this->maxBits-this->numBits);
    Timestamp b = (min(q.end,this->gend)-this->gstart)     >> (this->maxBits-this->numBits);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromRinA = -1, next_fromRaftA = -1, next_fromOinB = -1, next_fromOaftB = -1, next_fromOinAB = -1, next_toOinAB = -1, next_fromOaftAB = -1, next_toOaftAB = -1, next_fromR = -1, next_fromO = -1, next_toO = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results

            // Handle the partition that contains a: consider both originals and replicas
            this->scanPartition_NoChecks_intersect(l, a, this->pRepsIn_ioffsets, this->pRepsInIds, next_fromRinA, candidates, result);
            this->scanPartition_NoChecks_intersect(l, a, this->pRepsAft_ioffsets, this->pRepsAftIds, next_fromRaftA, candidates, result);

            this->scanPartitions_NoChecks_intersect(l, a, b, this->pOrgsIn_ioffsets, this->pOrgsInIds, next_fromOinAB, next_toOinAB, candidates, result);
            this->scanPartitions_NoChecks_intersect(l, a, b, this->pOrgsAft_ioffsets, this->pOrgsAftIds, next_fromOaftAB, next_toOaftAB, candidates, result);
        }
        else
        {
            // Comparisons needed

            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                this->scanPartition_CheckBoth_intersect(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, q, next_fromOinA, candidates, result);
                this->scanPartition_CheckStart_intersect(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, q, next_fromOaftA, candidates, result);
            }
            else
            {
                // Lemma 1
                this->scanPartition_CheckEnd_intersect(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, q, next_fromOinA, candidates, result);
                this->scanPartition_NoChecks_intersect(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftIds, next_fromOaftA, candidates, result);
            }

            // Lemma 1, 3
            this->scanPartition_CheckEnd_intersect(l, a, this->pRepsIn_ioffsets, this->pRepsInTimestamps, CompareTimestampPairsByEnd, q, next_fromRinA, candidates, result);
            this->scanPartition_NoChecks_intersect(l, a, this->pRepsAft_ioffsets, this->pRepsAftIds, next_fromRaftA, candidates, result);

            if (a < b)
            {
                // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                this->scanPartitions_NoChecks_intersect(l, a+1, b-1, this->pOrgsIn_ioffsets, this->pOrgsInIds, next_fromOinAB, next_toOinAB, candidates, result);
                this->scanPartitions_NoChecks_intersect(l, a+1, b-1, this->pOrgsAft_ioffsets, this->pOrgsAftIds, next_fromOaftAB, next_toOaftAB, candidates, result);

                // Handle the partition that contains b: consider only originals, comparisons needed
                this->scanPartition_CheckStart_intersect(l, b, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, q, next_fromOinB, candidates, result);
                this->scanPartition_CheckStart_intersect(l, b, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, q, next_fromOaftB, candidates, result);
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
        iterIBegin = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
        {
            if (binary_search(candidates.begin(), candidates.end(), *iterI))
                result.push_back(*iterI);
        }
    }
    else
    {
        // Comparisons needed
        iterI = this->pOrgsInIds[this->numBits].begin();
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = lower_bound(iterBegin, this->pOrgsInTimestamps[this->numBits].end(), make_pair<Timestamp, Timestamp>(q.end+1, q.end+1), CompareTimestampPairsByStart);
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if ((q.start <= iter->second) && (binary_search(candidates.begin(), candidates.end(), *iterI)))
                result.push_back(*iterI);
            iterI++;
        }
    }
    
    candidates.swap(result);
}


void HINT_M_SubsSort_SS_CM::intersectAndOutput(const RangeQuery &q, RelationId &candidates, RelationId &result)
{
    if (q.start > this->gend || q.end < this->gstart)
    {
        result.clear();
        return;
    }

    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    Timestamp a = (max(q.start,this->gstart)-this->gstart) >> (this->maxBits-this->numBits);
    Timestamp b = (min(q.end,this->gend)-this->gstart)     >> (this->maxBits-this->numBits);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromRinA = -1, next_fromRaftA = -1, next_fromOinB = -1, next_fromOaftB = -1, next_fromOinAB = -1, next_toOinAB = -1, next_fromOaftAB = -1, next_toOaftAB = -1, next_fromR = -1, next_fromO = -1, next_toO = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results

            // Handle the partition that contains a: consider both originals and replicas
            this->scanPartition_NoChecks_intersect(l, a, this->pRepsIn_ioffsets, this->pRepsInIds, next_fromRinA, candidates, result);
            this->scanPartition_NoChecks_intersect(l, a, this->pRepsAft_ioffsets, this->pRepsAftIds, next_fromRaftA, candidates, result);

            this->scanPartitions_NoChecks_intersect(l, a, b, this->pOrgsIn_ioffsets, this->pOrgsInIds, next_fromOinAB, next_toOinAB, candidates, result);
            this->scanPartitions_NoChecks_intersect(l, a, b, this->pOrgsAft_ioffsets, this->pOrgsAftIds, next_fromOaftAB, next_toOaftAB, candidates, result);
        }
        else
        {
            // Comparisons needed

            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                this->scanPartition_CheckBoth_intersect(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, q, next_fromOinA, candidates, result);
                this->scanPartition_CheckStart_intersect(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, q, next_fromOaftA, candidates, result);
            }
            else
            {
                // Lemma 1
                this->scanPartition_CheckEnd_intersect(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, q, next_fromOinA, candidates, result);
                this->scanPartition_NoChecks_intersect(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftIds, next_fromOaftA, candidates, result);
            }

            // Lemma 1, 3
            this->scanPartition_CheckEnd_intersect(l, a, this->pRepsIn_ioffsets, this->pRepsInTimestamps, CompareTimestampPairsByEnd, q, next_fromRinA, candidates, result);
            this->scanPartition_NoChecks_intersect(l, a, this->pRepsAft_ioffsets, this->pRepsAftIds, next_fromRaftA, candidates, result);

            if (a < b)
            {
                // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                this->scanPartitions_NoChecks_intersect(l, a+1, b-1, this->pOrgsIn_ioffsets, this->pOrgsInIds, next_fromOinAB, next_toOinAB, candidates, result);
                this->scanPartitions_NoChecks_intersect(l, a+1, b-1, this->pOrgsAft_ioffsets, this->pOrgsAftIds, next_fromOaftAB, next_toOaftAB, candidates, result);

                // Handle the partition that contains b: consider only originals, comparisons needed
                this->scanPartition_CheckStart_intersect(l, b, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, q, next_fromOinB, candidates, result);
                this->scanPartition_CheckStart_intersect(l, b, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, q, next_fromOaftB, candidates, result);
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
        iterIBegin = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
        {
            if (binary_search(candidates.begin(), candidates.end(), *iterI))
                result.push_back(*iterI);
        }
    }
    else
    {
        // Comparisons needed
        iterI = this->pOrgsInIds[this->numBits].begin();
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = lower_bound(iterBegin, this->pOrgsInTimestamps[this->numBits].end(), make_pair<Timestamp, Timestamp>(q.end+1, q.end+1), CompareTimestampPairsByStart);
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if ((q.start <= iter->second) && (binary_search(candidates.begin(), candidates.end(), *iterI)))
                result.push_back(*iterI);
            iterI++;
        }
    }
}
