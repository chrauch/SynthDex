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

#include "temporal_inverted_file.h"
#include <unordered_set>



TemporalInvertedFile::TemporalInvertedFile() : InvertedFileTemplate()
{
}


// Undecided if we should use two passes
TemporalInvertedFile::TemporalInvertedFile(const IRelation &R) : InvertedFileTemplate()
{
    this->R = &R;
    this->numIndexedRecords = R.size();

    for (auto &r : R)
        this->index(r);
}


void TemporalInvertedFile::index(const IRecord &r)
{
    for (auto &tid : r.elements)
    {
        Relation &list = this->lists[tid];
        list.emplace_back(r.id, r.start, r.end);
        if (list.gstart > r.start)
            list.gstart = r.start;
        if (list.gend < r.end)
            list.gend = r.end;
    }
}


void TemporalInvertedFile::index(const IRecord &r, const ElementId &min, const ElementId &max)
{
    for (auto &tid : r.elements)
    {
        if (tid > max || tid < min)
        {
            // If tid not in lists, create a dummy entry
            if (this->lists.find(tid) == this->lists.end()) 
            {
                this->lists[tid].emplace_back(-1, -1, -1);
            }
        }
        else
        {
            Relation &list = this->lists[tid];
            list.emplace_back(r.id, r.start, r.end);
            if (list.gstart > r.start)
                list.gstart = r.start;
            if (list.gend < r.end)
                list.gend = r.end;
        }
    }
}


void TemporalInvertedFile::extractRecords(
    unordered_map<RecordId, IRecord> &recordMap,
    const Timestamp time_offset) const
{
    // Extract all records from posting lists, merging duplicates by record ID
    for (const auto& [tid, list] : this->lists)
    {
        for (const auto& rec : list)
        {
            // Skip dummy entries
            if (rec.id == (RecordId)-1) continue;
            
            auto it = recordMap.find(rec.id);
            if (it == recordMap.end())
            {
                // New record: create with adjusted timestamps
                IRecord irec(rec.id, rec.start + time_offset, rec.end + time_offset);
                irec.elements.push_back(tid);
                recordMap[rec.id] = move(irec);
            }
            else
            {
                // Existing record: just add the element (avoid duplicates)
                auto& elems = it->second.elements;
                if (find(elems.begin(), elems.end(), tid) == elems.end())
                {
                    elems.push_back(tid);
                }
            }
        }
    }
}


void TemporalInvertedFile::getStats()
{
}


size_t TemporalInvertedFile::getSize()
{
    size_t size = 0;
    
    for (auto iter = this->lists.begin(); iter != this->lists.end(); iter++)
        size += iter->second.size() * sizeof(Record);
    
    return size;
}


TemporalInvertedFile::~TemporalInvertedFile()
{
}


void TemporalInvertedFile::print(char c)
{
    for (auto iter = this->lists.begin(); iter != this->lists.end(); iter++)
    {
        cout << "t" << iter->first << " (" << iter->second.size() << "):";
        for (auto &r : iter->second)
            cout << " " << c << r.id;
        cout << endl;
    }
}


// Querying
bool TemporalInvertedFile::moveOut_CheckBoth(const RangeIRQuery &q, RelationId &candidates)
{
    unordered_map<ElementId, Relation>::iterator iterE = this->lists.find(q.elems[0]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterE == this->lists.end())
        return false;
    else
    {
        for (const Record &r: iterE->second)
        {
            if ((r.start <= q.end) && (q.start <= r.end))
                candidates.push_back(r.id);
        }
        
        return (!candidates.empty());
    }
}


bool TemporalInvertedFile::moveOut_CheckStart(const RangeIRQuery &q, RelationId &candidates)
{
    unordered_map<ElementId, Relation>::iterator iterE = this->lists.find(q.elems[0]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterE == this->lists.end())
        return false;
    else
    {
        for (const Record &r: iterE->second)
        {
            if (r.start <= q.end)
                candidates.push_back(r.id);
        }
        
        return (!candidates.empty());
    }
}


bool TemporalInvertedFile::moveOut_CheckEnd(const RangeIRQuery &q, RelationId &candidates)
{
    unordered_map<ElementId, Relation>::iterator iterE = this->lists.find(q.elems[0]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterE == this->lists.end())
        return false;
    else
    {
        for (const Record &r: iterE->second)
        {
            if (q.start <= r.end)
            {
                candidates.push_back(r.id);
            }
        }
        
        return (!candidates.empty());
    }
}


bool TemporalInvertedFile::moveOut_NoChecks(const RangeIRQuery &q, RelationId &candidates)
{
    unordered_map<ElementId, Relation>::iterator iterE = this->lists.find(q.elems[0]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterE == this->lists.end())
        return false;
    else
    {
        for (const Record &r: iterE->second)
            candidates.push_back(r.id);

        return (!candidates.empty());
    }
}


bool TemporalInvertedFile::intersect(const RangeIRQuery &q, const unsigned int off, RelationId &candidates)
{
    for (auto prev = 0; prev < off; prev++)
    {
        if (this->lists.find(q.elems[prev]) == this->lists.end()) 
            return false;
    }

    auto iterE = this->lists.find(q.elems[off]);
    
    if (iterE == this->lists.end())
        return false;
    else
    {
        auto iterC    = candidates.begin();
        auto iterCEnd = candidates.end();
        auto iterL    = iterE->second.begin();
        auto iterLEnd = iterE->second.end();
        RelationId tmp;

        //cout << "posting list for t" << q.elems[off] << ": " << endl;
        //for (auto &r : iterE->second) cout << r.id << ",";
        //cout << endl;

        tmp.reserve(candidates.size());
        while ((iterC != iterCEnd) && (iterL != iterLEnd))
        {
            if (iterL->id < *iterC)
                iterL++;
            else if (iterL->id > *iterC)
                iterC++;
            else
            {
                tmp.push_back(iterL->id);
                iterL++;
            }
        }
        candidates.swap(tmp);
        
        return (!candidates.empty());
    }
}


bool TemporalInvertedFile::intersectAndOutput(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result)
{
    for (auto prev = 0; prev < off; prev++)
    {
        if (this->lists.find(q.elems[prev]) == this->lists.end()) 
            return false;
    }

    auto iterE = this->lists.find(q.elems[off]);
        
    if (iterE == this->lists.end())
        return false;
    else
    {
        auto iterC    = candidates.begin();
        auto iterCEnd = candidates.end();
        auto iterL    = iterE->second.begin();
        auto iterLEnd = iterE->second.end();

        while ((iterC != iterCEnd) && (iterL != iterLEnd))
        {
            if (iterL->id < *iterC)
                iterL++;
            else if (iterL->id > *iterC)
                iterC++;
            else
            {
                result.push_back(iterL->id);
                iterL++;
            }
        }
        
        return (!result.empty());
    }
}


bool TemporalInvertedFile::intersectAndOutputAndErase(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result)
{
    for (auto prev = 0; prev < off; prev++)
    {
        if (this->lists.find(q.elems[prev]) == this->lists.end()) 
            return false;
    }

    auto iterE = this->lists.find(q.elems[off]);
    
    if (iterE == this->lists.end())
        return false;
    else
    {
        auto iterC    = candidates.begin();
        auto iterL    = iterE->second.begin();
        auto iterLEnd = iterE->second.end();

        while ((iterC != candidates.end()) && (iterL != iterLEnd))
        {
            if (iterL->id < *iterC)
                iterL++;
            else if (iterL->id > *iterC)
                iterC++;
            else
            {
                //cout << "TODO" << endl;
                result.push_back(iterL->id);
                iterL++;
                iterC = candidates.erase(iterC);
            }
        }
        
        return (!result.empty());
    }
}



void TemporalInvertedFile::query(const RangeIRQuery &q, RelationId &result)
{
    RelationId candidates;
    auto numQueryElements = q.elems.size();
    
    if (numQueryElements == 1)
        this->moveOut_CheckBoth(q, result);
    else
    {
        if (!this->moveOut_CheckBoth(q, candidates))
            return;
        
        for (auto i = 1; i < numQueryElements-1; i++)
        {
            if (!this->intersect(q, i, candidates))
                return;
        }
        
        this->intersectAndOutput(q, numQueryElements-1, candidates, result);
    }
}


void TemporalInvertedFile::update(const IRelation &R)
{
    // Assumption: Update records have monotonically increasing IDs (higher than existing records)
    for (const auto &r : R)
    {
        this->index(r);
    }
}


void TemporalInvertedFile::remove(const vector<bool> &idsToDelete)
{
    // Remove records from all posting lists
    for (auto &[tid, list] : this->lists)
    {
        list.erase(
            remove_if(list.begin(), list.end(),
                [&idsToDelete](const Record &rec) { return inDeleteSet(rec.id, idsToDelete); }),
            list.end());
    }
}


void TemporalInvertedFile::softdelete(const vector<bool> &idsToDelete)
{
    // Replace matching record IDs with tombstone (-1) in all posting lists
    for (auto &[tid, list] : this->lists)
    {
        for (auto &rec : list)
        {
            if (inDeleteSet(rec.id, idsToDelete))
                rec.id = -1;
        }
    }
}
