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

#include "tif.h"
#include <unordered_set>


string tIF::str(const int l) const
{
    return "tIF";
}


tIF::tIF()
{
}


// Undecided if we should use two passes
tIF::tIF(const Boundaries &boundaries, const IRelation &R)
{
    this->elem_min = boundaries.elem_min;
    this->elem_max = boundaries.elem_max;
    for (auto &r : R) this->index(r);
}


void tIF::index(const IRecord &r)
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


void tIF::index(const IRecord &r, const ElementId &min, const ElementId &max)
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


void tIF::getStats()
{
}


size_t tIF::getSize()
{
    size_t size = 0;
    
    // Member variables
    size += sizeof(ElementId);  // elem_min
    size += sizeof(ElementId);  // elem_max
    
    // Size of the unordered_map structure itself
    size += sizeof(this->lists);
    
    // Map overhead: bucket array and nodes
    size += this->lists.bucket_count() * sizeof(void*);
    
    for (auto iter = this->lists.begin(); iter != this->lists.end(); iter++)
    {
        // Size of the key-value pair in the map (includes map node overhead)
        size += sizeof(ElementId) + sizeof(Relation);
        
        // Size of the actual records in the posting list
        size += iter->second.size() * sizeof(Record);
    }
    
    return size;
}


tIF::~tIF()
{
}


// Updating
void tIF::update(const IRelation &R)
{
    // Assumption: Update records have monotonically increasing IDs (higher than existing records)
    for (const auto &r : R)
    {
        for (const auto &tid : r.elements)
        {
            if (tid > elem_max) continue;
            if (tid < elem_min) break;

            Relation &list = this->lists[tid];
            list.emplace_back(r.id, r.start, r.end);
            
            // Update global start and end
            if (list.gstart > r.start)
                list.gstart = r.start;
            if (list.gend < r.end)
                list.gend = r.end;
        }
    }
}


void tIF::remove(const vector<bool> &idsToDelete)
{
    // Remove records by ID from posting lists
    for (auto &[tid, list] : this->lists)
    {
        // Remove records with matching IDs
        list.erase(
            remove_if(list.begin(), list.end(),
                [&idsToDelete](const Record &rec) { return inDeleteSet(rec.id, idsToDelete); }),
            list.end());
        
        // Recompute domain boundaries
        if (!list.empty())
        {
            list.gstart = numeric_limits<Timestamp>::max();
            list.gend = numeric_limits<Timestamp>::min();
            for (const auto &rec : list)
            {
                list.gstart = min(list.gstart, rec.start);
                list.gend = max(list.gend, rec.end);
            }
        }
    }
}


void tIF::softdelete(const vector<bool> &idsToDelete)
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


void tIF::print(char c)
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
bool tIF::moveOut_CheckBoth(const RangeIRQuery &q, RelationId &candidates)
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


bool tIF::moveOut_CheckStart(const RangeIRQuery &q, RelationId &candidates)
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


bool tIF::moveOut_CheckEnd(const RangeIRQuery &q, RelationId &candidates)
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


bool tIF::moveOut_NoChecks(const RangeIRQuery &q, RelationId &candidates)
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


bool tIF::intersectx(const RangeIRQuery &q, const unsigned int off, RelationId &candidates)
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


bool tIF::intersectAndOutput(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result)
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


bool tIF::intersectAndOutputAndErase(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result)
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
                // TODO
                result.push_back(iterL->id);
                iterL++;
                iterC = candidates.erase(iterC);
            }
        }
        
        return (!result.empty());
    }
}


void tIF::move_out(
    const RangeIRQuery &q,
    ElementId &elem_off,
    RelationId &result)
{
    RelationId candidates;
    auto numQueryElements = q.elems.size();
    
    if (numQueryElements == 1)
    {
        this->moveOut_CheckBoth(q, result);
        ++elem_off;
    }
    else
    {
        if (!this->moveOut_CheckBoth(q, candidates))
            return;

        ++elem_off;
        
        for (; elem_off < numQueryElements-1; elem_off++)
        {
            if (q.elems[elem_off] < this->elem_min)
            {
                result.swap(candidates);
                return;
            }

            if (!this->intersectx(q, elem_off, candidates))
                return;
        }

        if (q.elems[elem_off] < this->elem_min)
        {
            result.swap(candidates);
            return;
        }
        
        this->intersectAndOutput(q, numQueryElements-1, candidates, result);
        ++elem_off;
    }
}


void tIF::refine(
        const RangeIRQuery &q,
        ElementId &elem_off,
        RelationId &result)
{
    do
    {
        if (!this->intersectx(q, elem_off, result)) return;

        ++elem_off;
    }
    while (q.elems[elem_off] >= this->elem_min);
}

