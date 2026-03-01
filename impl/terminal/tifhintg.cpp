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

#include "tifhintg.h"
#include <unordered_set>


string tIFHINTg::str(const int l) const
{
    if (this->bits != 0)
        return "tIF-HINT-gamma m=" + to_string(this->bits);
    else
        return "tIF-HINT-gamma dynbase=" + to_string(this->dynbase) 
            + " dynratio=" + to_string(this->dynratio);
}


tIFHINTg::tIFHINTg()
{
}


tIFHINTg::tIFHINTg(
    const Boundaries &boundaries,
    const int &bits,
    const double &dynbase,
    const double &dynratio,
    const IRelation &R)
    : bits(bits), dynratio(dynratio), dynbase(dynbase)
{
    this->time_start = boundaries.time_start;
    this->elem_min = boundaries.elem_min;
    this->elem_max = boundaries.elem_max;

    unordered_map<ElementId, Relation> tmplists;

    // Step 3: fill temporary postings lists
    for (const auto &r : R)
    {
        for (const auto &tid : r.elements)
        {
            if (tid > elem_max) continue;
            if (tid < elem_min) break;

            auto &list = tmplists[tid];

            list.gstart = min(list.gstart, r.start - this->time_start);
            list.gend = max(list.gend, r.end - this->time_start);

            list.emplace_back(r.id, r.start - this->time_start, r.end - this->time_start);
        }
    }
    
    // Step 4: construct HINTs
    if (this->bits != 0)
        for (auto iterL = tmplists.begin(); iterL != tmplists.end(); iterL++)
        {
            this->hlists.insert(make_pair(iterL->first, new HINT_M_SubsSortByRecordId_CM(iterL->second, bits)));
        }
    else
        for (auto iterL = tmplists.begin(); iterL != tmplists.end(); iterL++)
        {
            double m_double = log10(iterL->second.size() / pow(10, this->dynbase)) * this->dynratio;
            uint m = max(1u, static_cast<uint>(round(max(1.0, m_double))));

            //if (iterL->first == 1 
            //    || iterL->first == 10 
            //    || iterL->first == 100
            //    || iterL->first == 1000
            //    || iterL->first == 10000
            //    || iterL->first == 100000)
            //cout << "Element " << iterL->first 
            //     << " with " << iterL->second.size() 
            //     << " records gets " << m << " levels." << endl;

            this->hlists.insert(make_pair(iterL->first, new HINT_M_SubsSortByRecordId_CM(iterL->second, m)));
        }

    tmplists.clear();
}


void tIFHINTg::getStats()
{
}


size_t tIFHINTg::getSize()
{
    size_t size = 0;
    
    // Member variables
    size += sizeof(int);        // bits
    size += sizeof(double);     // dynbase
    size += sizeof(double);     // dynratio
    size += sizeof(ElementId);  // elem_min
    size += sizeof(ElementId);  // elem_max
    
    // Size of the unordered_map structure itself
    size += sizeof(this->hlists);
    
    // Map overhead: bucket array and nodes
    size += this->hlists.bucket_count() * sizeof(void*);
    
    // Size of HINT lists
    for (auto iterL = this->hlists.begin(); iterL != this->hlists.end(); iterL++)
    {
        // Size of the key-value pair in the map (includes map node overhead)
        size += sizeof(ElementId) + sizeof(HINT_M_SubsSortByRecordId_CM*);
        
        // Size of the HINT structure
        size += iterL->second->getSize();
    }
    
    return size;
}


tIFHINTg::~tIFHINTg()
{
    for (auto iterL = this->hlists.begin(); iterL != this->hlists.end(); iterL++)
    {
        delete iterL->second;
        iterL->second = nullptr;
    }
    this->hlists.clear();
}


// Updating
void tIFHINTg::update(const IRelation &R)
{
    // Assumption: Update records have monotonically increasing IDs (higher than existing records)
    unordered_map<ElementId, Relation> tmplists;

    // Step 1: fill temporary postings lists with new records
    for (const auto &r : R)
    {
        for (const auto &tid : r.elements)
        {
            if (tid > elem_max) continue;
            if (tid < elem_min) break;

            auto &list = tmplists[tid];

            list.gstart = min(list.gstart, r.start - this->time_start);
            list.gend = max(list.gend, r.end - this->time_start);
            list.emplace_back(r.id, r.start - this->time_start, r.end - this->time_start);
        }
    }
    
    // Step 2: for each element with new records, extract, merge, and rebuild HINT
    for (auto iterL = tmplists.begin(); iterL != tmplists.end(); iterL++)
    {
        auto existingHint = this->hlists.find(iterL->first);
        
        if (existingHint == this->hlists.end())
        {
            // Create new HINT for this element
            if (this->bits != 0)
            {
                this->hlists.insert(make_pair(iterL->first,
                    new HINT_M_SubsSortByRecordId_CM(iterL->second, this->bits)));
            }
            else
            {
                double m_double = log10(iterL->second.size() / pow(10, this->dynbase)) * this->dynratio;
                uint m = max(1u, static_cast<uint>(round(max(1.0, m_double))));
                this->hlists.insert(make_pair(iterL->first,
                    new HINT_M_SubsSortByRecordId_CM(iterL->second, m)));
            }
        }
        else
        {
            // Check if domain changed to determine update strategy
            Relation &newRecords = iterL->second;
            HINT_M_SubsSortByRecordId_CM* hint = existingHint->second;
            
            bool domain_changed = (newRecords.gstart < hint->gstart) || (newRecords.gend > hint->gend);
            
            if (!domain_changed)
            {
                // Efficient incremental update: just insert new records into existing HINT
                // Domain unchanged, so existing partitioning still works
                for (const auto &rec : newRecords)
                {
                    hint->insert(rec);
                }
            }
            else
            {
                // Domain changed: need to repartition
                // Extract existing records, merge with new ones, and rebuild
                Relation records;
                hint->extractRecords(records);
                
                // Merge new records (monotonic IDs assumption: just append)
                for (const auto &rec : newRecords)
                {
                    records.push_back(rec);
                }
                
                // Update domain
                records.gstart = min(records.gstart, newRecords.gstart);
                records.gend = max(records.gend, newRecords.gend);
                
                // Rebuild HINT with merged data (partitioning may change with new size)
                HINT_M_SubsSortByRecordId_CM* oldHint = hint;
                
                if (this->bits != 0)
                {
                    existingHint->second = new HINT_M_SubsSortByRecordId_CM(records, this->bits);
                }
                else
                {
                    // Dynamic partitioning: number of levels depends on total record count
                    double m_double = log10(records.size() / pow(10, this->dynbase)) * this->dynratio;
                    uint m = max(1u, static_cast<uint>(round(max(1.0, m_double))));
                    existingHint->second = new HINT_M_SubsSortByRecordId_CM(records, m);
                }
                
                delete oldHint;
            }
        }
    }
    
    tmplists.clear();
}


void tIFHINTg::remove(const vector<bool> &idsToDelete)
{
    // For each element, extract records, filter out deleted ones, and rebuild HINT
    for (auto &[tid, hint] : this->hlists)
    {
        // Extract all records from existing HINT
        Relation records;
        hint->extractRecords(records);
        
        // Filter out records to delete
        records.erase(
            remove_if(records.begin(), records.end(),
                [&idsToDelete](const Record &rec) { return inDeleteSet(rec.id, idsToDelete); }),
            records.end());
        
        // If no records left, we could remove this element entirely, but keep empty HINT for now
        if (records.empty())
            continue;
        
        // Recompute domain
        records.gstart = numeric_limits<Timestamp>::max();
        records.gend = numeric_limits<Timestamp>::min();
        for (const auto &rec : records)
        {
            records.gstart = min(records.gstart, rec.start);
            records.gend = max(records.gend, rec.end);
        }
        
        // Rebuild HINT with filtered data
        HINT_M_SubsSortByRecordId_CM* oldHint = hint;
        
        if (this->bits != 0)
        {
            this->hlists[tid] = new HINT_M_SubsSortByRecordId_CM(records, this->bits);
        }
        else
        {
            double m_double = log10(records.size() / pow(10, this->dynbase)) * this->dynratio;
            uint m = max(1u, static_cast<uint>(round(max(1.0, m_double))));
            this->hlists[tid] = new HINT_M_SubsSortByRecordId_CM(records, m);
        }
        
        delete oldHint;
    }
}


void tIFHINTg::softdelete(const vector<bool> &idsToDelete)
{
    // Propagate soft-delete to all HINT structures
    for (auto &[tid, hint] : this->hlists)
    {
        hint->softdelete(idsToDelete);
    }
}


void tIFHINTg::print(char c)
{
//    for (auto iter = this->dlists.begin(); iter != this->dlists.end(); iter++)
//    {
//        cout << "t" << iter->first << " (" << iter->second.size() << "):";
//        for (auto &r : iter->second)
//            cout << " " << c << r.id;
//        cout << endl;
//    }
}


// Querying
bool tIFHINTg::moveOut(const RangeIRQuery &q, RelationId &candidates)
{
    auto iterL = this->hlists.find(q.elems[0]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterL == this->hlists.end())
        return false;
    else
    {
        iterL->second->moveOut(RangeQuery(q.id, q.start, q.end), candidates);

        return (!candidates.empty());
    }
}


bool tIFHINTg::intersect(const RangeIRQuery &q, const unsigned int off, RelationId &candidates)
{
    auto iterL = this->hlists.find(q.elems[off]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterL == this->hlists.end())
        return false;
    else
    {
        iterL->second->intersect(RangeQuery(q.id, q.start, q.end), candidates);

        return (!candidates.empty());
    }
}


bool tIFHINTg::intersectAndOutput(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result)
{
    auto iterL = this->hlists.find(q.elems[off]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterL == this->hlists.end())
        return false;
    else
    {
        iterL->second->intersectAndOutput(RangeQuery(q.id, q.start, q.end), candidates, result);

        return (!result.empty());
    }
}


bool tIFHINTg::moveOut(const RangeIRQuery &q, vector<RelationId> &vec_candidates)
{
    auto iterL = this->hlists.find(q.elems[0]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterL == this->hlists.end())
        return false;
    else
    {
        iterL->second->moveOut(RangeQuery(q.id, q.start, q.end), vec_candidates);

        return (!vec_candidates.empty());
    }
}


bool tIFHINTg::moveOut_NoChecks(const RangeIRQuery &q, const unsigned int off, vector<RelationId> &vec_candidates)
{
    auto iterL = this->hlists.find(q.elems[off]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterL == this->hlists.end())
        return false;
    else
    {
        iterL->second->moveOut_NoChecks(RangeQuery(q.id, q.start, q.end), vec_candidates);

        return (!vec_candidates.empty());
    }
}

bool tIFHINTg::intersect(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, vector<RelationId> &vec_candidates)
{
    auto iterL = this->hlists.find(q.elems[off]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterL == this->hlists.end())
        return false;
    else
    {
        iterL->second->intersect(RangeQuery(q.id, q.start, q.end), candidates, vec_candidates);

        return (!candidates.empty());
    }
}


class compareQueueElements
{
public:
  bool operator() (const pair<RelationIdIterator,RelationIdIterator> &lhs, const pair<RelationIdIterator,RelationIdIterator> &rhs) const
  {
      return (*(lhs.first) > *(rhs.first));
  }
};
inline void mwayMergeSort(vector<RelationId> &vec_candidates, RelationId &result)
{
    if (vec_candidates.empty())
    {
        result.clear();
        return;
    }
    result.swap(vec_candidates[0]);
    for (auto i = 1; i < vec_candidates.size(); i++)
    {
        result.insert(result.begin(), vec_candidates[i].begin(), vec_candidates[i].end());
        inplace_merge(result.begin(), result.begin()+vec_candidates[i].size(), result.end());
    }
}



inline void mergeSort(RelationId &candidates, RelationId &divisions)
{
    auto iterD = divisions.begin();
    auto iterDEnd = divisions.end();
    auto iterC    = candidates.begin();
    auto iterCEnd = candidates.end();
    RelationId result;

    while ((iterD != iterDEnd) && (iterC != iterCEnd))
    {
        if (*iterD < *iterC)
            iterD++;
        else if (*iterD > *iterC)
            iterC++;
        else
        {
            result.push_back(*iterD);
            iterD++;
            
            iterC++;
        }
    }
    
    candidates.swap(result);
}


void tIFHINTg::move_out(
        const RangeIRQuery &qo,
        ElementId &elem_off,
        RelationId &result)
{
    RangeIRQuery q(qo.id, qo.start, qo.end);
    q.elems = qo.elems;
    
    //cout << "tIFHINTg.1" << endl;
    auto numQueryElements = q.elems.size();
    
    if (numQueryElements == 1)
    {
        this->moveOut(q, result);
        ++elem_off;
    }
    else
    {
        RelationId candidates;
        vector<RelationId> vec_candidates;

        if (!this->moveOut(q, vec_candidates))
            return;

        ++elem_off;
        
        mwayMergeSort(vec_candidates, candidates);

        for (; elem_off < numQueryElements-1; elem_off++)
        {
            if (q.elems[elem_off] < this->elem_min)
            {
                result.swap(candidates);
                return;
            }

            vec_candidates.clear();
            if (!this->intersect(q, elem_off, candidates, vec_candidates))
                return;
            
            mwayMergeSort(vec_candidates, candidates);
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


void tIFHINTg::refine(
        const RangeIRQuery &qo,
        ElementId &elem_off,
        RelationId &result)
{
    RangeIRQuery q(qo.id, qo.start, qo.end);
    q.elems = qo.elems;
    
    ElementId elem = q.elems[elem_off];
    do
    {
        auto iterL = this->hlists.find(elem);
        if (iterL == this->hlists.end())
        {
            result.clear();
            return;
        }
        else
        {
            sort(result.begin(), result.end());
            iterL->second->intersect(RangeQuery(-1, q.start, q.end), result);
            if (result.empty()) return;
        }

        ++elem_off;

        if (elem_off == q.elems.size()) return;

        elem = q.elems[elem_off];
    }
    while (elem >= this->elem_min);
}
