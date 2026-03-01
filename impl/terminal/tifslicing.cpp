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

#include "tifslicing.h"
#include <unordered_set>


string tIFSLICING::str(const int l) const
{
    if (this->partitions == 0)
        return "tIF-SLICING dynbase=" + to_string(this->dynbase) 
            + " dynratio=" + to_string(this->dynratio);
    else
        return "tIF-SLICING p=" + to_string(this->partitions);
}


tIFSLICING::tIFSLICING()
{
}


tIFSLICING::tIFSLICING(
        const Boundaries &boundaries,
        const uint &partitions,
        const double &dynbase,
        const double &dynratio,
        const IRelation &O)
    : partitions(partitions), dynratio(dynratio), dynbase(dynbase)
{
    if ((partitions == 0 && (dynratio == 0.0 || dynbase == 0.0)) 
     || (partitions != 0 && (dynratio != 0.0 || dynbase != 0.0)))
        throw runtime_error("tIFSLICING: either (dynbase and dynratio) or partitions must be set, but not both.");

    this->time_start = boundaries.time_start;
    this->elem_min = boundaries.elem_min;
    this->elem_max = boundaries.elem_max;
    unordered_map<ElementId, Relation> tmplists;

    // Step 3: fill temporary postings lists
    for (const auto &o : O)
    {
        for (const auto &tid : o.elements)
        {
            if (tid > elem_max) continue;
            if (tid < elem_min) break;

            auto &list = tmplists[tid];

            list.gstart = min(list.gstart, o.start - this->time_start);
            list.gend = max(list.gend, o.end - this->time_start);
            list.emplace_back(o.id, o.start - this->time_start, o.end - this->time_start);
        }
    }
    
    // Step 4: construct grids
    if (this->partitions != 0)
        for (auto iterL = tmplists.begin(); iterL != tmplists.end(); iterL++)
        {
            this->lists.insert(make_pair(iterL->first,
                new OneDimensionalGrid(iterL->second, this->partitions)));
        }
    else
        for (auto iterL = tmplists.begin(); iterL != tmplists.end(); iterL++)
        {
            uint p = max(1u, static_cast<uint>(round(
                pow(10, (log10(iterL->second.size() / pow(10, this->dynbase))) * this->dynratio))));

            //if (iterL->first == 1 
            //    || iterL->first == 10 
            //    || iterL->first == 100
            //    || iterL->first == 1000
            //    || iterL->first == 10000
            //    || iterL->first == 100000)
            //cout << "Element " << iterL->first 
            //     << " with " << iterL->second.size() 
            //     << " records gets " << p << " partitions." << endl;
            this->lists.insert(make_pair(iterL->first,
                new OneDimensionalGrid(iterL->second, p)));
        }

    tmplists.clear();
}


void tIFSLICING::getStats()
{
}


size_t tIFSLICING::getSize()
{
    size_t size = 0;
    
    // Member variables
    size += sizeof(uint);       // partitions
    size += sizeof(double);     // dynratio
    size += sizeof(double);     // dynbase
    size += sizeof(ElementId);  // elem_min
    size += sizeof(ElementId);  // elem_max
    
    // Size of the unordered_map structure itself
    size += sizeof(this->lists);
    
    // Map overhead: bucket array and nodes
    size += this->lists.bucket_count() * sizeof(void*);
    
    for (auto iterL = this->lists.begin(); iterL != this->lists.end(); iterL++)
    {
        // Size of the key-value pair in the map (includes map node overhead)
        size += sizeof(ElementId) + sizeof(OneDimensionalGrid*);
        
        // Size of the grid structure
        size += iterL->second->getSize();
    }
    
    return size;
}


tIFSLICING::~tIFSLICING()
{
    for (auto iterL = this->lists.begin(); iterL != this->lists.end(); iterL++)
        delete iterL->second;
}


void tIFSLICING::update(const IRelation &R)
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
    
    // Step 2: for each element with new records, create or rebuild grid
    for (auto iterL = tmplists.begin(); iterL != tmplists.end(); iterL++)
    {
        auto existingGrid = this->lists.find(iterL->first);
        
        if (existingGrid == this->lists.end())
        {
            // Create new grid for this element
            if (this->partitions != 0)
            {
                this->lists.insert(make_pair(iterL->first,
                    new OneDimensionalGrid(iterL->second, this->partitions)));
            }
            else
            {
                uint p = max(1u, static_cast<uint>(round(
                    pow(10, (log10(iterL->second.size() / pow(10, this->dynbase))) * this->dynratio))));
                this->lists.insert(make_pair(iterL->first,
                    new OneDimensionalGrid(iterL->second, p)));
            }
        }
        else
        {
            // Check if domain changed to determine update strategy
            Relation &newRecords = iterL->second;
            OneDimensionalGrid* grid = existingGrid->second;
            
            bool domain_changed = (newRecords.gstart < grid->gstart) || (newRecords.gend > grid->gend);
            
            if (!domain_changed)
            {
                // Efficient incremental update: just add new records to existing partitions
                // Domain unchanged, so existing partition boundaries still work
                for (const auto &rec : newRecords)
                {
                    grid->updatePartitions(rec);
                }
            }
            else
            {
                // Domain changed: need to repartition
                // Extract existing records, merge with new ones, and rebuild
                Relation records;
                grid->extractRecords(records);
                
                // Merge new records into existing data (monotonic ID assumption: just append)
                for (const auto &rec : newRecords)
                {
                    records.push_back(rec);
                }
                
                // Update domain
                records.gstart = min(records.gstart, newRecords.gstart);
                records.gend = max(records.gend, newRecords.gend);
                
                // No sorting needed: extractRecords returns sorted IDs, new IDs are monotonically increasing
                
                // Rebuild grid with merged data
                OneDimensionalGrid* oldGrid = grid;
                
                if (this->partitions != 0)
                {
                    existingGrid->second = new OneDimensionalGrid(records, this->partitions);
                }
                else
                {
                    uint p = max(1u, static_cast<uint>(round(
                        pow(10, (log10(records.size() / pow(10, this->dynbase))) * this->dynratio))));
                    existingGrid->second = new OneDimensionalGrid(records, p);
                }
                
                delete oldGrid;
            }
        }
    }
    
    tmplists.clear();
}


void tIFSLICING::remove(const vector<bool> &idsToDelete)
{
    // For each element, extract records, filter out deleted ones, and rebuild grid
    for (auto &[tid, grid] : this->lists)
    {
        // Extract all records from existing grid
        Relation records;
        grid->extractRecords(records);
        
        // Filter out records to delete
        records.erase(
            remove_if(records.begin(), records.end(),
                [&idsToDelete](const Record &rec) { return inDeleteSet(rec.id, idsToDelete); }),
            records.end());
        
        // If no records left, skip (could delete element entirely)
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
        
        // Rebuild grid with filtered data
        OneDimensionalGrid* oldGrid = grid;
        
        if (this->partitions != 0)
        {
            this->lists[tid] = new OneDimensionalGrid(records, this->partitions);
        }
        else
        {
            uint p = max(1u, static_cast<uint>(round(
                pow(10, (log10(records.size() / pow(10, this->dynbase))) * this->dynratio))));
            this->lists[tid] = new OneDimensionalGrid(records, p);
        }
        
        delete oldGrid;
    }
}


void tIFSLICING::softdelete(const vector<bool> &idsToDelete)
{
    // Propagate soft-delete to all grid structures
    for (auto &[tid, grid] : this->lists)
    {
        grid->softdelete(idsToDelete);
    }
}


void tIFSLICING::print(char c)
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
bool tIFSLICING::moveOut_checkBoth(const RangeIRQuery &q, RelationId &candidates)
{
    auto iterL = this->lists.find(q.elems[0]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterL == this->lists.end())
        return false;
    else
    {
        iterL->second->moveOut_checkBoth(RangeQuery(q.id, q.start, q.end), candidates);
                
        return (!candidates.empty());
    }
}


bool tIFSLICING::intersectx(const RangeIRQuery &q, const unsigned int off, RelationId &candidates)
{
    auto iterL = this->lists.find(q.elems[off]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterL == this->lists.end())
        return false;
    else
    {
        iterL->second->interesect(RangeQuery(q.id, q.start, q.end), candidates);
                
        return (!candidates.empty());
    }
}


bool tIFSLICING::intersectAndOutput(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result)
{
    auto iterL = this->lists.find(q.elems[off]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterL == this->lists.end())
        return false;
    else
    {
        iterL->second->interesectAndOutput(RangeQuery(q.id, q.start, q.end), candidates, result);
                
        return (!result.empty());
    }
}


void tIFSLICING::move_out(
    const RangeIRQuery &qo,
    ElementId &elem_off,
    RelationId &result)
{
    RangeIRQuery q(qo.id, qo.start, qo.end);
    q.elems = qo.elems;
    
    auto numQueryElements = q.elems.size();
    
    if (numQueryElements == 1)
    {
        this->moveOut_checkBoth(q, result);
        ++elem_off;
    }
    else
    {
        RelationId candidates;

        if (!this->moveOut_checkBoth(q, candidates))
            return;

        ++elem_off;

        for (; elem_off < numQueryElements-1; elem_off++)
        {
            if (q.elems[elem_off] < this->elem_min)
            {
                result.swap(candidates);
                return;
            }

            sort(candidates.begin(), candidates.end());

            if (!this->intersectx(q, elem_off, candidates))
                return;
        }

        if (q.elems[elem_off] < this->elem_min)
        {
            result.swap(candidates);
            return;
        }

        sort(candidates.begin(), candidates.end());

        this->intersectAndOutput(q, numQueryElements-1, candidates, result);
        ++elem_off;
    }
}


void tIFSLICING::refine(
        const RangeIRQuery &qo,
        ElementId &elem_off,
        RelationId &result)
{
    RangeIRQuery q(qo.id, qo.start, qo.end);
    q.elems = qo.elems;

    ElementId elem = q.elems[elem_off];
    do
    {
        auto iterL = this->lists.find(elem);
        if (iterL == this->lists.end())
        {
            result.clear();
            return;
        }
        else
        {
            sort(result.begin(), result.end());

            iterL->second->interesect(RangeQuery(-1, q.start, q.end), result);

            if (result.empty() || elem_off == q.elems.size()-1)
            {
                ++elem_off;
                return;
            }
        }

        ++elem_off;
        elem = q.elems[elem_off];
    }
    while (elem >= this->elem_min);
}