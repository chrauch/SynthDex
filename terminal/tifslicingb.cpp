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

#include "tifslicingb.h"
#include <unordered_set>


string tIFSLICINGb::str(const int l) const
{
    if (this->partitions == 0)
        return "tIF-SLICING-B dynbase=" + to_string(this->dynbase) 
            + " dynratio=" + to_string(this->dynratio);
    else
        return "tIF-SLICING-B p=" + to_string(this->partitions);
}


tIFSLICINGb::tIFSLICINGb()
{
}


tIFSLICINGb::tIFSLICINGb(
        const Boundaries &boundaries,
        const uint &partitions,
        const double &dynbase,
        const double &dynratio,
        const IRelation &O)
    : partitions(partitions), dynratio(dynratio), dynbase(dynbase)
{
    if ((partitions == 0 && (dynratio == 0.0 || dynbase == 0.0)) 
     || (partitions != 0 && (dynratio != 0.0 || dynbase != 0.0)))
        throw runtime_error("tIFSLICINGb: either (dynbase and dynratio) or partitions must be set, but not both.");

    this->time_start = boundaries.time_start;
    this->elem_min = boundaries.elem_min;
    this->elem_max = boundaries.elem_max;
    unordered_map<ElementId, Relation> tmplists;

    // Step 3: fill temporary postings lists
    ElementId observed_min = numeric_limits<ElementId>::max();
    ElementId observed_max = numeric_limits<ElementId>::min();
    for (const auto &o : O)
    {
        for (const auto &tid : o.elements)
        {
            if (tid > elem_max) continue;
            if (tid < elem_min) break;

            observed_min = min(observed_min, tid);
            observed_max = max(observed_max, tid);

            auto &list = tmplists[tid];

            list.gstart = min(list.gstart, o.start - this->time_start);
            list.gend = max(list.gend, o.end - this->time_start);
            list.emplace_back(o.id, o.start - this->time_start, o.end - this->time_start);
        }
    }

    // Allocate vector based on observed element range
    if (tmplists.empty())
    {
        this->lists_offset = 0;
    }
    else
    {
        this->lists_offset = observed_min;
        this->lists.assign(observed_max - observed_min + 1, nullptr);
    }
    
    // Step 4: construct grids
    if (this->partitions != 0)
        for (auto iterL = tmplists.begin(); iterL != tmplists.end(); iterL++)
        {
            this->lists[iterL->first - this->lists_offset] =
                new OneDimensionalGrid(iterL->second, this->partitions);
        }
    else
        for (auto iterL = tmplists.begin(); iterL != tmplists.end(); iterL++)
        {
            uint p = max(1u, static_cast<uint>(round(
                pow(10, (log10(iterL->second.size() / pow(10, this->dynbase))) * this->dynratio))));

            this->lists[iterL->first - this->lists_offset] =
                new OneDimensionalGrid(iterL->second, p);
        }

    tmplists.clear();
}


void tIFSLICINGb::getStats()
{
}


size_t tIFSLICINGb::getSize()
{
    size_t size = 0;
    
    // Member variables
    size += sizeof(uint);       // partitions
    size += sizeof(double);     // dynratio
    size += sizeof(double);     // dynbase
    size += sizeof(ElementId);  // elem_min
    size += sizeof(ElementId);  // elem_max
    size += sizeof(ElementId);  // lists_offset
    
    // Size of the vector structure itself
    size += sizeof(this->lists);
    
    // Vector storage: pointer per slot
    size += this->lists.size() * sizeof(OneDimensionalGrid*);
    
    for (size_t i = 0; i < this->lists.size(); i++)
    {
        if (this->lists[i] != nullptr)
        {
            // Size of the grid structure
            size += this->lists[i]->getSize();
        }
    }
    
    return size;
}


tIFSLICINGb::~tIFSLICINGb()
{
    for (size_t i = 0; i < this->lists.size(); i++)
    {
        delete this->lists[i];
    }
}


void tIFSLICINGb::update(const IRelation &R)
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
    
    // Step 2: grow vector if new elements fall outside current range
    if (!tmplists.empty())
    {
        ElementId new_min = this->lists_offset;
        ElementId new_max = this->lists_offset + (ElementId)this->lists.size() - 1;
        for (auto &[eid, _] : tmplists)
        {
            new_min = min(new_min, eid);
            new_max = max(new_max, eid);
        }
        if (this->lists.empty())
        {
            this->lists_offset = new_min;
            this->lists.assign(new_max - new_min + 1, nullptr);
        }
        else if (new_min < this->lists_offset || new_max > this->lists_offset + (ElementId)this->lists.size() - 1)
        {
            vector<OneDimensionalGrid*> newlists(new_max - new_min + 1, nullptr);
            for (size_t i = 0; i < this->lists.size(); i++)
                newlists[this->lists_offset - new_min + i] = this->lists[i];
            this->lists = std::move(newlists);
            this->lists_offset = new_min;
        }
    }

    // Step 3: for each element with new records, create or rebuild grid
    for (auto iterL = tmplists.begin(); iterL != tmplists.end(); iterL++)
    {
        size_t idx = iterL->first - this->lists_offset;
        OneDimensionalGrid* existingGrid = this->lists[idx];
        
        if (existingGrid == nullptr)
        {
            // Create new grid for this element
            if (this->partitions != 0)
            {
                this->lists[idx] =
                    new OneDimensionalGrid(iterL->second, this->partitions);
            }
            else
            {
                uint p = max(1u, static_cast<uint>(round(
                    pow(10, (log10(iterL->second.size() / pow(10, this->dynbase))) * this->dynratio))));
                this->lists[idx] =
                    new OneDimensionalGrid(iterL->second, p);
            }
        }
        else
        {
            // Check if domain changed to determine update strategy
            Relation &newRecords = iterL->second;
            OneDimensionalGrid* grid = existingGrid;
            
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
                    this->lists[idx] = new OneDimensionalGrid(records, this->partitions);
                }
                else
                {
                    uint p = max(1u, static_cast<uint>(round(
                        pow(10, (log10(records.size() / pow(10, this->dynbase))) * this->dynratio))));
                    this->lists[idx] = new OneDimensionalGrid(records, p);
                }
                
                delete oldGrid;
            }
        }
    }
    
    tmplists.clear();
}


void tIFSLICINGb::remove(const vector<bool> &idsToDelete)
{
    // For each element, extract records, filter out deleted ones, and rebuild grid
    for (size_t i = 0; i < this->lists.size(); i++)
    {
        if (this->lists[i] == nullptr)
            continue;

        // Extract all records from existing grid
        Relation records;
        this->lists[i]->extractRecords(records);
        
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
        OneDimensionalGrid* oldGrid = this->lists[i];
        
        if (this->partitions != 0)
        {
            this->lists[i] = new OneDimensionalGrid(records, this->partitions);
        }
        else
        {
            uint p = max(1u, static_cast<uint>(round(
                pow(10, (log10(records.size() / pow(10, this->dynbase))) * this->dynratio))));
            this->lists[i] = new OneDimensionalGrid(records, p);
        }
        
        delete oldGrid;
    }
}


void tIFSLICINGb::softdelete(const vector<bool> &idsToDelete)
{
    // Propagate soft-delete to all grid structures
    for (size_t i = 0; i < this->lists.size(); i++)
    {
        if (this->lists[i] != nullptr)
            this->lists[i]->softdelete(idsToDelete);
    }
}


void tIFSLICINGb::print(char c)
{
}


// Querying
bool tIFSLICINGb::moveOut_checkBoth(const RangeIRQuery &q, RelationId &candidates)
{
    ElementId elem = q.elems[0];
    if (elem < this->elem_min || elem > this->elem_max)
        return false;

    OneDimensionalGrid* grid = this->lists[elem - this->lists_offset];
    
    // If the inverted file does not contain the element then result must be empty
    if (grid == nullptr)
        return false;
    else
    {
        grid->moveOut_checkBoth(RangeQuery(q.id, q.start, q.end), candidates);
                
        return (!candidates.empty());
    }
}


bool tIFSLICINGb::intersectx(const RangeIRQuery &q, const unsigned int off, RelationId &candidates)
{
    ElementId elem = q.elems[off];
    if (elem < this->elem_min || elem > this->elem_max)
        return false;

    OneDimensionalGrid* grid = this->lists[elem - this->lists_offset];
    
    // If the inverted file does not contain the element then result must be empty
    if (grid == nullptr)
        return false;
    else
    {
        grid->interesect(RangeQuery(q.id, q.start, q.end), candidates);
                
        return (!candidates.empty());
    }
}


bool tIFSLICINGb::intersectAndOutput(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result)
{
    ElementId elem = q.elems[off];
    if (elem < this->elem_min || elem > this->elem_max)
        return false;

    OneDimensionalGrid* grid = this->lists[elem - this->lists_offset];
    
    // If the inverted file does not contain the element then result must be empty
    if (grid == nullptr)
        return false;
    else
    {
        grid->interesectAndOutput(RangeQuery(q.id, q.start, q.end), candidates, result);
                
        return (!result.empty());
    }
}


void tIFSLICINGb::move_out(
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


void tIFSLICINGb::refine(
        const RangeIRQuery &qo,
        ElementId &elem_off,
        RelationId &result)
{
    RangeIRQuery q(qo.id, qo.start, qo.end);
    q.elems = qo.elems;

    ElementId elem = q.elems[elem_off];
    do
    {
        if (elem < this->elem_min || elem > this->elem_max)
        {
            result.clear();
            return;
        }

        OneDimensionalGrid* grid = this->lists[elem - this->lists_offset];
        if (grid == nullptr)
        {
            result.clear();
            return;
        }
        else
        {
            sort(result.begin(), result.end());

            grid->interesect(RangeQuery(-1, q.start, q.end), result);

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
