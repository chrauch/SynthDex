/******************************************************************************
 * Project:  synthdex
 * Purpose:  Adaptive TIR indexing
 * Author:   Christian Rauch
 ******************************************************************************
 * Copyright (c) 2025 - 2026
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

#include "../structure/framework.h"


void RefinementFanout_ElemFreq::move_out(
    const RangeIRQuery &qo, 
    ElementId &elem_offo,
    RelationId &result)
{
    if (qo.start > this->end) return;

    int elem_off = elem_offo;

    auto elem_first = qo.elems.front();
    auto elem_last = qo.elems.back();

    for (const auto &r : this->refinements)
    {
        const auto &b = get<0>(r);
        if (   b.elem_min   <= elem_last 
            && b.elem_max   >= elem_first)
        {
            auto* refinement = get<1>(r);
            refinement->move_out(qo, elem_off, result);
            return;
        }
    }

    auto refine_iter = this->refinements.cbegin();
    auto refine_end  = this->refinements.cend();
    while (refine_iter != refine_end)
    {
        const auto &b = get<0>(*refine_iter);

        if (b.elem_min <= elem_first)
        {
            auto* refinement = get<1>(*refine_iter);
            refinement->move_out(qo, elem_off, result);
            if (result.empty()) return;
            ++refine_iter;
            break;
        }
        
        ++refine_iter;
    }

    while (refine_iter != refine_end)
    {
        const auto &b = get<0>(*refine_iter);

        if (b.elem_min <= qo.elems[elem_off])
        {
            auto* refinement = get<1>(*refine_iter);
            refinement->refine(qo, elem_off, result);
            if (elem_off == qo.elems.size() || result.empty()) return;
        }
        
        ++refine_iter;
    }
}


void RefinementFanout_ElemFreq::update(const IRelation &R)
{
    // Propagate updates to all child refinement nodes
    for (const auto &[boundaries, refinement] : this->refinements)
    {
        refinement->update(R);
    }
}


void RefinementFanout_ElemFreq::remove(const RelationId &ids)
{
    // Propagate deletions to all child refinement nodes
    for (const auto &[boundaries, refinement] : this->refinements)
    {
        refinement->remove(ids);
    }
}


RefinementFanout_ElemFreq::~RefinementFanout_ElemFreq()
{
    for (auto &refine : this->refinements)
    {
        delete get<1>(refine);
    }
}


void SplitFanout_Temporal::move_out(
    const RangeIRQuery &q, 
    ElementId &elem_off, 
    RelationId &result)
{
    // Create query once and reuse
    RangeIRQuery q_mv(q.id, 0, 0);
    q_mv.elems.insert(q_mv.elems.end(),
        q.elems.begin() + elem_off, q.elems.end());
    
    for (const auto &page : this->pages)
    {
        const auto &b = get<0>(page);
        if (q.end < b.time_start || q.start > b.time_end) continue;

        // Adjust the time range for each page
        q_mv.start = max(b.time_start, q.start) - b.time_start;
        q_mv.end = min(b.time_end, q.end) - b.time_start;

        ElementId elem_offx = 0;
        auto* page_node = get<1>(page);
        page_node->move_out(q_mv, elem_offx, result);
    }

    elem_off = q.elems.size();
}


void SplitFanout_Temporal::update(const IRelation &R)
{
    // Propagate updates to all temporal pages
    for (const auto &[boundaries, page] : this->pages)
    {
        page->update(R);
    }
}


void SplitFanout_Temporal::remove(const RelationId &ids)
{
    // Propagate deletions to all temporal pages
    for (const auto &[boundaries, page] : this->pages)
    {
        page->remove(ids);
    }
}


SplitFanout_Temporal::~SplitFanout_Temporal()
{
    for (auto &page : this->pages)
    {
        delete get<1>(page);
    }
}


void SliceFanout_Temporal::move_out(
    const RangeIRQuery &q,
    ElementId &elem_off,
    RelationId &result)
{
    // First, count how many partitions overlap with the query
    int overlapping_count = 0;
    const tuple<Boundaries, Moveout*>* overlapping_page = nullptr;
    
    for (const auto &page : this->pages)
    {
        const auto &b = get<0>(page);
        if (q.end < b.time_start || q.start > b.time_end) continue;
        
        overlapping_count++;
        if (overlapping_count == 1)
        {
            overlapping_page = &page;
        }
        else
        {
            // Multiple pages overlap, break early
            break;
        }
    }
    
    // Fast path: only one partition accessed
    if (overlapping_count == 1)
    {
        const auto &b = get<0>(*overlapping_page);
        auto* page_node = get<1>(*overlapping_page);
        
        RangeIRQuery q_mv(q.id, 0, 0);
        q_mv.elems.insert(q_mv.elems.end(),
            q.elems.begin() + elem_off, q.elems.end());
        q_mv.start = max(b.time_start, q.start) - b.time_start;
        q_mv.end = min(b.time_end, q.end) - b.time_start;
        
        ElementId elem_offx = 0;
        page_node->move_out(q_mv, elem_offx, result);
        elem_off = q.elems.size();
        return;
    }
    
    // Slow path: multiple partitions accessed, need deduplication
    if (overlapping_count > 1)
    {
        set<RecordId> unique_results;
        
        RangeIRQuery q_mv(q.id, 0, 0);
        q_mv.elems.insert(q_mv.elems.end(),
            q.elems.begin() + elem_off, q.elems.end());
        
        for (const auto &page : this->pages)
        {
            const auto &b = get<0>(page);
            if (q.end < b.time_start || q.start > b.time_end) continue;

            q_mv.start = max(b.time_start, q.start) - b.time_start;
            q_mv.end = min(b.time_end, q.end) - b.time_start;

            ElementId elem_offx = 0;
            RelationId page_result;
            auto* page_node = get<1>(page);
            page_node->move_out(q_mv, elem_offx, page_result);
            
            unique_results.insert(page_result.begin(), page_result.end());
        }

        result.clear();
        result.insert(result.end(), unique_results.begin(), unique_results.end());
    }
    
    elem_off = q.elems.size();
}


void SliceFanout_Temporal::update(const IRelation &R)
{
    // Propagate updates to all temporal pages
    // Note: With slicing, records may be in multiple partitions,
    // so the same update will be applied to all relevant partitions
    for (const auto &[boundaries, page] : this->pages)
    {
        page->update(R);
    }
}


void SliceFanout_Temporal::remove(const RelationId &ids)
{
    // Propagate deletions to all temporal pages
    // Note: With slicing, the same record may exist in multiple partitions,
    // so we remove from all partitions to ensure complete deletion
    for (const auto &[boundaries, page] : this->pages)
    {
        page->remove(ids);
    }
}


SliceFanout_Temporal::~SliceFanout_Temporal()
{
    for (auto &page : this->pages)
    {
        delete get<1>(page);
    }
}


void MoveoutRefinementHybrid::move_out(
    const RangeIRQuery &q,
    ElementId &elem_off,
    RelationId &result)
{
    // Step 1: Use moveout_node for initial moveout
    RangeIRQuery q_mv(q.id, q.start, q.end);
    q_mv.elems = {q.elems[elem_off]};
    this->moveout_node->move_out(q_mv, elem_off, result);

    // Step 2: Use refine_node for refinement on the result
    if (!result.empty())
    {
        this->refine_node->refine(q, elem_off, result);
    }
}


void MoveoutRefinementHybrid::update(const IRelation &R)
{
    this->moveout_node->update(R);
    this->refine_node->update(R);
}


void MoveoutRefinementHybrid::remove(const RelationId &ids)
{
    this->moveout_node->remove(ids);
    this->refine_node->remove(ids);
}


MoveoutRefinementHybrid::~MoveoutRefinementHybrid()
{
    delete this->moveout_node;
    delete this->refine_node;
}