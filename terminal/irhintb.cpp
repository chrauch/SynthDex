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

#include "irhintb.h"
#include <unordered_set>


// ===========================================================================
// SortedTIF, non-inline methods
// ===========================================================================

void SortedTIF::finalize()
{
    if (!counts || counts->empty()) { delete counts; counts = nullptr; return; }

    // Build sorted element list
    elem_ids.reserve(counts->size());
    for (auto& [e, c] : *counts) elem_ids.push_back(e);
    sort(elem_ids.begin(), elem_ids.end());

    // Compute prefix sums
    list_offsets.resize(elem_ids.size() + 1, 0u);
    uint32_t total = 0;
    for (size_t i = 0; i < elem_ids.size(); ++i)
    {
        list_offsets[i] = total;
        total += (*counts)[elem_ids[i]];
    }
    list_offsets[elem_ids.size()] = total;

    // Allocate SoA arrays with exact capacity
    soa_ids.resize(total);
    soa_starts.resize(total);
    soa_ends.resize(total);

    // Initialize write cursors and build O(1) element → index map for fill pass
    write_pos  = new uint32_t[elem_ids.size()];
    elem_index = new unordered_map<ElementId, uint32_t>();
    elem_index->reserve(elem_ids.size());
    for (size_t i = 0; i < elem_ids.size(); ++i)
    {
        write_pos[i]          = list_offsets[i];
        (*elem_index)[elem_ids[i]] = static_cast<uint32_t>(i);
    }

    delete counts; counts = nullptr;
}


void SortedTIF::softdelete(const vector<bool> &idsToDelete)
{
    for (RecordId &id : soa_ids)
        if (id != (RecordId)-1 && id >= 0 &&
            static_cast<size_t>(id) < idsToDelete.size() && idsToDelete[id])
            id = (RecordId)-1;
}


void SortedTIF::extractRecords(unordered_map<RecordId, IRecord> &recordMap,
                                Timestamp time_offset) const
{
    for (size_t i = 0; i < elem_ids.size(); ++i)
    {
        const ElementId eid = elem_ids[i];
        const uint32_t  lo  = list_offsets[i];
        const uint32_t  hi  = list_offsets[i + 1];

        for (uint32_t j = lo; j < hi; ++j)
        {
            const RecordId rid = soa_ids[j];
            if (rid == (RecordId)-1) continue;

            const Timestamp s = soa_starts[j] + time_offset;
            const Timestamp e = soa_ends[j]   + time_offset;

            auto it = recordMap.find(rid);
            if (it == recordMap.end())
            {
                IRecord irec(rid, s, e);
                irec.elements.push_back(eid);
                recordMap[rid] = move(irec);
            }
            else
            {
                auto& elems = it->second.elements;
                if (find(elems.begin(), elems.end(), eid) == elems.end())
                    elems.push_back(eid);
            }
        }
    }
}


size_t SortedTIF::getSize() const
{
    return sizeof(*this)
         + elem_ids.capacity()    * sizeof(ElementId)
         + list_offsets.capacity()* sizeof(uint32_t)
         + soa_ids.capacity()     * sizeof(RecordId)
         + soa_starts.capacity()  * sizeof(Timestamp)
         + soa_ends.capacity()    * sizeof(Timestamp);
}


// ===========================================================================
// irHINTb, construction helpers
// ===========================================================================

string irHINTb::str(const int l) const
{
    return "irHINT-beta m=" + to_string(this->numBits);
}


// Pass 1: count how many records land in each (partition, element) cell.
// Timestamps are normalized by time_start (== 0 in practice, asserted).
inline void irHINTb::countPartitions(const IRecord &r_org)
{
    const Timestamp start = r_org.start - this->time_start;
    const Timestamp end   = r_org.end   - this->time_start;

    int       level      = 0;
    Timestamp a          = start >> (this->maxBits - this->numBits);
    Timestamp b          = end   >> (this->maxBits - this->numBits);
    Timestamp prevb;
    int firstfound = 0, lastfound = 0;

    while (level < this->height && a <= b)
    {
        if (a % 2)
        {
            if (firstfound)
            {
                if ((a == b) && (!lastfound))
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pRepsIn[level][a].count(tid);
                    }
                    lastfound = 1;
                }
                else
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pRepsAft[level][a].count(tid);
                    }
                }
            }
            else
            {
                if ((a == b) && (!lastfound))
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pOrgsIn[level][a].count(tid);
                    }
                }
                else
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pOrgsAft[level][a].count(tid);
                    }
                }
                firstfound = 1;
            }
            a++;
        }
        if (!(b % 2))
        {
            prevb = b;
            b--;
            if ((!firstfound) && b < a)
            {
                if (!lastfound)
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pOrgsIn[level][prevb].count(tid);
                    }
                }
                else
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pOrgsAft[level][prevb].count(tid);
                    }
                }
            }
            else
            {
                if (!lastfound)
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pRepsIn[level][prevb].count(tid);
                    }
                    lastfound = 1;
                }
                else
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pRepsAft[level][prevb].count(tid);
                    }
                }
            }
        }
        a >>= 1; b >>= 1; level++;
    }
}


// Pass 2: fill SoA arrays with actual record data.
inline void irHINTb::fillPartitions(const IRecord &r_org)
{
    const Timestamp start = r_org.start - this->time_start;
    const Timestamp end   = r_org.end   - this->time_start;
    const RecordId  id    = r_org.id;

    int       level      = 0;
    Timestamp a          = start >> (this->maxBits - this->numBits);
    Timestamp b          = end   >> (this->maxBits - this->numBits);
    Timestamp prevb;
    int firstfound = 0, lastfound = 0;

    while (level < this->height && a <= b)
    {
        if (a % 2)
        {
            if (firstfound)
            {
                if ((a == b) && (!lastfound))
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pRepsIn[level][a].insert(tid, id, start, end);
                    }
                    lastfound = 1;
                }
                else
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pRepsAft[level][a].insert(tid, id, start, end);
                    }
                }
            }
            else
            {
                if ((a == b) && (!lastfound))
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pOrgsIn[level][a].insert(tid, id, start, end);
                    }
                }
                else
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pOrgsAft[level][a].insert(tid, id, start, end);
                    }
                }
                firstfound = 1;
            }
            a++;
        }
        if (!(b % 2))
        {
            prevb = b;
            b--;
            if ((!firstfound) && b < a)
            {
                if (!lastfound)
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pOrgsIn[level][prevb].insert(tid, id, start, end);
                    }
                }
                else
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pOrgsAft[level][prevb].insert(tid, id, start, end);
                    }
                }
            }
            else
            {
                if (!lastfound)
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pRepsIn[level][prevb].insert(tid, id, start, end);
                    }
                    lastfound = 1;
                }
                else
                {
                    for (auto &tid : r_org.elements)
                    {
                        if (tid > this->elem_max) continue;
                        this->pRepsAft[level][prevb].insert(tid, id, start, end);
                    }
                }
            }
        }
        a >>= 1; b >>= 1; level++;
    }
}


void irHINTb::allocPartitions()
{
    this->pOrgsIn  = new SortedTIF*[this->height];
    this->pOrgsAft = new SortedTIF*[this->height];
    this->pRepsIn  = new SortedTIF*[this->height];
    this->pRepsAft = new SortedTIF*[this->height];
    for (unsigned int l = 0; l < this->height; ++l)
    {
        const int cnt = 1 << (this->numBits - l);
        this->pOrgsIn[l]  = new SortedTIF[cnt];
        this->pOrgsAft[l] = new SortedTIF[cnt];
        this->pRepsIn[l]  = new SortedTIF[cnt];
        this->pRepsAft[l] = new SortedTIF[cnt];
    }
}


void irHINTb::finalizePartitions()
{
    for (unsigned int l = 0; l < this->height; ++l)
    {
        const int cnt = 1 << (this->numBits - l);
        for (int pid = 0; pid < cnt; ++pid)
        {
            this->pOrgsIn[l][pid].finalize();
            this->pOrgsAft[l][pid].finalize();
            this->pRepsIn[l][pid].finalize();
            this->pRepsAft[l][pid].finalize();
        }
    }
}


void irHINTb::buildDonePartitions()
{
    for (unsigned int l = 0; l < this->height; ++l)
    {
        const int cnt = 1 << (this->numBits - l);
        for (int pid = 0; pid < cnt; ++pid)
        {
            this->pOrgsIn[l][pid].build_done();
            this->pOrgsAft[l][pid].build_done();
            this->pRepsIn[l][pid].build_done();
            this->pRepsAft[l][pid].build_done();
        }
    }
}


void irHINTb::freePartitions()
{
    for (unsigned int l = 0; l < this->height; ++l)
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


void irHINTb::rebuild_from(const IRelation &R)
{
    this->allocPartitions();
    for (const IRecord &r : R)   this->countPartitions(r);
    this->finalizePartitions();
    for (const IRecord &r : R)   this->fillPartitions(r);
    this->buildDonePartitions();
}


// ===========================================================================
// irHINTb, constructor / destructor
// ===========================================================================

irHINTb::irHINTb(
    const Boundaries &boundaries,
    const int &numBits,
    const IRelation &R)
{
    this->elem_min   = boundaries.elem_min;
    this->elem_max   = boundaries.elem_max;
    this->time_start = boundaries.time_start;
    this->time_end   = boundaries.time_end;

    if (this->elem_min != 0)
        throw runtime_error("irHINTb: elem_min != 0: " + boundaries.str());
    if (this->time_start != 0)
        throw runtime_error("irHINTb: time_start != 0: " + boundaries.str());

    this->numBits = static_cast<unsigned int>(numBits);
    this->maxBits = static_cast<unsigned int>(log2(this->time_end - this->time_start)) + 1;
    this->height  = this->numBits + 1;

    this->rebuild_from(R);
    this->m_scratch.reserve(256);
}


irHINTb::~irHINTb()
{
    this->freePartitions();
}


// ===========================================================================
// irHINTb, size / stats
// ===========================================================================

void irHINTb::getStats() {}


size_t irHINTb::getSize()
{
    size_t size = sizeof(*this);
    size += sizeof(SortedTIF*) * this->height * 4;
    for (unsigned int l = 0; l < this->height; ++l)
    {
        const int cnt = 1 << (this->numBits - l);
        size += sizeof(SortedTIF) * cnt * 4;
        for (int pid = 0; pid < cnt; ++pid)
        {
            size += this->pOrgsIn[l][pid].getSize();
            size += this->pOrgsAft[l][pid].getSize();
            size += this->pRepsIn[l][pid].getSize();
            size += this->pRepsAft[l][pid].getSize();
        }
    }
    return size;
}


// ===========================================================================
// irHINTb, update / remove / softdelete
// ===========================================================================

void irHINTb::extractRecords(IRelation &R) const
{
    R.clear();
    unordered_map<RecordId, IRecord> recordMap;

    for (unsigned int l = 0; l < this->height; ++l)
    {
        const int cnt = 1 << (this->numBits - l);
        for (int pid = 0; pid < cnt; ++pid)
        {
            this->pOrgsIn[l][pid].extractRecords(recordMap, this->time_start);
            this->pOrgsAft[l][pid].extractRecords(recordMap, this->time_start);
            this->pRepsIn[l][pid].extractRecords(recordMap, this->time_start);
            this->pRepsAft[l][pid].extractRecords(recordMap, this->time_start);
        }
    }

    R.reserve(recordMap.size());
    for (auto& [id, record] : recordMap)
        R.push_back(move(record));

    sort(R.begin(), R.end(), [](const IRecord &a, const IRecord &b)
        { return a.id < b.id; });
}


void irHINTb::update(const IRelation &R)
{
    if (R.empty()) return;

    // Extract existing records and merge (monotonic ID assumption: just append)
    IRelation existing;
    this->extractRecords(existing);

    IRelation merged;
    merged.reserve(existing.size() + R.size());
    merged.insert(merged.end(), existing.begin(), existing.end());
    merged.insert(merged.end(), R.begin(), R.end());

    // Recompute temporal domain
    Timestamp new_min = this->time_start;
    Timestamp new_max = this->time_end;
    for (const auto &r : R)
    {
        if (r.start < new_min) new_min = r.start;
        if (r.end   > new_max) new_max = r.end;
    }

    this->freePartitions();
    this->time_start = new_min;
    this->time_end   = new_max;
    this->maxBits    = static_cast<unsigned int>(log2(this->time_end - this->time_start)) + 1;
    this->height     = this->numBits + 1;
    this->rebuild_from(merged);
}


void irHINTb::remove(const vector<bool> &idsToDelete)
{
    IRelation existing;
    this->extractRecords(existing);

    existing.erase(
        remove_if(existing.begin(), existing.end(),
            [&idsToDelete](const IRecord &rec)
            {
                return rec.id >= 0 &&
                       static_cast<size_t>(rec.id) < idsToDelete.size() &&
                       idsToDelete[rec.id];
            }),
        existing.end());

    if (existing.empty()) return;

    Timestamp new_min = numeric_limits<Timestamp>::max();
    Timestamp new_max = numeric_limits<Timestamp>::min();
    for (const auto &r : existing)
    {
        if (r.start < new_min) new_min = r.start;
        if (r.end   > new_max) new_max = r.end;
    }

    this->freePartitions();
    this->time_start = new_min;
    this->time_end   = new_max;
    this->maxBits    = static_cast<unsigned int>(log2(this->time_end - this->time_start)) + 1;
    this->height     = this->numBits + 1;
    this->rebuild_from(existing);
}


void irHINTb::softdelete(const vector<bool> &idsToDelete)
{
    for (unsigned int l = 0; l < this->height; ++l)
    {
        const int cnt = 1 << (this->numBits - l);
        for (int pid = 0; pid < cnt; ++pid)
        {
            this->pOrgsIn[l][pid].softdelete(idsToDelete);
            this->pOrgsAft[l][pid].softdelete(idsToDelete);
            this->pRepsIn[l][pid].softdelete(idsToDelete);
            this->pRepsAft[l][pid].softdelete(idsToDelete);
        }
    }
}


// ===========================================================================
// irHINTb, inline scan helpers
// ===========================================================================

inline void irHINTb::scanPartitionContainment_CheckBoth(
    SortedTIF &tif, const ElementId* elems, size_t ne, Timestamp qstart, Timestamp qend, RelationId &result)
{
    if (tif.empty()) return;
    if (ne == 1)
    {
        tif.moveOut_CheckBoth(elems[0], qstart, qend, result);
        return;
    }
    m_scratch.clear();
    if (!tif.moveOut_CheckBoth(elems[0], qstart, qend, m_scratch)) return;
    for (size_t i = 1; i + 1 < ne; ++i)
        if (!tif.intersect(elems[i], m_scratch)) return;
    tif.intersectAndOutput(elems[ne - 1], m_scratch, result);
}


inline void irHINTb::scanPartitionContainment_CheckStart(
    SortedTIF &tif, const ElementId* elems, size_t ne, Timestamp qend, RelationId &result)
{
    if (tif.empty()) return;
    if (ne == 1)
    {
        tif.moveOut_CheckStart(elems[0], qend, result);
        return;
    }
    m_scratch.clear();
    if (!tif.moveOut_CheckStart(elems[0], qend, m_scratch)) return;
    for (size_t i = 1; i + 1 < ne; ++i)
        if (!tif.intersect(elems[i], m_scratch)) return;
    tif.intersectAndOutput(elems[ne - 1], m_scratch, result);
}


inline void irHINTb::scanPartitionContainment_CheckEnd(
    SortedTIF &tif, const ElementId* elems, size_t ne, Timestamp qstart, RelationId &result)
{
    if (tif.empty()) return;
    if (ne == 1)
    {
        tif.moveOut_CheckEnd(elems[0], qstart, result);
        return;
    }
    m_scratch.clear();
    if (!tif.moveOut_CheckEnd(elems[0], qstart, m_scratch)) return;
    for (size_t i = 1; i + 1 < ne; ++i)
        if (!tif.intersect(elems[i], m_scratch)) return;
    tif.intersectAndOutput(elems[ne - 1], m_scratch, result);
}


inline void irHINTb::scanPartitionContainment_NoChecks(
    SortedTIF &tif, const ElementId* elems, size_t ne, RelationId &result)
{
    if (tif.empty()) return;
    if (ne == 1)
    {
        tif.moveOut_NoChecks(elems[0], result);
        return;
    }
    m_scratch.clear();
    if (!tif.moveOut_NoChecks(elems[0], m_scratch)) return;
    for (size_t i = 1; i + 1 < ne; ++i)
        if (!tif.intersect(elems[i], m_scratch)) return;
    tif.intersectAndOutput(elems[ne - 1], m_scratch, result);
}


inline void irHINTb::scanPartition_intersect(
    SortedTIF &tif, const ElementId* elems, size_t ne,
    RelationId &candidates, RelationId &results)
{
    if (tif.empty()) return;
    if (ne == 1)
    {
        tif.intersectAndOutput(elems[0], candidates, results);
        return;
    }
    m_scratch.clear();
    if (!tif.intersectAndOutput(elems[0], candidates, m_scratch)) return;
    for (size_t i = 1; i + 1 < ne; ++i)
        if (!tif.intersect(elems[i], m_scratch)) return;
    tif.intersectAndOutput(elems[ne - 1], m_scratch, results);
}


// ===========================================================================
// irHINTb, move_out
// ===========================================================================

void irHINTb::move_out(
    const RangeIRQuery &qo,
    ElementId &elem_off,
    RelationId &result)
{
    const ElementId* elems  = qo.elems.data() + elem_off;
    const size_t     ne     = qo.elems.size() - elem_off;
    const Timestamp  qstart = qo.start;
    const Timestamp  qend   = qo.end;

    Timestamp a = qstart >> (this->maxBits - this->numBits);
    Timestamp b = qend   >> (this->maxBits - this->numBits);
    bool foundzero = false;
    bool foundone  = false;

    for (auto l = 0u; l < this->numBits; ++l)
    {
        if (foundone && foundzero)
        {
            // Query range fully covers the parent partition: no bounds checks needed.
            this->scanPartitionContainment_NoChecks(this->pRepsIn[l][a],  elems, ne, result);
            this->scanPartitionContainment_NoChecks(this->pRepsAft[l][a], elems, ne, result);
            for (auto i = a; i <= b; ++i)
            {
                this->scanPartitionContainment_NoChecks(this->pOrgsIn[l][i],  elems, ne, result);
                this->scanPartitionContainment_NoChecks(this->pOrgsAft[l][i], elems, ne, result);
            }
        }
        else
        {
            if (a == b)
            {
                this->scanPartitionContainment_CheckBoth(this->pOrgsIn[l][a],   elems, ne, qstart, qend, result);
                this->scanPartitionContainment_CheckStart(this->pOrgsAft[l][a], elems, ne, qend,         result);
            }
            else
            {
                // Lemma 1
                this->scanPartitionContainment_CheckEnd(this->pOrgsIn[l][a],   elems, ne, qstart, result);
                this->scanPartitionContainment_NoChecks(this->pOrgsAft[l][a], elems, ne,         result);
            }
            // Lemma 1, 3
            this->scanPartitionContainment_CheckEnd(this->pRepsIn[l][a],   elems, ne, qstart, result);
            this->scanPartitionContainment_NoChecks(this->pRepsAft[l][a], elems, ne,         result);

            if (a < b)
            {
                for (auto i = a + 1; i < b; ++i)
                {
                    this->scanPartitionContainment_NoChecks(this->pOrgsIn[l][i],  elems, ne, result);
                    this->scanPartitionContainment_NoChecks(this->pOrgsAft[l][i], elems, ne, result);
                }
                this->scanPartitionContainment_CheckStart(this->pOrgsIn[l][b],  elems, ne, qend, result);
                this->scanPartitionContainment_CheckStart(this->pOrgsAft[l][b], elems, ne, qend, result);
            }

            if (b % 2)    foundone  = true;
            if (!(a % 2)) foundzero = true;
        }
        a >>= 1; b >>= 1;
    }

    // Root level
    if (foundone && foundzero)
        this->scanPartitionContainment_NoChecks(this->pOrgsIn[this->numBits][0], elems, ne, result);
    else
        this->scanPartitionContainment_CheckBoth(this->pOrgsIn[this->numBits][0], elems, ne, qstart, qend, result);

    elem_off = static_cast<ElementId>(qo.elems.size());
}


// ===========================================================================
// irHINTb, refine
// ===========================================================================

void irHINTb::refine(
    const RangeIRQuery &qo,
    ElementId &elem_off,
    RelationId &candidates)
{
    sort(candidates.begin(), candidates.end());

    const ElementId* elems = qo.elems.data() + elem_off;
    const size_t     ne    = qo.elems.size() - elem_off;

    RelationId results;
    results.reserve(candidates.size() / 4);

    Timestamp a = qo.start >> (this->maxBits - this->numBits);
    Timestamp b = qo.end   >> (this->maxBits - this->numBits);
    bool foundzero = false;
    bool foundone  = false;

    for (auto l = 0u; l < this->numBits; ++l)
    {
        if (foundone && foundzero)
        {
            this->scanPartition_intersect(this->pRepsIn[l][a],  elems, ne, candidates, results);
            this->scanPartition_intersect(this->pRepsAft[l][a], elems, ne, candidates, results);
            for (auto i = a; i <= b; ++i)
            {
                this->scanPartition_intersect(this->pOrgsIn[l][i],  elems, ne, candidates, results);
                this->scanPartition_intersect(this->pOrgsAft[l][i], elems, ne, candidates, results);
            }
        }
        else
        {
            this->scanPartition_intersect(this->pOrgsIn[l][a],  elems, ne, candidates, results);
            this->scanPartition_intersect(this->pOrgsAft[l][a], elems, ne, candidates, results);
            this->scanPartition_intersect(this->pRepsIn[l][a],  elems, ne, candidates, results);
            this->scanPartition_intersect(this->pRepsAft[l][a], elems, ne, candidates, results);

            if (a < b)
            {
                for (auto i = a + 1; i < b; ++i)
                {
                    this->scanPartition_intersect(this->pOrgsIn[l][i],  elems, ne, candidates, results);
                    this->scanPartition_intersect(this->pOrgsAft[l][i], elems, ne, candidates, results);
                }
                this->scanPartition_intersect(this->pOrgsIn[l][b],  elems, ne, candidates, results);
                this->scanPartition_intersect(this->pOrgsAft[l][b], elems, ne, candidates, results);
            }

            if (b % 2)    foundone  = true;
            if (!(a % 2)) foundzero = true;
        }
        a >>= 1; b >>= 1;
    }

    // Root level
    this->scanPartition_intersect(this->pOrgsIn[this->numBits][0], elems, ne, candidates, results);

    candidates.swap(results);
    elem_off = static_cast<ElementId>(qo.elems.size());
}
