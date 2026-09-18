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

#ifndef _irHINTb_H_
#define _irHINTb_H_

#include "../structure/framework.h"
#include <unordered_map>


// ---------------------------------------------------------------------------
// SortedTIF, SoA temporal inverted file with sorted-element binary search.
//
// Replaces TemporalInvertedFile (unordered_map per partition) with:
//   - sorted elem_ids vector: O(log K) lookup, fully cache-resident for small K
//   - packed SoA arrays: soa_ids / soa_starts / soa_ends are separate, so
//     temporal-check loops are SIMD-vectorizable by GCC -O3 -mavx
//   - two-pass construction: count pass -> exact allocation -> fill pass
//     (zero rehashing, zero wasted capacity)
//   - no per-scan heap allocation (caller supplies scratch buffer)
//
// Restrictions:
//   - Not copyable (use move semantics; default-constructible for new[]).
//   - build_done() must be called after the fill pass to release cursors.
//   - insert() must only be called after finalize() and before build_done().
// ---------------------------------------------------------------------------
struct SortedTIF
{
    // ---- query-phase fields (permanent after finalize()) ----
    vector<ElementId> elem_ids;     // sorted; binary-searched at query time
    vector<uint32_t>  list_offsets; // prefix sums; size = elem_ids.size() + 1
    vector<RecordId>  soa_ids;      // packed record IDs (all elements)
    vector<Timestamp> soa_starts;   // parallel start timestamps
    vector<Timestamp> soa_ends;     // parallel end timestamps

    // ---- build-phase fields (released after build_done()) ----
    unordered_map<ElementId, uint32_t> *counts     = nullptr;
    unordered_map<ElementId, uint32_t> *elem_index = nullptr;  // elem → index in elem_ids
    uint32_t                           *write_pos   = nullptr;

    SortedTIF() = default;
    SortedTIF(const SortedTIF&)            = delete;
    SortedTIF& operator=(const SortedTIF&) = delete;
    SortedTIF(SortedTIF&&) noexcept            = default;
    SortedTIF& operator=(SortedTIF&&) noexcept = default;
    ~SortedTIF() { delete counts; delete elem_index; delete[] write_pos; }

    // ---- Pass 1: counting ----

    inline void count(ElementId elem)
    {
        if (!counts) counts = new unordered_map<ElementId, uint32_t>();
        (*counts)[elem]++;
    }

    // ---- Between passes: finalize ----

    void finalize();    // defined in .cpp (not performance-critical)

    // ---- Pass 2: filling ----

    inline void insert(ElementId elem, RecordId id, Timestamp start, Timestamp end)
    {
        const uint32_t pos = write_pos[(*elem_index)[elem]]++;
        soa_ids[pos]    = id;
        soa_starts[pos] = start;
        soa_ends[pos]   = end;
    }

    void build_done() { delete elem_index; elem_index = nullptr; delete[] write_pos; write_pos = nullptr; }

    // ---- Queries ----

    bool empty() const { return elem_ids.empty(); }

    // Returns [lo, hi) range in SoA; lo == hi means element not present.
    inline pair<uint32_t, uint32_t> find_range(ElementId elem) const
    {
        auto it = lower_bound(elem_ids.begin(), elem_ids.end(), elem);
        if (it == elem_ids.end() || *it != elem) return {0u, 0u};
        const size_t idx = static_cast<size_t>(it - elem_ids.begin());
        return {list_offsets[idx], list_offsets[idx + 1]};
    }

    // Append all IDs in posting list for elem, no temporal check.
    // Fast path: single memcpy-like insert.
    inline bool moveOut_NoChecks(ElementId elem, RelationId &out) const
    {
        auto [lo, hi] = find_range(elem);
        if (lo == hi) return false;
        out.insert(out.end(), soa_ids.begin() + lo, soa_ids.begin() + hi);
        return true;
    }

    // Append IDs where record interval overlaps [q_start, q_end].
    inline bool moveOut_CheckBoth(ElementId elem,
                                  Timestamp q_start, Timestamp q_end,
                                  RelationId &out) const
    {
        auto [lo, hi] = find_range(elem);
        if (lo == hi) return false;
        const size_t before = out.size();
        for (uint32_t i = lo; i < hi; ++i)
            if (soa_starts[i] <= q_end && q_start <= soa_ends[i])
                out.push_back(soa_ids[i]);
        return out.size() > before;
    }

    // Append IDs where record.start <= q_end (start guaranteed ≥ partition start).
    inline bool moveOut_CheckStart(ElementId elem, Timestamp q_end,
                                   RelationId &out) const
    {
        auto [lo, hi] = find_range(elem);
        if (lo == hi) return false;
        const size_t before = out.size();
        for (uint32_t i = lo; i < hi; ++i)
            if (soa_starts[i] <= q_end)
                out.push_back(soa_ids[i]);
        return out.size() > before;
    }

    // Append IDs where q_start <= record.end (end guaranteed ≥ partition end).
    inline bool moveOut_CheckEnd(ElementId elem, Timestamp q_start,
                                 RelationId &out) const
    {
        auto [lo, hi] = find_range(elem);
        if (lo == hi) return false;
        const size_t before = out.size();
        for (uint32_t i = lo; i < hi; ++i)
            if (q_start <= soa_ends[i])
                out.push_back(soa_ids[i]);
        return out.size() > before;
    }

    // Merge-join sorted candidates with posting list for elem; modifies candidates in place.
    // Zero heap allocation: survivors are written back into candidates from position 0.
    inline bool intersect(ElementId elem, RelationId &candidates) const
    {
        auto [lo, hi] = find_range(elem);
        if (lo == hi) { candidates.clear(); return false; }

        size_t   w  = 0;              // write cursor
        size_t   r  = 0;              // read cursor into candidates
        uint32_t li = lo;
        const size_t nc = candidates.size();

        while (r < nc && li < hi)
        {
            if      (soa_ids[li] < candidates[r]) { ++li; }
            else if (soa_ids[li] > candidates[r]) { ++r;  }
            else    { candidates[w++] = candidates[r]; ++li; ++r; }
        }
        candidates.resize(w);
        return w > 0;
    }

    // Merge-join and append matches to result; candidates unchanged.
    inline bool intersectAndOutput(ElementId elem,
                                   RelationId &candidates,
                                   RelationId &result) const
    {
        auto [lo, hi] = find_range(elem);
        if (lo == hi) return false;

        const size_t before = result.size();
        auto     cit  = candidates.begin();
        auto     cend = candidates.end();
        uint32_t li   = lo;

        while (cit != cend && li < hi)
        {
            if      (soa_ids[li] < *cit) { ++li;        }
            else if (soa_ids[li] > *cit) { ++cit;       }
            else                         { result.push_back(soa_ids[li]); ++li; ++cit; }
        }
        return result.size() > before;
    }

    // ---- Misc ----

    void softdelete(const vector<bool> &idsToDelete);     // defined in .cpp
    void extractRecords(unordered_map<RecordId, IRecord> &recordMap,
                        Timestamp time_offset) const;     // defined in .cpp
    size_t getSize() const;                               // defined in .cpp
};


/**
 * irHINT, variant b  (sorted SoA posting lists)
 *
 * Drop-in replacement for irHINTa.  Uses the same HINT tree structure but
 * replaces each TemporalInvertedFile (unordered_map -> AoS records) with a
 * SortedTIF (sorted element array + SoA id/start/end vectors). 
 * Element lookup is a binary search; temporal scans benefit from SIMD
 * auto-vectorisation on the contiguous SoA arrays.
 *
 * move_out:  binary-search for the element in each SortedTIF, then scan
 *            the SoA posting list into the pre-reserved m_scratch buffer
 *            (zero heap allocation in the hot path).
 * refine:    same tree walk; intersect the sorted candidate set with each
 *            SortedTIF posting list using a two-pointer advance.
 *
 * Constraints: requires elem_min == 0 and time_start == 0 (asserted in
 * constructor).  Two-pass construction (count -> finalize -> fill) allocates
 * exactly the right memory with no rehashing.
 *
 * Trade-off: faster query throughput than variant a for read-heavy
 * workloads; slightly slower construction and higher code complexity.
 */
class irHINTb final : public Refinement_ElemFreq
{
private:
    ElementId    elem_min;
    ElementId    elem_max;
    Timestamp    time_start;
    Timestamp    time_end;
    unsigned int maxBits;
    unsigned int numBits;
    unsigned int height;

    SortedTIF **pOrgsIn;
    SortedTIF **pOrgsAft;
    SortedTIF **pRepsIn;
    SortedTIF **pRepsAft;

    // Reused scratch buffer: cleared before each multi-element partition scan,
    // so there is no heap allocation in the hot query path.
    mutable RelationId m_scratch;

    // Construction
    inline void countPartitions(const IRecord &r);
    inline void fillPartitions(const IRecord &r);
    void        allocPartitions();
    void        finalizePartitions();
    void        buildDonePartitions();
    void        freePartitions();
    void        rebuild_from(const IRelation &R);
    void        extractRecords(IRelation &R) const;

    // Querying, inline scan helpers (defined in .cpp)
    // Callers pass elems/ne directly to avoid copying qo.elems into a local RangeIRQuery.
    inline void scanPartitionContainment_CheckBoth(SortedTIF &tif, const ElementId* elems, size_t ne, Timestamp qstart, Timestamp qend, RelationId &result);
    inline void scanPartitionContainment_CheckStart(SortedTIF &tif, const ElementId* elems, size_t ne, Timestamp qend, RelationId &result);
    inline void scanPartitionContainment_CheckEnd(SortedTIF &tif, const ElementId* elems, size_t ne, Timestamp qstart, RelationId &result);
    inline void scanPartitionContainment_NoChecks(SortedTIF &tif, const ElementId* elems, size_t ne, RelationId &result);
    inline void scanPartition_intersect(SortedTIF &tif, const ElementId* elems, size_t ne,
                                        RelationId &candidates, RelationId &results);

public:
    irHINTb(const Boundaries &boundaries, const int &numBits, const IRelation &R);
    void   getStats();
    size_t getSize();
    string str(int l) const override;
    ~irHINTb();

    void update(const IRelation &R) override;
    void remove(const vector<bool> &idsToDelete) override;
    void softdelete(const vector<bool> &idsToDelete) override;

    void move_out(const RangeIRQuery &q, ElementId &elem_off, RelationId &result) override;
    void refine(const RangeIRQuery &q, ElementId &elem_off, RelationId &candidates) override;
};


#endif // _irHINTb_H_
