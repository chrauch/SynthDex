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

#include "tifsharding.h"
#include <unordered_set>


string tIFSHARDING::str(const int l) const
{
    if (this->relaxation != 0.0f)
        return "tIF-SHARDING relax=" + to_string(this->relaxation);
    else
        return "tIF-SHARDING dynbase=" + to_string(this->dynbase) 
            + " dynratio=" + to_string(this->dynratio);
}


tIFSHARDING::tIFSHARDING()
{
}


tIFSHARDING::tIFSHARDING(
        const Boundaries &boundaries,
        const float relaxation,
        const double dynbase,
        const double dynratio,
        const IRelation &R)
    : relaxation(relaxation), dynbase(dynbase), dynratio(dynratio)
{
    if ((relaxation == 0.0f && (dynratio == 0.0 || dynbase == 0.0)) 
     || (relaxation != 0.0f && (dynratio != 0.0 || dynbase != 0.0)))
        throw runtime_error("tIFSHARDING: either relaxation or (dynbase and dynratio) must be set, but not both.");

    this->elem_min = boundaries.elem_min;
    this->elem_max = boundaries.elem_max;

    // Create a sorted copy for sharding construction
    IRelation sortedR = R;
    sortedR.determineDomain();
    sortedR.sortByStart();
    
    // Calculate domain size for tolerance calculation
    Timestamp domainSize = sortedR.gend - sortedR.gstart;

    // Group records by element first for dynamic relaxation calculation
    unordered_map<ElementId, vector<const IRecord*>> tmpLists;
    for (const auto& r : sortedR)
    {
        for (const auto& tid : r.elements)
        {
            if (tid > elem_max) continue;
            if (tid < elem_min) break;
            tmpLists[tid].push_back(&r);
        }
    }

    // Build sharded posting lists with per-list relaxation
    for (auto& [tid, records] : tmpLists)
    {
        auto& list = this->lists[tid];
        list = new ShardedPostingList();

        // Calculate relaxation and tolerance for this posting list
        Timestamp tolerance;
        if (this->relaxation != 0.0f)
        {
            // Static mode
            tolerance = domainSize * (this->relaxation / 100.0);
        }
        else
        {
            // Dynamic mode: fewer records -> higher relaxation -> fewer shards
            double log_size = log10(records.size());
            double exponent = -this->dynratio * max(0.0, log_size - this->dynbase);
            float list_relaxation = min(100.0f, static_cast<float>(100.0 * exp(exponent)));
            tolerance = domainSize * (list_relaxation / 100.0);
            
            //if (tid == 1 
            //    || tid == 10 
            //    || tid == 100
            //    || tid == 1000
            //    || tid == 10000
            //    || tid == 100000)
            //cout << "Element " << tid 
            //     << " with " << records.size() 
            //     << " records gets relaxation " << list_relaxation << "%." << endl;
        }

        // Add records to the sharded posting list
        for (const auto* r : records)
        {
            list->addRecord(r->id, r->start, r->end, this->impactListGap, tolerance);
        }
    }
}


void tIFSHARDING::getStats()
{
}


size_t tIFSHARDING::getSize()
{
    size_t size = 0;
    
    // Member variables
    size += sizeof(float);        // relaxation
    size += sizeof(Timestamp);    // tolerance
    size += sizeof(ElementId);    // elem_min
    size += sizeof(ElementId);    // elem_max
    size += sizeof(unsigned int); // impactListGap
    
    // Size of the unordered_map structure itself
    size += sizeof(this->lists);
    
    // Map overhead: bucket array and nodes
    size += this->lists.bucket_count() * sizeof(void*);
    
    for (auto iterL = this->lists.begin(); iterL != this->lists.end(); iterL++)
    {
        // Size of the key-value pair in the map (includes map node overhead)
        size += sizeof(ElementId) + sizeof(ShardedPostingList*);
        
        // Size of the sharded posting list structure
        size += iterL->second->getSize();
    }
    
    return size;
}


tIFSHARDING::~tIFSHARDING()
{
    for (auto iterL = this->lists.begin(); iterL != this->lists.end(); iterL++)
        delete iterL->second;
}


void tIFSHARDING::update(const IRelation &R)
{
    // Assumption: Update records have monotonically increasing IDs (higher than existing records)
    // Extract existing records from all sharded posting lists
    unordered_map<ElementId, Relation> extracted;
    
    for (auto& [tid, list] : this->lists)
    {
        if (list != nullptr)
        {
            Relation tmpR;
            list->extractRecords(tmpR);
            extracted[tid] = move(tmpR);
            
            delete list;
            list = nullptr;
        }
    }
    
    // Merge new records with extracted records for each element
    for (const auto& r : R)
    {
        for (const auto& tid : r.elements)
        {
            if (tid > elem_max) continue;
            if (tid < elem_min) break;
            
            extracted[tid].emplace_back(r.id, r.start, r.end);
        }
    }
    
    // Rebuild sharded posting lists with merged data
    for (auto& [tid, tmpR] : extracted)
    {
        if (tmpR.empty()) continue;
        
        // Sort by start time (required for addRecord assumption about temporal ordering)
        sort(tmpR.begin(), tmpR.end(), [](const Record &lhs, const Record &rhs) {
            return lhs.start < rhs.start;
        });
        
        // Calculate domain size for this posting list
        tmpR.determineDomain();
        Timestamp domainSize = tmpR.gend - tmpR.gstart;
        
        // Calculate relaxation and tolerance for this posting list
        Timestamp tolerance;
        if (this->relaxation != 0.0f)
        {
            // Static mode
            tolerance = domainSize * (this->relaxation / 100.0);
        }
        else
        {
            // Dynamic mode: fewer records → higher relaxation → fewer shards
            double log_size = log10(tmpR.size());
            double exponent = -this->dynratio * max(0.0, log_size - this->dynbase);
            float list_relaxation = min(100.0f, static_cast<float>(100.0 * exp(exponent)));
            tolerance = domainSize * (list_relaxation / 100.0);
        }
        
        auto& list = this->lists[tid];
        list = new ShardedPostingList();
        
        for (const auto& rec : tmpR)
        {
            list->addRecord(rec.id, rec.start, rec.end, impactListGap, tolerance);
        }
    }
}


void tIFSHARDING::remove(const RelationId &ids)
{
    // Build set of IDs to delete
    unordered_set<RecordId> idsToDelete(ids.begin(), ids.end());
    
    // Extract existing records from all sharded posting lists
    unordered_map<ElementId, Relation> extracted;
    
    for (auto& [tid, list] : this->lists)
    {
        if (list != nullptr)
        {
            Relation tmpR;
            list->extractRecords(tmpR);
            extracted[tid] = move(tmpR);
            
            delete list;
            list = nullptr;
        }
    }
    
    // Filter out deleted records for each element
    for (auto& [tid, tmpR] : extracted)
    {
        tmpR.erase(
            remove_if(tmpR.begin(), tmpR.end(),
                [&idsToDelete](const Record &rec) { return idsToDelete.count(rec.id) > 0; }),
            tmpR.end());
    }
    
    // Rebuild sharded posting lists with filtered data
    for (auto& [tid, tmpR] : extracted)
    {
        if (tmpR.empty())
        {
            this->lists.erase(tid);
            continue;
        }
        
        // Sort by start time (required for addRecord assumption about temporal ordering)
        sort(tmpR.begin(), tmpR.end(), [](const Record &lhs, const Record &rhs) {
            return lhs.start < rhs.start;
        });
        
        // Calculate domain size for this posting list
        tmpR.determineDomain();
        Timestamp domainSize = tmpR.gend - tmpR.gstart;
        
        // Calculate relaxation and tolerance for this posting list
        Timestamp tolerance;
        if (this->relaxation != 0.0f)
        {
            // Static mode
            tolerance = domainSize * (this->relaxation / 100.0);
        }
        else
        {
            // Dynamic mode: fewer records → higher relaxation → fewer shards
            double log_size = log10(tmpR.size());
            double exponent = -this->dynratio * max(0.0, log_size - this->dynbase);
            float list_relaxation = min(100.0f, static_cast<float>(100.0 * exp(exponent)));
            tolerance = domainSize * (list_relaxation / 100.0);
        }
        
        auto& list = this->lists[tid];
        list = new ShardedPostingList();
        
        for (const auto& rec : tmpR)
        {
            list->addRecord(rec.id, rec.start, rec.end, impactListGap, tolerance);
        }
    }
}


void tIFSHARDING::print(char c)
{
    cout << "List count: " << this->lists.size() << endl;

    for (auto iter = this->lists.begin(); iter != this->lists.end(); ++iter)
    {
        cout << "[element:" << iter->first << "] posting list: " << endl;
        iter->second->print();
    }
}


// Querying
bool tIFSHARDING::moveOut_checkBoth(const RangeIRQuery &q, RelationId &candidates)
{
    auto iterL = this->lists.find(q.elems[0]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterL == this->lists.end())
        return false;
    else
    {
        iterL->second->getCandidates(q.start, q.end, candidates);
                
        return (!candidates.empty());
    }
}


bool tIFSHARDING::intersectx(const RangeIRQuery &q, const unsigned int off, RelationId &candidates)
{
    auto iterL = this->lists.find(q.elems[off]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterL == this->lists.end())
    {
        candidates.clear();
        return false;
    }

    RelationId temp;
    RelationId plist;
    iterL->second->getCandidates(q.start, q.end, plist);

    sort(plist.begin(), plist.end());

    RelationId::iterator list_begin = plist.begin(), list_end = plist.end();
    RelationId::iterator cand_begin = candidates.begin(), cand_end = candidates.end();

    while ((list_begin != list_end) && (cand_begin != cand_end))
    {
        if (*list_begin < *cand_begin) list_begin++;
        else if (*list_begin > *cand_begin) cand_begin++;
        else
        {
            temp.push_back(*list_begin++);
            cand_begin++;
        }
    }

    candidates.swap(temp);
    
    return (!candidates.empty());
}


bool tIFSHARDING::intersectAndOutput(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result)
{
    auto iterL = this->lists.find(q.elems[off]);
    
    // If the inverted file does not contain the element then result must be empty
    if (iterL == this->lists.end())
        return false;

    RelationId plist;
    iterL->second->getCandidates(q.start, q.end, plist);

    sort(plist.begin(), plist.end());

    RelationId::iterator list_begin = plist.begin(), list_end = plist.end();
    RelationId::iterator cand_begin = candidates.begin(), cand_end = candidates.end();

    while ((list_begin != list_end) && (cand_begin != cand_end))
    {
        if (*list_begin < *cand_begin) list_begin++;
        else if (*list_begin > *cand_begin) cand_begin++;
        else
        {
            result.push_back(*list_begin++);
            cand_begin++;
        }
    }
                
    return (!result.empty());
}


void tIFSHARDING::move_out(
    const RangeIRQuery &q,
    ElementId &elem_off,
    RelationId &result)
{
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


void tIFSHARDING::refine(
        const RangeIRQuery &qo,
        ElementId &elem_off,
        RelationId &result)
{
    ElementId elem = qo.elems[elem_off];
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
            
            RelationId temp;
            RelationId plist;
            iterL->second->getCandidates(qo.start, qo.end, plist);

            sort(plist.begin(), plist.end());

            RelationId::iterator list_begin = plist.begin(), list_end = plist.end();
            RelationId::iterator res_begin = result.begin(), res_end = result.end();

            while ((list_begin != list_end) && (res_begin != res_end))
            {
                if (*list_begin < *res_begin) list_begin++;
                else if (*list_begin > *res_begin) res_begin++;
                else
                {
                    temp.push_back(*list_begin++);
                    res_begin++;
                }
            }

            result.swap(temp);
            
            if (result.empty() || elem_off >= qo.elems.size()-1) return;
        }

        ++elem_off;
        elem = qo.elems[elem_off];
    }
    while (elem >= this->elem_min);
}



// ShardedPostingList implementation

void ShardedPostingList::print() const
{
    for (size_t i = 0; i < size(); i++)
    {
        cout << "  Shard " << i+1 << " of " << size() << ":" << endl;
        (*this)[i].print();
        cout << endl;
    }
}


size_t ShardedPostingList::getSize()
{
    size_t size = 0;
    
    // Size of the vector structure itself
    size += sizeof(*this);
    
    // Size of each shard
    for (auto& s : *this) 
        size += s.getSize();
    
    return size;
}


void ShardedPostingList::getCandidates(const Timestamp& qstart, const Timestamp& qend, RelationId& candidates) const
{
    for (const auto& shard : *this)
    {
        shard.getCandidates(qstart, qend, candidates);
    }
}


// Shard implementation

void Shard::print() const
{
    cout << "    start:" << this->sstart << " ..end:" << this->send << endl;

    cout << "    impact list:" << endl;
    for (const auto& entry : this->impactList)
    {
        cout << "      t:" << entry.first << " -> i:" << entry.second << endl;
    }
        
    cout << "    record entries:" << endl;
    auto i = 0;
    for (const auto& entry : *this)
    {
        cout << "     [i:" << i++ << "] ";
        entry.print('r');
        cout << endl;
    }
}


size_t Shard::getSize()
{
    size_t size = 0;
    
    // Shard start and end timestamps
    size += 2 * sizeof(Timestamp);
    
    // Size of the vector structure itself (inherits from vector<Record>)
    size += sizeof(vector<Record>);
    
    // Records stored in the vector
    size += this->size() * sizeof(Record);
    
    // Size of the impact list map structure
    size += sizeof(this->impactList);
    
    // Impact list entries
    for (const auto& entry : this->impactList)
    {
        size += sizeof(Timestamp) + sizeof(size_t);
    }

    return size;
}


void Shard::getCandidates(const Timestamp& qstart, const Timestamp& qend, RelationId& candidates) const
{
    // Query does not overlap with shard
    if (this->sstart > qend || this->send < qstart) return;

    // Get the offset from the impact list
    auto offset = 0;
    auto impact_iter = this->impactList.upper_bound(qstart);
    
    if (impact_iter != this->impactList.begin())
    {
        --impact_iter;
        offset = (*impact_iter).second;
    };

    auto shard_iter = this->begin() + offset;

    // Iterate over all entries starting at the offset
    for (;shard_iter != this->end(); ++shard_iter)
    {
        if (shard_iter->end < qstart) continue;
        if (shard_iter->start > qend) break;

        candidates.push_back(shard_iter->id);
    }
}


void ShardedPostingList::extractRecords(Relation &R) const
{
    // Extract all records from all shards
    R.clear();
    
    for (const auto& shard : *this)
    {
        for (const auto& entry : shard)
        {
            R.emplace_back(entry.id, entry.start, entry.end);
        }
    }
}


void ShardedPostingList::addRecord(const RecordId rid, const Timestamp rstart, const Timestamp rend, const RecordId impact_list_gap, const Timestamp relaxation)
{
    // Algorithm for idealized sharding (η = 0) without wasted reads
    // Assumption: records-to-add are ordered by start

    // Remark (see Anand (2013), p. 40):
    // When using idealized sharding, a large number of shards can be generated
    // depending on the distribution of time intervals. The performance of 
    // query processing may suffer as each shard necessitates a costly 
    // random-seek operation. In some cases, it might be advantageous to 
    // decrease the number of shards produced even if in incurs wasted reads.

    // Ensure there is a shard with s.end <= r.end
    if (find_if(
            (*this).begin(), 
            (*this).end(), 
            [&rend,&rstart,&relaxation](const auto& s)
            {
                 return s.sstart_last < rstart && s.send < (rend + relaxation); 
            })
        == (*this).end())
    {
        auto s = Shard();
        s.sstart = rstart;
        s.sstart_last = rstart;
        s.send = rend;
        s.emplace_back(rid, rstart, rend);

        (*this).push_back(s);

        return;
    }

    // Find the shard with argmin(r.end - s.end), i.e., smallest gap
    auto shard_idx = 0;
    auto min_gap = numeric_limits<size_t>::max();
    for (size_t i = 0; i < this->size(); ++i)
    {
        const auto& s = (*this)[i];

        if (s.sstart_last > rstart) continue;

        if ((s.send - relaxation) > rend) continue;

        if (abs(rend - s.send) < min_gap)
        {
            min_gap = abs(rend - s.send);
            shard_idx = i;
        }
    }

    auto& shard = (*this)[shard_idx]; 

    // If there is a gap between old s.end r.start, add offset to impact list
    if (rstart > (shard.send))
    {
        shard.impactList[rstart] = shard.size(); 
    }
    
    if (impact_list_gap != 0 && shard.size() % impact_list_gap == 0)
    {
        // Only add an entry if it differs from the last
        auto new_timespamp = shard.send+1;
        auto create_entry = true;
        if (!shard.impactList.empty())
        {
            auto& last_timestamp = (--shard.impactList.end())->first;

            // Update last entry
            if (new_timespamp == last_timestamp)
            {
                (--shard.impactList.end())->second = shard.size();
                create_entry = false;
            }
        }

        if (create_entry) shard.impactList[new_timespamp] = shard.size(); 
    }

    shard.emplace_back(rid, rstart, rend);

    shard.sstart_last = rstart;
    shard.send = max(rend, shard.send);
}


