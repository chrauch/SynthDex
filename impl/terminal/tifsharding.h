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

#ifndef _tIFSHARDING_
#define _tIFSHARDING_

#include <unordered_map>
#include <map>
#include <vector>
#include "../structure/framework.h"
#include "../containers/relations.h"
#include "../utils/global.h"


// Shard entry containing a single record
struct ShardEntry
{
    RecordId id;
    Timestamp start;
    Timestamp end;

    ShardEntry(RecordId rid, Timestamp rstart, Timestamp rend)
        : id(rid), start(rstart), end(rend) {}

    void print(const char c) const
    {
        cout << c << id << " [" << start << ".." << end << "]";
    }
};


// A shard containing multiple records with impact list for fast access
class Shard : public vector<ShardEntry>
{
public:
    Timestamp sstart;       // Shard start
    Timestamp sstart_last;  // Start of last added record
    Timestamp send;         // Shard end
    map<Timestamp, size_t> impactList; // Impact list: timestamp -> offset

    Shard() : sstart(0), sstart_last(0), send(0) {}

    void print() const;
    size_t getSize();
    void getCandidates(const Timestamp& qstart, const Timestamp& qend, RelationId& candidates) const;
};


// Sharded posting list containing multiple shards
class ShardedPostingList : public vector<Shard>
{
public:
    void addRecord(const RecordId rid, const Timestamp rstart, const Timestamp rend, 
                   const RecordId impact_list_gap, const Timestamp relaxation);
    void getCandidates(const Timestamp& qstart, const Timestamp& qend, RelationId& candidates) const;
    void extractRecords(Relation &R) const;
    void print() const;
    size_t getSize();
};


class tIFSHARDING : public Refinement_ElemFreq
{
private:
    float relaxation;
    double dynbase;
    double dynratio;
    Timestamp tolerance;
    ElementId elem_min;
    ElementId elem_max;

    // TODO: impact list gap as tunable parameter
    unsigned int impactListGap = 1000;
    
    // Posting lists
    unordered_map<ElementId, ShardedPostingList*> lists;
    
public:
    // Constructing
    tIFSHARDING();
    tIFSHARDING(
        const Boundaries &boundaries,
        const float relaxation,
        const double dynbase,
        const double dynratio,
        const IRelation &R);
    void getStats();
    size_t getSize();
    void print(char c);
    ~tIFSHARDING();

    // Updating
    void update(const IRelation &R);

    // Deleting
    void remove(const vector<bool> &idsToDelete);

    // Soft-deleting (tombstone: replace id with -1)
    void softdelete(const vector<bool> &idsToDelete);

    // Querying
    bool moveOut_checkBoth(const RangeIRQuery &q, RelationId &candidates);
    bool intersectx(const RangeIRQuery &q, const unsigned int elemoff, RelationId &candidates);
    bool intersectAndOutput(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result);
    
    void move_out(
        const RangeIRQuery &q,
        ElementId &elem_off,
        RelationId &candidates);

    void refine(
        const RangeIRQuery &q,
        ElementId &elem_off,
        RelationId &candidates);

    string str(const int level) const;
};


#endif
