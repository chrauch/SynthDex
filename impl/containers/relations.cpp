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
 *
 * Extending
 *
 * Project:  hint
 * Purpose:  Indexing interval data
 * Author:   Panagiotis Bouros, pbour@github.io
 * Author:   George Christodoulou
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

#include "relations.h"
#include <unordered_set>



// Record class
Record::Record()
{
}


Record::Record(RecordId id)
    : id(id), start(0), end(0)
{
}


Record::Record(RecordId id, Timestamp start, Timestamp end)
    : id(id), start(start), end(end)
{
}


bool Record::operator < (const Record& rhs) const
{
    if (this->start == rhs.start)
        return this->end < rhs.end;
    else
        return this->start < rhs.start;
}


bool Record::operator >= (const Record& rhs) const
{
    return !((*this) < rhs);
}


void Record::print(const char c) const
{
    cout << c << this->id << ": [" << this->start << ".." << this->end << "]" << endl;
}


string Record::str() const
{
    return "[" + to_string(start) + "," + to_string(end) + "] ";
}


Record::~Record()
{
}


// RecordStart class
RecordStart::RecordStart()
{
}


RecordStart::RecordStart(RecordId id, Timestamp start)
{
    this->id = id;
    this->start = start;
}


bool RecordStart::operator < (const RecordStart& rhs) const
{
    if (this->start == rhs.start)
        return this->id < rhs.id;
    else
        return this->start < rhs.start;
}


bool RecordStart::operator >= (const RecordStart& rhs) const
{
    return !((*this) < rhs);
}


void RecordStart::print(const char c) const
{
    cout << c << this->id << ": " << this->start << endl;
}


RecordStart::~RecordStart()
{
}


// IRecord class
IRecord::IRecord()
{
}


IRecord::IRecord(RecordId id)
    : Record(id)
{
}


IRecord::IRecord(RecordId id, Timestamp start, Timestamp end)
    : Record(id, start, end)
{
}


// For now only a binary search approach is implemented
bool IRecord::containsElements(const vector<ElementId> &elements, const unsigned int offset) const
{
    vector<ElementId>::const_iterator iterTBegin = this->elements.begin();
    vector<ElementId>::const_iterator iterTEnd = this->elements.end();

    for (auto i = offset; i < elements.size(); i++)
    {
        if (!binary_search(iterTBegin, iterTEnd, elements[i], greater<ElementId>()))
            return false;
    }
    
    return true;
}


string IRecord::str(bool with_id = true) const
{
    string str = with_id ? (to_string(id) + " ") : "";
    str += to_string(start) + " " + to_string(end) + " ";

    for (auto t : elements)
    {
        str += to_string(t) + ",";
    }
    str.pop_back();
    return str;
};


// Relation class
Relation::Relation()
{
    this->init();
}


Relation::Relation(const Relation &R) : vector<Record>(R)
{
    this->gstart          = R.gstart;
    this->gend            = R.gend;
}


Relation::Relation(const char *filename)
{
    Timestamp rstart, rend;
    ifstream inp(filename);
    RecordId numRecords = 0;

    if (!inp)
    {
        cerr << endl << "Cannot open data file \"" << filename << "\"" << endl << endl;
        exit(1);
    }

    this->init();
    while (inp >> rstart >> rend)
    {
        if (rstart > rend)
        {
            cerr << endl << "Start is after end for interval [" << rstart << ".." << rend << "]" << endl << endl;
            exit(1);
        }
        
        this->emplace_back(numRecords, rstart, rend);
        numRecords++;

        this->gstart = min(this->gstart, rstart);
        this->gend   = max(this->gend  , rend);
    }
    inp.close();
}


void Relation::init()
{
    this->gstart          = numeric_limits<Timestamp>::max();
    this->gend            = numeric_limits<Timestamp>::min();
}


void Relation::sortById()
{
    sort(this->begin(), this->end(), compareRecordsById);
}


void Relation::sortByStart()
{
    sort(this->begin(), this->end());
}


void Relation::sortByEnd()
{
    sort(this->begin(), this->end(), compareRecordsByEnd);
}


void Relation::determineDomain()
{
    this->gstart = numeric_limits<Timestamp>::max();
    this->gend   = numeric_limits<Timestamp>::min();

    for (const Record& rec : *this)
    {
        this->gstart = min(this->gstart, rec.start);
        this->gend   = max(this->gend, rec.end);
    }
}


void Relation::print(char c) const
{
    for (const Record& rec : (*this))
        rec.print(c);
}


string Relation::str(const size_t top) const
{
    string str = "Relation " + to_string(this->size()) + " [" + to_string(this->gstart) + ".." + to_string(this->gend) + "]\n";
    size_t count = 0;
    for (const Record& rec : (*this)) {
        if (top > 0 && count >= top)
            break;
        str += rec.str() + "\n";
        ++count;
    }
    return str;
}


Relation::~Relation()
{
}



// RelationStart class
RelationStart::RelationStart()
{
    this->init();
}


RelationStart::RelationStart(const RelationStart &R) : vector<RecordStart>(R)
{
    this->gstart = R.gstart;
}


void RelationStart::init()
{
    this->gstart = numeric_limits<Timestamp>::max();
}


void RelationStart::sortByStart()
{
    sort(this->begin(), this->end());
}


void RelationStart::print(char c) const
{
    for (const RecordStart& rec : (*this))
        rec.print(c);
}


RelationStart::~RelationStart()
{
}



// IRelation class
IRelation::IRelation()
{
    this->init();
}


IRelation::IRelation(const IRelation &iR) : vector<IRecord>(iR)
{
    this->gstart = iR.gstart;
    this->gend   = iR.gend;
}


void IRelation::init()
{
    this->gstart = numeric_limits<Timestamp>::max();
    this->gend   = numeric_limits<Timestamp>::min();
}


IRelation IRelation::subset(const ElementId &elemId_min, const ElementId &elemId_max)
{
    IRelation R_subset;
    for (auto &r : *this)
    {
        IRecord r_copy {r.id, r.start, r.end};
        
        for (const auto &tid : r.elements)
        {
            if (tid >= elemId_min && tid <= elemId_max)
            {
                r_copy.elements.push_back(tid);
            }
        }

        if (!r_copy.elements.empty())
        {
            R_subset.push_back(r_copy);
        }
    }

    R_subset.determineDomain();

    return R_subset;
}


void IRelation::determineDomain()
{
    this->gstart = numeric_limits<Timestamp>::max();
    this->gend   = numeric_limits<Timestamp>::min();

    for (const IRecord& rec : *this)
    {
        this->gstart = min(this->gstart, rec.start);
        this->gend   = max(this->gend, rec.end);
    }
}


void IRelation::sortById()
{
    sort(this->begin(), this->end(), compareRecordsById);
}


void IRelation::sortByStart()
{
    sort(this->begin(), this->end());
}


void IRelation::sortByEnd()
{
    sort(this->begin(), this->end(), compareRecordsByEnd);
}


void IRelation::print(char c) const
{
    for (const IRecord& rec : (*this))
        rec.print(c);
}


string IRelation::str(const size_t top) const
{
    string str = "Relation " + to_string(this->size()) + " [" + to_string(this->gstart) + ".." + to_string(this->gend) + "]\n";
    size_t count = 0;
    for (const IRecord& rec : (*this)) {
        if (top > 0 && count >= top)
            break;
        str += rec.str() + "\n";
        ++count;
    }
    return str;
}


void IRelation::query(const RangeIRQuery &q, RelationId &result)
{
    for (const IRecord &r : (*this))
    {
        if ((r.start <= q.end) && (q.start <= r.end) && (r.containsElements(q.elems, 0)))
            result.push_back(r.id);
    }
}


IRelation::~IRelation()
{
}
