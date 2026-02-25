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

#ifndef _RELATIONS_H_
#define _RELATIONS_H_

#include "../utils/global.h"
#include <unordered_map>


// print containers
template <typename Container>
string container_str(const Container& container, const string& delimiter=",")
{
    ostringstream oss;
    oss << "{";
    auto it = container.begin();
    for (auto it = container.begin(); it != container.end(); ++it)
    {
        oss << *it << delimiter;
    }
    oss << "}";
    return oss.str();
}


class Record
{
public:
    RecordId id;
    Timestamp start;
    Timestamp end;
    
    // Constructing
    Record();
    Record(RecordId id);
    Record(RecordId id, Timestamp start, Timestamp end);

    // Comparing; by default compared by start
    bool operator < (const Record &rhs) const;
    bool operator >= (const Record &rhs) const;
    
    // Printing
    void print(const char c) const;
    string str() const;
    
    // Destructing
    ~Record();
};

// Auxiliary comparators
inline bool compareRecordsById(const Record &lhs, const Record &rhs)
{
    return (lhs.id < rhs.id);
}

inline bool compareRecordsByEnd(const Record &lhs, const Record &rhs)
{
    if (lhs.end == rhs.end)
        return (lhs.id < rhs.id);
    else
        return (lhs.end < rhs.end);
}


class RecordStart
{
public:
    RecordId id;
    Timestamp start;
    
    // Constructing
    RecordStart();
    RecordStart(RecordId id, Timestamp start);

    // Comparing; by default compared by start
    bool operator < (const RecordStart &rhs) const;
    bool operator >= (const RecordStart &rhs) const;
    
    // Printing
    void print(const char c) const;
    
    // Destructing
    ~RecordStart();
};


// An IR record
class IRecord : public Record
{
public:
    // Set of elements contained in the record
    vector<ElementId> elements;

    // Constructing
    IRecord();
    IRecord(RecordId id);
    IRecord(RecordId id, Timestamp start, Timestamp end);

    // Containment checking
    bool containsElements(const vector<ElementId> &elems, const unsigned int offset) const;

    string str(bool with_id) const;
};



class Relation : public vector<Record>
{
public:
    // Stats
    Timestamp gstart;
    Timestamp gend;
    
    // Constructing
    Relation();                     // Construct an empty relation
    Relation(const char *filename); // Load a relation from a file
    Relation(const Relation &R);    // Construct a relation from another (copy)
    void init();                    // Initialize stats
    void determineDomain();

    // Sorting
    void sortById();
    void sortByStart();
    void sortByEnd();

    
    // Printing
    void print(char c) const;
    string str(const size_t top=-1) const;
    
    // Destructor
    ~Relation();
};
typedef Relation::const_iterator RelationIterator;


class RelationStart : public vector<RecordStart>
{
public:
    // Stats
    Timestamp gstart;
    
    // Constructing
    RelationStart();                       // Construct an empty relation
    RelationStart(const RelationStart &R); // Construct a relation from another (copy)
    void init();                           // Initialize stats

    // Sorting
//    void sortById();
    void sortByStart();
    
    // Printing
    void print(char c) const;
    
    // Destructor
    ~RelationStart();
};
typedef RelationStart::const_iterator RelationStartIterator;


typedef vector<RecordId> RelationId;
typedef vector<RecordId>::iterator RelationIdIterator;


class IRelation : public vector<IRecord>
{
public:
    // Stats
    Timestamp gstart;
    Timestamp gend;
    
    // Constructing
    IRelation();                       // Construct an empty relation
    IRelation(const IRelation &iR);    // Construct from another (copy)
    void init();                       // Initialize stats
    void determineDomain();
    IRelation subset(const ElementId &elemId_min, const ElementId &elemId_max);

    // Sorting
    void sortById();
    void sortByStart();
    void sortByEnd();
    
    // Printing
    void print(char c) const;
    string str(const size_t top=-1) const;
    
    // Querying
    void query(const RangeIRQuery &q, RelationId &result);
    
    // Destructor
    ~IRelation();
};
typedef IRelation::const_iterator IRelationIterator;



// Auxiliary comparators
inline bool compareTimestampsByStart(
    const pair<Timestamp, Timestamp> &lhs, 
    const pair<Timestamp, Timestamp> &rhs)
{
    return (lhs.first < rhs.first);
}

inline bool compareTimestampsByEnd(
    const pair<Timestamp, Timestamp> &lhs, 
    const pair<Timestamp, Timestamp> &rhs)
{
    return (lhs.second < rhs.second);
}
#endif //_RELATIONS_H_
