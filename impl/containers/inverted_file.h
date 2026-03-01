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
 * Copyright (c) 2022 - 2024
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

#ifndef _INVERTED_FILE_H_
#define _INVERTED_FILE_H_

#include <unordered_map>
#include "hierarchical_index.h"
#include "../containers/relations.h"
#include "../utils/global.h"



class InvertedFileTemplate : public IRIndex
{
public:
    // Statistics
    RecordId numIndexedRecords;

    // Constructing
    InvertedFileTemplate()          { };
    virtual void getStats()         { };
    virtual size_t getSize()        { return 0; };
    virtual void print(char c)      { };
    virtual ~InvertedFileTemplate() { };
        
    // Querying
    virtual void query(const RangeIRQuery &q, RelationId &result) { };

    // Soft-deleting (tombstone)
    virtual void softdelete(const vector<bool> &idsToDelete) { };
};



class InvertedFile : public InvertedFileTemplate
{
private:
    // Indexed relation
    IRelation R;

    // Posting lists
    unordered_map<ElementId, RelationId> lists;
    
public:

    // Constructing
    InvertedFile();
    InvertedFile(const IRelation &R);
    void index(const IRecord &r);
    void getStats();
    size_t getSize();
    void print(char c);
    ~InvertedFile();

    // Querying
    bool moveOut(const RangeIRQuery &q, RelationId &candidates);
    bool intersect(const RangeIRQuery &q, const unsigned int elemoff, RelationId &candidates);
    void intersectAndOutput(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result);

    // Soft-deleting (tombstone: replace id with -1)
    void softdelete(const vector<bool> &idsToDelete);
};
#endif // _INVERTED_FILE_H_
