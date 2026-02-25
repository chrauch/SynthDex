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

#ifndef _tIF_
#define _tIF_

#include <unordered_map>
#include <queue>
#include "../structure/framework.h"
#include "../containers/relations.h"
#include "../containers/inverted_file.h"
#include "../containers/1dgrid.h"
#include "../containers/hint_m.h"
#include "../utils/global.h"



class tIF : public Refinement_ElemFreq
{
private:
    ElementId elem_min;
    ElementId elem_max;
    // Querying
    
public:
    // Posting lists
    unordered_map<ElementId, Relation> lists;

    // Constructing
    tIF();
    tIF(const Boundaries &boundaries, const IRelation &R);
    void index(const IRecord &r);
    void index(const IRecord &r, const ElementId &min, const ElementId &max);
    void getStats();
    size_t getSize();
    void print(char c);
    ~tIF();

    // Updating
    void update(const IRelation &R);

    // Deleting
    void remove(const RelationId &ids);

    // Querying
    bool moveOut_NoChecks(const RangeIRQuery &q, RelationId &candidates);
    bool moveOut_CheckBoth(const RangeIRQuery &q, RelationId &candidates);
    bool moveOut_CheckStart(const RangeIRQuery &q, RelationId &candidates);
    bool moveOut_CheckEnd(const RangeIRQuery &q, RelationId &candidates);
    bool intersectx(const RangeIRQuery &q, const unsigned int elemoff, RelationId &candidates);
    bool intersectAndOutput(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result);
    bool intersectAndOutputAndErase(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result);

    // Querying
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
