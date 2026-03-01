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

#ifndef _SYNTHDEX_H_
#define _SYNTHDEX_H_

#include "../utils/global.h"
#include "../learning/stats.h"
#include "../containers/relations.h"
#include "../containers/hierarchical_index.h"
#include "../structure/framework.h"
#include "../structure/idxschemaserializer.h"


class SynthDex : public IRIndex
{
public:
    SynthDex(
        const IRelation &O,
        const IdxSchema &schema,
        const OStats &ostats);

    void query(const RangeIRQuery &q, RelationId &result);
    
    void update(const IRelation &R);
    
    void remove(const vector<bool> &idsToDelete);

    void softdelete(const vector<bool> &idsToDelete);
    
    size_t getSize();
    
    ~SynthDex();
    
    string str() const;
    
private:
    IdxSchema schema;
    OStats ostats;
    
    Moveout* index;

    Moveout* construct(
        const IRelation &O,
        const IdxSchema &schema,
        const Boundaries &boundaries);

    Moveout* new_node(
        const IRelation &O,
        const IdxSchema &schema,
        const Boundaries &boundaries);

    Moveout* new_terminal_node(
        const IRelation &O,
        const vector<string> &method,
        const Boundaries &boundaries);

    Moveout* new_fanout_node(
        const IRelation &O,
        const IdxSchema &schema,
        const Boundaries &boundaries);

    Moveout* new_fanout_node_hybrid(
        const IdxSchema &schema,
        const IRelation &O);

    Moveout* new_fanout_node_refined(
        const IdxSchema &schema,
        const IRelation &O);

    Moveout* new_fanout_node_splitted(
        const IdxSchema &schema,
        const IRelation &O);

    Moveout* new_fanout_node_sliced(
        const IdxSchema &schema,
        const IRelation &O);
};

#endif // _SYNTHDEX_H_