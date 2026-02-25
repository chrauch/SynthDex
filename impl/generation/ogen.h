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

#ifndef _OGEN_H_
#define _OGEN_H_

#include "../utils/global.h"
#include "../containers/relations.h"
#include "../containers/hierarchical_index.h"
#include "../learning/stats.h"
#include "../learning/statsserializer.h"
#include "randomgen.h"

using namespace std;


class OGen
{
public:
    OGen(OStats &ostats);
    IRelation construct_O();

private:
    IRecord construct_o(RecordId &oid);
    void sanitize(IRelation &O);

    int num_threads;
    int max_elems_per_o;
    OStats os;
    double dev_clamp;
};

#endif // _OGEN_H_