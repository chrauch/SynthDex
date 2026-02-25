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

#ifndef _INDEXEVALUATOR_H_
#define _INDEXEVALUATOR_H_

#include "../containers/relations.h"
#include "../structure/framework.h"
#include "../learning/stats.h"
#include <vector>

using namespace std;

class IndexEvaluator
{
public:
    IndexEvaluator(
        const IRelation& O,
        const vector<tuple<string,vector<RangeIRQuery>>>& Q,
        const OStats& Ostats);

    virtual ~IndexEvaluator() = default;
    virtual void run(IRIndex* idx, const iStats& Istats, const double& construction) = 0;

protected:
    const IRelation& O;
    const vector<tuple<string,vector<RangeIRQuery>>>& Q;
    const OStats& Ostats;
};

#endif // _INDEXEVALUATOR_H_
