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

#ifndef _PERFORMANCEANALYZER_H_
#define _PERFORMANCEANALYZER_H_

#include "../containers/relations.h"
#include "../structure/framework.h"
#include "../learning/stats.h"
#include "../learning/statscomp.h"
#include "../utils/persistence.h"
#include "../utils/logging.h"
#include "../utils/cfg.h"
#include "../utils/global.h"
#include "../utils/indexevaluator.h"
#include <vector>
#include <set>

using namespace std;

class PerformanceAnalyzer : public IndexEvaluator
{
public:
    PerformanceAnalyzer(
        const IRelation& O,
        const vector<tuple<string,vector<RangeIRQuery>>>& Q,
        const OStats& Ostats);

    void run(IRIndex *idx, const iStats &Istats, const double &construction) override;

    void process_query_set(
        IRIndex *idx, 
        const tuple<string, vector<RangeIRQuery>> &Qx,
        size_t size,
        const double &construction,
        const iStats &Istats);

    void emit_score(double total_qtime, int runs_per_q, double total_median_qtime, size_t qnum, const tuple<string, vector<RangeIRQuery>> &Qx, size_t &total_qresult_xor, size_t size, const double &construction, const iStats &Istats);

private:
    const int runs_per_q;
    
    StatsComp statscomp;

    map<string,size_t> result_cnts;
    map<string,size_t> result_xors;
    set<string> history;
};

#endif // _PERFORMANCEANALYZER_H_
