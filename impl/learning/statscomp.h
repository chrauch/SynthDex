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

#ifndef _STATSCOMP_H_
#define _STATSCOMP_H_


#include "../utils/global.h"
#include "../structure/framework.h"
#include "stats.h"
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <limits>
#include <vector>
#include <string>
#include <algorithm>
#include <tuple>
#include <map>
#include <omp.h>
using namespace std;


class StatsComp
{
public:
    StatsComp();
    OStats analyze_O(const IRelation &O, const string &name);
    iStats analyze_i(const IdxSchema &schema, const string &category);
    void analyze_Q(const vector<RangeIRQuery> &Q);
    void analyze_q(const RangeIRQuery &q);
    void analyze_p(
        const size_t &results,
        const double &querytime,
        const size_t &result_xor,
        const size_t &size);
        
    const size_t element_bins;
    const size_t extent_bins;
    const size_t q_elements;
    
    const double dev_clamp;
    const int num_threads;
    
    iStats Istats;
    OStats Ostats;
    vector<qStats> Qstats;
    vector<pStats> Pstats;

    static double median(vector<double> &values);
    static void fill(OStats &stats);
    
    static void estimate_bytes(OStats &Ostats);
    
private:
    void compute_extents(const IRelation &O);
    void compute_elements(const IRelation &O);
    void compute_bytes(const IRelation &O);
    void compute_slices(const IRelation &O);

};
#endif // _STATSCOMP_H_