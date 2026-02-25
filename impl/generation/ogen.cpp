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

#include <string>
#include <random>
#include <cmath>
#include <map>
#include <vector>
#include <set>
#include <algorithm>
#include <omp.h>

#include "ogen.h"


OGen::OGen(OStats &os)
    : os(os),
      dev_clamp(Cfg::get<double>("o.ext.elem-dev-clamp")),
      num_threads(Cfg::get_threads())
{
    if (this->os.card < 100000)
        this->num_threads = min(this->num_threads, 4);

    this->max_elems_per_o
        = Cfg::get<double>("o.elem.max-dict-ratio-per-o")
        * (double)this->os.dict;
}


IRelation OGen::construct_O()
{
    omp_set_num_threads(this->num_threads);

    Timer tim;
    tim.start();

    auto O = IRelation();
    O.resize(this->os.card);

    //int progress_step = max(1, this->os.card / 10);

    #pragma omp parallel for schedule(dynamic)
    for (auto oid = 0; oid < this->os.card; oid++)
    {
        O[oid] = this->construct_o(oid);

        //if (progress_step > 0 && (oid + 1) % progress_step == 0)
        //{
        //#pragma omp critical
        //    {
        //        int percent = ((oid + 1) * 100) / this->os.card;
        //        Log::w(1, "Progress: " + to_string(percent + 1) + "%");
        //    }
        //}
    }

    this->sanitize(O);

    double time = tim.stop();
    Log::w(1, "Construction [s]", to_string(time));

    return O;
}


IRecord OGen::construct_o(RecordId &oid)
{
    // Create thread-local random generator for parallel execution
    thread_local RandomGen local_randomgen;

    IRecord o(oid);

    // Extent
    double ext_rnd = local_randomgen.rndd(0, 1);
    int ext_q = static_cast<int>(ext_rnd * this->os.extents.size());
    auto ext_b = this->os.extents[ext_q];

    auto min_len = ext_q == 0 ? 1
        : pow(10, this->os.extents[ext_q - 1].min_len_log);
    auto max_len = pow(10, this->os.extents[ext_q].max_len_log);

    double ext_frac = ext_rnd * this->os.extents.size() - ext_q;
    auto ext_raw = static_cast<long long>(min_len + ext_frac * (max_len - min_len));
    
    // Safely clamp to valid range using long long arithmetic
    long long domain_ll = static_cast<long long>(this->os.domain);
    long long ext_ll = min(ext_raw, domain_ll - 1);
    if (ext_ll < 1) ext_ll = 1;
    
    auto ext = static_cast<int>(ext_ll);

    if (this->os.domain <= 0 || ext < 0 || ext >= this->os.domain) ext = 1;
    
    long long max_start_pos_ll = domain_ll - ext_ll;
    if (max_start_pos_ll < 0)
    {
        max_start_pos_ll = 0;
        ext = this->os.domain;
    }
    
    int max_start_pos = static_cast<int>(max_start_pos_ll);
    auto start = local_randomgen.rndi(0, max_start_pos);
    
    // Use long long for end calculation to avoid overflow
    long long end_ll = static_cast<long long>(start) + static_cast<long long>(ext);
    auto end = static_cast<int>(end_ll);

    o.start = start;
    o.end = end;

    // Elements - using log-normal distribution
    int elems_cnt = 1;
    auto avg = pow(10, ext_b.avg_elem_cnt_log);
    auto dev = pow(10, ext_b.dev_elem_cnt_log);

    if (dev <= this->dev_clamp)
        elems_cnt = avg;
    else
    {
        // Calculate log-space statistics from normal space statistics
        double log_std_dev = sqrt(log(1.0 + (dev * dev) / (avg * avg)));
        double log_mean = log(avg) - (log_std_dev * log_std_dev) / 2.0;

        // Generate normal random in log space
        double log_value = local_randomgen.rndd_gauss(log_mean, log_std_dev);

        // Transform back to normal space
        elems_cnt = (int)round(exp(log_value));
    }

    if (elems_cnt < 1) elems_cnt = 1;

    if (elems_cnt > max_elems_per_o)
    {
        Log::w(3, "Warning - Large number of elements in object " 
            + to_string(oid) + ": " + to_string(elems_cnt)
            + ", setting it to max: " + to_string(max_elems_per_o) + ".");
        elems_cnt = max_elems_per_o;
    }

    set<ElementId> elems;
    while (elems_cnt > elems.size())
    {
        double elem_rnd = local_randomgen.rndd(0, 1);
        int elem_q = static_cast<int>(elem_rnd * this->os.elements.size());

        auto lo_rank_log = elem_q == 0 ? 0
            : this->os.elements[elem_q - 1].lo_rank_log;
        auto hi_rank_log = this->os.elements[elem_q].hi_rank_log;

        auto rnd_rank_log = lo_rank_log == hi_rank_log
            ? hi_rank_log
            : local_randomgen.rndd(lo_rank_log, hi_rank_log);

        auto rnd_eid = static_cast<ElementId>(pow(10, rnd_rank_log)) - 1;

        elems.emplace(rnd_eid);
    }

    vector<ElementId> elemss(elems.begin(), elems.end());
    sort(elemss.begin(), elemss.end(), greater<ElementId>());

    o.elements = elemss;

    return o;
}


void OGen::sanitize(IRelation &O)
{
    RandomGen randomgen;
    int random_index = randomgen.rndi(0, O.size() - 1);

    auto &o = O[random_index];
    o.start = 0;
    o.elements[0] = this->os.dict - 1;

    O.gstart = 0;
    O.gend = this->os.domain;
}