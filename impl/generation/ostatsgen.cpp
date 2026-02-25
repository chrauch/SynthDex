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

#include "ostatsgen.h"
#include "../learning/statscomp.h"
#include "../utils/persistence.h"
#include "../learning/statsserializer.h"
#include <cmath>
#include <limits>
#include <stdexcept>
#include <set>


OStatsGen::OStatsGen()
    : templates(Persistence::read_O_stats_jsons("",
          Cfg::get<vector<string>>("o.gen.patterns-active"))),
      element_bins(Cfg::get<int>("o.elem.bins")),
      extent_bins(Cfg::get<int>("o.ext.bins"))
{
    this->min_bytes 
        = Cfg::get<double>("o.gen.min-estimated-megabytes")
        * 1024 * 1024;
    this->max_bytes 
        = Cfg::get<double>("o.gen.max-estimated-megabytes")
        * 1024 * 1024;

    Log::w(2, "Min size (estimated)", 
        to_string(this->min_bytes / (1024 * 1024)) + " MB");
    Log::w(2, "Max size (estimated)", 
        to_string(this->max_bytes / (1024 * 1024)) + " MB");

    Log::w(2, "Available templates", this->templates,
        [](const auto &t) { return t.name; });

    for (const auto &t : this->templates)
    {
        if (t.elements.size() != this->element_bins)
            throw invalid_argument("Elements mismatch");
        if (t.extents.size() != this->extent_bins)
            throw invalid_argument("Extents mismatch");
    }
}


OStats OStatsGen::create(const int &i)
{
    string name = "O-rnd_" + Persistence::timestamp(true, "O-rnd") + "-" + to_string(i);

    OStats os;
    int retries = 0;
    while (true)
    {
        os = this->create(name);
        if (os.est_bytes <= this->max_bytes && os.est_bytes >= this->min_bytes)
        {
            Log::w(1, "Identifier", name);
            Log::w(1, "Estimated size [MB]",
                to_string(os.est_bytes / (1024.0 * 1024.0)));
            Log::w(2, "O statistics (short)",
                StatsSerializer::to_json(os, false));
            break;
        }
        else
            retries++;
    }

    if (retries > 0)
        Log::w(3, to_string(retries) + " retries needed to generate OStats whose estimated size was within the specified range.");

    return os;
}


OStats OStatsGen::create(const string &name)
{
    if (this->templates.empty())
        throw invalid_argument("At least one template is required");

    //// Select a random subset of templates
    //vector<OStats> templates_subset = this->randomgen.rnd_select<OStats>(
    //    this->templates, this->randomgen.rndi(1, this->templates.size()));
    //
    //Log::w(3, "Selected templates", templates_subset,
    //    [](const auto &t) { return t.name; });

    // The off parameter extends the ranges
    double off = Cfg::get<double>("o.gen.vary-template-fields");

    OStats mix;
    mix.name = name;

    // Select random values within the observed min-max range
    auto get = [&](auto getter)
    {
        double minval = numeric_limits<double>::max();
        double maxval = numeric_limits<double>::lowest();
        for (const auto &s : this->templates)
        {
            double val = getter(s);
            if (val < minval) minval = val;
            if (val > maxval) maxval = val;
        }
        return this->randomgen.rnd_between(minval, maxval, off);
    };

    // Core fields
    mix.card_log   = get([](const auto &s) { return s.card_log; });
    mix.dict_log   = get([](const auto &s) { return s.dict_log; });
    mix.domain_log = get([](const auto &s) { return s.domain_log; });

    // Elements
    vector<double> elem_hi_rank_logs;

    for (auto &s : this->templates)
        for (const auto &elem : s.elements)
            elem_hi_rank_logs.push_back(elem.hi_rank_log);

    elem_hi_rank_logs = this->randomgen.rnd_vary(elem_hi_rank_logs, off);

    elem_hi_rank_logs = this->randomgen.rnd_select<double>(
        elem_hi_rank_logs, this->element_bins);

    sort(elem_hi_rank_logs.begin(), elem_hi_rank_logs.end());
    mix.dict_log = elem_hi_rank_logs.back();

    mix.elements.reserve(this->element_bins);
    for (size_t i = 0; i < this->element_bins; ++i)
    {
        OElemStats s;
        s.hi_rank_log = elem_hi_rank_logs[i];
        mix.elements.push_back(s);
    }

    // Extents
    mix.extents.reserve(this->extent_bins);
    for (size_t i = 0; i < this->extent_bins; ++i)
    {
        OExtStats s;

        s.max_len_log = get(
            [&](const auto &t) { return t.extents[i].max_len_log; });
        
        s.avg_elem_cnt_log = get(
            [&](const auto &t) { return t.extents[i].avg_elem_cnt_log; });

        s.dev_elem_cnt_log = get(
            [&](const auto &t) { return t.extents[i].dev_elem_cnt_log; });

        mix.extents.push_back(s);
    }

    sort(mix.extents.begin(), mix.extents.end(),
        [](const OExtStats &a, const OExtStats &b)
        { return a.max_len_log < b.max_len_log; });

    mix.domain_log = max(mix.domain_log, mix.extents.back().max_len_log);

    // Fill non-log values
    StatsComp::fill(mix);
    
    StatsComp::estimate_bytes(mix);

    return mix;
}
