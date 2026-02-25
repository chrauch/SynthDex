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

#include "qgenselect.h"
#include "randomgen.h"
#include <algorithm>
#include <cmath>


QGenSelect::QGenSelect(const IRelation &O)
    : O(O)
{
}


QGenSelect::~QGenSelect() = default;


void QGenSelect::ensure_index()
{
    if (this->index) return;

    Log::w(1, "Building irHINT(10) for selectivity filtering");

    Timer tim;
    tim.start();

    Boundaries bounds(
        0, numeric_limits<ElementId>::max(),
        this->O.gstart, this->O.gend);

    this->index = make_unique<irHINTa>(bounds, 10, this->O);

    auto time = tim.stop();
    Log::w(1, "irHINT(10) built [s]", time);
}


optional<pair<double, double>> QGenSelect::parse_select(const string &pattern)
{
    string key = "q.gen.workload." + pattern + ".select";

    try
    {
        string val = Cfg::get<string>(key);

        size_t dash_pos = val.find('-');
        double lo, hi;

        if (dash_pos != string::npos)
        {
            lo = stod(val.substr(0, dash_pos));
            hi = stod(val.substr(dash_pos + 1));
        }
        else
        {
            lo = stod(val);
            hi = lo;
        }

        if (lo < 0 || hi < 0 || lo > hi)
            throw runtime_error("Invalid select range: " + val);

        return make_pair(lo, hi);
    }
    catch (const runtime_error &e)
    {
        // Key not found – pattern has no selectivity constraint
        string msg(e.what());
        if (msg.find("Path not found") != string::npos)
            return nullopt;

        // Re-throw if it was a parse / range error
        throw;
    }
}


vector<RangeIRQuery> QGenSelect::filter_by_selectivity(
    const vector<RangeIRQuery> &candidates,
    size_t sel_min_count,
    size_t sel_max_count,
    size_t &out_min,
    size_t &out_max) const
{
    vector<RangeIRQuery> accepted;
    accepted.reserve(candidates.size());

    for (const auto &q : candidates)
    {
        RelationId result;
        result.reserve(128);

        ElementId elem_off = 0;
        const_cast<irHINTa *>(this->index.get())
            ->move_out(q, elem_off, result);

        size_t cnt = result.size();
        if (cnt >= sel_min_count && cnt <= sel_max_count)
        {
            accepted.push_back(q);
            if (cnt < out_min) out_min = cnt;
            if (cnt > out_max) out_max = cnt;
        }
    }

    return accepted;
}


vector<RangeIRQuery> QGenSelect::construct_Q_filtered(
    function<vector<RangeIRQuery>(int)> gen_batch,
    int needed,
    double sel_lo_pct,
    double sel_hi_pct,
    const string &pattern)
{
    // Special case: select 0-0 or 0 means zero-result queries
    if (sel_lo_pct == 0.0 && sel_hi_pct == 0.0)
        return this->construct_Q_zero_result(gen_batch, needed, pattern);

    this->ensure_index();

    size_t O_size = this->O.size();
    double sel_lo = sel_lo_pct / 100.0;
    double sel_hi = sel_hi_pct / 100.0;

    size_t sel_min_count = static_cast<size_t>(floor(sel_lo * O_size));
    size_t sel_max_count = static_cast<size_t>(ceil(sel_hi * O_size));

    Log::w(1, "Selectivity-filtered generation",
        pattern + " | target=" + to_string(needed)
        + " | select=" + to_string(sel_lo_pct)
        + "-" + to_string(sel_hi_pct) + "%"
        + " | result-count range=[" + to_string(sel_min_count)
        + "," + to_string(sel_max_count) + "]");

    vector<RangeIRQuery> accepted;
    accepted.reserve(needed);

    int round = 0;
    int total_generated = 0;
    size_t observed_min = numeric_limits<size_t>::max();
    size_t observed_max = 0;

    while (static_cast<int>(accepted.size()) < needed
           && round < max_rounds)
    {
        int still_needed = needed - static_cast<int>(accepted.size());
        int batch_size = needed * batch_multiplier;

        auto candidates = gen_batch(batch_size);
        total_generated += static_cast<int>(candidates.size());

        auto good = this->filter_by_selectivity(
            candidates, sel_min_count, sel_max_count,
            observed_min, observed_max);

        for (auto &q : good)
        {
            if (static_cast<int>(accepted.size()) >= needed) break;
            accepted.push_back(move(q));
        }

        double accept_rate = candidates.empty()
            ? 0.0
            : static_cast<double>(good.size()) / candidates.size() * 100.0;

        Log::w(2, "Round " + to_string(round + 1),
            "generated=" + to_string(candidates.size())
            + " accepted=" + to_string(good.size())
            + " (" + to_string(accept_rate) + "%)"
            + " total=" + to_string(accepted.size())
            + "/" + to_string(needed));

        ++round;
    }

    if (static_cast<int>(accepted.size()) < needed)
    {
        Log::w(1, "WARNING: selectivity filter could not produce enough queries",
            pattern + " | produced=" + to_string(accepted.size())
            + "/" + to_string(needed)
            + " after " + to_string(round) + " rounds"
            + " (" + to_string(total_generated) + " candidates tested)");
    }

    // Re-number ids
    for (size_t i = 0; i < accepted.size(); ++i)
        accepted[i].id = i;

    Log::w(1, "Selectivity-filtered result",
        pattern + " | " + to_string(accepted.size()) + " queries"
        + " from " + to_string(total_generated) + " candidates"
        + " in " + to_string(round) + " rounds"
        + " | result-count [" + to_string(observed_min)
        + "," + to_string(observed_max) + "]");

    return accepted;
}


vector<RangeIRQuery> QGenSelect::construct_Q_zero_result(
    function<vector<RangeIRQuery>(int)> gen_batch,
    int needed,
    const string &pattern)
{
    this->ensure_index();

    Log::w(1, "Zero-result query generation",
        pattern + " | target=" + to_string(needed));

    // Generate a pool of base queries to recombine
    int pool_size = needed * batch_multiplier;
    auto pool = gen_batch(pool_size);

    Log::w(2, "Base pool generated", to_string(pool.size()) + " queries");

    if (pool.size() < 2)
    {
        Log::w(1, "WARNING: not enough base queries for recombination",
            pattern + " | pool=" + to_string(pool.size()));
        return {};
    }

    RandomGen rng;
    vector<RangeIRQuery> accepted;
    accepted.reserve(needed);

    int round = 0;
    int total_tested = 0;

    while (static_cast<int>(accepted.size()) < needed
           && round < max_rounds)
    {
        int still_needed = needed - static_cast<int>(accepted.size());
        int batch_tested = 0;
        int batch_accepted = 0;

        // Try recombinations from the current pool
        int attempts = needed * batch_multiplier;
        for (int a = 0; a < attempts
                && static_cast<int>(accepted.size()) < needed; ++a)
        {
            // Pick two distinct random queries from the pool
            int idx_elems = rng.rndi(0, pool.size() - 1);
            int idx_ext   = rng.rndi(0, pool.size() - 1);
            while (idx_ext == idx_elems && pool.size() > 1)
                idx_ext = rng.rndi(0, pool.size() - 1);

            // Recombine: elements from one, extent from the other
            RangeIRQuery q;
            q.start = pool[idx_ext].start;
            q.end   = pool[idx_ext].end;
            q.elems = pool[idx_elems].elems;
            q.id    = accepted.size();

            // Test against the index
            RelationId result;
            result.reserve(16);
            ElementId elem_off = 0;
            this->index->move_out(q, elem_off, result);

            ++batch_tested;

            if (result.empty())
            {
                accepted.push_back(move(q));
                ++batch_accepted;
            }
        }

        total_tested += batch_tested;

        double accept_rate = batch_tested > 0
            ? static_cast<double>(batch_accepted) / batch_tested * 100.0
            : 0.0;

        Log::w(2, "Round " + to_string(round + 1),
            "tested=" + to_string(batch_tested)
            + " accepted=" + to_string(batch_accepted)
            + " (" + to_string(accept_rate) + "%)"
            + " total=" + to_string(accepted.size())
            + "/" + to_string(needed));

        // If acceptance rate is very low, regenerate a fresh pool
        if (batch_accepted == 0 && round < max_rounds - 1)
        {
            pool = gen_batch(pool_size);
            Log::w(2, "Refreshed base pool", to_string(pool.size()) + " queries");
        }

        ++round;
    }

    if (static_cast<int>(accepted.size()) < needed)
    {
        Log::w(1, "WARNING: zero-result generation could not produce enough queries",
            pattern + " | produced=" + to_string(accepted.size())
            + "/" + to_string(needed)
            + " after " + to_string(round) + " rounds"
            + " (" + to_string(total_tested) + " recombinations tested)");
    }

    // Re-number ids
    for (size_t i = 0; i < accepted.size(); ++i)
        accepted[i].id = i;

    Log::w(1, "Zero-result generation result",
        pattern + " | " + to_string(accepted.size()) + " queries"
        + " from " + to_string(total_tested) + " recombinations"
        + " in " + to_string(round) + " rounds");

    return accepted;
}
