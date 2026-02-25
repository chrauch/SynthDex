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

#ifndef _QGENSELECT_H_
#define _QGENSELECT_H_

#include "../utils/global.h"
#include "../containers/relations.h"
#include "../terminal/irhint.h"
#include "../learning/stats.h"
#include <optional>
#include <memory>

using namespace std;


/// Handles selectivity-filtered query generation for workload patterns
/// that carry a "select" attribute (e.g. "select": "0-1" means every
/// produced query must select between 0 and 1 percent of the objects).
///
/// Owned by QGen. For each sub-pattern that has "select", QGen calls
/// construct_Q_filtered() which:
///   1) Generates a batch of candidate queries (via a callback to QGen).
///   2) Executes each candidate against an irHINT(10) index and counts results.
///   3) Keeps only those whose result count falls within the selectivity range.
///   4) Repeats until the target number of queries is reached (or max rounds).
///
/// The irHINT(10) index is constructed exactly once and reused across
/// all filter rounds and all patterns.
class QGenSelect
{
public:
    QGenSelect(const IRelation &O);
    ~QGenSelect();

    /// Try to parse the "select" value for a workload sub-pattern.
    /// Returns nullopt when the key is absent.
    static optional<pair<double, double>> parse_select(const string &pattern);

    /// Generate queries with selectivity filtering.
    /// @param gen_batch  Callback that generates a batch of candidate queries;
    ///                   receives the desired batch size, returns candidates.
    /// @param needed     Target number of queries to produce.
    /// @param sel_lo_pct Lower selectivity bound (percent, e.g. 0.0).
    /// @param sel_hi_pct Upper selectivity bound (percent, e.g. 1.0).
    /// @param pattern    Pattern name (for logging).
    vector<RangeIRQuery> construct_Q_filtered(
        function<vector<RangeIRQuery>(int)> gen_batch,
        int needed,
        double sel_lo_pct,
        double sel_hi_pct,
        const string &pattern);

private:
    const IRelation &O;

    /// The irHINT(10) index used for selectivity filtering.
    /// Built lazily on the first call to construct_Q_filtered().
    unique_ptr<irHINTa> index;

    /// Build the irHINT(10) index (called at most once).
    void ensure_index();

    /// Filter candidates, keeping only those whose result count
    /// is within [sel_min_count, sel_max_count].
    /// Updates out_min and out_max with the observed result counts
    /// of accepted queries.
    vector<RangeIRQuery> filter_by_selectivity(
        const vector<RangeIRQuery> &candidates,
        size_t sel_min_count,
        size_t sel_max_count,
        size_t &out_min,
        size_t &out_max) const;

    /// Generate zero-result queries by recombining generated queries:
    /// take elements from one random query, extent from another.
    /// Only keeps recombined queries that return exactly 0 results.
    vector<RangeIRQuery> construct_Q_zero_result(
        function<vector<RangeIRQuery>(int)> gen_batch,
        int needed,
        const string &pattern);

    /// Maximum number of generate-filter rounds before giving up.
    static constexpr int max_rounds = 100;

    /// Batch multiplier – how many extra queries to generate per round
    /// relative to the number still needed.
    static constexpr int batch_multiplier = 3;
};


#endif // _QGENSELECT_H_
