/******************************************************************************
 * Project:  SynthDex
 * Purpose:  Adaptive Ensemble Indexing for Temporal Information Retrieval via Learned Cost Models
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

#ifndef _RANKINGRUNNER_H_
#define _RANKINGRUNNER_H_

#include "../containers/relations.h"
#include "../generation/igen.h"
#include "../generation/ogen.h"
#include "../generation/ostatsgen.h"
#include "../generation/qgen.h"
#include "../learning/stats.h"
#include "../learning/statscomp.h"
#include "../learning/statsserializer.h"
#include "../structure/synthdex.h"
#include "../structure/synthdexopt.h"
#include "../structure/idxschema.h"
#include "../structure/idxschemaencoder.h"
#include "../structure/idxschemaserializer.h"
#include "../utils/persistence.h"
#include "../utils/processexec.h"
#include "../utils/cfg.h"
#include "../utils/logging.h"
#include "../utils/global.h"
#include <string>
#include <vector>
#include <fstream>

using namespace std;


struct RankingCandidate
{
    IdxSchema schema;
    string i_schema_json;
    double predicted_tp_log = 0.0;
    double actual_tp_log    = 0.0;
    int predicted_rank      = 0;    // 1 = best predicted (lowest predicted s/q)
    int actual_rank         = 0;    // 1 = best actual    (lowest measured s/q)
};


/**
 * Evaluates whether the LCM correctly ranks index configurations. For each
 * object collection and query workload: runs a grid search using the LCM to
 * enumerate candidate configurations, selects a spread-out subset spanning
 * the performance range, executes each candidate with real queries, then
 * computes Kendall's τ (rank correlation) and score loss (the performance
 * gap when following the LCM's top-1 recommendation vs. the true best).
 * Requires synthesis.store-all-evaluations: true so the grid CSV contains
 * the full set of evaluated configurations.
 */
class RankingRunner
{
public:
    RankingRunner(StatsComp& statscomp, const vector<string>& groups);

    void run();

private:
    vector<RankingCandidate> run_grid_synthesis(
        const string& oq_file,
        const string& path_I_stats);

    vector<RankingCandidate> select_candidates(
        vector<RankingCandidate>& all_candidates,
        int num_to_select,
        double min_pct,
        double delta_pct);

    double execute_actual(
        const RankingCandidate& c,
        const vector<RangeIRQuery>& queries);

    double kendall_tau(
        const vector<int>& pred_ranks,
        const vector<int>& actual_ranks);

    void write_header(ofstream& csv);

    void write_row(ofstream& csv,
        const string& o_name,
        const string& q_name,
        const RankingCandidate& c);

    void write_summary_header(ofstream& scsv);

    void write_summary(ofstream& scsv,
        const string& o_name,
        const string& q_name,
        const vector<RankingCandidate>& candidates);

    void write_metasummary_header(ofstream& mcsv);

    void write_metasummary(ofstream& mcsv,
        const string& q_name,
        const vector<tuple<double, bool, double>>& per_oq_metrics);

    StatsComp& statscomp;
    const vector<string> groups;
    IRelation O;
    OStats Ostats;
};

#endif // _RANKINGRUNNER_H_
