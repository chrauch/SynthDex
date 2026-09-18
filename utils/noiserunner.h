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

#ifndef _NOISERUNNER_H_
#define _NOISERUNNER_H_

#include "../containers/relations.h"
#include "../generation/igen.h"
#include "../generation/ogen.h"
#include "../generation/ostatsgen.h"
#include "../generation/qgen.h"
#include "../learning/stats.h"
#include "../learning/statscomp.h"
#include "../structure/synthdex.h"
#include "../structure/synthdexopt.h"
#include "../structure/idxschema.h"
#include "../structure/idxschemaserializer.h"
#include "../utils/cfg.h"
#include "../utils/logging.h"
#include "../utils/global.h"
#include <string>
#include <vector>
#include <map>
#include <fstream>

using namespace std;


/**
 * Measures execution noise (throughput fluctuation) by running the same query
 * set repeatedly against a fixed randomly-generated index. Each run optionally
 * shuffles the query order, executes every query with multiple timing reps
 * (median per query), and averages across queries to produce one throughput
 * scalar per run. The output CSV is directly comparable to the
 * actual_throughput_log column in the lcm-accuracy output, allowing noise to
 * be contextualised against prediction error. Summary reports min, max, mean,
 * stddev, and CV (coefficient of variation = stddev / mean) over all runs.
 */
class NoiseRunner
{
public:
    NoiseRunner(StatsComp& statscomp, const vector<string>& groups);

    void run();

private:
    double execute_run(
        IRIndex* idx,
        const vector<RangeIRQuery>& queries,
        int runs_per_q);

    void write_header(ofstream& csv);
    void write_row(ofstream& csv, const string& o_name, const string& q_name, int o_idx, int i_idx, const string& i_schema, int run_idx, double avg_tp, double avg_tp_log);
    void write_summary_header(ofstream& summary_csv);
    void write_summary(ofstream& summary_csv, const string& o_name, const string& q_name, const string& i_schema, const vector<double>& tp_log_values);
    void write_metasummary_header(ofstream& meta_csv);
    void write_metasummary(ofstream& meta_csv, const string& q_name, const vector<double>& stddevs);

    StatsComp& statscomp;
    const vector<string> groups;
    IRelation O;
    OStats Ostats;
};

#endif // _NOISERUNNER_H_
