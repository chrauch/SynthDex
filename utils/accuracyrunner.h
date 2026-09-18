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

#ifndef _ACCURACYRUNNER_H_
#define _ACCURACYRUNNER_H_

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


/**
 * Evaluates learned cost model (LCM) accuracy by comparing predicted vs. actual
 * query throughput and index size. For each of N object collections x M random
 * index schemas: the real index is built and queries are timed (median over
 * multiple runs, averaged per query); the LCM is invoked via prediction.py to
 * produce per-query predictions, then averaged to a single scalar. Both actuals
 * and predictions are stored in log10 scale. The final summary reports MAE
 * (log10 space) and MRE (linear space) over all evaluations, written to a
 * timestamped CSV under output/accuracy/. Supports a parquet-based fast path
 * that delegates entirely to learning/accuracy.py.
 */
class AccuracyRunner
{
public:
    AccuracyRunner(StatsComp& statscomp, const vector<string>& groups);

    void run();

private:
    struct AccuracyResult
    {
        int o_idx;
        int i_idx;
        string i_schema;
        double actual_throughput_log;
        double actual_size_log;
        double predicted_throughput_log;
        double predicted_size_log;
    };

    void generate_O(int i);

    vector<tuple<string,vector<RangeIRQuery>>> generate_Q();

    AccuracyResult evaluate(
        int o_idx, int i_idx,
        const vector<tuple<string,vector<RangeIRQuery>>>& Q);

    pair<double,double> execute_actual(
        const IdxSchema& schema,
        const iStats& Istats,
        const vector<tuple<string,vector<RangeIRQuery>>>& Q);

    pair<double,double> execute_predicted(
        const IdxSchema& schema,
        const iStats& Istats,
        const vector<tuple<string,vector<RangeIRQuery>>>& Q);

    pair<double,double> parse_predictions(const string& prediction_file);

    void write_header(ofstream& csv);
    void write_row(ofstream& csv, const AccuracyResult& r);
    void write_summary(ofstream& csv, const vector<AccuracyResult>& results);

    StatsComp& statscomp;
    const vector<string> groups;

    IRelation O;
    OStats Ostats;
};

#endif // _ACCURACYRUNNER_H_
