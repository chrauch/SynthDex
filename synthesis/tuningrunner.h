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

#ifndef _TUNINGRUNNER_H_
#define _TUNINGRUNNER_H_

#include "../containers/relations.h"
#include "../learning/stats.h"
#include "../learning/statscomp.h"
#include "../structure/idxschema.h"
#include "../utils/cfg.h"
#include <string>
#include <vector>
#include <map>
#include <fstream>

using namespace std;


/**
 * One hyper-parameter setting under test: a deep-merge overlay applied on top
 * of synthesis.<type> for the duration of a single measurement.
 */
struct TuningSetting
{
    string   exp;        // experiment name, e.g. "grid-exp"
    int      index = 0;  // 0-based position within the experiment's setting list
    string   type;       // synthesis method: grid | grad | gene | bayo
    string   json;       // the overlay, compacted to a single line for the CSV
    SetValue overrides;  // the overlay itself
};


/**
 * Outcome of running one synthesis (reference or setting) on one (O, Q) pair.
 */
struct TuningObservation
{
    long      probes           = 0;    // LCM evaluations performed
    double    synthesis_time   = 0.0;  // net synthesis seconds, I/O excluded
    double    predicted_tp_log = 0.0;  // log10(s/q) predicted for the winner
    double    actual_tp_log    = 0.0;  // log10(s/q) measured for the winner
    string    i_schema;                // winning configuration as JSON
    IdxSchema schema;
    bool      valid            = false;
};


/**
 * Per-setting accumulator over all measured (O, Q) pairs.
 */
struct TuningAggregate
{
    vector<double> probes;
    vector<double> synthesis_time;
    vector<double> predicted_degradation;
    vector<double> actual_degradation;
};


/**
 * Calibrates the hyper-parameters of the synthesis strategies. For every
 * randomly generated object collection and query workload, it first runs the
 * configured synthesis to obtain a reference configuration, then runs each
 * setting listed under tuning.exps and records how far the configuration it
 * returns falls behind that reference, at what probe count and in what time.
 * Degradation is reported both on the LCM-predicted cost and, when
 * tuning.measure-actual is enabled, on the throughput of the actually built
 * index. The summary CSV aggregates average, standard deviation and worst
 * observed degradation per setting, which is what the calibration study in
 * the paper reports.
 */
class TuningRunner
{
public:
    TuningRunner(StatsComp& statscomp, const vector<string>& groups);

    void run();

private:
    vector<TuningSetting> load_settings();

    TuningObservation synthesize(
        const tuple<string, vector<RangeIRQuery>>& Q);

    // Reads output/synthesis/run-metrics.tsv written by synthesisbase.py.
    void read_run_metrics(TuningObservation& obs);

    double measure_actual(
        const TuningObservation& obs,
        const vector<RangeIRQuery>& queries);

    static string compact_json(const string& json);
    static double mean(const vector<double>& v);
    static double stddev(const vector<double>& v);
    static double max_of(const vector<double>& v);

    void write_header(ofstream& csv);
    void write_row(ofstream& csv,
        const string& o_name, const string& q_name,
        const TuningSetting& setting,
        const TuningObservation& ref,
        const TuningObservation& obs,
        double predicted_degradation,
        double actual_degradation);
    void write_summary_header(ofstream& scsv);
    void write_summary(ofstream& scsv,
        const TuningSetting& setting,
        const TuningAggregate& agg);

    StatsComp& statscomp;
    const vector<string> groups;
    IRelation O;
    OStats Ostats;
    bool measure_actual_enabled = true;

    // Repeated settings frequently converge on the same configuration; caching
    // the measurement per (O, Q) pair avoids rebuilding an identical index.
    map<string, double> actual_cache;
};

#endif // _TUNINGRUNNER_H_
