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

#ifndef _EXECUTIONRUNNER_H_
#define _EXECUTIONRUNNER_H_

#include "../containers/relations.h"
#include "../generation/igen.h"
#include "../learning/stats.h"
#include "../learning/statscomp.h"
#include "../learning/statsserializer.h"
#include "../structure/synthdex.h"
#include "../structure/synthdexopt.h"
#include "../structure/idxschema.h"
#include "../structure/idxschemaserializer.h"
#include "../structure/bandedsynthdex.h"
#include "../utils/persistence.h"
#include "../utils/indexevaluator.h"
#include "../utils/resultvalidator.h"
#include "../utils/performanceanalyzer.h"
#include "../utils/comparisonanalyzer.h"
#include "../utils/processexec.h"
#include "../synthesis/synthesisrunner.h"
#include <string>
#include <vector>
#include <set>
#include <optional>
#include <numeric>
#include <iomanip>
#include <sstream>
#include <fstream>
#include <filesystem>
#include <unordered_map>
#include <memory>

using namespace std;


class ExecutionRunner
{
public:
    ExecutionRunner(
        const string& idxschema_str,
        bool do_query,
        bool do_predict,
        const string& file_O,
        const string& source_file,
        const string& file_Q,
        const IRelation& O,
        const vector<tuple<string,vector<RangeIRQuery>>>& Q,
        const OStats& Ostats,
        StatsComp& statscomp,
        const vector<string>& groups = {},
        const unordered_map<string,string>& Q_patterns = {},
        ComparisonMeasurements* comparison_measurements = nullptr);

    int execute();

    static void update(const string& file_O, const string& file_O2,
        const string& idxschema_str, StatsComp& statscomp,
        const string& category = "manual");
    static void remove(const string& file_O, const string& file_O2,
        const string& idxschema_str, StatsComp& statscomp,
        const string& category = "manual");
    static void softdelete(const string& file_O, const string& file_O2,
        const string& idxschema_str, StatsComp& statscomp,
        const string& category = "manual");
    static void synthesize(
        const vector<tuple<string,vector<RangeIRQuery>>>& Q,
        const OStats& Ostats, StatsComp& statscomp,
        const vector<string>& groups = {});
    static void synthesize(
        const vector<tuple<string,vector<RangeIRQuery>>>& Q,
        const vector<OStats>& per_q_ostats, StatsComp& statscomp,
        const vector<pair<Timestamp, Timestamp>>& all_slice_bounds = {},
        const vector<string>& groups = {},
        const vector<IdxSchema>& custom_templates = {});

private:
    static string resolve_idxschema(const string& idxschema_str);

    void execute_synthetic_for_groups(const vector<string>& groups);

    double construct(const IdxSchema& schema);
    double construct_banded(const vector<IdxSchema>& band_schemas,
        const vector<OStats>& per_band_ostats);
    void predict();

    const IRelation& source() const;
    void load_source();
    void release_source();

    const string idxschema_str;
    const bool do_query;
    const bool do_predict;
    const string file_O;
    const string source_file;
    const string file_Q;
    const IRelation* borrowed_O;
    unique_ptr<IRelation> owned_O;
    const bool reload_source;
    const ObjectDomain object_domain;
    const vector<tuple<string,vector<RangeIRQuery>>>& Q;
    const OStats& Ostats;
    StatsComp& statscomp;
    const vector<string> groups;
    const unordered_map<string,string> Q_patterns;
    ComparisonMeasurements* comparison_measurements;

    void evaluate_performance(
        const tuple<string,vector<RangeIRQuery>>& Qx,
        bool skip_oqip,
        const double& construction,
        bool collect_comparison_measurement = true,
        const string& comparison_category = "manual");

    IdxSchema idxschema;
    iStats Istats;
    unique_ptr<IRIndex> idx;
};

#endif // _EXECUTIONRUNNER_H_
