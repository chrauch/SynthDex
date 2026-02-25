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

#ifndef SYNTHESIS_RUNNER_H
#define SYNTHESIS_RUNNER_H

#include "../utils/persistence.h"
#include "../utils/logging.h"
#include "../structure/idxschema.h"
#include "../structure/idxschemaserializer.h"
#include <vector>
#include <string>
#include <tuple>

// Forward declarations
class RangeIRQuery;
class StatsComp;

struct IdxSchemaSuggestion
{
    int synth_id;
    double predicted_performance;
    double predicted_size;
    string variant;
    string i_template;
    string oq_file;
    vector<string> workload;
    IdxSchema idxschema;

    string str() const
    {
        return "Meta { Synthesis: " + variant 
            + ", Workload: " + strs(workload, "|") 
            + ", I template: " + i_template 
            + ", OQ file: " + oq_file
            + ", Predicted performance: " + to_string(predicted_performance)
            + ", Predicted size: " + to_string(predicted_size)
            + "}\nIdxSchema: " + IdxSchemaSerializer::to_json_line(idxschema);
    }
};


class SynthesisRunner
{
private:
    const vector<tuple<string, vector<RangeIRQuery>>>& Q;
    StatsComp& statscomp;
    const string& cfg_dir;

public:
    SynthesisRunner(
        const vector<tuple<string, vector<RangeIRQuery>>>& queries,
        StatsComp& stats_comp,
        const string& config_dir);

    /**
     * Runs the complete synthesis workflow:
     * 1. Generates OQ statistics for all query sets
     * 2. Computes index design space patterns  
     * 3. Generates I statistics for min/max pairs
     * 4. Executes Python synthesis variants
     */
    map<string, vector<IdxSchemaSuggestion>> run();

private:

    vector<string> generate_oq_statistics();

    string generate_i_statistics();

    void execute_synthesis_variants(
        const string& path_I_stats,
        const vector<string>& path_OQ_stats);

    map<string, vector<IdxSchemaSuggestion>> process_suggestions(
        const vector<string>& path_OQ_stats);
};

#endif // SYNTHESIS_RUNNER_H