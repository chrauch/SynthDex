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

#ifndef SYNTHESIS_RUNNER_H
#define SYNTHESIS_RUNNER_H

#include "../utils/persistence.h"
#include "../utils/logging.h"
#include "../structure/idxschema.h"
#include "../structure/idxschemaserializer.h"
#include <vector>
#include <string>
#include <tuple>
#include <regex>

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
    string temporal_band;
    vector<string> workload;
    IdxSchema idxschema;

    string str() const
    {
        return "Meta { Synthesis: " + variant 
            + ", Workload: " + strs(workload, "|") 
            + ", I template: " + i_template 
            + ", OQ file: " + oq_file
            + (temporal_band.empty() ? "" : ", Temporal band: " + temporal_band)
            + ", Predicted performance: " + to_string(predicted_performance)
            + ", Predicted size: " + to_string(predicted_size)
            + "}\nIdxSchema: " + IdxSchemaSerializer::to_json_line(idxschema);
    }
};

struct BandedSuggestion
{
    IdxSchema schema;
    double predicted_performance;  // log10(s/q) for this band
    double predicted_size;         // log10(bytes) for this band
    size_t query_count;            // queries assigned to this band
};

struct BandedResult
{
    vector<BandedSuggestion> suggestions;  // per-band predictions + query counts
    vector<IdxSchema> schemas;             // gap-filled, sorted by band start
};

struct BandingPrep
{
    vector<tuple<string, vector<RangeIRQuery>>> batched_Q;
    vector<OStats> per_q_ostats;
    vector<pair<Timestamp, Timestamp>> all_slice_bounds;
    vector<IdxSchema> band_inner_templates;
    bool has_banded_templates = false;
    bool has_non_banded_templates = false;
    // Group names whose templates carry no temporal bands (e.g. "dict2").
    // Used by execute_synthetic to run a separate non-banded synthesis phase
    // when groups contains a mix of banded and non-banded templates.
    vector<string> non_banded_groups;
};


class SynthesisRunner
{
private:
    const vector<tuple<string, vector<RangeIRQuery>>>& Q;
    StatsComp& statscomp;
    vector<OStats> per_q_ostats;
    const vector<string> groups;
    const vector<IdxSchema> custom_templates;

public:
    SynthesisRunner(
        const vector<tuple<string, vector<RangeIRQuery>>>& queries,
        StatsComp& stats_comp,
        const vector<OStats>& per_q_ostats = {},
        const vector<string>& groups = {},
        const vector<IdxSchema>& custom_templates = {});

    static const IdxSchemaSuggestion* select_size_aware(
        const vector<IdxSchemaSuggestion>& suggestions);

    /**
     * Runs the complete synthesis workflow:
     * 1. Generates OQ statistics for all query sets
     * 2. Computes index design space patterns  
     * 3. Generates I statistics for min/max pairs
     * 4. Executes Python synthesis variants
     */
    map<string, vector<IdxSchemaSuggestion>> run();

    /**
     * Merges the best suggestion per temporal band into a banded schema.
     * Parses absolute timestamps from temporal_band strings (e.g., "slice0[0-330270146)"),
     * normalizes to relative [0,1] using the given domain, picks the best
     * suggestion per band, and returns a vector of IdxSchema with band_range fields.
     * Returns empty vector if no temporal bands are present.
     */
    static vector<BandedSuggestion> merge_best_per_band(
        const map<string, vector<IdxSchemaSuggestion>>& result,
        const map<int, size_t>& band_query_counts = {});

    /**
     * Full banded post-processing pipeline:
     * merge best per band, gap-fill uncovered slices, sort, and log metrics.
     * Returns the BandedSuggestion vector (for metrics) and gap-filled schemas
     * (for construction). Empty result if no temporal bands found.
     */
    static BandedResult process_banded(
        const map<string, vector<IdxSchemaSuggestion>>& suggestions,
        const vector<tuple<string, vector<RangeIRQuery>>>& batched_Q,
        const vector<pair<Timestamp, Timestamp>>& all_slice_bounds,
        Timestamp domain = 0);

    /**
     * Scan active templates for temporal banding configs, compute temporal
     * slices once, then cluster per distinct bands config. Returns batched
     * queries, per-Q OStats, slice bounds, and banded/non-banded flags.
     */
    static BandingPrep prepare_banding(
        const IRelation& O, const string& file_O,
        const vector<tuple<string, vector<RangeIRQuery>>>& Q,
        StatsComp& statscomp,
        const vector<string>& groups = {});

    /**
     * Partition O into per-band sub-relations based on each schema's
     * band_range, then compute OStats for each band.
     */
    static vector<OStats> compute_per_band_ostats(
        const vector<IdxSchema>& banded_schemas,
        const IRelation& O, StatsComp& statscomp);

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