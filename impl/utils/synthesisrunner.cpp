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

#include "synthesisrunner.h"
#include "global.h"
#include "logging.h"
#include "persistence.h"
#include "processexec.h"
#include "../learning/statsserializer.h"
#include "../learning/statscomp.h"
#include "../generation/igen.h"
#include "../structure/idxschemaencoder.h"

using namespace std;

SynthesisRunner::SynthesisRunner(
    const vector<tuple<string, vector<RangeIRQuery>>>& queries,
    StatsComp& stats_comp,
    const string& config_dir
) : Q(queries), statscomp(stats_comp), cfg_dir(config_dir)
{
}


map<string, vector<IdxSchemaSuggestion>> SynthesisRunner::run()
{
    vector<string> path_OQ_stats = this->generate_oq_statistics();
    
    string path_I_stats = this->generate_i_statistics();
    
    ProcessExec::python_setup(false);

    this->execute_synthesis_variants(path_I_stats, path_OQ_stats);

    return this->process_suggestions(path_OQ_stats);
}


map<string, vector<IdxSchemaSuggestion>> SynthesisRunner::process_suggestions(
    const vector<string>& path_OQ_stats)
{
    map<string, vector<IdxSchemaSuggestion>> suggestions_map;

    for (const auto& oq_file : path_OQ_stats)
    {
        vector<IdxSchemaSuggestion> suggestions;

        string isynth_file = oq_file;
        size_t pos = isynth_file.find(".OQ.csv");
        if (pos != string::npos)
            isynth_file.replace(pos, 7, ".I-synthesis.csv");

        pos = isynth_file.find("/OQ/");
        if (pos != string::npos)
            isynth_file.replace(pos, 4, "/I-synthesis/");

        auto suggestion_raw = Persistence::read_I_synthesis_csv(isynth_file);
        
        for (const auto& [metadata, encoding] : suggestion_raw)
        {
            IdxSchemaSuggestion suggestion;
            suggestion.variant = metadata[0];          // Column 0: variant name
            suggestion.oq_file = metadata[1];          // Column 1: OQ file path
            suggestion.i_template = metadata[2];       // Column 2: template_id
            suggestion.synth_id = stoi(metadata[3]);   // Column 3: synth_id
            suggestion.predicted_performance = stod(metadata[4]);  // Column 4: predicted_throughput
            suggestion.predicted_size = stod(metadata[5]);         // Column 5: predicted_size
            suggestion.idxschema = IdxSchemaEncoder::decode(encoding);

            // Extract setting from oq_file
            auto oq_file_name = suggestion.oq_file;
            size_t last_slash = oq_file_name.find_last_of("/\\");
            if (last_slash != string::npos)
                oq_file_name = oq_file_name.substr(last_slash + 1);

            vector<string> parts;
            size_t start = 0, end = 0;
            while ((end = oq_file_name.find('.', start)) != string::npos)
            {
                parts.push_back(oq_file_name.substr(start, end - start));
                start = end + 1;
            }
            parts.push_back(oq_file_name.substr(start));

            if (parts.size() > 2) parts.resize(parts.size() - 2);

            if (parts.size() > 2 && parts[2].substr(0, 6) == "Q-rnd_")
                parts[2] = parts[2].substr(6);

            suggestion.workload = parts;
            
            suggestions.push_back(suggestion);
        }

        sort(suggestions.begin(), suggestions.end(),
            [](const IdxSchemaSuggestion& a, const IdxSchemaSuggestion& b)
            { return a.predicted_performance < b.predicted_performance; });

        suggestions_map.emplace(oq_file, suggestions);

        for (auto& suggestion : suggestions)
            Log::w(2, "Suggestion", suggestion.str());
    }

    return suggestions_map;
}


vector<string> SynthesisRunner::generate_oq_statistics()
{
    vector<string> path_OQ_statss;
    
    for (auto& Q : this->Q)
    {
        // Sample queries before analysis
        auto queries = get<1>(Q);
        int sample_size = Cfg::get<int>("synthesis.sample-size");
        
        vector<RangeIRQuery> sampled_queries;
        if (sample_size >= (int)queries.size())
        {
            sampled_queries = queries;
            //Log::w(2, "Using all queries (no sampling)", to_string(queries.size()));
        }
        else
        {
            vector<size_t> indices(queries.size());
            iota(indices.begin(), indices.end(), 0);
            random_shuffle(indices.begin(), indices.end());
            
            for (int i = 0; i < sample_size; ++i)
                sampled_queries.push_back(queries[indices[i]]);
            
            //Log::w(2, "Sampled queries for analysis", to_string(sample_size) + " out of " + to_string(queries.size()));
        }
        
        this->statscomp.analyze_Q(sampled_queries);

        auto name = Persistence::compose_name(this->statscomp.Ostats, Q);

        auto path_OQ_stats = Persistence::write_OQ_stats_csv(
            this->statscomp.Ostats, this->statscomp.Qstats, name);

        path_OQ_statss.push_back(path_OQ_stats);
    }

    Log::w(2, "Generated OQ statistics files", strs(path_OQ_statss));
    return path_OQ_statss;
}


string SynthesisRunner::generate_i_statistics()
{
    Log::w(0, "Design Space");

    auto design_space = IGen().compute_design_space();

    vector<iStats> istatss;
    for (const auto& [min_i, max_i] : design_space) {
        auto istats_min = this->statscomp.analyze_i(min_i, "design-space-min");
        auto istats_max = this->statscomp.analyze_i(max_i, "design-space-max");

        istatss.push_back(istats_min);
        istatss.push_back(istats_max);

        Log::w(2, "i pattern (encoded) ",
            StatsSerializer::to_csv_header(istats_max, false) 
            + "\n" + StatsSerializer::to_csv(istats_max, false)
            + "\n" + StatsSerializer::to_csv(istats_min, false));
    }

    auto path_I_stats = Persistence::write_I_stats_csv(istatss);

    Log::w(2, "Generated I statistics file", path_I_stats);
    return path_I_stats;
}


void SynthesisRunner::execute_synthesis_variants(
    const string& path_I_stats,
    const vector<string>& path_OQ_stats)
{
    vector<string> quoted_paths;
    for (const auto& path : path_OQ_stats)
        quoted_paths.push_back("\"" + path + "\"");
    
    auto path_OQ_stats_str = strs(quoted_paths, " ");

    auto variants = Cfg::get<vector<string>>("synthesis.variant");

    for (const auto& v : variants)
    {
        Log::w(0, "Synthesis");
        Log::w(1, "Optimization runner", "sy_" + v + ".py");

        auto start = chrono::high_resolution_clock::now();

        ProcessExec::python_run("Inference",
            "./synthesis/sy_" + v + ".py \"" + this->cfg_dir
            + "\" \"" + path_I_stats + "\" " + path_OQ_stats_str, 1);

        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::seconds>(end - start).count();

        Log::w(1, "Synthesis duration (s)", to_string(duration));
    }
}