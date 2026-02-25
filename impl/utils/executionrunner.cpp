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

#include "executionrunner.h"
#include "controller.h"
#include <iostream>
#include <sstream>
#include <random>
#include <algorithm>

using namespace std;


ExecutionRunner::ExecutionRunner(
    const Cmd& cmd,
    const IRelation& O,
    const vector<tuple<string,vector<RangeIRQuery>>>& Q,
    const OStats& Ostats,
    StatsComp& statscomp)
    : cmd(cmd), O(O), Q(Q), Ostats(Ostats), statscomp(statscomp),
      idx(nullptr)
{
}


int ExecutionRunner::execute()
{
    if (this->cmd.idxschema == "!")
    {
        this->execute_synthetic();
        return (int)this->Q.size();
    }
    else
        return this->execute_userdefined();
}


double ExecutionRunner::construct(const IdxSchema &schema)
{
    Log::w(1, "Schema", IdxSchemaSerializer::to_json(schema));

    // Choose between SynthDex (runtime) and SynthDexOpt (template-based)
    bool use_optimized = Cfg::get<bool>("use-templated-synthdex");
    Log::w(2, "Using", use_optimized ? "SynthDexOpt (template-based)" : "SynthDex (runtime)");

    Timer timer;
    timer.start();
    if (use_optimized)
        this->idx = new SynthDexOpt(this->O, schema, this->Ostats);
    else
        this->idx = new SynthDex(this->O, schema, this->Ostats);
    double time = timer.stop();

    Log::w(1, "Structure", this->idx->str());

    Log::w(1, "Indexing time [s]", time);
    Log::w(1, "Size [megabytes]", this->idx->getSize() / (1024.0 * 1024.0));

    return time;
}


void ExecutionRunner::execute_synthetic()
{
    SynthesisRunner synthesisrunner(
        this->Q, this->statscomp, this->cmd.cfg_dir);
    auto suggestions = synthesisrunner.run();

    auto Q_num = 0;
    for (auto &Qx : this->Q)
    {
        Log::w(0, progress("Index", Q_num++, (int)this->Q.size()));

        auto Q_single = vector<tuple<string,vector<RangeIRQuery>>>{Qx};
        IndexEvaluator* evaluator = Cfg::get<bool>("check-results") 
        ? static_cast<IndexEvaluator*>(
            new ResultValidator(
                this->O, Q_single, this->Ostats,
                this->cmd.file_O.empty(), this->cmd.file_Q.empty()))
        : static_cast<IndexEvaluator*>(
            new PerformanceAnalyzer(
                this->O, Q_single, this->Ostats));

        auto Qname = get<0>(Qx);

        auto it = find_if(suggestions.begin(), suggestions.end(),
            [&](const auto& pair)
            { return pair.second.front().oq_file.find(Qname) != string::npos; });
        auto suggestion = it->second.front();

        Log::w(1, "Workload adaptation", strs(suggestion.workload, "\n"));
        Log::w(1, "Optimization", suggestion.variant);

        this->idxschema = suggestion.idxschema;
        this->Istats = this->statscomp.analyze_i(this->idxschema, "synthetic");

        auto time = this->construct(this->idxschema);

        evaluator->run(this->idx, this->Istats, time);

        delete this->idx;

        delete evaluator;
    }
}


int ExecutionRunner::execute_userdefined()
{
    // Determine index schemas to explore
    struct IndexTask { optional<string> pattern; string label; };
    vector<IndexTask> tasks;

    if (this->cmd.idxschema != "")
    {
        // User-provided explicit schema(s) via pipe-separated string
        stringstream ss(this->cmd.idxschema);
        string part;
        while (getline(ss, part, '|'))
            if (!part.empty())
                tasks.push_back({optional<string>(part), part});
    }
    else
    {
        // Per-template exploration: for each active template, generate
        // the number of random configurations defined in
        // "i.gen.exploration-per-O" (falls back to 0 if not specified).
        auto patterns = Cfg::get<vector<string>>("i.gen.patterns-active");
        for (const auto& pattern_name : patterns)
        {
            int count = 0;
            try {
                count = Cfg::get<int>(
                    "i.gen.exploration-per-O." + pattern_name);
            } catch (...) {
                // Template not listed in exploration-per-O, skip
                continue;
            }

            auto pattern_json = Cfg::get_json(
                "i.gen.design-space." + pattern_name);

            for (int j = 0; j < count; j++)
                tasks.push_back({optional<string>(pattern_json),
                    pattern_name + " (" + to_string(j+1)
                    + "/" + to_string(count) + ")"});
        }
    }

    auto idx_num = (int)tasks.size();

    // Randomize task order
    random_device rd;
    mt19937 rng(rd());
    shuffle(tasks.begin(), tasks.end(), rng);

    // Create one evaluator per Q entry before the index loop so that
    // result_cnts/result_xors survive across index configs (cross-index
    // consistency checking is preserved).
    vector<vector<tuple<string,vector<RangeIRQuery>>>> single_Qs;
    single_Qs.reserve(this->Q.size());
    for (const auto& Qx : this->Q)
        single_Qs.push_back({Qx});

    bool check = Cfg::get<bool>("check-results");
    vector<IndexEvaluator*> evaluators;
    evaluators.reserve(single_Qs.size());
    for (const auto& sq : single_Qs)
    {
        evaluators.push_back(check
            ? static_cast<IndexEvaluator*>(new ResultValidator(
                this->O, sq, this->Ostats,
                this->cmd.file_O.empty(), this->cmd.file_Q.empty()))
            : static_cast<IndexEvaluator*>(new PerformanceAnalyzer(
                this->O, sq, this->Ostats)));
    }

    Timer eta_timer;
    eta_timer.start();

    for (int i = 0; i < idx_num; i++)
    {
        Log::w(0, progress("Index", i, idx_num));
        Log::w(1, "Template", tasks[i].label);

        this->idxschema = IGen().construct_I(tasks[i].pattern);

        auto idxschema_str 
            = IdxSchemaSerializer::to_json_line(this->idxschema);

        this->Istats = this->statscomp.analyze_i(this->idxschema, "manual");

        if (this->cmd.predict || Cfg::get<bool>("prediction.predict-before-run"))
            this->predict();

        if (!this->cmd.query) continue;

        auto time = this->construct(this->idxschema);

        for (auto* evaluator : evaluators)
            evaluator->run(this->idx, this->Istats, time);
        
        delete this->idx;

        if (idx_num > 1)
        {
            double elapsed = eta_timer.stop();
            double avg = elapsed / (i + 1);
            double remaining = avg * (idx_num - i - 1);
            int rem_min = (int)(remaining / 60);
            int rem_sec = (int)remaining % 60;
            Log::w(0, "ETA all Is for O", to_string(rem_min) + "m " + to_string(rem_sec) + "s"
                + " (" + to_string((int)elapsed) + "s elapsed, "
                + to_string(idx_num - i - 1) + " remaining)");
        }
    }

    for (auto* e : evaluators) delete e;

    return idx_num;
}


void ExecutionRunner::predict()
{
    ProcessExec::python_setup(false);

    Log::w(0, "Prediction");

    Log::w(1, "Schema", IdxSchemaSerializer::to_json(this->idxschema));

    auto lcm_regex = Cfg::get<string>("out.machine-prefix");
    Log::w(1, "LCM regex", lcm_regex);

    this->statscomp.Qstats.clear();

    vector<string> files_OQI;
    for (auto& Qx : this->Q)
    {
        auto name = Persistence::compose_name(this->Ostats, Qx);

        this->statscomp.analyze_Q(get<1>(Qx));

        auto file_OQI = Persistence::write_OQI_stats_csv(
            this->Ostats, this->statscomp.Qstats, this->Istats, name);

        this->statscomp.Qstats.clear();

        files_OQI.push_back(file_OQI);
    }

    auto files_OQI_str = accumulate(
        next(files_OQI.begin()), files_OQI.end(), "\"" + files_OQI[0] + "\"",
        [](const string &a, const string &b) { return a + " \"" + b + "\""; });

    ProcessExec::python_run("Inference",
        "./learning/prediction.py \"" + this->cmd.cfg_dir
        + "\" " + files_OQI_str, 1);
}


void ExecutionRunner::update(const Cmd& cmd, StatsComp& statscomp)
{
    Log::w(0, "Constructing and Updating Index");

    // Load original objects
    auto O = Persistence::read_O_dat(cmd.file_O);
    Log::w(1, "Records count", to_string(O.size()));
    
    // Compute statistics for original objects
    auto Ostats = statscomp.analyze_O(O, cmd.file_O);

    // Load new objects for update
    auto O2 = Persistence::read_O_dat(cmd.file_O2);
    Log::w(1, "Records count", to_string(O2.size()));
    double ratio_org = (double)O.size() / (O.size() + O2.size()) * 100.0;
    double ratio_upd = (double)O2.size() / (O.size() + O2.size()) * 100.0;
    
    char ratio_str[100];
    snprintf(ratio_str, sizeof(ratio_str), "%.2f%% Construction + %.2f%% Update", ratio_org, ratio_upd);
    Log::w(1, "Records ratio", string(ratio_str));

    for (auto& r : O2) r.id += O.size();
    
    auto idxschema = IGen().construct_I(optional<string>(cmd.idxschema));

    // Construct initial index with original objects
    Log::w(0, "Construction");

    Log::w(1, "Schema", IdxSchemaSerializer::to_json(idxschema));

    bool use_optimized = Cfg::get<bool>("use-templated-synthdex");
    Log::w(2, "Using", use_optimized ? "SynthDexOpt (template-based)" : "SynthDex (runtime)");

    IRIndex* idx;
    Timer timer;
    timer.start();
    if (use_optimized)
        idx = new SynthDexOpt(O, idxschema, Ostats);
    else
        idx = new SynthDex(O, idxschema, Ostats);
    double construction_time = timer.stop();

    Log::w(1, "Structure", idx->str());
    Log::w(1, "Indexing time [s]", construction_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));

    // Update index with new objects
    Log::w(0, "Update");

    timer.start();

    idx->update(O2);

    double update_time = timer.stop();

    Log::w(1, "Updating time [s]", update_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));
    
    delete idx;
}


void ExecutionRunner::remove(const Cmd& cmd, StatsComp& statscomp)
{
    Log::w(0, "Constructing Index and Deleting Records");

    // Load original objects
    auto O = Persistence::read_O_dat(cmd.file_O);
    Log::w(1, "Records count", to_string(O.size()));
    
    // Compute statistics for original objects
    auto Ostats = statscomp.analyze_O(O, cmd.file_O);

    // Load IDs to delete
    auto idsToDelete = Persistence::read_Oids_dat(cmd.file_O2);
    Log::w(1, "Records to delete", to_string(idsToDelete.size()));
    double ratio_org = (double)O.size() / (O.size() + idsToDelete.size()) * 100.0;
    double ratio_del = (double)idsToDelete.size() / (O.size() + idsToDelete.size()) * 100.0;
    
    char ratio_str[100];
    snprintf(ratio_str, sizeof(ratio_str), "%.2f%% Construction + %.2f%% Deletion", ratio_org, ratio_del);
    Log::w(1, "Records ratio", string(ratio_str));
    
    auto idxschema = IGen().construct_I(optional<string>(cmd.idxschema));

    // Construct initial index with original objects
    Log::w(0, "Construction");

    Log::w(1, "Schema", IdxSchemaSerializer::to_json(idxschema));

    bool use_optimized = Cfg::get<bool>("use-templated-synthdex");
    Log::w(2, "Using", use_optimized ? "SynthDexOpt (template-based)" : "SynthDex (runtime)");

    IRIndex* idx;
    Timer timer;
    timer.start();
    if (use_optimized)
        idx = new SynthDexOpt(O, idxschema, Ostats);
    else
        idx = new SynthDex(O, idxschema, Ostats);
    double construction_time = timer.stop();

    Log::w(1, "Structure", idx->str());
    Log::w(1, "Indexing time [s]", construction_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));

    // Delete objects from index
    Log::w(0, "Delete");

    timer.start();

    idx->remove(idsToDelete);

    double delete_time = timer.stop();

    Log::w(1, "Deletion time [s]", delete_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));
    
    delete idx;
}


void ExecutionRunner::synthesize(const Cmd& cmd,
    const vector<tuple<string,vector<RangeIRQuery>>>& Q,
    const OStats& Ostats, StatsComp& statscomp)
{
    SynthesisRunner synthesisrunner(Q, statscomp, cmd.cfg_dir);
    auto result = synthesisrunner.run();

    for (const auto& [oq_file, suggestions] : result)
    {
        // Suggestions are already sorted by synthesisrunner (best first = smallest predicted_performance)
        
        // Get best values for percentage calculations
        double best_throughput = suggestions.empty() ? 0.0 : 
            1.0 / pow(10, suggestions[0].predicted_performance);
        double best_size_mb = suggestions.empty() ? 0.0 : 
            pow(10, suggestions[0].predicted_size) / (1024*1024);
        
        int top = 0;
        int top_limit = Cfg::get<int>("synthesis.top-k");
        double top_relative = Cfg::get<double>("synthesis.top-relative");
        double prev_throughput = 0.0;
        double prev_size_mb = 0.0;
        
        for (const auto& suggestion : suggestions)
        {
            // Convert from log10 space to linear space
            // predicted_performance is log10(s/q), so 10^predicted_performance gives s/q
            auto time_per_query_sq = pow(10, suggestion.predicted_performance);
            auto throughput_qps = 1.0 / time_per_query_sq;
            auto size_mb = pow(10, suggestion.predicted_size) / (1024*1024);
            
            // Skip if both throughput and size are only marginally different from previous
            if (top > 0 && prev_throughput > 0 && prev_size_mb > 0)
            {
                double throughput_diff_pct = abs((prev_throughput - throughput_qps) / prev_throughput) * 100.0;
                double size_diff_pct = abs((prev_size_mb - size_mb) / prev_size_mb) * 100.0;
                
                if (throughput_diff_pct < top_relative && size_diff_pct < top_relative)
                {
                    Log::w(2, "Skipping suggestion (only " + to_string(throughput_diff_pct) + 
                           "% throughput diff, " + to_string(size_diff_pct) + "% size diff)");
                    continue;
                }
            }
            
            // Calculate percentage scores relative to best (100% = best for throughput, smaller % = better for size)
            double throughput_pct = (best_throughput > 0) ? (throughput_qps / best_throughput) * 100.0 : 100.0;
            double size_pct = (best_size_mb > 0) ? (size_mb / best_size_mb) * 100.0 : 100.0;
            
            // Format percentages with one decimal place
            stringstream throughput_pct_str, size_pct_str;
            throughput_pct_str << fixed << setprecision(1) << throughput_pct;
            size_pct_str << fixed << setprecision(1) << size_pct;

            if (top >= top_limit)
            {
                Log::w(0, "Top-k limit reached, stopping further suggestions.");
                break;
            }
            
            Log::w(0, "Suggestion (" + to_string(top+1) + "/" + to_string(suggestions.size()) + ")");
            Log::w(2, "OQ statistics file", suggestion.oq_file);
            Log::w(1, "Optimization", suggestion.variant);
            Log::w(1, "Workload", str(suggestion.workload, "\n"));
            Log::w(1, "Synth ID", to_string(suggestion.synth_id));
            Log::w(2, "Avg time per query [s/q] (predicted)", to_string(time_per_query_sq));
            Log::w(1, "Throughput [q/s] (predicted)", to_string(throughput_qps));
            Log::w(1, "Size [megabytes] (predicted)", to_string(size_mb));
            Log::w(1, "Relative to winning suggestion", throughput_pct_str.str() + "% Throughput, " + size_pct_str.str() + "% Size");
            Log::w(1, "Schema", IdxSchemaSerializer::to_json(suggestion.idxschema));
            Log::w(1, "Schema (single line)", "'" + IdxSchemaSerializer::to_json_line(suggestion.idxschema) + "'");
            
            prev_throughput = throughput_qps;
            prev_size_mb = size_mb;
            top++;
        }
    }
}
