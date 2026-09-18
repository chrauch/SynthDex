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

#include "executionrunner.h"
#include <iostream>
#include <sstream>
#include <random>
#include <algorithm>
#include <fstream>
#include <filesystem>
#include <regex>
#include <unordered_map>

using namespace std;


ExecutionRunner::ExecutionRunner(
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
    const vector<string>& groups,
    const unordered_map<string,string>& Q_patterns,
    ComparisonMeasurements* comparison_measurements)
    : idxschema_str(resolve_idxschema(idxschema_str)), do_query(do_query), do_predict(do_predict),
      file_O(file_O), source_file(source_file), file_Q(file_Q),
    borrowed_O(do_query && !source_file.empty() ? nullptr : &O), owned_O(nullptr),
      reload_source(do_query && !source_file.empty()),
      object_domain{O.gstart, O.gend},
      Q(Q), Ostats(Ostats), statscomp(statscomp),
      groups(groups), Q_patterns(Q_patterns),
      comparison_measurements(comparison_measurements),
      idx(nullptr)
{
}


void ExecutionRunner::evaluate_performance(
    const tuple<string,vector<RangeIRQuery>>& Qx,
    bool skip_oqip,
    const double& construction,
    bool collect_comparison_measurement,
    const string& comparison_category)
{
    this->release_source();

    if (collect_comparison_measurement
        && this->comparison_measurements != nullptr)
    {
        ComparisonAnalyzer evaluator(
            this->object_domain, Qx, this->Ostats,
            *this->comparison_measurements, comparison_category);
        evaluator.run(this->idx.get(), this->Istats, construction);
    }
    else
    {
        PerformanceAnalyzer evaluator(
            this->object_domain, Qx, this->Ostats,
            skip_oqip, this->Q_patterns);
        evaluator.run(this->idx.get(), this->Istats, construction);
    }
}


const IRelation& ExecutionRunner::source() const
{
    if (this->owned_O != nullptr)
        return *this->owned_O;
    if (this->borrowed_O != nullptr)
        return *this->borrowed_O;
    throw runtime_error("Object collection is not loaded");
}


void ExecutionRunner::load_source()
{
    if (!this->reload_source || this->owned_O != nullptr)
        return;

    this->owned_O = make_unique<IRelation>(
        Persistence::read_O_dat(this->source_file));
}


void ExecutionRunner::release_source()
{
    if (this->reload_source)
        this->owned_O.reset();
}


string ExecutionRunner::resolve_idxschema(const string& idxschema_str)
{
    if (idxschema_str.size() >= 4
        && idxschema_str.substr(idxschema_str.size() - 4) == ".idx")
    {
        if (!filesystem::exists(idxschema_str))
            throw runtime_error("Index schema file not found: " + idxschema_str);
        ifstream ifs(idxschema_str);
        string content((istreambuf_iterator<char>(ifs)),
                        istreambuf_iterator<char>());
        // Trim trailing whitespace/newlines
        while (!content.empty() && (content.back() == '\n' || content.back() == '\r' || content.back() == ' '))
            content.pop_back();
        return content;
    }
    return idxschema_str;
}


double ExecutionRunner::construct(const IdxSchema &schema)
{
    Log::w(1, "Schema", IdxSchemaSerializer::to_json(schema));

    // Choose between SynthDex (runtime), SynthDexOpt (template-based), and SynthDexStatic (hardcoded)
    bool use_static    = Cfg::get<bool>("synthesis.use-static-synthdex");
    bool use_optimized = Cfg::get<bool>("synthesis.use-templated-synthdex");
    Log::w(2, "Using", use_static && is_static_synthdex_eligible(schema) ? "SynthDexStatic (hardcoded)" : use_optimized ? "SynthDexOpt (template-based)" : "SynthDex (runtime)");

    Timer timer;
    timer.start();
    if (use_static && is_static_synthdex_eligible(schema))
        this->idx = make_unique<SynthDexStatic>(this->source(), schema, this->Ostats);
    else if (use_optimized)
        this->idx = make_unique<SynthDexOpt>(this->source(), schema, this->Ostats);
    else
        this->idx = make_unique<SynthDex>(this->source(), schema, this->Ostats);
    double time = timer.stop();

    Log::w(1, "Structure", this->idx->str());

    Log::w(1, "Indexing time [s]", time);
    Log::w(1, "Size [megabytes]", this->idx->getSize() / (1024.0 * 1024.0));

    return time;
}


double ExecutionRunner::construct_banded(const vector<IdxSchema> &band_schemas,
    const vector<OStats> &per_band_ostats)
{
    Log::w(1, "Banded schema (compact)",
        IdxSchemaSerializer::to_json_banded_line(band_schemas));

    Timer timer;
    timer.start();
    this->idx = make_unique<BandedSynthDex>(this->source(), band_schemas, per_band_ostats);
    double time = timer.stop();

    Log::w(1, "Structure", this->idx->str());
    Log::w(1, "Indexing time [s]", time);
    Log::w(1, "Size [megabytes]", this->idx->getSize() / (1024.0 * 1024.0));

    return time;
}


void ExecutionRunner::execute_synthetic_for_groups(const vector<string>& groups)
{
    this->load_source();

    // Two-phase synthesis with per-Q winner selection.
    //
    // Phase 1 (banded): run temporal clustering and per-band LCM synthesis for
    // each Q workload; collect results WITHOUT constructing or evaluating yet.
    //
    // Phase 2 (non-banded): run full-collection synthesis once across all Q
    // workloads.
    //
    // Per-Q decision: when both phases produced a result for the same Q, compare
    // their predicted average seconds-per-query.  Construct and evaluate only the
    // winner.  When only one phase applies (pure banded or pure non-banded groups)
    // the existing logic is preserved unchanged.
    //
    // compute_per_band_ostats() calls analyze_O() per band and leaves Ostats at
    // the last band, we save/restore around each call so the full-collection
    // value is always in effect for Phase 2 and the comparison step.

    struct Phase1Entry
    {
        BandedResult br;
        vector<OStats> per_band_ostats;
        // Weighted-average predicted seconds-per-query across all bands.
        // Used to compare against Phase 2 predicted performance.
        double predicted_avg_spq = 0.0;
    };

    bool has_banded_templates = false;
    vector<string> non_banded_groups;
    // One entry per this->Q slot; nullopt when Phase 1 produced no result for
    // that workload (no bands, empty suggestions, etc.).
    vector<optional<Phase1Entry>> phase1_results(this->Q.size(), nullopt);

    for (size_t qi = 0; qi < this->Q.size(); ++qi)
    {
        auto& Qx = this->Q[qi];

        // Single-entry Q: temporal slices are computed from this workload only.
        auto prep = SynthesisRunner::prepare_banding(
            this->source(), this->file_O,
            { Qx },
            this->statscomp, groups);

        // Capture Q-independent flags on the first pass.
        if (!has_banded_templates && prep.has_banded_templates)
        {
            has_banded_templates = true;
            non_banded_groups = prep.non_banded_groups;
        }

        if (!prep.has_banded_templates || prep.batched_Q.empty()) continue;

        SynthesisRunner synthesisrunner(
            prep.batched_Q, this->statscomp, prep.per_q_ostats,
            groups, prep.band_inner_templates);
        auto suggestions = synthesisrunner.run();

        if (suggestions.empty()) continue;

        bool has_bands = any_of(suggestions.begin(), suggestions.end(),
            [](const auto& p) { return any_of(p.second.begin(), p.second.end(),
                [](const auto& s) { return !s.temporal_band.empty(); }); });

        if (!has_bands) continue;

        auto br = SynthesisRunner::process_banded(
            suggestions, prep.batched_Q, prep.all_slice_bounds, this->source().gend);

        if (br.schemas.empty()) continue;

        Log::w(0, "Banded Index (" + to_string(br.schemas.size()) + " bands)");
        Log::w(1, "Banded schema",
            IdxSchemaSerializer::to_json_banded(br.schemas));

        // Compute combined predicted s/q for later comparison with Phase 2.
        double total_spq_time = 0.0;
        size_t total_q_count = 0;
        for (const auto& bs : br.suggestions)
        {
            total_spq_time += bs.query_count * pow(10.0, bs.predicted_performance);
            total_q_count  += bs.query_count;
        }
        double predicted_avg_spq = (total_q_count > 0)
            ? total_spq_time / total_q_count
            : numeric_limits<double>::max();

        // Save full-collection Ostats: compute_per_band_ostats() calls
        // analyze_O() for each band and leaves Ostats at the last band.
        auto saved_ostats = this->statscomp.Ostats;
        auto per_band_ostats = SynthesisRunner::compute_per_band_ostats(
            br.schemas, this->source(), this->statscomp);
        this->statscomp.Ostats = saved_ostats;

        phase1_results[qi] = Phase1Entry{
            move(br), move(per_band_ostats), predicted_avg_spq};
    }

    this->release_source();

    // Phase 2: non-banded groups (templates without temporal bands, e.g. dict2).
    // In mixed mode: use only non_banded_groups so that banded templates are not
    // re-evaluated here.  In pure non-banded mode: use all groups.
    // statscomp.Ostats is guaranteed to be the full-collection value here
    // thanks to the per-Q save/restore above.
    const vector<string>& nb_groups = has_banded_templates
        ? non_banded_groups
        : groups;

    // When only banded groups exist, skip Phase 2 entirely and evaluate all
    // Phase 1 results unconditionally.
    if (nb_groups.empty())
    {
        for (size_t qi = 0; qi < this->Q.size(); ++qi)
        {
            if (!phase1_results[qi]) continue;
            auto& p1 = *phase1_results[qi];
            auto& Qx = this->Q[qi];

            this->load_source();
            auto time = this->construct_banded(p1.br.schemas, p1.per_band_ostats);
            this->Istats.category = "synthetic";
            this->Istats.params = IdxSchemaSerializer::to_json_banded_line(p1.br.schemas);

            if (Cfg::get<bool>("check-results"))
            {
                ResultValidator evaluator(
                    this->source(), Qx, this->Ostats,
                    this->file_O.empty(), this->file_Q.empty());
                evaluator.run(this->idx.get(), this->Istats, time);
                this->idx.reset();
                this->release_source();
                continue;
            }
            else
            {
                this->evaluate_performance(Qx, true, time, true, "synthetic");
                this->idx.reset();
            }

        }
        return;
    }

    // Run Phase 2 synthesis.
    SynthesisRunner synthesisrunner2(this->Q, this->statscomp, {}, nb_groups);
    auto suggestions2 = synthesisrunner2.run();

    // Per-Q: pick the better of Phase 1 (banded) and Phase 2 (non-banded) and
    // construct+evaluate only the winner.  When Phase 1 produced no result for a
    // workload, always fall back to Phase 2.
    for (size_t qi = 0; qi < this->Q.size(); ++qi)
    {
        auto& Qx = this->Q[qi];
        Log::w(0, progress("Index", (int)qi, (int)this->Q.size()));

        bool check = Cfg::get<bool>("check-results");

        // Locate Phase 2 suggestion for this workload.
        const IdxSchemaSuggestion* suggestion2 = nullptr;
        if (!suggestions2.empty())
        {
            auto Qname = get<0>(Qx);
            auto it = find_if(suggestions2.begin(), suggestions2.end(),
                [&](const auto& pair)
                { return !pair.second.empty()
                    && pair.second.front().oq_file.find(Qname) != string::npos; });
            if (it != suggestions2.end())
                suggestion2 = SynthesisRunner::select_size_aware(it->second);
        }

        // Decide: prefer banded when both exist and banded is faster.
        bool use_banded = false;
        if (phase1_results[qi].has_value() && suggestion2 != nullptr)
        {
            double p2_spq = pow(10.0, suggestion2->predicted_performance);
            use_banded = phase1_results[qi]->predicted_avg_spq < p2_spq;
            Log::w(1, "Synthesis comparison",
                string("banded=") + to_string(phase1_results[qi]->predicted_avg_spq)
                + " s/q  non-banded=" + to_string(p2_spq) + " s/q  winner="
                + string(use_banded ? "banded" : "non-banded"));
        }
        else if (phase1_results[qi].has_value())
        {
            use_banded = true;   // no Phase 2 result available
        }

        if (use_banded)
        {
            auto& p1 = *phase1_results[qi];
            this->load_source();
            auto time = this->construct_banded(p1.br.schemas, p1.per_band_ostats);
            this->Istats.category = "synthetic";
            this->Istats.params = IdxSchemaSerializer::to_json_banded_line(p1.br.schemas);

            if (check)
            {
                ResultValidator evaluator(
                    this->source(), Qx, this->Ostats,
                    this->file_O.empty(), this->file_Q.empty());
                evaluator.run(this->idx.get(), this->Istats, time);
                this->idx.reset();
                this->release_source();
            }
            else
            {
                this->evaluate_performance(Qx, true, time, true, "synthetic");
                this->idx.reset();
            }
        }
        else if (suggestion2 != nullptr)
        {
            Log::w(1, "Workload adaptation", strs(suggestion2->workload, "\n"));
            Log::w(1, "Optimization", suggestion2->variant);

            this->idxschema = suggestion2->idxschema;
            this->Istats = this->statscomp.analyze_i(this->idxschema, "synthetic");
            this->load_source();
            auto time = this->construct(this->idxschema);

            if (check)
            {
                ResultValidator evaluator(
                    this->source(), Qx, this->Ostats,
                    this->file_O.empty(), this->file_Q.empty());
                evaluator.run(this->idx.get(), this->Istats, time);
                this->idx.reset();
                this->release_source();
            }
            else
            {
                this->evaluate_performance(Qx, false, time, true, "synthetic");
                this->idx.reset();
            }
        }
        // If neither phase produced a result, skip this Q (shouldn't happen in
        // normal operation since suggestions2 is checked above).
    }
}


int ExecutionRunner::execute()
{
    struct IndexTask { optional<string> pattern; string label; };
    vector<IndexTask> tasks;
    // Synthesis tokens ("!:groups") are separated from concrete schema tasks
    // and processed after the concrete task loop via execute_synthetic_for_groups().
    vector<vector<string>> synth_groups_list;

    if (this->idxschema_str != "")
    {
        // Check if the user-provided string is a banded schema (JSON array)
        stringstream ss_check(this->idxschema_str);
        string first_part;
        getline(ss_check, first_part, '|');
        if (!first_part.empty() && IdxSchemaSerializer::is_banded(first_part))
        {
            // Banded schema path, single banded schema, no pipe splitting
            tasks.push_back({optional<string>(this->idxschema_str), "banded"});
        }
        else
        {
            // User-provided explicit schema(s) via pipe-separated string.
            // Tokens prefixed with "!:" are synthesis requests; all others are
            // concrete schemas (JSON objects or method strings).
            stringstream ss(this->idxschema_str);
            string part;
            while (getline(ss, part, '|'))
            {
                if (part.empty()) continue;
                if (part.rfind("!:", 0) == 0)
                {
                    // Parse "!:group1,group2" into a groups vector.
                    vector<string> sg;
                    if (part.size() > 2)
                    {
                        stringstream sg_ss(part.substr(2));
                        string g;
                        while (getline(sg_ss, g, ','))
                            if (!g.empty()) sg.push_back(g);
                    }
                    synth_groups_list.push_back(move(sg));
                }
                else
                {
                    tasks.push_back({optional<string>(part), part});
                }
            }
        }
    }
    else
    {
        // Per-template exploration: for each active template, generate
        // the number of random configurations defined in
        // "i.gen.exploration-per-O" (falls back to 0 if not specified).
        auto resolved = IGen::resolve_active_templates(this->groups);
        for (const auto& [group, pattern_name] : resolved)
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
                "i.gen.design-space." + group + "." + pattern_name);

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

    // Performance evaluators persist across index configs so result_cnts/
    // result_xors continue to provide cross-index consistency checking.
    bool check = Cfg::get<bool>("check-results");
    vector<IndexEvaluator*> evaluators;
    if (!check)
    {
        evaluators.reserve(this->Q.size());
        for (const auto& Qx : this->Q)
        {
            if (this->comparison_measurements != nullptr)
                evaluators.push_back(new ComparisonAnalyzer(
                    this->object_domain, Qx, this->Ostats,
                    *this->comparison_measurements, "manual"));
            else
                evaluators.push_back(new PerformanceAnalyzer(
                    this->object_domain, Qx, this->Ostats,
                    false, this->Q_patterns));
        }
    }

    Timer eta_timer;
    eta_timer.start();

    for (int i = 0; i < idx_num; i++)
    {
        Log::w(0, progress("Index", i, idx_num));
        Log::w(1, "Template", tasks[i].label);

        // Check if this task is a banded schema
        bool is_banded_task = tasks[i].pattern.has_value()
            && IdxSchemaSerializer::is_banded(tasks[i].pattern.value());

        if (is_banded_task)
        {
            auto banded_schemas = IdxSchemaSerializer::from_json_banded(
                tasks[i].pattern.value());

            Log::w(1, "Banded schema (" + to_string(banded_schemas.size()) + " bands)");

            this->load_source();
            auto per_band_ostats = SynthesisRunner::compute_per_band_ostats(
                banded_schemas, this->source(), this->statscomp);

            auto time = this->construct_banded(banded_schemas, per_band_ostats);
            this->release_source();

            // Record the banded schema in Istats so it appears in the score CSV.
            this->Istats.category = "synthetic";
            this->Istats.params = IdxSchemaSerializer::to_json_banded_line(banded_schemas);

            for (const auto& Qx : this->Q)
            {
                this->evaluate_performance(Qx, true, time, true, "synthetic");
            }

            this->idx.reset();
        }
        else
        {
        this->idxschema = IGen(this->groups).construct_I(tasks[i].pattern);

        auto idxschema_json_line
            = IdxSchemaSerializer::to_json_line(this->idxschema);

        this->Istats = this->statscomp.analyze_i(this->idxschema, "manual");

        if (this->do_predict || Cfg::get<bool>("prediction.predict-before-run"))
            this->predict();

        if (!this->do_query) continue;

        this->load_source();
        auto time = this->construct(this->idxschema);

        if (check)
        {
            for (const auto& Qx : this->Q)
            {
                ResultValidator evaluator(
                    this->source(), Qx, this->Ostats,
                    this->file_O.empty(), this->file_Q.empty());
                evaluator.run(this->idx.get(), this->Istats, time);
            }
        }
        else
        {
            this->release_source();
            for (auto* evaluator : evaluators)
                evaluator->run(this->idx.get(), this->Istats, time);
        }
        
        this->idx.reset();
        this->release_source();
        }

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

    // Run per-Q synthesis for each !: token in the mixed pipe.
    // Each Q workload receives its own independently synthesized index.
    for (const auto& sg : synth_groups_list)
        this->execute_synthetic_for_groups(sg.empty() ? this->groups : sg);

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
        "./learning/prediction.py \"" + Cfg::config_path
        + "\" \"" + Cfg::get_out_dir() + "\" " + files_OQI_str, 1);
}


void ExecutionRunner::update(const string& file_O, const string& file_O2,
    const string& idxschema_str, StatsComp& statscomp, const string& category)
{
    Log::w(0, "Constructing and Updating Index");

    // Load original objects
    auto O = Persistence::read_O_dat(file_O);
    Log::w(1, "Records count", to_string(O.size()));
    
    // Compute statistics for original objects
    auto Ostats = statscomp.analyze_O(O, file_O);

    // Load new objects for update
    auto O2 = Persistence::read_O_dat(file_O2);
    Log::w(1, "Records count", to_string(O2.size()));
    double ratio_org = (double)O.size() / (O.size() + O2.size()) * 100.0;
    double ratio_upd = (double)O2.size() / (O.size() + O2.size()) * 100.0;
    
    char ratio_str[100];
    snprintf(ratio_str, sizeof(ratio_str), "%.2f%% Construction + %.2f%% Update", ratio_org, ratio_upd);
    Log::w(1, "Records ratio", string(ratio_str));

    for (auto& r : O2) r.id += O.size();

    auto resolved = resolve_idxschema(idxschema_str);

    // Construct initial index with original objects
    Log::w(0, "Construction");

    unique_ptr<IRIndex> idx;
    Timer timer;

    if (IdxSchemaSerializer::is_banded(resolved))
    {
        // Synthesis produced a banded schema, build a BandedSynthDex.
        auto banded_schemas = IdxSchemaSerializer::from_json_banded(resolved);
        Log::w(1, "Banded schema (" + to_string(banded_schemas.size()) + " bands)",
            IdxSchemaSerializer::to_json_banded_line(banded_schemas));

        auto saved_ostats = Ostats;
        auto per_band_ostats = SynthesisRunner::compute_per_band_ostats(
            banded_schemas, O, statscomp);
        statscomp.Ostats = saved_ostats;

        timer.start();
        idx = make_unique<BandedSynthDex>(O, banded_schemas, per_band_ostats);
    }
    else
    {
        auto idxschema_parsed = IGen().construct_I(optional<string>(resolved));
        Log::w(1, "Schema", IdxSchemaSerializer::to_json(idxschema_parsed));

        bool use_static    = Cfg::get<bool>("synthesis.use-static-synthdex");
        bool use_optimized = Cfg::get<bool>("synthesis.use-templated-synthdex");
        Log::w(2, "Using", use_static && is_static_synthdex_eligible(idxschema_parsed) ? "SynthDexStatic (hardcoded)" : use_optimized ? "SynthDexOpt (template-based)" : "SynthDex (runtime)");

        timer.start();
        if (use_static && is_static_synthdex_eligible(idxschema_parsed))
            idx = make_unique<SynthDexStatic>(O, idxschema_parsed, Ostats);
        else if (use_optimized)
            idx = make_unique<SynthDexOpt>(O, idxschema_parsed, Ostats);
        else
            idx = make_unique<SynthDex>(O, idxschema_parsed, Ostats);
    }
    double construction_time = timer.stop();

    Log::w(1, "Structure", idx->str());
    Log::w(1, "Indexing time [s]", construction_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));
    const size_t update_size_before = idx->getSize();

    // Update index with new objects
    Log::w(0, "Update");

    timer.start();

    idx->update(O2);

    double update_time = timer.stop();

    Log::w(1, "Updating time [s]", update_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));

    const size_t update_before_cnt = O.size();
    const size_t update_after_cnt = O.size() + O2.size();
    double update_affected_pct = update_before_cnt > 0
        ? ((double)O2.size() / (double)update_before_cnt) * 100.0 : 0.0;

    Persistence::write_mutation_score_csv(
        Ostats, "update",
        filesystem::path(file_O2).filename().string(),
        update_affected_pct,
        update_before_cnt, update_after_cnt,
        update_size_before, idx->getSize(),
        construction_time, update_time,
        category, resolved);
    
}


void ExecutionRunner::remove(const string& file_O, const string& file_O2,
    const string& idxschema_str, StatsComp& statscomp, const string& category)
{
    Log::w(0, "Constructing Index and Deleting Records");

    // Load original objects
    auto O = Persistence::read_O_dat(file_O);
    Log::w(1, "Records count", to_string(O.size()));
    
    // Compute statistics for original objects
    auto Ostats = statscomp.analyze_O(O, file_O);

    // Load IDs to delete
    auto idsToDelete = Persistence::read_Oids_dat(file_O2);
    Log::w(1, "Records to delete", to_string(idsToDelete.size()));
    double ratio_org = (double)O.size() / (O.size() + idsToDelete.size()) * 100.0;
    double ratio_del = (double)idsToDelete.size() / (O.size() + idsToDelete.size()) * 100.0;
    
    char ratio_str[100];
    snprintf(ratio_str, sizeof(ratio_str), "%.2f%% Construction + %.2f%% Deletion", ratio_org, ratio_del);
    Log::w(1, "Records ratio", string(ratio_str));

    auto resolved = resolve_idxschema(idxschema_str);

    // Construct initial index with original objects
    Log::w(0, "Construction");

    unique_ptr<IRIndex> idx;
    Timer timer;

    if (IdxSchemaSerializer::is_banded(resolved))
    {
        // Synthesis produced a banded schema, build a BandedSynthDex.
        auto banded_schemas = IdxSchemaSerializer::from_json_banded(resolved);
        Log::w(1, "Banded schema (" + to_string(banded_schemas.size()) + " bands)",
            IdxSchemaSerializer::to_json_banded_line(banded_schemas));

        auto saved_ostats = Ostats;
        auto per_band_ostats = SynthesisRunner::compute_per_band_ostats(
            banded_schemas, O, statscomp);
        statscomp.Ostats = saved_ostats;

        timer.start();
        idx = make_unique<BandedSynthDex>(O, banded_schemas, per_band_ostats);
    }
    else
    {
        auto idxschema_parsed = IGen().construct_I(optional<string>(resolved));
        Log::w(1, "Schema", IdxSchemaSerializer::to_json(idxschema_parsed));

        bool use_static    = Cfg::get<bool>("synthesis.use-static-synthdex");
        bool use_optimized = Cfg::get<bool>("synthesis.use-templated-synthdex");
        Log::w(2, "Using", use_static && is_static_synthdex_eligible(idxschema_parsed) ? "SynthDexStatic (hardcoded)" : use_optimized ? "SynthDexOpt (template-based)" : "SynthDex (runtime)");

        timer.start();
        if (use_static && is_static_synthdex_eligible(idxschema_parsed))
            idx = make_unique<SynthDexStatic>(O, idxschema_parsed, Ostats);
        else if (use_optimized)
            idx = make_unique<SynthDexOpt>(O, idxschema_parsed, Ostats);
        else
            idx = make_unique<SynthDex>(O, idxschema_parsed, Ostats);
    }
    double construction_time = timer.stop();

    Log::w(1, "Structure", idx->str());
    Log::w(1, "Indexing time [s]", construction_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));
    const size_t delete_size_before = idx->getSize();

    // Delete objects from index
    Log::w(0, "Delete");

    timer.start();

    vector<bool> idsBitmap(O.size(), false);
    size_t skipped_ids = 0;
    for (auto id : idsToDelete)
    {
        if (id >= 0 && (size_t)id < O.size())
            idsBitmap[(size_t)id] = true;
        else
            skipped_ids++;
    }
    if (skipped_ids > 0)
        Log::w(1, "Skipped out-of-range delete IDs", to_string(skipped_ids));

    const size_t effective_delete_cnt = count(idsBitmap.begin(), idsBitmap.end(), true);
    Log::w(1, "Effective unique IDs to delete", to_string(effective_delete_cnt));
    idx->remove(idsBitmap);

    double delete_time = timer.stop();

    Log::w(1, "Deletion time [s]", delete_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));

    const size_t delete_before_cnt = O.size();
    const size_t delete_after_cnt = delete_before_cnt >= effective_delete_cnt
        ? (delete_before_cnt - effective_delete_cnt) : 0;
    double delete_affected_pct = delete_before_cnt > 0
        ? ((double)effective_delete_cnt / (double)delete_before_cnt) * 100.0 : 0.0;

    Persistence::write_mutation_score_csv(
        Ostats, "delete",
        filesystem::path(file_O2).filename().string(),
        delete_affected_pct,
        delete_before_cnt, delete_after_cnt,
        delete_size_before, idx->getSize(),
        construction_time, delete_time,
        category, resolved);
    
}


void ExecutionRunner::softdelete(const string& file_O, const string& file_O2,
    const string& idxschema_str, StatsComp& statscomp, const string& category)
{
    Log::w(0, "Constructing Index and Soft-Deleting Records");

    // Load original objects
    auto O = Persistence::read_O_dat(file_O);
    Log::w(1, "Records count", to_string(O.size()));
    
    // Compute statistics for original objects
    auto Ostats = statscomp.analyze_O(O, file_O);

    // Load IDs to soft-delete
    auto idsToDelete = Persistence::read_Oids_dat(file_O2);
    Log::w(1, "Records to soft-delete", to_string(idsToDelete.size()));
    double ratio_org = (double)O.size() / (O.size() + idsToDelete.size()) * 100.0;
    double ratio_del = (double)idsToDelete.size() / (O.size() + idsToDelete.size()) * 100.0;
    
    char ratio_str[100];
    snprintf(ratio_str, sizeof(ratio_str), "%.2f%% Construction + %.2f%% Soft-Deletion", ratio_org, ratio_del);
    Log::w(1, "Records ratio", string(ratio_str));

    auto resolved = resolve_idxschema(idxschema_str);

    // Construct initial index with original objects
    Log::w(0, "Construction");

    unique_ptr<IRIndex> idx;
    Timer timer;

    if (IdxSchemaSerializer::is_banded(resolved))
    {
        // Synthesis produced a banded schema, build a BandedSynthDex.
        auto banded_schemas = IdxSchemaSerializer::from_json_banded(resolved);
        Log::w(1, "Banded schema (" + to_string(banded_schemas.size()) + " bands)",
            IdxSchemaSerializer::to_json_banded_line(banded_schemas));

        auto saved_ostats = Ostats;
        auto per_band_ostats = SynthesisRunner::compute_per_band_ostats(
            banded_schemas, O, statscomp);
        statscomp.Ostats = saved_ostats;

        timer.start();
        idx = make_unique<BandedSynthDex>(O, banded_schemas, per_band_ostats);
    }
    else
    {
        auto idxschema_parsed = IGen().construct_I(optional<string>(resolved));
        Log::w(1, "Schema", IdxSchemaSerializer::to_json(idxschema_parsed));

        bool use_static    = Cfg::get<bool>("synthesis.use-static-synthdex");
        bool use_optimized = Cfg::get<bool>("synthesis.use-templated-synthdex");
        Log::w(2, "Using", use_static && is_static_synthdex_eligible(idxschema_parsed) ? "SynthDexStatic (hardcoded)" : use_optimized ? "SynthDexOpt (template-based)" : "SynthDex (runtime)");

        timer.start();
        if (use_static && is_static_synthdex_eligible(idxschema_parsed))
            idx = make_unique<SynthDexStatic>(O, idxschema_parsed, Ostats);
        else if (use_optimized)
            idx = make_unique<SynthDexOpt>(O, idxschema_parsed, Ostats);
        else
            idx = make_unique<SynthDex>(O, idxschema_parsed, Ostats);
    }
    double construction_time = timer.stop();

    Log::w(1, "Structure", idx->str());
    Log::w(1, "Indexing time [s]", construction_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));
    const size_t softdelete_size_before = idx->getSize();

    // Soft-delete objects from index (replace IDs with tombstone -1)
    Log::w(0, "Soft-Delete");

    timer.start();

    // Build bitmap once, pass by reference through the entire index tree
    vector<bool> idsBitmap(O.size(), false);
    size_t skipped_ids = 0;
    for (auto id : idsToDelete)
    {
        if (id >= 0 && (size_t)id < O.size())
            idsBitmap[(size_t)id] = true;
        else
            skipped_ids++;
    }
    if (skipped_ids > 0)
        Log::w(1, "Skipped out-of-range soft-delete IDs", to_string(skipped_ids));

    const size_t effective_softdelete_cnt = count(idsBitmap.begin(), idsBitmap.end(), true);
    Log::w(1, "Effective unique IDs to soft-delete", to_string(effective_softdelete_cnt));
    idx->softdelete(idsBitmap);

    double softdelete_time = timer.stop();

    Log::w(1, "Soft-deletion time [s]", softdelete_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));

    const size_t softdelete_before_cnt = O.size();
    const size_t softdelete_after_cnt = O.size();
    double softdelete_affected_pct = softdelete_before_cnt > 0
        ? ((double)effective_softdelete_cnt / (double)softdelete_before_cnt) * 100.0 : 0.0;

    Persistence::write_mutation_score_csv(
        Ostats, "softdelete",
        filesystem::path(file_O2).filename().string(),
        softdelete_affected_pct,
        softdelete_before_cnt, softdelete_after_cnt,
        softdelete_size_before, idx->getSize(),
        construction_time, softdelete_time,
        category, resolved);
    
}


void ExecutionRunner::synthesize(
    const vector<tuple<string,vector<RangeIRQuery>>>& Q,
    const OStats& Ostats, StatsComp& statscomp,
    const vector<string>& groups)
{
    SynthesisRunner synthesisrunner(Q, statscomp, {}, groups);
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


void ExecutionRunner::synthesize(
    const vector<tuple<string,vector<RangeIRQuery>>>& Q,
    const vector<OStats>& per_q_ostats, StatsComp& statscomp,
    const vector<pair<Timestamp, Timestamp>>& all_slice_bounds,
    const vector<string>& groups,
    const vector<IdxSchema>& custom_templates)
{
    SynthesisRunner synthesisrunner(Q, statscomp, per_q_ostats, groups, custom_templates);
    auto result = synthesisrunner.run();

    // Group suggestions by temporal band
    map<string, vector<const IdxSchemaSuggestion*>> by_band;
    for (const auto& [oq_file, suggestions] : result)
        for (const auto& s : suggestions)
        {
            string band = s.temporal_band.empty() ? "(global)" : s.temporal_band;
            by_band[band].push_back(&s);
        }

    // Sort within each band by predicted performance (best first)
    for (auto& [band, band_suggestions] : by_band)
        sort(band_suggestions.begin(), band_suggestions.end(),
            [](const IdxSchemaSuggestion* a, const IdxSchemaSuggestion* b)
            { return a->predicted_performance < b->predicted_performance; });

    int top_limit = Cfg::get<int>("synthesis.top-k");
    double top_relative = Cfg::get<double>("synthesis.top-relative");

    for (const auto& [band, band_suggestions] : by_band)
    {
        Log::w(0, "Temporal Band: " + band);

        double best_throughput = band_suggestions.empty() ? 0.0 :
            1.0 / pow(10, band_suggestions[0]->predicted_performance);
        double best_size_mb = band_suggestions.empty() ? 0.0 :
            pow(10, band_suggestions[0]->predicted_size) / (1024*1024);

        int top = 0;
        double prev_throughput = 0.0;
        double prev_size_mb = 0.0;

        for (const auto* suggestion : band_suggestions)
        {
            auto time_per_query_sq = pow(10, suggestion->predicted_performance);
            auto throughput_qps = 1.0 / time_per_query_sq;
            auto size_mb = pow(10, suggestion->predicted_size) / (1024*1024);

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

            double throughput_pct = (best_throughput > 0) ? (throughput_qps / best_throughput) * 100.0 : 100.0;
            double size_pct = (best_size_mb > 0) ? (size_mb / best_size_mb) * 100.0 : 100.0;

            stringstream throughput_pct_str, size_pct_str;
            throughput_pct_str << fixed << setprecision(1) << throughput_pct;
            size_pct_str << fixed << setprecision(1) << size_pct;

            if (top >= top_limit)
            {
                Log::w(0, "Top-k limit reached, stopping further suggestions.");
                break;
            }

            Log::w(0, "Suggestion (" + to_string(top+1) + "/" + to_string(band_suggestions.size()) + ")");
            Log::w(1, "Temporal band", band);
            Log::w(2, "OQ statistics file", suggestion->oq_file);
            Log::w(1, "Optimization", suggestion->variant);
            Log::w(1, "Workload", str(suggestion->workload, "\n"));
            Log::w(1, "Synth ID", to_string(suggestion->synth_id));
            Log::w(2, "Avg time per query [s/q] (predicted)", to_string(time_per_query_sq));
            Log::w(1, "Throughput [q/s] (predicted)", to_string(throughput_qps));
            Log::w(1, "Size [megabytes] (predicted)", to_string(size_mb));
            Log::w(1, "Relative to winning suggestion", throughput_pct_str.str() + "% Throughput, " + size_pct_str.str() + "% Size");
            Log::w(1, "Schema", IdxSchemaSerializer::to_json(suggestion->idxschema));
            Log::w(1, "Schema (single line)", "'" + IdxSchemaSerializer::to_json_line(suggestion->idxschema) + "'");

            prev_throughput = throughput_qps;
            prev_size_mb = size_mb;
            top++;
        }
    }

    // Emit merged banded schema if temporal bands are present
    bool has_bands = any_of(result.begin(), result.end(),
        [](const auto& p) { return any_of(p.second.begin(), p.second.end(),
            [](const auto& s) { return !s.temporal_band.empty(); }); });

    if (has_bands)
    {
        auto br = SynthesisRunner::process_banded(
            result, Q, all_slice_bounds);

        if (!br.schemas.empty())
        {
            Log::w(0, "Merged Banded Schema (" + to_string(br.schemas.size()) + " bands)");
            Log::w(1, "Schema (banded)", IdxSchemaSerializer::to_json_banded(br.schemas));
            Log::w(1, "Schema (banded, single line)",
                "'" + IdxSchemaSerializer::to_json_banded_line(br.schemas) + "'");
        }
    }
}
