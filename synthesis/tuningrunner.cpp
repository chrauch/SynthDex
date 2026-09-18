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

#include "tuningrunner.h"
#include "synthesisrunner.h"
#include "../generation/ogen.h"
#include "../generation/ostatsgen.h"
#include "../generation/qgen.h"
#include "../structure/synthdex.h"
#include "../structure/synthdexopt.h"
#include "../structure/idxschemaserializer.h"
#include "../utils/logging.h"
#include "../utils/global.h"
#include "../utils/processexec.h"
#include <algorithm>
#include <numeric>
#include <cmath>
#include <filesystem>
#include <sstream>
#include <iomanip>
#include <chrono>

using namespace std;


TuningRunner::TuningRunner(StatsComp& statscomp, const vector<string>& groups)
    : statscomp(statscomp), groups(groups)
{
}


void TuningRunner::run()
{
    int num_O = Cfg::get<int>("tuning.num-object-collections");
    this->measure_actual_enabled = Cfg::get<bool>("tuning.measure-actual");

    auto settings = this->load_settings();
    if (settings.empty())
        throw runtime_error("No settings configured under tuning.exps");

    auto reference_variants = Cfg::get<vector<string>>("synthesis.variant");

    Log::w(0, "Synthesis Tuning");
    Log::w(1, "Object collections", num_O);
    Log::w(1, "Settings under test", (int)settings.size());
    Log::w(1, "Reference synthesis", strs(reference_variants, ","));
    Log::w(1, "Measure actual throughput", this->measure_actual_enabled ? "yes" : "no");

    string out_dir = Cfg::get_out_dir() + "/tuning";
    filesystem::create_directories(out_dir);

    auto now = chrono::system_clock::now();
    time_t t = chrono::system_clock::to_time_t(now);
    tm tm_buf;
    localtime_r(&t, &tm_buf);
    ostringstream ts;
    ts << put_time(&tm_buf, "%Y%m%d-%H%M%S");
    string base_path    = out_dir + "/tuning-" + ts.str();
    string path_main    = base_path + ".csv";
    string path_summary = base_path + ".summary.csv";

    ofstream csv(path_main);
    ofstream scsv(path_summary);
    if (!csv.is_open() || !scsv.is_open())
        throw runtime_error("Cannot open tuning output files under: " + out_dir);

    this->write_header(csv);
    this->write_summary_header(scsv);

    // Keyed by "<exp>#<index>" so that aggregates survive across collections.
    map<string, TuningAggregate> aggregates;

    // The base configuration defines the reference synthesis and is restored
    // before every measurement so that settings never accumulate.
    SetValue base_config = Cfg::instance().snapshot();

    ProcessExec::python_setup(false);

    Timer eta_timer;
    eta_timer.start();

    for (int oi = 0; oi < num_O; oi++)
    {
        Log::w(0, progress("Object collection", oi, num_O));

        this->Ostats = OStatsGen().create(oi);
        this->O      = OGen(this->Ostats).construct_O();
        this->Ostats = this->statscomp.analyze_O(
            this->O, this->Ostats.name + "-tuning");

        auto Q_named = QGen(this->O, this->Ostats).construct_Q();
        if (Q_named.empty())
        {
            Log::w(1, "No queries generated, skipping O", oi);
            continue;
        }

        string o_name = this->Ostats.name;

        for (const auto& Qx : Q_named)
        {
            const string& q_name  = get<0>(Qx);
            const auto&   queries = get<1>(Qx);

            Log::w(1, "Workload", q_name);
            this->actual_cache.clear();

            // Reference: the synthesis as configured, on the untouched config.
            Cfg::instance().restore(base_config);
            Log::w(1, "Reference synthesis", strs(reference_variants, ","));
            auto reference = this->synthesize(Qx);
            if (!reference.valid)
            {
                Log::w(1, "Reference produced no configuration, skipping", q_name);
                continue;
            }
            reference.actual_tp_log = this->measure_actual(reference, queries);
            Log::w(1, "Reference configuration", reference.i_schema);
            Log::w(1, "Reference probes",        (int)reference.probes);
            Log::w(1, "Reference time [s]",      reference.synthesis_time);

            for (const auto& setting : settings)
            {
                Cfg::instance().restore(base_config);
                SetValue variant_array;
                variant_array.type = SetValue::Array;
                variant_array.array_values.push_back(SetValue(setting.type));
                Cfg::instance().set_at("synthesis.variant", variant_array);
                Cfg::instance().merge_at("synthesis." + setting.type, setting.overrides);

                Log::w(1, "Setting " + setting.exp + "#" + to_string(setting.index),
                    setting.json);

                auto obs = this->synthesize(Qx);
                if (!obs.valid)
                {
                    Log::w(1, "Setting produced no configuration, skipping",
                        setting.exp + "#" + to_string(setting.index));
                    continue;
                }
                obs.actual_tp_log = this->measure_actual(obs, queries);

                // Positive degradation = slower than the reference. Both terms
                // are log10(s/q), so the ratio is taken in linear space.
                double pred_deg = (pow(10.0, obs.predicted_tp_log)
                                 - pow(10.0, reference.predicted_tp_log))
                                 / pow(10.0, reference.predicted_tp_log);

                double act_deg = 0.0;
                if (this->measure_actual_enabled)
                    act_deg = (pow(10.0, obs.actual_tp_log)
                             - pow(10.0, reference.actual_tp_log))
                             / pow(10.0, reference.actual_tp_log);

                Log::w(1, "Probes",              (int)obs.probes);
                Log::w(1, "Synthesis time [s]",  obs.synthesis_time);
                Log::w(1, "Predicted degrad.",   pred_deg);
                if (this->measure_actual_enabled)
                    Log::w(1, "Actual degrad.",  act_deg);

                this->write_row(csv, o_name, q_name, setting,
                    reference, obs, pred_deg, act_deg);
                csv.flush();

                auto& agg = aggregates[setting.exp + "#" + to_string(setting.index)];
                agg.probes.push_back((double)obs.probes);
                agg.synthesis_time.push_back(obs.synthesis_time);
                agg.predicted_degradation.push_back(pred_deg);
                if (this->measure_actual_enabled)
                    agg.actual_degradation.push_back(act_deg);
            }
        }

        Cfg::instance().restore(base_config);

        if (num_O > 1)
        {
            double elapsed   = eta_timer.stop();
            double avg       = elapsed / (oi + 1);
            double remaining = avg * (num_O - oi - 1);
            int rem_min      = (int)(remaining / 60);
            int rem_sec      = (int)remaining % 60;
            Log::w(0, "ETA", to_string(rem_min) + "m " + to_string(rem_sec) + "s");
        }

        // Running aggregates so a long run can be inspected while in progress.
        for (const auto& setting : settings)
        {
            auto key = setting.exp + "#" + to_string(setting.index);
            auto it  = aggregates.find(key);
            if (it == aggregates.end() || it->second.probes.empty()) continue;

            const auto& agg = it->second;
            const auto& deg = this->measure_actual_enabled
                ? agg.actual_degradation : agg.predicted_degradation;

            ostringstream oss;
            oss << fixed << setprecision(4)
                << "probes=" << setprecision(0) << mean(agg.probes)
                << "  time=" << setprecision(2) << mean(agg.synthesis_time) << "s"
                << "  degr_avg=" << setprecision(4) << mean(deg)
                << "  degr_max=" << max_of(deg)
                << "  (n=" << deg.size() << ")";
            Log::w(0, " " + key, oss.str());
        }
    }

    for (const auto& setting : settings)
    {
        auto it = aggregates.find(setting.exp + "#" + to_string(setting.index));
        if (it == aggregates.end()) continue;
        this->write_summary(scsv, setting, it->second);
    }

    Cfg::instance().restore(base_config);

    csv.close();
    scsv.close();

    Log::w(0, "Tuning CSV",  path_main);
    Log::w(0, "Summary CSV", path_summary);
}


vector<TuningSetting> TuningRunner::load_settings()
{
    vector<TuningSetting> settings;

    if (!Cfg::has("tuning.exps"))
        return settings;

    for (const auto& exp_name : Cfg::get_keys("tuning.exps"))
    {
        string base = "tuning.exps." + exp_name;
        string type = Cfg::get<string>(base + ".type");

        if (type != "grid" && type != "grad" && type != "gene" && type != "bayo")
            throw runtime_error("Unknown synthesis type '" + type
                + "' in " + base + ".type");

        const SetValue& list = Cfg::node(base + ".settings");
        if (list.type != SetValue::Array)
            throw runtime_error("Expected an array at " + base + ".settings");

        for (size_t i = 0; i < list.array_values.size(); ++i)
        {
            const SetValue& overlay = list.array_values[i];
            if (overlay.type != SetValue::Object)
                throw runtime_error("Expected an object at "
                    + base + ".settings[" + to_string(i) + "]");

            TuningSetting s;
            s.exp       = exp_name;
            s.index     = (int)i;
            s.type      = type;
            s.overrides = overlay;
            s.json      = compact_json(Cfg::instance().str(overlay, 0));
            settings.push_back(move(s));
        }
    }

    return settings;
}


TuningObservation TuningRunner::synthesize(
    const tuple<string, vector<RangeIRQuery>>& Q)
{
    TuningObservation obs;

    vector<tuple<string, vector<RangeIRQuery>>> single_Q { Q };

    auto start = chrono::high_resolution_clock::now();
    auto result = SynthesisRunner(
        single_Q, this->statscomp, {}, this->groups).run();
    auto end = chrono::high_resolution_clock::now();

    // Wall-clock fallback; overwritten by the net time reported by Python.
    obs.synthesis_time = chrono::duration<double>(end - start).count();

    vector<IdxSchemaSuggestion> all;
    for (const auto& [oq_file, suggestions] : result)
        all.insert(all.end(), suggestions.begin(), suggestions.end());

    if (all.empty()) return obs;

    const IdxSchemaSuggestion* best = SynthesisRunner::select_size_aware(all);
    if (!best) return obs;

    obs.predicted_tp_log = best->predicted_performance;
    obs.schema           = best->idxschema;
    obs.i_schema         = IdxSchemaSerializer::to_json_line(best->idxschema);
    obs.valid            = true;

    this->read_run_metrics(obs);

    return obs;
}


void TuningRunner::read_run_metrics(TuningObservation& obs)
{
    string path = Cfg::get_out_dir() + "/synthesis/run-metrics.tsv";
    ifstream in(path);
    if (!in.is_open())
    {
        Log::w(2, "No synthesis run metrics found", path);
        return;
    }

    string header, row;
    getline(in, header);
    if (!getline(in, row)) return;

    vector<string> cells;
    stringstream ss(row);
    string cell;
    while (getline(ss, cell, '\t'))
        cells.push_back(cell);

    // variant, evaluated_configs, stored_configs, queries_per_eval,
    // total_time_s, io_time_s, synthesis_time_s
    if (cells.size() < 7) return;

    obs.probes         = stol(cells[1]);
    obs.synthesis_time = stod(cells[6]);
}


double TuningRunner::measure_actual(
    const TuningObservation& obs,
    const vector<RangeIRQuery>& queries)
{
    if (!this->measure_actual_enabled) return 0.0;

    auto cached = this->actual_cache.find(obs.i_schema);
    if (cached != this->actual_cache.end())
    {
        Log::w(2, "Reusing measurement for identical configuration", obs.i_schema);
        return cached->second;
    }

    bool use_static    = Cfg::get<bool>("synthesis.use-static-synthdex");
    bool use_optimized = Cfg::get<bool>("synthesis.use-templated-synthdex");
    IRIndex* idx;
    if (use_static && is_static_synthdex_eligible(obs.schema))
        idx = new SynthDexStatic(this->O, obs.schema, this->Ostats);
    else if (use_optimized)
        idx = new SynthDexOpt(this->O, obs.schema, this->Ostats);
    else
        idx = new SynthDex(this->O, obs.schema, this->Ostats);

    int runs_per_q = Cfg::get<int>("q.runs");
    Timer timer;
    double total_median = 0.0;
    size_t total_q      = 0;

    for (const auto& q : queries)
    {
        RelationId result;
        vector<double> times;
        times.reserve(runs_per_q);

        for (int r = 0; r < runs_per_q; ++r)
        {
            result.clear();
            result.reserve(100);
            timer.start();
            idx->query(q, result);
            times.push_back(timer.stop());
        }

        total_median += StatsComp::median(times);
        ++total_q;
    }

    delete idx;

    double avg_tp = (total_q > 0) ? total_median / total_q : 0.0;
    double tp_log = (avg_tp > 0.0) ? log10(avg_tp) : -10.0;

    this->actual_cache[obs.i_schema] = tp_log;
    return tp_log;
}


string TuningRunner::compact_json(const string& json)
{
    string out;
    out.reserve(json.size());

    bool in_string = false;
    bool pending_space = false;

    for (char c : json)
    {
        if (c == '"') in_string = !in_string;

        if (!in_string && (c == '\n' || c == '\t' || c == ' '))
        {
            pending_space = !out.empty();
            continue;
        }

        if (pending_space)
        {
            // Whitespace only matters between tokens, never around structure.
            char last = out.empty() ? '\0' : out.back();
            if (last != '{' && last != '[' && last != ':' && last != ','
                && c != '}' && c != ']' && c != ':' && c != ',')
                out += ' ';
            pending_space = false;
        }

        out += c;
    }

    return out;
}


double TuningRunner::mean(const vector<double>& v)
{
    if (v.empty()) return 0.0;
    return accumulate(v.begin(), v.end(), 0.0) / v.size();
}


double TuningRunner::stddev(const vector<double>& v)
{
    if (v.size() < 2) return 0.0;
    double m = mean(v);
    double acc = 0.0;
    for (double x : v) acc += (x - m) * (x - m);
    return sqrt(acc / (v.size() - 1));
}


double TuningRunner::max_of(const vector<double>& v)
{
    if (v.empty()) return 0.0;
    return *max_element(v.begin(), v.end());
}


void TuningRunner::write_header(ofstream& csv)
{
    csv << "O_name\tQ_name\texp\tsetting_idx\ttype\tsetting"
        << "\tprobes\tsynthesis_time_s"
        << "\tpredicted_tp_log\tref_predicted_tp_log\tpredicted_degradation"
        << "\tactual_tp_log\tref_actual_tp_log\tactual_degradation"
        << "\tref_probes\tref_synthesis_time_s"
        << "\ti_schema\tref_i_schema\n";
}


void TuningRunner::write_row(ofstream& csv,
    const string& o_name, const string& q_name,
    const TuningSetting& setting,
    const TuningObservation& ref,
    const TuningObservation& obs,
    double predicted_degradation,
    double actual_degradation)
{
    csv << o_name                  << "\t"
        << q_name                  << "\t"
        << setting.exp             << "\t"
        << setting.index           << "\t"
        << setting.type            << "\t"
        << setting.json            << "\t"
        << obs.probes              << "\t"
        << obs.synthesis_time      << "\t"
        << obs.predicted_tp_log    << "\t"
        << ref.predicted_tp_log    << "\t"
        << predicted_degradation   << "\t"
        << obs.actual_tp_log       << "\t"
        << ref.actual_tp_log       << "\t"
        << actual_degradation      << "\t"
        << ref.probes              << "\t"
        << ref.synthesis_time      << "\t"
        << obs.i_schema            << "\t"
        << ref.i_schema            << "\n";
}


void TuningRunner::write_summary_header(ofstream& scsv)
{
    scsv << "exp\tsetting_idx\ttype\tsetting\tn"
         << "\tprobes_avg\tprobes_dev\tprobes_max"
         << "\tsynthesis_time_avg_s\tsynthesis_time_dev_s\tsynthesis_time_max_s"
         << "\tpredicted_degradation_avg\tpredicted_degradation_dev\tpredicted_degradation_max"
         << "\tactual_degradation_avg\tactual_degradation_dev\tactual_degradation_max\n";
}


void TuningRunner::write_summary(ofstream& scsv,
    const TuningSetting& setting,
    const TuningAggregate& agg)
{
    scsv << setting.exp                        << "\t"
         << setting.index                      << "\t"
         << setting.type                       << "\t"
         << setting.json                       << "\t"
         << agg.probes.size()                  << "\t"
         << mean(agg.probes)                   << "\t"
         << stddev(agg.probes)                 << "\t"
         << max_of(agg.probes)                 << "\t"
         << mean(agg.synthesis_time)           << "\t"
         << stddev(agg.synthesis_time)         << "\t"
         << max_of(agg.synthesis_time)         << "\t"
         << mean(agg.predicted_degradation)    << "\t"
         << stddev(agg.predicted_degradation)  << "\t"
         << max_of(agg.predicted_degradation)  << "\t"
         << mean(agg.actual_degradation)       << "\t"
         << stddev(agg.actual_degradation)     << "\t"
         << max_of(agg.actual_degradation)     << "\n";
}
