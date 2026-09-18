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

#include "noiserunner.h"
#include <algorithm>
#include <numeric>
#include <cmath>
#include <filesystem>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <random>

using namespace std;


NoiseRunner::NoiseRunner(StatsComp& statscomp, const vector<string>& groups)
    : statscomp(statscomp), groups(groups)
{
}


void NoiseRunner::run()
{
    int num_O          = Cfg::get<int>("noise.num-object-collections");
    int num_I          = Cfg::get<int>("noise.num-indices-per-workload");
    int num_runs       = Cfg::get<int>("noise.num-runs-per-index");
    bool shuffle_q     = Cfg::get<bool>("noise.shuffle-queries");
    bool shuffle_wl    = Cfg::get<bool>("noise.shuffle-workloads");
    int runs_per_q     = Cfg::get<int>("q.runs");

    Log::w(0, "Query Noise Measurement");
    Log::w(1, "Object collections",      num_O);
    Log::w(1, "Indices per workload",    num_I);
    Log::w(1, "Runs per index",          num_runs);
    Log::w(1, "Shuffle queries",         shuffle_q  ? "yes" : "no");
    Log::w(1, "Shuffle workloads",       shuffle_wl ? "yes" : "no");
    Log::w(1, "Reps per query (median)", runs_per_q);

    // Create output directory and CSV
    string out_dir = Cfg::get_out_dir() + "/noise";
    filesystem::create_directories(out_dir);

    auto now = chrono::system_clock::now();
    time_t t = chrono::system_clock::to_time_t(now);
    tm tm;
    localtime_r(&t, &tm);
    ostringstream ts;
    ts << put_time(&tm, "%Y%m%d-%H%M%S");
    string out_path              = out_dir + "/noise-" + ts.str() + ".csv";
    string summary_out_path      = out_dir + "/noise-" + ts.str() + ".summary.csv";
    string metasummary_out_path  = out_dir + "/noise-" + ts.str() + ".metasummary.csv";

    ofstream csv(out_path);
    if (!csv.is_open())
        throw runtime_error("Cannot open noise output file: " + out_path);

    this->write_header(csv);

    mt19937 rng(42);
    bool use_static    = Cfg::get<bool>("synthesis.use-static-synthdex");
    bool use_optimized = Cfg::get<bool>("synthesis.use-templated-synthdex");

    // per (O_name, Q_name, i_schema) accumulator
    map<tuple<string,string,string>, vector<double>> summaries;
    // per Q_name: collect per-(O,I) stddevs for meta-summary
    map<string, vector<double>> q_stddevs;

    Timer eta_timer;
    eta_timer.start();

    for (int oi = 0; oi < num_O; oi++)
    {
        Log::w(0, progress("Object collection", oi, num_O));

        this->Ostats    = OStatsGen().create(oi);
        string o_name   = this->Ostats.name;
        this->O         = OGen(this->Ostats).construct_O();
        this->Ostats    = this->statscomp.analyze_O(this->O, o_name + "-noise");

        auto Q_named = QGen(this->O, this->Ostats).construct_Q();
        if (Q_named.empty()) { Log::w(1, "No queries, skipping O", oi); continue; }

        Log::w(1, "Workloads", Q_named.size());

        for (int ii = 0; ii < num_I; ii++)
        {
            Log::w(1, progress("Index", ii, num_I));

            auto idxschema = IGen(this->groups).construct_I(nullopt);
            auto idxschema_json = IdxSchemaSerializer::to_json_line(idxschema);
            Log::w(1, "Schema", idxschema_json);
            auto Istats = this->statscomp.analyze_i(idxschema, "noise");

            Timer timer;
            IRIndex* idx;
            timer.start();
            if (use_static && is_static_synthdex_eligible(idxschema))
                idx = new SynthDexStatic(this->O, idxschema, this->Ostats);
            else if (use_optimized)
                idx = new SynthDexOpt(this->O, idxschema, this->Ostats);
            else
                idx = new SynthDex(this->O, idxschema, this->Ostats);
            Log::w(2, "Indexing time [s]", timer.stop());

            // Build run schedule: each workload appears num_runs times
            vector<size_t> run_schedule;
            for (int ri = 0; ri < num_runs; ri++)
                for (size_t wi = 0; wi < Q_named.size(); wi++)
                    run_schedule.push_back(wi);

            if (shuffle_wl)
                shuffle(run_schedule.begin(), run_schedule.end(), rng);

            // Per-workload run counter for run_idx column
            vector<int> wl_run_counters(Q_named.size(), 0);
            int total_runs = (int)run_schedule.size();

            for (int si = 0; si < total_runs; si++)
            {
                size_t wi = run_schedule[si];
                const string& q_name    = get<0>(Q_named[wi]);
                const vector<RangeIRQuery>& wl_queries = get<1>(Q_named[wi]);
                if (wl_queries.empty()) continue;

                int ri = wl_run_counters[wi]++;

                Log::w(1, progress("Run", si, total_runs));
                Log::w(1, "Workload", q_name);

                vector<RangeIRQuery> run_queries = wl_queries;
                if (shuffle_q)
                    shuffle(run_queries.begin(), run_queries.end(), rng);

                double avg_tp = this->execute_run(idx, run_queries, runs_per_q);
                double avg_tp_log = (avg_tp > 0) ? log10(avg_tp) : -10.0;

                this->write_row(csv, o_name, q_name, oi, ii, idxschema_json, ri, avg_tp, avg_tp_log);
                csv.flush();
                summaries[{o_name, q_name, idxschema_json}].push_back(avg_tp_log);

                Log::w(1, "Throughput [s/q]", "10^" + to_string(avg_tp_log));
            }

            delete idx;
        }

        // ETA
        if (num_O > 1)
        {
            double elapsed = eta_timer.stop();
            double avg = elapsed / (oi + 1);
            double remaining = avg * (num_O - oi - 1);
            int rem_min = (int)(remaining / 60);
            int rem_sec = (int)remaining % 60;
            Log::w(0, "ETA", to_string(rem_min) + "m " + to_string(rem_sec) + "s");
        }
    }

    csv.close();

    ofstream summary_csv(summary_out_path);
    if (!summary_csv.is_open())
        throw runtime_error("Cannot open noise summary file: " + summary_out_path);

    this->write_summary_header(summary_csv);
    for (const auto& [key, values] : summaries)
    {
        this->write_summary(summary_csv, get<0>(key), get<1>(key), get<2>(key), values);

        // compute stddev for this (O,I) combination and collect for meta-summary
        const string& q_name = get<1>(key);
        if (!values.empty())
        {
            double s = accumulate(values.begin(), values.end(), 0.0);
            double m = s / values.size();
            double sq = 0;
            for (double v : values) sq += (v - m) * (v - m);
            q_stddevs[q_name].push_back(values.size() > 1 ? sqrt(sq / (values.size() - 1)) : 0.0);
        }
    }

    summary_csv.close();

    ofstream meta_csv(metasummary_out_path);
    if (!meta_csv.is_open())
        throw runtime_error("Cannot open noise meta-summary file: " + metasummary_out_path);

    this->write_metasummary_header(meta_csv);
    for (const auto& [q_name, stddevs] : q_stddevs)
        this->write_metasummary(meta_csv, q_name, stddevs);

    meta_csv.close();

    Log::w(0, "Noise CSV",             out_path);
    Log::w(0, "Noise summary CSV",     summary_out_path);
    Log::w(0, "Noise metasummary CSV", metasummary_out_path);
}


double NoiseRunner::execute_run(
    IRIndex* idx,
    const vector<RangeIRQuery>& queries,
    int runs_per_q)
{
    Timer timer;
    double total_median_qtime = 0;

    for (const auto& q : queries)
    {
        RelationId qresult;
        vector<double> qtimes;
        qtimes.reserve(runs_per_q);

        for (int r = 0; r < runs_per_q; r++)
        {
            qresult.clear();
            qresult.reserve(100);
            timer.start();
            idx->query(q, qresult);
            qtimes.push_back(timer.stop());
        }

        total_median_qtime += StatsComp::median(qtimes);
    }

    return queries.empty() ? 0.0 : total_median_qtime / queries.size();
}


void NoiseRunner::write_header(ofstream& csv)
{
    csv << "O_name"             << "\t"
        << "Q_name"             << "\t"
        << "O_idx"              << "\t"
        << "I_idx"              << "\t"
        << "I_schema"           << "\t"
        << "run_idx"            << "\t"
        << "avg_throughput"     << "\t"
        << "avg_throughput_log" << "\n";
}


void NoiseRunner::write_row(ofstream& csv, const string& o_name, const string& q_name, int o_idx, int i_idx, const string& i_schema, int run_idx, double avg_tp, double avg_tp_log)
{
    csv << fixed << setprecision(8)
        << o_name     << "\t"
        << q_name     << "\t"
        << o_idx      << "\t"
        << i_idx      << "\t"
        << i_schema   << "\t"
        << run_idx    << "\t"
        << avg_tp     << "\t"
        << avg_tp_log << "\n";
}


void NoiseRunner::write_summary_header(ofstream& summary_csv)
{
    summary_csv << "O_name"               << "\t"
                << "Q_name"               << "\t"
                << "i_schema"             << "\t"
                << "n"                    << "\t"
                << "min"                  << "\t"
                << "max"                  << "\t"
                << "mean"                 << "\t"
                << "stddev"               << "\t"
                << "coeff_of_variation"   << "\n";
}


void NoiseRunner::write_summary(ofstream& summary_csv, const string& o_name, const string& q_name, const string& i_schema, const vector<double>& tp_log_values)
{
    if (tp_log_values.empty()) return;

    size_t n = tp_log_values.size();
    double mn  = *min_element(tp_log_values.begin(), tp_log_values.end());
    double mx  = *max_element(tp_log_values.begin(), tp_log_values.end());
    double sum = accumulate(tp_log_values.begin(), tp_log_values.end(), 0.0);
    double mean = sum / n;

    double sq_sum = 0;
    for (double v : tp_log_values)
        sq_sum += (v - mean) * (v - mean);
    double stddev = (n > 1) ? sqrt(sq_sum / (n - 1)) : 0.0;
    double cv = (mean != 0) ? stddev / fabs(mean) : 0.0;

    summary_csv << fixed << setprecision(6)
                << o_name   << "\t"
                << q_name   << "\t"
                << i_schema << "\t"
                << n        << "\t"
                << mn       << "\t"
                << mx       << "\t"
                << mean     << "\t"
                << stddev   << "\t"
                << cv       << "\n";

    Log::w(0, "\nNoise Summary [" + o_name + "/" + q_name + "] (" + to_string(n) + " runs, log10 throughput [s/q])");
    Log::w(1, "min",    mn);
    Log::w(1, "max",    mx);
    Log::w(1, "mean",   mean);
    Log::w(1, "stddev", stddev);
    Log::w(1, "CV (coeff. of variation = stddev/|mean|)", cv);
}


void NoiseRunner::write_metasummary_header(ofstream& meta_csv)
{
    meta_csv << "Q_name"            << "\t"
             << "num_combinations"   << "\t"
             << "mean_stddev"        << "\t"
             << "mean_stddev_linear_pct" << "\t"
             << "stddev_stddev"      << "\n";
}


void NoiseRunner::write_metasummary(ofstream& meta_csv, const string& q_name, const vector<double>& stddevs)
{
    if (stddevs.empty()) return;

    size_t n = stddevs.size();
    double sum = accumulate(stddevs.begin(), stddevs.end(), 0.0);
    double mean_stddev = sum / n;

    double sq_sum = 0;
    for (double v : stddevs) sq_sum += (v - mean_stddev) * (v - mean_stddev);
    double stddev_stddev = (n > 1) ? sqrt(sq_sum / (n - 1)) : 0.0;
    double mean_stddev_pct = (pow(10.0, mean_stddev) - 1.0) * 100.0;

    meta_csv << fixed << setprecision(6)
             << q_name          << "\t"
             << n               << "\t"
             << mean_stddev     << "\t"
             << mean_stddev_pct << "\t"
             << stddev_stddev   << "\n";

    Log::w(0, "\nNoise Meta-Summary [" + q_name + "] (" + to_string(n) + " O\u00d7I combinations, log10 throughput [s/q])");
    Log::w(1, "mean_stddev     (noise floor, directly comparable to LCM MAE)", mean_stddev);
    Log::w(1, "mean_stddev_linear_pct (linear throughput variation, +/- %)",  mean_stddev_pct);
    Log::w(1, "stddev_stddev   (consistency of noise across configurations)",  stddev_stddev);
}
