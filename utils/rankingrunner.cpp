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

#include "rankingrunner.h"
#include <algorithm>
#include <numeric>
#include <cmath>
#include <filesystem>
#include <sstream>
#include <iomanip>
#include <chrono>

using namespace std;


RankingRunner::RankingRunner(StatsComp& statscomp, const vector<string>& groups)
    : statscomp(statscomp), groups(groups)
{
}


void RankingRunner::run()
{
    if (!Cfg::get<bool>("synthesis.store-all-evaluations"))
        throw runtime_error("RankingRunner requires synthesis.store-all-evaluations: true");

    int num_O       = Cfg::get<int>("ranking.num-object-collections");
    int num_I       = Cfg::get<int>("ranking.num-indices-per-OQ");
    double min_pct  = Cfg::get<double>("ranking.indices-min-performance-to-top-pct");
    double delta_pct = Cfg::get<double>("ranking.indices-min-performance-delta-to-next-pct");
    int runs_per_q  = Cfg::get<int>("q.runs");

    Log::w(0, "LCM Ranking Check");
    Log::w(1, "Object collections", num_O);
    Log::w(1, "Indices per OQ",     num_I);
    Log::w(1, "Min pct to top",     min_pct);
    Log::w(1, "Min delta pct",      delta_pct);
    Log::w(1, "Runs per query",     runs_per_q);

    string out_dir = Cfg::get_out_dir() + "/ranking";
    filesystem::create_directories(out_dir);

    auto now = chrono::system_clock::now();
    time_t t = chrono::system_clock::to_time_t(now);
    tm tm_buf;
    localtime_r(&t, &tm_buf);
    ostringstream ts;
    ts << put_time(&tm_buf, "%Y%m%d-%H%M%S");
    string base_path    = out_dir + "/ranking-" + ts.str();
    string path_main    = base_path + ".csv";
    string path_summary = base_path + ".summary.csv";
    string path_meta    = base_path + ".metasummary.csv";

    ofstream csv(path_main);
    ofstream scsv(path_summary);
    ofstream mcsv(path_meta);
    if (!csv.is_open() || !scsv.is_open() || !mcsv.is_open())
        throw runtime_error("Cannot open ranking output files under: " + out_dir);

    this->write_header(csv);
    this->write_summary_header(scsv);
    this->write_metasummary_header(mcsv);

    // per Q_name: list of (kendall_tau, top1_correct, score_loss) across all (O, Q) pairs
    map<string, vector<tuple<double, bool, double>>> q_metrics;

    Timer eta_timer;
    eta_timer.start();

    for (int oi = 0; oi < num_O; oi++)
    {
        Log::w(0, progress("Object collection", oi, num_O));

        // Generate O
        this->Ostats = OStatsGen().create(oi);
        this->O      = OGen(this->Ostats).construct_O();
        this->Ostats = this->statscomp.analyze_O(
            this->O, this->Ostats.name + "-ranking");

        // Generate Q
        auto Q_named = QGen(this->O, this->Ostats).construct_Q();
        if (Q_named.empty())
        {
            Log::w(1, "No queries generated, skipping O", oi);
            continue;
        }

        string o_name = this->Ostats.name;

        // Generate I stats (design space) for this O
        auto design_space = IGen(this->groups).compute_design_space();
        vector<iStats> istatss;
        for (const auto& [min_i, max_i] : design_space)
        {
            istatss.push_back(this->statscomp.analyze_i(min_i, "design-space-min"));
            istatss.push_back(this->statscomp.analyze_i(max_i, "design-space-max"));
        }
        string path_I_stats = Persistence::write_I_stats_csv(istatss);

        for (const auto& Qx : Q_named)
        {
            const string& q_name  = get<0>(Qx);
            const auto&   queries = get<1>(Qx);

            Log::w(1, "Workload", q_name);

            // Generate OQ stats (with optional sampling, matching SynthesisRunner)
            this->statscomp.Qstats.clear();
            int sample_size = Cfg::get<int>("synthesis.sample-size");
            vector<RangeIRQuery> sampled;
            if (sample_size >= (int)queries.size())
            {
                sampled = queries;
            }
            else
            {
                vector<size_t> idx(queries.size());
                iota(idx.begin(), idx.end(), 0);
                random_shuffle(idx.begin(), idx.end());
                for (int i = 0; i < sample_size; ++i)
                    sampled.push_back(queries[idx[i]]);
            }
            this->statscomp.analyze_Q(sampled);

            string name    = Persistence::compose_name(this->statscomp.Ostats, Qx);
            string oq_file = Persistence::write_OQ_stats_csv(
                this->statscomp.Ostats, this->statscomp.Qstats, name);

            // Run grid synthesis -> parse all candidates
            auto all_candidates = this->run_grid_synthesis(oq_file, path_I_stats);

            if (all_candidates.empty())
            {
                Log::w(1, "No candidates from grid, skipping workload", q_name);
                continue;
            }

            // Select spread-out subset
            auto selected = this->select_candidates(all_candidates, num_I, min_pct, delta_pct);

            if (selected.size() < 2)
            {
                Log::w(1, "Too few candidates selected, skipping workload", q_name);
                continue;
            }

            // Execute each candidate with actual queries
            Log::w(1, "Candidates (by predicted rank)", to_string(selected.size()));
            {
                double best_log = selected[0].predicted_tp_log;
                ostringstream oss;
                for (const auto& c : selected)
                {
                    double pct = (pow(10.0, best_log) / pow(10.0, c.predicted_tp_log)) * 100.0;
                    oss.str(""); oss.clear();
                    oss << " pred_rank=" << c.predicted_rank << ":";
                    ostringstream val;
                    val << fixed << setprecision(0) << pct << "% " << c.i_schema_json;
                    Log::w(1, oss.str(), val.str());
                }
            }

            for (auto& c : selected)
            {
                Log::w(1, "Executing predicted_rank=" + to_string(c.predicted_rank),
                    c.i_schema_json);
                c.actual_tp_log = this->execute_actual(c, queries);
            }

            // Assign actual ranks (1 = best = lowest s/q)
            vector<size_t> order(selected.size());
            iota(order.begin(), order.end(), 0);
            sort(order.begin(), order.end(), [&](size_t a, size_t b) {
                return selected[a].actual_tp_log < selected[b].actual_tp_log;
            });
            for (size_t rank = 0; rank < order.size(); ++rank)
                selected[order[rank]].actual_rank = (int)(rank + 1);

            Log::w(1, "Candidates (by actual rank)", to_string(selected.size()));
            {
                double best_actual_log = selected[order[0]].actual_tp_log;
                ostringstream oss;
                for (size_t rank = 0; rank < order.size(); ++rank)
                {
                    const auto& c = selected[order[rank]];
                    double pct = (pow(10.0, best_actual_log) / pow(10.0, c.actual_tp_log)) * 100.0;
                    oss.str(""); oss.clear();
                    oss << " actual_rank=" << c.actual_rank
                        << " (pred_rank=" << c.predicted_rank << "):";
                    ostringstream val;
                    val << fixed << setprecision(0) << pct << "% " << c.i_schema_json;
                    Log::w(1, oss.str(), val.str());
                }
            }

            // Compute Kendall's τ
            Log::w(1, "Kendall tau",
                "computing rank correlation over " + to_string(selected.size())
                + " configs: tau = (concordant - discordant) / C(n,2),"
                " where concordant = pairs with same relative order in predicted vs. actual rank");
            vector<int> pred_ranks, act_ranks;
            for (const auto& c : selected)
            {
                pred_ranks.push_back(c.predicted_rank);
                act_ranks.push_back(c.actual_rank);
            }
            double tau = this->kendall_tau(pred_ranks, act_ranks);

            // top1_correct: is the LCM's top-1 prediction the true best?
            bool top1_correct = false;
            for (const auto& c : selected)
                if (c.predicted_rank == 1 && c.actual_rank == 1)
                    top1_correct = true;

            // score_loss: performance penalty from following LCM's top-1
            double true_best_tp_log = selected[order[0]].actual_tp_log;  // min actual_tp_log
            double pred_best_actual_tp_log = 0.0;
            for (const auto& c : selected)
                if (c.predicted_rank == 1)
                    pred_best_actual_tp_log = c.actual_tp_log;
            // Linear: (pred_best_s_per_q - true_best_s_per_q) / true_best_s_per_q
            double score_loss = (pow(10.0, pred_best_actual_tp_log) - pow(10.0, true_best_tp_log))
                                / pow(10.0, true_best_tp_log);

            Log::w(1, "Kendall tau",   tau);
            Log::w(1, "Top-1 correct", top1_correct ? "yes" : "no");
            Log::w(1, "Score loss",    score_loss);

            // Write per-candidate rows
            for (const auto& c : selected)
                this->write_row(csv, o_name, q_name, c);
            csv.flush();

            // Write summary row for this (O, Q) pair
            this->write_summary(scsv, o_name, q_name, selected);
            scsv.flush();

            // Accumulate for metasummary
            q_metrics[q_name].emplace_back(tau, top1_correct, score_loss);
        }

        if (num_O > 1)
        {
            double elapsed   = eta_timer.stop();
            double avg       = elapsed / (oi + 1);
            double remaining = avg * (num_O - oi - 1);
            int rem_min      = (int)(remaining / 60);
            int rem_sec      = (int)remaining % 60;
            Log::w(0, "ETA", to_string(rem_min) + "m " + to_string(rem_sec) + "s");
        }

        // Running aggregates across all accumulated Q metrics so far
        {
            ostringstream oss;
            for (const auto& [qn, metrics] : q_metrics)
            {
                if (metrics.empty()) continue;
                double sum_tau = 0.0;
                int top1_count = 0;
                for (const auto& [t, top1, loss] : metrics)
                {
                    sum_tau += t;
                    if (top1) ++top1_count;
                }
                double mean_tau  = sum_tau / metrics.size();
                double top1_rate = (double)top1_count / metrics.size();
                oss.str(""); oss.clear();
                oss << fixed << setprecision(6)
                    << "mean_kendall_tau=" << mean_tau
                    << "  top1_accuracy_rate=" << setprecision(6) << top1_rate
                    << "  (n=" << metrics.size() << ")";
                Log::w(0, " " + qn, oss.str());
            }
        }
    } // end for (oi)

    // Write metasummary
    for (const auto& [q_name, metrics] : q_metrics)
        this->write_metasummary(mcsv, q_name, metrics);

    csv.close();
    scsv.close();
    mcsv.close();

    Log::w(0, "Ranking CSV",        path_main);
    Log::w(0, "Summary CSV",        path_summary);
    Log::w(0, "Metasummary CSV",    path_meta);
}


vector<RankingCandidate> RankingRunner::run_grid_synthesis(
    const string& oq_file,
    const string& path_I_stats)
{
    // Derive I-synthesis output path (same formula used after the run)
    string isynth_file = oq_file;
    {
        size_t p = isynth_file.find(".OQ.csv");
        if (p != string::npos) isynth_file.replace(p, 7, ".I-synthesis.csv");
        p = isynth_file.find("/OQ/");
        if (p != string::npos) isynth_file.replace(p, 4, "/I-synthesis/");
    }

    // Remove any stale I-synthesis file so the grid run starts clean
    // (synthesisbase.py appends to existing files, which would mix candidates from prior runs)
    if (filesystem::exists(isynth_file))
    {
        filesystem::remove(isynth_file);
        Log::w(2, "Removed stale I-synthesis file", isynth_file);
    }

    // Run grid search via Python
    ProcessExec::python_run("Grid ranking",
        "./synthesis/sy_grid.py \"" + Cfg::config_path
        + "\" \"" + Cfg::get_out_dir()
        + "\" \"" + path_I_stats
        + "\" \"" + oq_file + "\"", 1);

    // Parse candidates
    auto raw = Persistence::read_I_synthesis_csv(isynth_file);

    vector<RankingCandidate> candidates;
    candidates.reserve(raw.size());

    for (const auto& [metadata, encoding] : raw)
    {
        RankingCandidate c;
        c.predicted_tp_log = metadata[5].empty() ? 0.0 : stod(metadata[5]);
        c.schema           = IdxSchemaEncoder::decode(encoding);
        c.i_schema_json    = IdxSchemaSerializer::to_json_line(c.schema);
        candidates.push_back(move(c));
    }

    // Sort ascending (lowest predicted s/q = best = rank 1)
    sort(candidates.begin(), candidates.end(), [](const RankingCandidate& a, const RankingCandidate& b) {
        return a.predicted_tp_log < b.predicted_tp_log;
    });

    return candidates;
}


vector<RankingCandidate> RankingRunner::select_candidates(
    vector<RankingCandidate>& all_candidates,
    int num_to_select,
    double min_pct,
    double delta_pct)
{
    if (all_candidates.empty()) return {};

    // Threshold: keep only configs at most log10(100/min_pct) slower than the best
    double best_log     = all_candidates[0].predicted_tp_log;
    double threshold    = best_log + log10(100.0 / min_pct);

    // Greedy spread: pick configs that are at least delta_pct apart in linear space
    double min_log_step = log10(1.0 + delta_pct / 100.0);

    vector<RankingCandidate*> pool;
    for (auto& c : all_candidates)
        if (c.predicted_tp_log <= threshold)
            pool.push_back(&c);

    vector<RankingCandidate> selected;
    double last_log = -1e30;
    for (auto* cp : pool)
    {
        if (selected.empty() || (cp->predicted_tp_log - last_log) >= min_log_step)
        {
            selected.push_back(*cp);
            last_log = cp->predicted_tp_log;
            if ((int)selected.size() >= num_to_select) break;
        }
    }

    // Assign predicted ranks (1 = best = first in sorted order)
    for (int i = 0; i < (int)selected.size(); ++i)
        selected[i].predicted_rank = i + 1;

    return selected;
}


double RankingRunner::execute_actual(
    const RankingCandidate& c,
    const vector<RangeIRQuery>& queries)
{
    bool use_static    = Cfg::get<bool>("synthesis.use-static-synthdex");
    bool use_optimized = Cfg::get<bool>("synthesis.use-templated-synthdex");
    IRIndex* idx;
    if (use_static && is_static_synthdex_eligible(c.schema))
        idx = new SynthDexStatic(this->O, c.schema, this->Ostats);
    else if (use_optimized)
        idx = new SynthDexOpt(this->O, c.schema, this->Ostats);
    else
        idx = new SynthDex(this->O, c.schema, this->Ostats);

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

    double avg_tp    = (total_q > 0) ? total_median / total_q : 0.0;
    double tp_log    = (avg_tp > 0.0) ? log10(avg_tp) : -10.0;
    double pred_log  = c.predicted_tp_log;
    double disc      = tp_log - pred_log;

    Log::w(1, "Actual throughput [s/q]",
        "10^" + to_string(tp_log)
        + "  (predicted: 10^" + to_string(pred_log)
        + ",  discrepancy: " + to_string(disc) + ")");

    return tp_log;
}


double RankingRunner::kendall_tau(
    const vector<int>& pred_ranks,
    const vector<int>& actual_ranks)
{
    int n = (int)pred_ranks.size();
    if (n < 2) return 1.0;

    // Build: for each item, map predicted_rank -> actual_rank
    // Concordant pair (i,j): same relative order in predicted and actual
    // Discordant pair: opposite order
    int concordant = 0, discordant = 0;

    for (int i = 0; i < n; ++i)
        for (int j = i + 1; j < n; ++j)
        {
            int dp = pred_ranks[i] - pred_ranks[j];
            int da = actual_ranks[i] - actual_ranks[j];
            if ((dp > 0 && da > 0) || (dp < 0 && da < 0))
                ++concordant;
            else if ((dp > 0 && da < 0) || (dp < 0 && da > 0))
                ++discordant;
            // ties: ignored (not counted)
        }

    int pairs = n * (n - 1) / 2;
    return (pairs > 0) ? (double)(concordant - discordant) / pairs : 0.0;
}


void RankingRunner::write_header(ofstream& csv)
{
    csv << "O_name\tQ_name\tpredicted_rank\tactual_rank\ti_schema"
        << "\tpredicted_tp_log\tactual_tp_log\tdisc_tp_log\n";
}


void RankingRunner::write_row(ofstream& csv,
    const string& o_name,
    const string& q_name,
    const RankingCandidate& c)
{
    double disc = c.actual_tp_log - c.predicted_tp_log;
    csv << o_name         << "\t"
        << q_name         << "\t"
        << c.predicted_rank << "\t"
        << c.actual_rank    << "\t"
        << c.i_schema_json  << "\t"
        << c.predicted_tp_log << "\t"
        << c.actual_tp_log    << "\t"
        << disc               << "\n";
}


void RankingRunner::write_summary_header(ofstream& scsv)
{
    scsv << "O_name\tQ_name\tn_configs\tkendall_tau\ttop1_correct\tscore_loss\n";
}


void RankingRunner::write_summary(ofstream& scsv,
    const string& o_name,
    const string& q_name,
    const vector<RankingCandidate>& candidates)
{
    int n = (int)candidates.size();

    vector<int> pred_ranks, act_ranks;
    for (const auto& c : candidates)
    {
        pred_ranks.push_back(c.predicted_rank);
        act_ranks.push_back(c.actual_rank);
    }
    double tau = this->kendall_tau(pred_ranks, act_ranks);

    bool top1_correct = false;
    for (const auto& c : candidates)
        if (c.predicted_rank == 1 && c.actual_rank == 1)
            top1_correct = true;

    // score_loss: (actual s/q of LCM's top-1 - actual s/q of true best) / actual s/q of true best
    double true_best_log = candidates[0].actual_tp_log;
    for (const auto& c : candidates)
        if (c.actual_rank == 1) { true_best_log = c.actual_tp_log; break; }

    double pred_best_actual_log = 0.0;
    for (const auto& c : candidates)
        if (c.predicted_rank == 1) { pred_best_actual_log = c.actual_tp_log; break; }

    double score_loss = (pow(10.0, pred_best_actual_log) - pow(10.0, true_best_log))
                        / pow(10.0, true_best_log);

    scsv << o_name      << "\t"
         << q_name      << "\t"
         << n           << "\t"
         << tau         << "\t"
         << (top1_correct ? 1 : 0) << "\t"
         << score_loss  << "\n";
}


void RankingRunner::write_metasummary_header(ofstream& mcsv)
{
    mcsv << "Q_name\tnum_OQ_combinations\tmean_kendall_tau"
         << "\ttop1_accuracy_rate\tmean_score_loss\n";
}


void RankingRunner::write_metasummary(ofstream& mcsv,
    const string& q_name,
    const vector<tuple<double, bool, double>>& per_oq_metrics)
{
    int n = (int)per_oq_metrics.size();
    if (n == 0) return;

    double sum_tau = 0.0, sum_loss = 0.0;
    int top1_count = 0;
    for (const auto& [tau, top1, loss] : per_oq_metrics)
    {
        sum_tau   += tau;
        sum_loss  += loss;
        if (top1) ++top1_count;
    }

    mcsv << q_name                          << "\t"
         << n                               << "\t"
         << (sum_tau  / n)                  << "\t"
         << ((double)top1_count / n)        << "\t"
         << (sum_loss / n)                  << "\n";
}
