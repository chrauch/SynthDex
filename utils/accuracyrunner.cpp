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

#include "accuracyrunner.h"
#include <algorithm>
#include <numeric>
#include <cmath>
#include <filesystem>
#include <sstream>
#include <iomanip>
#include <chrono>

using namespace std;


AccuracyRunner::AccuracyRunner(StatsComp& statscomp, const vector<string>& groups)
    : statscomp(statscomp), groups(groups)
{
}


void AccuracyRunner::run()
{
    int num_O = Cfg::get<int>("accuracy.num-object-collections");
    int num_I = Cfg::get<int>("accuracy.num-indices-per-OQ");

    Log::w(0, "LCM Accuracy Check");

    // Parquet-based mode: use pre-recorded test data instead of live execution
    if (Cfg::get<bool>("accuracy.from-parquet"))
    {
        Log::w(0, "Mode", "parquet-based (from output/train/test/)");
        ProcessExec::python_run("Parquet accuracy",
            "./learning/accuracy.py \"" + Cfg::config_path
            + "\" \"" + Cfg::get_out_dir() + "\"", 0);
        return;
    }

    Log::w(1, "Object collections", num_O);
    Log::w(1, "Indices per OQ", num_I);

    // Create output directory
    string out_dir = Cfg::get_out_dir() + "/accuracy";
    filesystem::create_directories(out_dir);

    // Generate timestamped filename
    auto now = chrono::system_clock::now();
    time_t t = chrono::system_clock::to_time_t(now);
    tm tm;
    localtime_r(&t, &tm);
    ostringstream ts;
    ts << put_time(&tm, "%Y%m%d-%H%M%S");
    string out_path = out_dir + "/accuracy-" + ts.str() + ".csv";

    ofstream csv(out_path);
    if (!csv.is_open())
        throw runtime_error("Cannot open accuracy output file: " + out_path);

    this->write_header(csv);

    vector<AccuracyResult> results;
    results.reserve(num_O * num_I);

    Timer eta_timer;
    eta_timer.start();

    for (int oi = 0; oi < num_O; oi++)
    {
        Log::w(0, progress("Object collection", oi, num_O));

        this->generate_O(oi);

        auto Q = this->generate_Q();

        if (Q.empty())
        {
            Log::w(1, "No queries generated, skipping O", oi);
            continue;
        }

        for (int ii = 0; ii < num_I; ii++)
        {
            Log::w(1, progress("Index", ii, num_I));

            auto result = this->evaluate(oi, ii, Q);
            this->write_row(csv, result);
            csv.flush();
            results.push_back(result);
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

    this->write_summary(csv, results);
    csv.close();

    Log::w(0, "Accuracy CSV", out_path);
    Log::w(0, "Total evaluations", results.size());
}


void AccuracyRunner::generate_O(int i)
{
    this->Ostats = OStatsGen().create(i);

    this->O = OGen(this->Ostats).construct_O();

    this->Ostats = this->statscomp.analyze_O(
        this->O, this->Ostats.name + "-accuracy");
}


vector<tuple<string,vector<RangeIRQuery>>> AccuracyRunner::generate_Q()
{
    return QGen(this->O, this->Ostats).construct_Q();
}


AccuracyRunner::AccuracyResult AccuracyRunner::evaluate(
    int o_idx, int i_idx,
    const vector<tuple<string,vector<RangeIRQuery>>>& Q)
{
    // Generate random index schema
    auto idxschema = IGen(this->groups).construct_I(nullopt);
    auto idxschema_json = IdxSchemaSerializer::to_json_line(idxschema);
    auto Istats = this->statscomp.analyze_i(idxschema, "accuracy");

    Log::w(1, "Schema", idxschema_json);

    // Actual execution
    auto [actual_tp_log, actual_sz_log] = this->execute_actual(idxschema, Istats, Q);

    // Predicted execution
    auto [pred_tp_log, pred_sz_log] = this->execute_predicted(idxschema, Istats, Q);

    return AccuracyResult{
        o_idx, i_idx, idxschema_json,
        actual_tp_log, actual_sz_log,
        pred_tp_log, pred_sz_log
    };
}


pair<double,double> AccuracyRunner::execute_actual(
    const IdxSchema& schema,
    const iStats& Istats,
    const vector<tuple<string,vector<RangeIRQuery>>>& Q)
{
    bool emit_training = Cfg::get<bool>("accuracy.emit-training-data");

    // Construct index
    bool use_static    = Cfg::get<bool>("synthesis.use-static-synthdex");
    bool use_optimized = Cfg::get<bool>("synthesis.use-templated-synthdex");
    IRIndex* idx;
    Timer timer;
    timer.start();
    if (use_static && is_static_synthdex_eligible(schema))
        idx = new SynthDexStatic(this->O, schema, this->Ostats);
    else if (use_optimized)
        idx = new SynthDexOpt(this->O, schema, this->Ostats);
    else
        idx = new SynthDex(this->O, schema, this->Ostats);
    double construction_time = timer.stop();

    Log::w(1, "Indexing time [s]", construction_time);

    auto size = idx->getSize();
    double size_log = log10((double)size);

    int runs_per_q = Cfg::get<int>("q.runs");

    // Execute queries across all query sets, aggregate throughput
    double total_median_qtime = 0;
    size_t total_qnum = 0;

    for (const auto& Qx : Q)
    {
        if (emit_training)
        {
            this->statscomp.Qstats.clear();
            this->statscomp.Pstats.clear();
        }

        const auto& queries = get<1>(Qx);

        for (const auto& q : queries)
        {
            RelationId qresult;
            vector<double> qtimes;

            for (int r = 0; r < runs_per_q; r++)
            {
                qresult.clear();
                qresult.reserve(100);

                timer.start();
                idx->query(q, qresult);
                double qtime = timer.stop();
                qtimes.push_back(qtime);
            }

            double median_qtime = StatsComp::median(qtimes);
            total_median_qtime += median_qtime;
            total_qnum++;

            if (emit_training)
            {
                size_t qresult_xor = 0;
                for (const RecordId &rid : qresult) qresult_xor ^= rid;

                this->statscomp.analyze_q(q);
                this->statscomp.analyze_p(
                    qresult.size(), median_qtime, qresult_xor, size);
            }
        }

        if (emit_training)
        {
            Persistence::write_OQIP_stats_csv(
                this->statscomp.Ostats, this->statscomp.Qstats,
                Istats, this->statscomp.Pstats,
                Cfg::get<bool>("out.detailed"));
        }
    }

    delete idx;

    // Average throughput per query (seconds/query) in log scale
    double avg_throughput = (total_qnum > 0)
        ? total_median_qtime / total_qnum : 0;
    double throughput_log = (avg_throughput > 0)
        ? log10(avg_throughput) : -10;

    Log::w(1, "Actual throughput [s/q]", "10^" + to_string(throughput_log));
    Log::w(1, "Actual size [bytes]", size);

    return {throughput_log, size_log};
}


pair<double,double> AccuracyRunner::execute_predicted(
    const IdxSchema& schema,
    const iStats& Istats,
    const vector<tuple<string,vector<RangeIRQuery>>>& Q)
{
    this->statscomp.Qstats.clear();

    vector<string> files_OQI;
    for (const auto& Qx : Q)
    {
        auto name = Persistence::compose_name(this->Ostats, Qx);

        this->statscomp.analyze_Q(get<1>(Qx));

        auto file_OQI = Persistence::write_OQI_stats_csv(
            this->Ostats, this->statscomp.Qstats, Istats, name);

        this->statscomp.Qstats.clear();
        files_OQI.push_back(file_OQI);
    }

    // Build file list for Python
    auto files_OQI_str = accumulate(
        next(files_OQI.begin()), files_OQI.end(),
        "\"" + files_OQI[0] + "\"",
        [](const string& a, const string& b)
        { return a + " \"" + b + "\""; });

    // Call Python prediction
    ProcessExec::python_run("Inference",
        "./learning/prediction.py \"" + Cfg::config_path
        + "\" \"" + Cfg::get_out_dir() + "\" " + files_OQI_str, 1);

    // Parse predictions from output file(s)
    // prediction.py writes to: output/stats/OQIP-predictions/{input_name}P-predictions.csv
    double total_pred_tp = 0;
    double total_pred_sz = 0;
    size_t total_queries = 0;

    for (const auto& Qx : Q)
        total_queries += get<1>(Qx).size();

    for (const auto& file_OQI : files_OQI)
    {
        // Derive prediction output filename from OQI input filename
        string input_name = filesystem::path(file_OQI).stem().string();
        string pred_file = Cfg::get_out_dir()
            + "/stats/OQIP-predictions/" + input_name + "P-predictions.csv";

        auto [tp_sum, sz_sum] = this->parse_predictions(pred_file);

        total_pred_tp += tp_sum;
        total_pred_sz += sz_sum;
    }

    // Average predictions in log space: predictions are per-query log10 values
    // We average the linear values then take log10
    double avg_pred_tp = (total_queries > 0)
        ? total_pred_tp / total_queries : 0;
    double avg_pred_sz = (total_queries > 0)
        ? total_pred_sz / total_queries : 0;

    double pred_tp_log = (avg_pred_tp > 0) ? log10(avg_pred_tp) : -10;
    double pred_sz_log = (avg_pred_sz > 0) ? log10(avg_pred_sz) : -10;

    Log::w(1, "Predicted throughput [s/q]", "10^" + to_string(pred_tp_log));
    Log::w(1, "Predicted size [bytes]", "10^" + to_string(pred_sz_log));

    return {pred_tp_log, pred_sz_log};
}


pair<double,double> AccuracyRunner::parse_predictions(const string& prediction_file)
{
    ifstream fs(prediction_file);
    if (!fs.is_open())
        throw runtime_error("Cannot open prediction file: " + prediction_file);

    string header_line;
    getline(fs, header_line);

    // Find column indices for predicted targets
    vector<string> headers;
    {
        istringstream iss(header_line);
        string col;
        while (getline(iss, col, '\t'))
            headers.push_back(col);
    }

    int tp_col = -1, sz_col = -1;
    for (int i = 0; i < (int)headers.size(); i++)
    {
        if (headers[i] == "predicted_p_throughput_log") tp_col = i;
        if (headers[i] == "predicted_p_size_log") sz_col = i;
    }

    if (tp_col < 0 || sz_col < 0)
        throw runtime_error("Missing prediction columns in: " + prediction_file);

    // Sum linear values (10^log) across all queries
    double tp_linear_sum = 0;
    double sz_linear_sum = 0;

    string line;
    while (getline(fs, line))
    {
        if (line.empty()) continue;
        vector<string> cols;
        {
            istringstream iss(line);
            string col;
            while (getline(iss, col, '\t'))
                cols.push_back(col);
        }

        if (tp_col < (int)cols.size() && sz_col < (int)cols.size())
        {
            tp_linear_sum += pow(10.0, stod(cols[tp_col]));
            sz_linear_sum += pow(10.0, stod(cols[sz_col]));
        }
    }

    return {tp_linear_sum, sz_linear_sum};
}


void AccuracyRunner::write_header(ofstream& csv)
{
    csv << "o_idx" << "\t"
        << "i_idx" << "\t"
        << "i_schema" << "\t"
        << "actual_throughput_log" << "\t"
        << "predicted_throughput_log" << "\t"
        << "disc_throughput_log" << "\t"
        << "actual_throughput" << "\t"
        << "predicted_throughput" << "\t"
        << "disc_throughput_rel" << "\t"
        << "actual_size_log" << "\t"
        << "predicted_size_log" << "\t"
        << "disc_size_log" << "\t"
        << "actual_size" << "\t"
        << "predicted_size" << "\t"
        << "disc_size_rel"
        << "\n";
}


void AccuracyRunner::write_row(ofstream& csv, const AccuracyResult& r)
{
    double actual_tp = pow(10.0, r.actual_throughput_log);
    double actual_sz = pow(10.0, r.actual_size_log);
    double pred_tp = pow(10.0, r.predicted_throughput_log);
    double pred_sz = pow(10.0, r.predicted_size_log);

    double disc_tp_log = r.predicted_throughput_log - r.actual_throughput_log;
    double disc_sz_log = r.predicted_size_log - r.actual_size_log;
    double disc_tp_rel = (actual_tp > 0) ? abs(pred_tp - actual_tp) / actual_tp : 0;
    double disc_sz_rel = (actual_sz > 0) ? abs(pred_sz - actual_sz) / actual_sz : 0;

    csv << fixed << setprecision(8)
        << r.o_idx << "\t"
        << r.i_idx << "\t"
        << r.i_schema << "\t"
        << r.actual_throughput_log << "\t"
        << r.predicted_throughput_log << "\t"
        << disc_tp_log << "\t"
        << actual_tp << "\t"
        << pred_tp << "\t"
        << disc_tp_rel << "\t"
        << r.actual_size_log << "\t"
        << r.predicted_size_log << "\t"
        << disc_sz_log << "\t"
        << actual_sz << "\t"
        << pred_sz << "\t"
        << disc_sz_rel
        << "\n";
}


void AccuracyRunner::write_summary(ofstream& csv, const vector<AccuracyResult>& results)
{
    if (results.empty()) return;

    double sum_abs_tp_log = 0, sum_abs_sz_log = 0;
    double sum_rel_tp = 0, sum_rel_sz = 0;
    size_t n = results.size();

    for (const auto& r : results)
    {
        double actual_tp = pow(10.0, r.actual_throughput_log);
        double actual_sz = pow(10.0, r.actual_size_log);
        double pred_tp = pow(10.0, r.predicted_throughput_log);
        double pred_sz = pow(10.0, r.predicted_size_log);

        sum_abs_tp_log += abs(r.predicted_throughput_log - r.actual_throughput_log);
        sum_abs_sz_log += abs(r.predicted_size_log - r.actual_size_log);
        sum_rel_tp += (actual_tp > 0) ? abs(pred_tp - actual_tp) / actual_tp : 0;
        sum_rel_sz += (actual_sz > 0) ? abs(pred_sz - actual_sz) / actual_sz : 0;
    }

    double mae_tp_log = sum_abs_tp_log / n;
    double mae_sz_log = sum_abs_sz_log / n;
    double mre_tp = sum_rel_tp / n;
    double mre_sz = sum_rel_sz / n;

    // Write summary as comment lines at end of CSV
    csv << "\n"
        << "# Summary (" << n << " evaluations)\n"
        << "# MAE throughput (log):  " << fixed << setprecision(6) << mae_tp_log << "\n"
        << "# MAE size (log):        " << mae_sz_log << "\n"
        << "# MRE throughput:        " << mre_tp << "\n"
        << "# MRE size:              " << mre_sz << "\n";

    // Also print to stdout
    Log::w(0, "\nAccuracy Summary (" + to_string(n) + " evaluations)");
    Log::w(0, "MAE = Mean Absolute Error in log10 space: 1/n * sum(|predicted_log - actual_log|)");
    Log::w(1, "MAE throughput (log)", mae_tp_log);
    Log::w(1, "MAE size (log)", mae_sz_log);
    Log::w(0, "MRE = Mean Relative Error in linear space: 1/n * sum(|10^pred - 10^actual| / 10^actual)");
    Log::w(1, "MRE throughput", mre_tp);
    Log::w(1, "MRE size", mre_sz);
}
