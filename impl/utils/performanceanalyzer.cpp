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

#include "performanceanalyzer.h"
#include <algorithm>
#include <numeric>

PerformanceAnalyzer::PerformanceAnalyzer(
    const IRelation& O,
    const vector<tuple<string,vector<RangeIRQuery>>>& Q,
    const OStats& Ostats)
    : IndexEvaluator(O, Q, Ostats),
      runs_per_q(Cfg::get<int>("q.runs"))
{
    this->statscomp.Ostats = Ostats;
}

void PerformanceAnalyzer::run(IRIndex* idx, const iStats& Istats, const double& construction)
{
    auto Q_sum = accumulate(this->Q.begin(), this->Q.end(), 0,
        [](size_t sum, const tuple<string,vector<RangeIRQuery>>& q)
        { return sum + get<1>(q).size(); });

    int runs_per_q = Cfg::get<int>("q.runs");

    Log::w(1, "Num of q runs",
        to_string(Q_sum) + " * " + to_string(runs_per_q));

    if (Q_sum == 0) return;

    // Check if this index has already been analyzed
    string idx_str = idx->str();
    if (history.find(idx_str) != history.end())
    {
        Log::w(1, "Index already analyzed, skipping.");
        return;
    }
    
    // Add to history
    history.insert(idx_str);

    auto size = idx->getSize();

    for (const auto &Qx : this->Q)
    {
        this->process_query_set(
            idx, Qx, size, construction, Istats);
    }
}


void PerformanceAnalyzer::process_query_set(
    IRIndex *idx,
    const tuple<string, vector<RangeIRQuery>> &Qx,
    size_t size,
    const double &construction,
    const iStats &Istats)
{
    Timer tim;
    RelationId qresult;

    size_t Q_qresult_cnt = 0, Q_qresult_xor = 0;
    size_t qresult_cnt = 0, qresult_xor = 0, qnum = 0;
    double Q_qtime = 0, Q_median_qtime = 0, qtime = 0;

    Log::w(0, "Workload");
    Log::w(1, "O name", this->statscomp.Ostats.name);
    Log::w(1, "Q name", get<0>(Qx));

    const auto &queries = get<1>(Qx);

    for (const auto &q : queries)
    {
        vector<double> qtimes;
        for (int r = 0; r < runs_per_q; r++)
        {
            qresult.clear();
            qresult.reserve(100);
            // qresult.reserve(iR.getFrequency(q.elems[0]));

            tim.start();
            idx->query(q, qresult);
            qtime = tim.stop();

            qtimes.push_back(qtime);
            Q_qtime += qtime;
        }

        qresult_cnt = qresult.size();
        qresult_xor = 0;
        for (const RecordId &rid : qresult) qresult_xor ^= rid;

        Q_qresult_cnt += qresult_cnt;
        Q_qresult_xor += qresult_xor;
        qnum++;

        auto median_qtime = StatsComp::median(qtimes);

        this->statscomp.analyze_q(q);

        this->statscomp.analyze_p(
            qresult_cnt, median_qtime, qresult_xor, size);

        Q_median_qtime += median_qtime;
    }

    // Results correct?
    if (this->result_cnts.find(get<0>(Qx)) != this->result_cnts.end())
    {
        if (Q_qresult_xor != this->result_xors[get<0>(Qx)]
            || Q_qresult_cnt != this->result_cnts[get<0>(Qx)])
            throw runtime_error("Result mismatch, discard all results");
    }
    else
    {
        this->result_cnts[get<0>(Qx)] = Q_qresult_cnt;
        this->result_xors[get<0>(Qx)] = Q_qresult_xor;
    }

    Log::w(2, "Result count", Q_qresult_cnt);
    Log::w(2, "Result XOR", Q_qresult_xor);
    Log::w(2, "Size [bytes]", size);
    
    this->emit_score(Q_qtime, runs_per_q, Q_median_qtime, qnum, Qx, Q_qresult_xor, size, construction, Istats);

    Persistence::write_OQIP_stats_csv(
        this->statscomp.Ostats, this->statscomp.Qstats,
        Istats, this->statscomp.Pstats,
        Cfg::get<bool>("out.detailed"));

    this->statscomp.Qstats.clear();
    this->statscomp.Pstats.clear();
}


void PerformanceAnalyzer::emit_score(
    double qtime, int runs_per_q, double median_qtime, 
    size_t qnum, const tuple<string, vector<RangeIRQuery>> &Qx,
    size_t &total_qresult_xor,
    size_t size,
    const double &construction,
    const iStats &Istats)
{
    Log::w(2, "Total querying time [s]", to_string(qtime));
    Log::w(2, "Throughput [s/q]", "10^" + to_string(log10(median_qtime / qnum)));
    {
        ostringstream oss;
        oss.precision(12);
        oss << fixed << (median_qtime / qnum);
        Log::w(2, "Throughput [s/q]", oss.str());
    }
    Log::w(1, "Throughput [q/s]", (qnum / median_qtime));

    Persistence::write_score_csv(
        this->statscomp.Ostats,
        get<0>(Qx),
        total_qresult_xor,
        size,
        construction,
        qnum / median_qtime,
        Istats);
}
