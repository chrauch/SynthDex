#include "comparisonanalyzer.h"
#include "../utils/cfg.h"
#include "../utils/global.h"
#include "../utils/logging.h"
#include "../learning/statscomp.h"
#include <numeric>
#include <stdexcept>
#include <utility>

ComparisonAnalyzer::ComparisonAnalyzer(
    const ObjectDomain& domain,
    const tuple<string,vector<RangeIRQuery>>& Q,
    const OStats& Ostats,
        ComparisonMeasurements& measurements,
        const string& category)
    : IndexEvaluator(domain, Q, Ostats),
            measurements(measurements),
            category(category)
{
}

void ComparisonAnalyzer::run(
    IRIndex* idx, const iStats&, const double& construction)
{
    const auto& queries = get<1>(this->Q);
    const int runs_per_q = Cfg::get<int>("q.runs");
    const size_t query_count = queries.size();
    ComparisonMeasurement measurement;
    measurement.category = this->category;
    measurement.query_count = query_count;
    measurement.construction_s = construction;
    measurement.size_mb = idx->getSize() / (1024.0 * 1024.0);
    measurement.median_query_times.reserve(queries.size());
    size_t result_count = 0;
    size_t result_xor = 0;
    double total_query_time = 0.0;

    Log::w(0, "Workload");
    Log::w(1, "O name", this->Ostats.name);
    Log::w(1, "Q name", get<0>(this->Q));
    Log::w(1, "Num of q runs",
        to_string(query_count) + " * " + to_string(runs_per_q));

    for (const auto& q : queries)
    {
        auto clamped_opt = clamp_to_domain(q, this->domain);
        if (!clamped_opt)
        {
            Log::w(1, "Skipped q (out of domain)", q.str(false));
            measurement.median_query_times.push_back(0.0);
            continue;
        }

        vector<double> query_times;
        query_times.reserve(runs_per_q);
        RelationId qresult;
        for (int run_idx = 0; run_idx < runs_per_q; ++run_idx)
        {
            qresult.clear();
            qresult.reserve(100);
            Timer timer;
            timer.start();
            idx->query(*clamped_opt, qresult);
            const double query_time = timer.stop();
            query_times.push_back(query_time);
            total_query_time += query_time;
        }

        result_count += qresult.size();
        for (const RecordId& rid : qresult) result_xor ^= rid;

        measurement.median_query_times.push_back(
            StatsComp::median(query_times));
    }

    const double median_query_time = accumulate(
        measurement.median_query_times.begin(),
        measurement.median_query_times.end(), 0.0);
    const double seconds_per_query = query_count > 0
        ? median_query_time / query_count : 0.0;
    const double throughput = median_query_time > 0.0
        ? query_count / median_query_time : 0.0;

    Log::w(2, "Result count", result_count);
    Log::w(2, "Result XOR", result_xor);
    Log::w(2, "Total querying time [s]", total_query_time);
    Log::w(2, "Throughput [s/q]", seconds_per_query);
    Log::w(1, "Throughput [q/s]", throughput);

    const string workload_name = get<0>(this->Q);
    auto existing_count = this->result_counts.find(workload_name);
    if (existing_count != this->result_counts.end())
    {
        if (existing_count->second != result_count
            || this->result_xors.at(workload_name) != result_xor)
            throw runtime_error("Result mismatch, discard all results");
    }
    else
    {
        this->result_counts[workload_name] = result_count;
        this->result_xors[workload_name] = result_xor;
    }

    this->measurements[workload_name].push_back(move(measurement));
}