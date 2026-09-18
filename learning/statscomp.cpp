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

#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <iostream>
#include <map>
#include <iomanip>
#include <omp.h>
#include "../utils/global.h"
#include "statscomp.h"
#include "statscomptemp.h"
#include "statsserializer.h"
#include "../structure/idxschemaencoder.h"
#include "../structure/idxschemaserializer.h"
#include <unordered_set>

using namespace std;


StatsComp::StatsComp()
    : element_bins(Cfg::get<int>("o.elem.bins")),
      extent_bins(Cfg::get<int>("o.ext.bins")),
      q_elements(Cfg::get<int>("q.elem.max-cnt")),
      dev_clamp(Cfg::get<double>("o.ext.elem-dev-clamp")),
      num_threads(Cfg::get_threads())
{
    if (element_bins <= 0 || extent_bins <= 0 || q_elements <= 0)
        throw invalid_argument("Bins and elements must be positive integers.");
}


OStats StatsComp::analyze_O(const IRelation &O, const string &name)
{
    Timer tim;
    tim.start();

    this->Ostats = OStats();

    // Extract filename without path and remove .dat extension
    size_t last_separator = name.find_last_of("/\\");
    string clean = (last_separator == string::npos) 
        ? name : name.substr(last_separator + 1);
    size_t dot_pos = clean.rfind(".dat");
    if (dot_pos != string::npos && dot_pos == clean.length() - 4)
        clean = clean.substr(0, dot_pos);
    
    this->Ostats.name = clean;
    this->Ostats.card = O.size();
    this->Ostats.card_log = log10(this->Ostats.card);

    this->compute_extents(O);
    this->compute_elements(O);
    this->estimate_bytes(this->Ostats);
    if (Cfg::get<bool>("o.gen.calculate-memory"))
        this->compute_bytes(O);

    StatsComp::fill(this->Ostats);

    double time = tim.stop();
    Log::w(1, "Analysis [s]", time);

    Log::w(2, "O statistics (short)",
        StatsSerializer::to_json(this->Ostats, false));

    return this->Ostats;
}


OStats StatsComp::analyze_O(
    const IRelation &O,
    const string &name,
    const vector<RangeIRQuery> &Q)
{
    // Run the full O-analysis first (no slice clustering).
    OStats stats = this->analyze_O(O, name);

    // Compute temporal slice stats (O+Q) for logging purposes.
    {
        StatsCompTemp sct(O, this->Ostats);
        auto slices = sct.compute_slices();         // O-side stats
        slices = sct.compute_slices(slices, Q);     // overlay Q-side stats
    }

    return stats;
}


vector<tuple<SliceCluster, IRelation, vector<RangeIRQuery>, OStats>> StatsComp::analyze_Osliced(
    const IRelation &O,
    const string &name,
    const vector<RangeIRQuery> &Q,
    int band_count,
    bool auto_k)
{
    auto [full_stats, slices] = this->compute_temporal_slices(O, name, Q);
    return this->cluster_and_partition(O, name, Q, slices, full_stats, band_count, auto_k);
}


tuple<OStats, vector<OSliceStats>> StatsComp::compute_temporal_slices(
    const IRelation &O,
    const string &name,
    const vector<RangeIRQuery> &Q)
{
    Timer tim;
    tim.start();

    OStats full_stats = this->analyze_O(O, name);

    StatsCompTemp sct(O, full_stats);
    auto slices = sct.compute_slices();
    slices = sct.compute_slices(slices, Q);

    double time = tim.stop();
    Log::w(1, "Compute temporal slices [s]", time);

    return {full_stats, slices};
}


vector<tuple<SliceCluster, IRelation, vector<RangeIRQuery>, OStats>> StatsComp::cluster_and_partition(
    const IRelation &O,
    const string &name,
    const vector<RangeIRQuery> &Q,
    const vector<OSliceStats> &slices,
    const OStats &full_stats,
    int band_count,
    bool auto_k)
{
    Timer tim;
    tim.start();

    StatsCompTemp sct(O, full_stats);

    vector<SliceCluster> clusters;
    if (band_count > 0)
    {
        clusters = sct.cluster_slices(slices, band_count, auto_k);
    }
    else
    {
        // No clustering: one cluster per slice (analysis-only mode).
        for (size_t i = 0; i < slices.size(); ++i)
        {
            SliceCluster cl;
            cl.cluster_id      = static_cast<int>(i);
            cl.temporal_start   = slices[i].s_start;
            cl.temporal_end     = slices[i].s_end;
            cl.centroid_count        = static_cast<double>(slices[i].count);
            cl.centroid_avg_elems    = slices[i].avg_elems;
            cl.query_count           = slices[i].query_count;
            cl.centroid_q_length     = slices[i].avg_q_length;
            cl.centroid_q_elem_count = slices[i].avg_q_elem_count;
            cl.centroid_q_elem0_rank = slices[i].avg_q_elem0_rank;
            cl.member_indices.push_back(slices[i].index);
            clusters.push_back(cl);
        }
    }

    vector<tuple<SliceCluster, IRelation, vector<RangeIRQuery>, OStats>> result;
    result.reserve(clusters.size());

    for (size_t c = 0; c < clusters.size(); ++c)
    {
        const SliceCluster &cl = clusters[c];
        Timestamp cl_start = static_cast<Timestamp>(cl.temporal_start);
        Timestamp cl_end   = static_cast<Timestamp>(cl.temporal_end);

        // Build partitioned relation: only records overlapping [cl_start, cl_end).
        IRelation Ox;
        for (const auto &rec : O)
        {
            if (rec.end <= cl_start || rec.start >= cl_end)
                continue;

            IRecord local(rec.id);
            local.start    = max(rec.start, cl_start) - cl_start;
            local.end      = min(rec.end, cl_end) - cl_start;
            local.elements = rec.elements;
            Ox.push_back(local);
        }
        Ox.determineDomain();

        // Build partitioned queries: only queries overlapping [cl_start, cl_end).
        vector<RangeIRQuery> Qx;
        for (const auto &q : Q)
        {
            if (q.end <= cl_start || q.start >= cl_end)
                continue;

            RangeIRQuery local(q.id,
                max(q.start, cl_start) - cl_start,
                min(q.end,   cl_end)   - cl_start);
            local.elems = q.elems;
            Qx.push_back(local);
        }

        // Compute OStats for the partition.
        string part_name = name + ".slice" + to_string(c) + "[" + to_string(cl_start) + "-" + to_string(cl_end) + ")";
        OStats part_stats = this->analyze_O(Ox, part_name);

        result.emplace_back(cl, move(Ox), move(Qx), part_stats);
    }

    double time = tim.stop();
    Log::w(1, "Cluster and partition [s]", time);
    Log::w(1, "  partitions", result.size());

    return result;
}


vector<tuple<SliceCluster, IRelation, vector<RangeIRQuery>, OStats>> StatsComp::partition_by_ranges(
    const IRelation &O,
    const string &name,
    const vector<RangeIRQuery> &Q,
    const vector<string> &ranges)
{
    if (ranges.empty())
        throw invalid_argument("Static temporal ranges must not be empty.");

    const double domain = static_cast<double>(this->Ostats.domain);
    if (domain <= 0.0)
        throw invalid_argument("Static temporal ranges require a positive temporal domain.");

    vector<pair<double, double>> relative_ranges;
    relative_ranges.reserve(ranges.size());

    for (size_t i = 0; i < ranges.size(); ++i)
    {
        const auto &range = ranges[i];
        const size_t dash = range.find('-');
        if (dash == string::npos || dash == 0 || dash + 1 >= range.size())
            throw invalid_argument(
                "Invalid static temporal range '" + range + "' at position "
                + to_string(i) + ". Expected 'start-end'.");

        size_t start_chars = 0;
        size_t end_chars = 0;
        double start = 0.0;
        double end = 0.0;
        try
        {
            start = stod(range.substr(0, dash), &start_chars);
            end = stod(range.substr(dash + 1), &end_chars);
        }
        catch (const exception &)
        {
            throw invalid_argument(
                "Invalid static temporal range '" + range + "' at position "
                + to_string(i) + ". Expected numeric boundaries.");
        }

        if (start_chars != dash || end_chars != range.size() - dash - 1
            || !isfinite(start) || !isfinite(end))
            throw invalid_argument(
                "Invalid static temporal range '" + range + "' at position "
                + to_string(i) + ". Expected numeric boundaries.");

        relative_ranges.emplace_back(start, end);
    }

    constexpr double epsilon = 1e-9;
    if (abs(relative_ranges.front().first) > epsilon
        || abs(relative_ranges.back().second - 1.0) > epsilon)
        throw invalid_argument(
            "Static temporal ranges must cover the complete relative domain [0,1].");

    for (size_t i = 0; i < relative_ranges.size(); ++i)
    {
        const auto [start, end] = relative_ranges[i];
        if (start < -epsilon || end > 1.0 + epsilon || end - start <= epsilon)
            throw invalid_argument(
                "Static temporal ranges must be non-empty and lie in [0,1].");
        if (i > 0 && abs(start - relative_ranges[i - 1].second) > epsilon)
            throw invalid_argument(
                "Static temporal ranges must be sorted and contiguous.");
    }

    const Timestamp domain_timestamp = static_cast<Timestamp>(domain);
    vector<Timestamp> integer_boundaries;
    integer_boundaries.reserve(relative_ranges.size() + 1);
    integer_boundaries.push_back(0);

    for (size_t i = 0; i < relative_ranges.size(); ++i)
    {
        const Timestamp start = integer_boundaries.back();
        const Timestamp configured_start = static_cast<Timestamp>(
            llround(relative_ranges[i].first * domain));
        const Timestamp end = (i + 1 == relative_ranges.size())
            ? domain_timestamp
            : static_cast<Timestamp>(
                llround(relative_ranges[i].second * domain));

        if (configured_start != start)
            throw invalid_argument(
                "Static temporal range boundary does not remain contiguous "
                "after conversion to timestamps at position " + to_string(i) + ".");
        if (end <= start)
            throw invalid_argument(
                "Static temporal ranges collapse after conversion to timestamp boundaries.");
        integer_boundaries.push_back(end);
    }

    vector<tuple<SliceCluster, IRelation, vector<RangeIRQuery>, OStats>> result;
    result.reserve(relative_ranges.size());

    for (size_t i = 0; i < relative_ranges.size(); ++i)
    {
        const Timestamp start = integer_boundaries[i];
        const Timestamp end = integer_boundaries[i + 1];
        SliceCluster cluster;
        cluster.cluster_id = static_cast<int>(i);
        cluster.temporal_start = start;
        cluster.temporal_end = end;
        cluster.member_indices.push_back(static_cast<int>(i));

        IRelation Ox;
        for (const auto &rec : O)
        {
            if (rec.end <= start || rec.start >= end)
                continue;

            IRecord local(rec.id);
            local.start = max(rec.start, start) - start;
            local.end = min(rec.end, end) - start;
            local.elements = rec.elements;
            Ox.push_back(local);
        }
        Ox.determineDomain();

        vector<RangeIRQuery> Qx;
        for (const auto &q : Q)
        {
            if (q.end <= start || q.start >= end)
                continue;

            RangeIRQuery local(q.id,
                max(q.start, start) - start,
                min(q.end, end) - start);
            local.elems = q.elems;
            Qx.push_back(local);
        }

        if (Ox.empty())
        {
            if (!Qx.empty())
                throw runtime_error(
                    "Static temporal range [" + to_string(start) + "-"
                    + to_string(end) + ") contains queries but no objects.");

            result.emplace_back(cluster, move(Ox), move(Qx), OStats{});
            continue;
        }

        string part_name = name + ".slice" + to_string(i)
            + "[" + to_string(start) + "-" + to_string(end) + ")";
        OStats part_stats = this->analyze_O(Ox, part_name);
        result.emplace_back(cluster, move(Ox), move(Qx), part_stats);
    }

    Log::w(1, "Partition static temporal ranges",
        to_string(result.size()) + " partitions");
    return result;
}


void StatsComp::analyze_Q(const vector<RangeIRQuery> &Q)
{
    this->Qstats.clear();

    // Only parallelize for large query sets to avoid overhead
    if (Q.size() > 100 && num_threads > 1)
    {
        omp_set_num_threads(this->num_threads);
        
        // Pre-allocate results vector
        this->Qstats.resize(Q.size());
        
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < Q.size(); ++i)
        {
            auto qs = qStats();
            const auto &q = Q[i];
            
            qs.query = q.str();

            auto dom = static_cast<double>(this->Ostats.domain);
            qs.start_rel      = q.start / dom;
            qs.end_rel        = q.end / dom;
            qs.extent_rel_log = log10((q.end - q.start) / dom);
            qs.elem_count     = q.elems.size();

            auto rank = [&](int j) 
                { return log10(q.elems[j]+1) / this->Ostats.dict_log; };

            for (size_t j = 0; j < min(q.elems.size(), StatsComp::q_elements); j++)
                qs.element_rank_logs.push_back(rank(j));
            
            // Fill remaining slots with -1
            for (size_t j = q.elems.size(); j < StatsComp::q_elements; j++)
                qs.element_rank_logs.push_back(-1);

            this->Qstats[i] = qs;
        }
    }
    else
    {
        // Sequential processing for small query sets
        for (const auto &q : Q) 
            this->analyze_q(q);
    }
}


void StatsComp::analyze_q(const RangeIRQuery &q)
{
    auto qs = qStats();
    qs.query = q.str();

    auto dom = static_cast<double>(this->Ostats.domain);
    qs.start_rel      = q.start / dom;
    qs.end_rel        = q.end / dom;
    qs.extent_rel_log = log10((q.end - q.start) / dom);
    qs.elem_count     = q.elems.size();

    auto rank = [&](int i) 
        { return log10(q.elems[i]+1) / this->Ostats.dict_log; };

    for (size_t i = 0; i < min(q.elems.size(), StatsComp::q_elements);i++)
        qs.element_rank_logs.push_back(rank(i));
    
    // Fill remaining slots with -1
    for (size_t i = q.elems.size(); i < StatsComp::q_elements; i++)
        qs.element_rank_logs.push_back(-1);

    this->Qstats.push_back(qs);
}


void StatsComp::analyze_p(
    const size_t &results,
    const double &querytime,
    const size_t &result_xor,
    const size_t &size)
{
    auto ps = pStats();

    ps.throughput = querytime;
    ps.throughput_log = log10(ps.throughput);
    ps.size = size;
    ps.size_log = log10(ps.size);
    ps.result_xor = result_xor;
    ps.result_cnt = results;
    ps.selectivity_ratio_log 
        = log10(results / static_cast<double>(this->Ostats.card) * 100);

    this->Pstats.push_back(ps);
}


iStats StatsComp::analyze_i(const IdxSchema &schema, const string &category)
{
    this->Istats.category = category;
    this->Istats.params = IdxSchemaSerializer::to_json_line(schema);
    this->Istats.encoding = IdxSchemaEncoder::encode(schema);

    return this->Istats;
}


double StatsComp::median(vector<double> &values)
{
    sort(values.begin(), values.end());
    double med = values.size() % 2 == 0
        ? (values[values.size() / 2 - 1] + values[values.size() / 2]) / 2.0
        : values[values.size() / 2];

    return med;
}


void StatsComp::fill(OStats &stats)
{
    stats.card = pow(10, stats.card_log);
    stats.domain = pow(10, stats.domain_log);
    stats.dict = pow(10, stats.dict_log);

    for (auto &b : stats.extents)
    {
        b.avg_elem_cnt = pow(10, b.avg_elem_cnt_log);
        b.dev_elem_cnt = pow(10, b.dev_elem_cnt_log);
        b.min_len = pow(10, b.min_len_log);
        b.max_len = pow(10, b.max_len_log);
    }

    for (auto &b : stats.elements)
    {
        b.lo_rank = static_cast<ElementId>(pow(10, b.lo_rank_log));
        b.hi_rank = static_cast<ElementId>(pow(10, b.hi_rank_log));
        b.max_freq = pow(10, b.max_freq_log);
        b.dict_ratio = pow(10, b.dict_ratio_log);
        b.max_post_ratio = pow(10, b.max_post_ratio_log);
        b.rel_drop = pow(10, b.rel_drop_log);
    }
}


void StatsComp::compute_extents(const IRelation &O)
{
    auto end_max = 0;
    vector<const IRecord*> records;
    for (const auto& record : O)
    {
        records.push_back(&record);
        end_max = max(end_max, record.end);
    }

    this->Ostats.domain = end_max;
    this->Ostats.domain_log = log10(this->Ostats.domain);

    // Primary sorting by extent, secondary by number of elements
    sort(records.begin(), records.end(),
        [](const IRecord* a, const IRecord* b)
        {
            int a_extent = a->end - a->start;
            int b_extent = b->end - b->start;
            
            if (a_extent == b_extent)
                return a->elements.size() < b->elements.size();
            
            return a_extent < b_extent;
        });

    size_t total_records = records.size();
    size_t bin_size = total_records / StatsComp::extent_bins;
    size_t remainder = total_records % StatsComp::extent_bins;
    
    // Pre-calculate start indices to avoid recalculation in parallel loop
    vector<size_t> start_indices(StatsComp::extent_bins);
    start_indices[0] = 0;
    for (int i = 1; i < StatsComp::extent_bins; ++i)
    {
        start_indices[i] = start_indices[i-1] + bin_size 
            + ((i-1) < remainder ? 1 : 0);
    }
    
    // Pre-allocate results vector
    this->Ostats.extents.resize(StatsComp::extent_bins);

    // Only parallelize if we have enough work and threads
    if (total_records > 10000 && num_threads > 1)
    {
        omp_set_num_threads(num_threads);
        
        #pragma omp parallel for schedule(static)
        for (int i = 0; i < StatsComp::extent_bins; ++i)
        {
            size_t start_idx = start_indices[i];
            size_t current_bin_size = bin_size + (i < remainder ? 1 : 0);
            size_t end_idx = start_idx + current_bin_size;

            OExtStats ext_stats;

            if (current_bin_size > 0)
            {
                double min_len = numeric_limits<double>::max();
                double max_len = numeric_limits<double>::lowest();
                
                vector<double> elem_counts;
                vector<double> log_elem_counts;
                elem_counts.reserve(current_bin_size);
                log_elem_counts.reserve(current_bin_size);
                
                for (size_t j = start_idx; j < end_idx; ++j)
                {
                    const IRecord* rec = records[j];
                    double len = rec->end - rec->start;

                    min_len = min(min_len, len);
                    max_len = max(max_len, len);
                    
                    double count = rec->elements.size();
                    elem_counts.push_back(count);
                    log_elem_counts.push_back(log(count));
                }

                ext_stats.min_len_log = log10(min_len);
                ext_stats.max_len_log = log10(max_len);
                
                // Calculate arithmetic mean for element count
                double sum = 0.0;
                for (const auto& count : elem_counts) sum += count;
                double avg = sum / elem_counts.size();
                ext_stats.avg_elem_cnt = avg;
                ext_stats.avg_elem_cnt_log = log10(avg);
                
                // Calculate log-normal standard deviation
                double log_sum = 0.0;
                for (const auto& log_count : log_elem_counts) log_sum += log_count;
                double log_mean = log_sum / log_elem_counts.size();
                
                double log_sq_sum = 0.0;
                for (const auto& log_count : log_elem_counts)
                    log_sq_sum += (log_count - log_mean) * (log_count - log_mean);

                double log_std_dev = sqrt(log_sq_sum / log_elem_counts.size());
                
                // Convert log-space std dev to normal space
                double std_dev = avg * sqrt(exp(log_std_dev * log_std_dev) - 1.0);
                
                ext_stats.dev_elem_cnt = std_dev < this->dev_clamp
                    ? this->dev_clamp : std_dev;
                ext_stats.dev_elem_cnt_log = log10(ext_stats.dev_elem_cnt);
            }

            this->Ostats.extents[i] = ext_stats;
        }
    }
    else
    {
        // Sequential processing for small datasets
        for (int i = 0; i < StatsComp::extent_bins; ++i)
        {
            size_t start_idx = start_indices[i];
            size_t current_bin_size = bin_size + (i < remainder ? 1 : 0);
            size_t end_idx = start_idx + current_bin_size;

            OExtStats ext_stats;

            if (current_bin_size > 0)
            {
                double min_len = numeric_limits<double>::max();
                double max_len = numeric_limits<double>::lowest();
                
                vector<double> elem_counts;
                vector<double> log_elem_counts;
                
                for (size_t j = start_idx; j < end_idx; ++j)
                {
                    const IRecord* rec = records[j];
                    double len = rec->end - rec->start;

                    min_len = min(min_len, len);
                    max_len = max(max_len, len);
                    
                    double count = rec->elements.size();
                    elem_counts.push_back(count);
                    log_elem_counts.push_back(log(count));
                }

                ext_stats.min_len_log = log10(min_len);
                ext_stats.max_len_log = log10(max_len);
                
                // Calculate arithmetic mean for element count
                double sum = 0.0;
                for (const auto& count : elem_counts) sum += count;
                double avg = sum / elem_counts.size();
                ext_stats.avg_elem_cnt = avg;
                ext_stats.avg_elem_cnt_log = log10(avg);
                
                // Calculate log-normal standard deviation
                double log_sum = 0.0;
                for (const auto& log_count : log_elem_counts)
                    log_sum += log_count;
                double log_mean = log_sum / log_elem_counts.size();
                
                double log_sq_sum = 0.0;
                for (const auto& log_count : log_elem_counts)
                    log_sq_sum += (log_count - log_mean) * (log_count - log_mean);

                double log_std_dev = sqrt(log_sq_sum / log_elem_counts.size());
                
                // Convert log-space std dev to normal space
                double std_dev =
                    avg * sqrt(exp(log_std_dev * log_std_dev) - 1.0);
                
                ext_stats.dev_elem_cnt = std_dev < this->dev_clamp
                    ? this->dev_clamp : std_dev;
                ext_stats.dev_elem_cnt_log = log10(ext_stats.dev_elem_cnt);
            }

            this->Ostats.extents[i] = ext_stats;
        }
    }
}


void StatsComp::compute_elements(const IRelation &O)
{
    map<ElementId, size_t> elems_map;

    // Only parallelize for large datasets to avoid overhead
    if (O.size() > 50000 && num_threads > 1)
    {
        omp_set_num_threads(num_threads);
        
        // Use a private map for each thread to avoid contention
        #pragma omp parallel
        {
            map<ElementId, size_t> local_map;
            
            #pragma omp for schedule(static)
            for (size_t i = 0; i < O.size(); ++i)
            {
                const auto &record = O[i];
                for (const auto &elem : record.elements)
                    local_map[elem]++;
            }
            
            // Merge local maps into global map
            // (only one critical section per thread)
            #pragma omp critical
            {
                for (const auto &pair : local_map)
                    elems_map[pair.first] += pair.second;
            }
        }
    }
    else
    {
        // Sequential processing for small datasets
        for (const auto &record : O)
            for (const auto &elem : record.elements)
                elems_map[elem]++;
    }

    ElementId max_e = 0;
    for (const auto &pair : elems_map) max_e = max(max_e, pair.first);

    this->Ostats.dict = max_e;
    this->Ostats.dict_log = max_e > 0 ? log10(max_e) : 0.0;

    vector<pair<ElementId, size_t>> elems(elems_map.begin(), elems_map.end());
    sort(elems.begin(), elems.end(),
        [](const pair<ElementId, size_t>& a, const pair<ElementId, size_t>& b)
        { return a.second > b.second; });

    double min_log = 0.0;
    double max_log = log10(elems.size());

    double freq_prev = static_cast<double>(this->Ostats.card);

    for (int i = 0; i < StatsComp::element_bins; ++i)
    {
        size_t pos;

        if (i == 0)
            pos = 0;
        else
        {
            double log_pos = min_log + 
                (i / static_cast<double>(StatsComp::element_bins))
                * (max_log - min_log);
            pos = static_cast<size_t>(pow(10.0, log_pos));
            pos = min(pos, elems.size() - 1);
        }

        auto elem_freq = elems[pos];
        auto elem = elem_freq.first;
        auto freq = static_cast<double>(elem_freq.second);

        OElemStats elem_stats;
        elem_stats.lo_rank = elem_freq.first + 1;
        elem_stats.lo_rank_log = log10(elem_stats.lo_rank);

        elem_stats.rel_drop = freq / freq_prev;
        elem_stats.rel_drop_log = log10(elem_stats.rel_drop);
        freq_prev = freq;

        elem_stats.max_freq = elem_freq.second;
        elem_stats.max_freq_log = log10(elem_stats.max_freq);

        elem_stats.max_post_ratio 
            = (static_cast<double>(elem_freq.second) / this->Ostats.card);
        elem_stats.max_post_ratio_log = log10(elem_stats.max_post_ratio);

        if (!this->Ostats.elements.empty())
        {
            this->Ostats.elements.back().hi_rank = max(
                this->Ostats.elements.back().lo_rank, elem_stats.lo_rank - 1);

            this->Ostats.elements.back().hi_rank_log = log10(
                this->Ostats.elements.back().hi_rank);
        }

        this->Ostats.elements.push_back(elem_stats);
    }

    this->Ostats.elements.back().hi_rank_log = this->Ostats.dict_log;
    this->Ostats.elements.back().hi_rank = this->Ostats.dict;

    Log::w(3, "Dictionary size", this->Ostats.dict);
    Log::w(3, "Dictionary size (postings)", elems_map.size());
    Log::w(3, "Dictionary fill ratio", this->Ostats.dict > 0
        ? static_cast<double>(elems_map.size()) / this->Ostats.dict : 0.0);

    for (auto &b : this->Ostats.elements)
    {
        b.dict_ratio = static_cast<double>(b.hi_rank - b.lo_rank + 1)
            / static_cast<double>(this->Ostats.dict);
        b.dict_ratio_log = log10(b.dict_ratio);
    }
}


void StatsComp::estimate_bytes(OStats &os)
{
    // Calculate average element count per record across all extent buckets
    size_t elem_cnt_bucket_sum = 0;
    for (auto const &e : os.extents) 
        elem_cnt_bucket_sum += e.avg_elem_cnt;
    size_t avg_elem_cnt_bucket = 
        elem_cnt_bucket_sum / os.extents.size();

    // Total elements across all records
    size_t total_elem_cnt = avg_elem_cnt_bucket * os.card;

    size_t size = 0;
    
    // Elements
    size += sizeof(ElementId) * total_elem_cnt;
    size += sizeof(vector<ElementId>) * os.card;
    
    // Start, End, Id
    size += (2 * sizeof(Timestamp) + sizeof(RecordId)) * os.card;
    
    // Object overhead
    size += sizeof(IRecord) * os.card;

    // Object collection overhead (gStart, gEnd)
    size += sizeof(vector<IRecord>);
    size += 2 * sizeof(Timestamp);

    os.est_bytes = size;
}


void StatsComp::compute_bytes(const IRelation &O)
{
    // Compute the actual memory usage
    size_t size = 0;

    // Elements
    for (const auto &rec : O)
    {
        size += sizeof(ElementId) * rec.elements.size(); // element storage
        size += sizeof(vector<ElementId>);               // vector container
    }

    // Start, End, Id
    size += (2 * sizeof(Timestamp) + sizeof(RecordId)) * O.size();

    // Object overhead
    size += sizeof(IRecord) * O.size();

    // Object collection overhead (gStart, gEnd)
    size += sizeof(vector<IRecord>);
    size += 2 * sizeof(Timestamp);

    Log::w(1, "Actual memory size",
        to_string(size / (1024.0 * 1024.0)) + " MB");
}


