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
    if (Cfg::get<bool>("out.detailed"))
        this->compute_slices(O);

    StatsComp::fill(this->Ostats);

    double time = tim.stop();
    Log::w(1, "Analysis [s]", time);

    Log::w(2, "O statistics (short)",
        StatsSerializer::to_json(this->Ostats, false));

    return this->Ostats;
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


void StatsComp::compute_slices(const IRelation &O)
{
    int num_slices = Cfg::get<int>("o.stats.slices");
    if (num_slices <= 0)
        return;

    double domain = static_cast<double>(this->Ostats.domain);
    double slice_width = domain / num_slices;

    Log::w(1, "Slice analysis",
        to_string(num_slices) + " slices of width " + to_string(slice_width));

    for (int s = 0; s < num_slices; ++s)
    {
        double s_start = s * slice_width;
        double s_end = (s + 1) * slice_width;

        size_t count = 0;
        size_t total_elems = 0;

        for (const auto &rec : O)
        {
            // Object intersects slice if rec.start < s_end && rec.end > s_start
            if (rec.start < s_end && rec.end > s_start)
            {
                count++;
                total_elems += rec.elements.size();
            }
        }

        double avg_elems = count > 0
            ? static_cast<double>(total_elems) / count : 0.0;

        Log::w(1, "Slice " + to_string(s)
            + " [" + to_string(static_cast<int>(s_start))
            + "," + to_string(static_cast<int>(s_end)) + ")",
            "objects=" + to_string(count)
            + " avg_elems=" + to_string(avg_elems));
    }
}