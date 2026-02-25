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

#include "qgen.h"
#include "qgenselect.h"
#include <unordered_map>
#include <numeric>


QGen::QGen(const IRelation &O, const OStats &ostats)
    : os(ostats),
      O(O),
      num_threads(Cfg::get_threads()),
      max_gen_tries(Cfg::get<int>("q.gen.max-gen-tries")),
      patterns(Cfg::get<vector<string>>("q.gen.patterns-active"))
{
    this->compute_frequencies();
}


QGen::~QGen() = default;


void QGen::load_pattern(const string &pattern)
{
    string prefix = "q.gen.workload." + pattern + ".";

    Log::w(1, "Q pattern", prefix.substr(0, prefix.length() - 1));
    
    this->domain = (int)pow(10, this->os.domain_log);

    this->num_q = Cfg::get<int>(prefix + "num");

    this->elem_cnt = Cfg::get<vector<int>>(prefix + "elem.cnt");

    auto parse_ranges =
        [](const string &key, vector<pair<double, double>> &res)
    {
        res.clear();

        auto strs = Cfg::get<vector<string>>(key);
        for (const auto &e : strs)
        {
            size_t dash_pos = e.find('-');
            if (dash_pos != string::npos)
            {
                double start = stod(e.substr(0, dash_pos)) / 100;
                double end = stod(e.substr(dash_pos + 1)) / 100;
                if (start < 0 || end < 0 || start >= end)
                    throw runtime_error("Invalid range: " + e);

                res.emplace_back(start, end);
            }
            else
            {
                double val = stod(e) / 100;
                if (val < 0)
                    throw runtime_error("Invalid value: " + e);

                res.emplace_back(val, val);
            }
        }
    };

    parse_ranges(prefix + "ext.ranges", this->ext_range_buckets);
    parse_ranges(prefix + "ext.skew", this->ext_skew_buckets);
    parse_ranges(prefix + "elem.freqs", this->elem_buckets);

    if (this->elem_cnt.empty())
        throw runtime_error("No element counts configured");

    if (this->ext_range_buckets.empty())
        throw runtime_error("No extent range buckets configured");

    if (this->ext_skew_buckets.empty())
        throw runtime_error("No extent skew buckets configured");
}


vector<RangeIRQuery> QGen::construct_Q(const string &pattern, int num_q_override)
{
    vector<RangeIRQuery> Q;

    this->load_pattern(pattern);

    this->num_q = num_q_override > 0 ? num_q_override : this->num_q;

    this->compute_element_buckets();

    this->compute_temporal_skew_buckets();

    if (this->elem_buckets_eids.empty())
    {
        Log::w(1, "No element buckets available to generate Q");
        return Q;
    }

    vector<RecordId> Q_gen_failed;

    omp_set_num_threads(this->num_threads);

    Timer tim;
    tim.start();

    Q.resize(this->num_q);

    #pragma omp parallel for schedule(dynamic)
    for (int i = 0; i < this->num_q; ++i)
    {
        // Create thread-local random generator for parallel execution
        thread_local static RandomGen local_randomgen;

        int attempts = 0;
        bool success = false;

        // Stays fixed across retry attempts
        if (this->elem_cnt.empty()) continue;

        auto const temp_buckets_tids_selected = this->temp_buckets_tids[
            local_randomgen.rndi(0, this->temp_buckets_tids.size() - 1)];
        
        auto elem_cnt = this->elem_cnt[
            local_randomgen.rndi(0, this->elem_cnt.size() - 1)];

        // Each bucket has equal probability of being selected
        vector<vector<ElementId>> elem_buckets_eids_selected;
        elem_buckets_eids_selected.reserve(elem_cnt);
        
        for (int j = 0; j < elem_cnt; ++j)
        {
            int bucket_idx = local_randomgen.rndi(
                0, this->elem_buckets_eids.size() - 1);
            elem_buckets_eids_selected.push_back(
                this->elem_buckets_eids[bucket_idx]);
        }

        if (this->O.empty()) continue;

        while (attempts++ < this->max_gen_tries && !success)
        {
            auto q = this->construct_q(
                temp_buckets_tids_selected,
                elem_buckets_eids_selected,
                this->ext_range_buckets,
                elem_cnt,
                this->domain);

            if (!q)  continue;

            q->id = i;
            Q[i] = *q;
            success = true;
        }

        if (!success)
        {
            #pragma omp critical(q_gen_failed)
            {
                Q_gen_failed.push_back(i);
            }
        }
    }

    // Remove queries with empty elements
    Q.erase(remove_if(Q.begin(), Q.end(),
        [](const RangeIRQuery &q) { return q.elems.empty(); }), Q.end());

    auto time = tim.stop();

    Log::w(1, "Num of q generations", Q.size());
    if (Q.size() != this->num_q)
        Log::w(1, "Num of failed q generations",
            to_string(this->num_q - Q.size())
            + " (" + to_string(this->max_gen_tries) + " attempts per q)");
    Log::w(1, "Generation [s] ([q/s])", to_string(time)
        + " (" + to_string(this->num_q / time) + ")");

    return Q;
}


vector<tuple<string,vector<RangeIRQuery>>> QGen::construct_Q()
{
    vector<tuple<string,vector<RangeIRQuery>>> Qx;

    auto num_q_override = Cfg::get<int>("q.gen.num-override");

    for (const auto &pattern : this->patterns)
    {
        // Split pattern by '+' to handle sub-patterns
        vector<string> sub_patterns;
        size_t start = 0;
        size_t pos = 0;
        
        while ((pos = pattern.find('+', start)) != string::npos)
        {
            if (pos > start)
                sub_patterns.push_back(pattern.substr(start, pos - start));
            start = pos + 1;
        }
        if (start < pattern.length())
            sub_patterns.push_back(pattern.substr(start));
        
        // If no '+' found, sub_patterns will contain the single pattern
        if (sub_patterns.empty())
            sub_patterns.push_back(pattern);

        vector<RangeIRQuery> Q;
        
        // Calculate ratios if there are sub-patterns and override is active
        if (num_q_override > 0 && sub_patterns.size() > 1)
        {
            // First pass: collect original num values for each sub-pattern
            vector<int> original_nums;
            int total_num = 0;
            
            for (const auto &sub_pattern : sub_patterns)
            {
                string prefix = "q.gen.workload." + sub_pattern + ".";
                int sub_num = Cfg::get<int>(prefix + "num");
                original_nums.push_back(sub_num);
                total_num += sub_num;
            }
            
            // Generate queries for each sub-pattern with proportional override
            // Use largest-remainder method to ensure allocations sum exactly to num_q_override
            vector<int> allocs(sub_patterns.size(), 0);
            vector<double> remainders(sub_patterns.size(), 0.0);
            int alloc_sum = 0;
            for (size_t i = 0; i < sub_patterns.size(); ++i)
            {
                double exact = total_num > 0
                    ? (static_cast<double>(num_q_override) * original_nums[i]) / total_num
                    : 0.0;
                allocs[i] = static_cast<int>(exact);
                remainders[i] = exact - allocs[i];
                alloc_sum += allocs[i];
            }
            // Distribute leftover queries to sub-patterns with largest remainders
            int leftover = num_q_override - alloc_sum;
            vector<size_t> order(sub_patterns.size());
            iota(order.begin(), order.end(), 0);
            sort(order.begin(), order.end(),
                [&](size_t a, size_t b) { return remainders[a] > remainders[b]; });
            for (int k = 0; k < leftover; ++k)
                allocs[order[k]]++;

            for (size_t i = 0; i < sub_patterns.size(); ++i)
            {
                Log::w(2, "Generating queries for sub-pattern", 
                    sub_patterns[i] + " (override: " + to_string(allocs[i]) + ")");

                auto select_opt = QGenSelect::parse_select(sub_patterns[i]);
                if (select_opt)
                {
                    if (!this->selectivity_filter)
                        this->selectivity_filter = make_unique<QGenSelect>(this->O);

                    int needed = allocs[i] > 0
                        ? allocs[i]
                        : Cfg::get<int>("q.gen.workload." + sub_patterns[i] + ".num");

                    auto sub_Q = this->selectivity_filter->construct_Q_filtered(
                        [&](int batch_size) { return this->construct_Q(sub_patterns[i], batch_size); },
                        needed,
                        select_opt->first,
                        select_opt->second,
                        sub_patterns[i]);
                    Q.insert(Q.end(), sub_Q.begin(), sub_Q.end());
                }
                else
                {
                    auto sub_Q = this->construct_Q(sub_patterns[i], allocs[i]);
                    Q.insert(Q.end(), sub_Q.begin(), sub_Q.end());
                }
            }
        }
        else
        {
            // No sub-patterns or no override: generate normally
            for (const auto &sub_pattern : sub_patterns)
            {
                Log::w(2, "Generating queries for sub-pattern", sub_pattern);

                auto select_opt = QGenSelect::parse_select(sub_pattern);
                if (select_opt)
                {
                    if (!this->selectivity_filter)
                        this->selectivity_filter = make_unique<QGenSelect>(this->O);

                    int needed = num_q_override > 0
                        ? num_q_override
                        : Cfg::get<int>("q.gen.workload." + sub_pattern + ".num");

                    auto sub_Q = this->selectivity_filter->construct_Q_filtered(
                        [&](int batch_size) { return this->construct_Q(sub_pattern, batch_size); },
                        needed,
                        select_opt->first,
                        select_opt->second,
                        sub_pattern);
                    Q.insert(Q.end(), sub_Q.begin(), sub_Q.end());
                }
                else
                {
                    auto sub_Q = this->construct_Q(sub_pattern, num_q_override);
                    Q.insert(Q.end(), sub_Q.begin(), sub_Q.end());
                }
            }
        }
        
        // Shuffle if configured
        if (Cfg::get<bool>("q.gen.shuffle-workloads"))
        {
            Log::w(2, "Shuffling sub-pattern queries", pattern);
            random_shuffle(Q.begin(), Q.end());
        }

        if(!Q.empty()) Qx.emplace_back(
            "qcnt" + to_string(Q.size()) + "-" + pattern, move(Q));
    }

    if (Cfg::get<bool>("q.gen.combine-all-workloads"))
    {
        vector<RangeIRQuery> combined_queries;
        string combined_name;
        for (size_t i = 0; i < Qx.size(); ++i)
        {
            if (i > 0) combined_name += "_&_";
            combined_name += get<0>(Qx[i]);
            const auto &queries = get<1>(Qx[i]);
            combined_queries.insert(
                combined_queries.end(), queries.begin(), queries.end());
        }

        if (Cfg::get<bool>("q.gen.shuffle-workloads"))
        {
            random_shuffle(combined_queries.begin(), combined_queries.end());
        }

        // Shorten combined name if it exceeds reasonable length
        const size_t max_name_length = 120;
        if (combined_name.length() > max_name_length)
        {
            // Create a hash of the full name for uniqueness
            hash<string> hasher;
            size_t name_hash = hasher(combined_name);
            
            // Use first part + hash + workload count
            string short_name = combined_name.substr(0, max_name_length);
            short_name += "_..._" + to_string(Qx.size()) + "workloads_hash" + to_string(name_hash);
            
            Log::w(1, "Q name shortened", combined_name + "\n->\n" + short_name);
            
            combined_name = short_name;
        }

        Qx.clear();
        Qx.emplace_back(combined_name, move(combined_queries));
    }

    return Qx;
}


void QGen::compute_frequencies()
{
    Timer freq_timer;
    freq_timer.start();

    // Parallelize frequency counting for large datasets
    if (this->O.size() > 10000 && this->num_threads > 1)
    {
        omp_set_num_threads(this->num_threads);
        
        #pragma omp parallel
        {
            unordered_map<ElementId, int> local_freq_map;
            
            #pragma omp for schedule(static)
            for (size_t i = 0; i < this->O.size(); ++i)
            {
                const auto &o = this->O[i];
                for (const auto &eid : o.elements)
                {
                    local_freq_map[eid]++;
                }
            }
            
            // Merge local results into global map
            #pragma omp critical(freq_merge)
            {
                for (const auto &pair : local_freq_map)
                    this->elem_freq_map[pair.first] += pair.second;
            }
        }
    }
    else
    {
        // Sequential processing for smaller datasets
        for (const auto &o : this->O)
            for (const auto &eid : o.elements)
                this->elem_freq_map[eid]++;
    }

    double freq_time = freq_timer.stop();
    Log::w(1, "O freq. comp. [s]", freq_time);
}


void QGen::compute_element_buckets()
{
    auto card = pow(10, this->os.card_log);

    // Pre-allocate bucket storage
    this->elem_buckets_eids.clear();
    this->elem_buckets_eids.reserve(this->elem_buckets.size());

    // Single pass through frequency map
    for (const auto &b : this->elem_buckets)
    {
        double min_cnt = b.first * card;
        double max_cnt = b.second * card;

        vector<ElementId> bucket_eids;
        bucket_eids.reserve(
            this->elem_freq_map.size() / this->elem_buckets.size()); // Estimate

        for (const auto &ef : this->elem_freq_map)
            if (ef.second > min_cnt && ef.second <= max_cnt)
                bucket_eids.push_back(ef.first);

        if (!bucket_eids.empty())
        {
            sort(bucket_eids.begin(), bucket_eids.end(), greater<ElementId>());
            this->elem_buckets_eids.push_back(move(bucket_eids));
        }
    }
}


void QGen::compute_temporal_skew_buckets()
{
    this->temp_buckets_tids.clear();

    int domain = (int)pow(10, this->os.domain_log);

    for (const auto &b : this->ext_skew_buckets)
    {
        if (b.first < 0 || b.second > domain)
            throw runtime_error("Invalid temporal skew bucket: "
                + to_string(b.first) + "-" + to_string(b.second));

        Timestamp start = static_cast<Timestamp>(b.first * domain);
        Timestamp end = static_cast<Timestamp>(b.second * domain);

        if (end < start)
            throw runtime_error("Invalid temporal skew bucket: "
                + to_string(start) + "-" + to_string(end));

        this->temp_buckets_tids.emplace_back(start, end);
    }
}


optional<RangeIRQuery> QGen::construct_q(
    const pair<Timestamp,Timestamp> &temp_buckets_tids_selected,
    const vector<vector<ElementId>> &elem_buckets_eids_selected,
    const vector<pair<double, double>> &ext_range_buckets,
    int elem_cnt,
    int domain)
{
    if (this->O.empty()) return nullopt;

    // Create thread-local random generator for parallel execution
    thread_local static RandomGen local_randomgen;

    int o_idx = local_randomgen.rndi(0, this->O.size() - 1);

    const auto &o = this->O[o_idx];

    if (elem_cnt > o.elements.size()) return nullopt;

    if (o.end < temp_buckets_tids_selected.first
        || o.start > temp_buckets_tids_selected.second)
        return nullopt;

    set<ElementId> elem_set;
    bool bucket_failed = false;

    for (size_t bucket_idx = 0; bucket_idx < elem_buckets_eids_selected.size(); ++bucket_idx)
    {
        auto &es = elem_buckets_eids_selected[bucket_idx];
        size_t i = 0, j = 0;
        vector<ElementId> candidates;

        // Merge-join elements from the bucket and object
        while (i < es.size() && j < o.elements.size())
        {
            if (es[i] > o.elements[j])
                i++;
            else if (es[i] < o.elements[j])
                j++;
            else
            {
                candidates.push_back(es[i]);
                i++;
                j++;
            }
        }

        // Remove candidates already used
        candidates.erase(
            remove_if(candidates.begin(), candidates.end(),
                [&elem_set](const ElementId &eid)
                { return elem_set.find(eid) != elem_set.end(); }),
            candidates.end());

        if (!candidates.empty())
        {
            // Select a random element from candidates
            int selected_idx = local_randomgen.rndi(
                0, candidates.size() - 1);
            ElementId selected_eid = candidates[selected_idx];
            elem_set.insert(selected_eid);
        }
        else
        {
            // No valid candidates in this bucket for this object
            bucket_failed = true;
            break;
        }
    }

    if (bucket_failed || elem_set.empty()) return nullopt;

    // Select a random extent from the configured ranges
    if (ext_range_buckets.empty()) return nullopt;
        
    int ext_idx = local_randomgen.rndi(0, ext_range_buckets.size() - 1);
    auto ext_range = ext_range_buckets[ext_idx];

    auto ext_min = static_cast<int>(ext_range.first * domain);
    auto ext_max = static_cast<int>(ext_range.second * domain);
    
    if (ext_max < ext_min) return nullopt;

    auto ext = max(1, local_randomgen.rndi(ext_min, ext_max));
    
    int min_start_for_o = o.start - ext;
    int max_start_for_o = o.end;
    
    int min_start_for_temp = temp_buckets_tids_selected.first - ext;
    int max_start_for_temp = temp_buckets_tids_selected.second;
    
    int min_start = max({0, min_start_for_o, min_start_for_temp});
    int max_start = min({domain - ext, max_start_for_o, max_start_for_temp});
    
    if (max_start < min_start) return nullopt;

    RangeIRQuery q;

    q.start = local_randomgen.rndi(min_start, max_start);
    q.end = q.start + ext;

    q.elems.assign(elem_set.begin(), elem_set.end());
    sort(q.elems.begin(), q.elems.end(), greater<ElementId>());

    return q;
}