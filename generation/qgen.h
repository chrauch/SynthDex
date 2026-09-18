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

#ifndef _QGEN_H_
#define _QGEN_H_

#include "../utils/global.h"
#include "../containers/relations.h"
#include "../containers/hierarchical_index.h"
#include "../learning/stats.h"
#include "../learning/statsserializer.h"
#include "randomgen.h"
#include <optional>
#include <memory>
#include <functional>
#include <unordered_map>
#include <cstdint>

using namespace std;

class QGenSelect;

// Parse override blocks from a full pattern spec (possibly compound with '+').
// Returns {clean_pattern, map<bare_name, map<key, comma_value_string>>}.
pair<string, unordered_map<string, unordered_map<string,string>>>
parse_pattern_overrides(const string &full_pattern);

// Build a canonical, sorted string from all overrides (for hashing/logging).
string overrides_canonical(
    const unordered_map<string, unordered_map<string,string>> &ov);

// FNV-1a 32-bit hash.
uint32_t fnv1a_32(const string &s);

class QGen
{
public:
    QGen(const IRelation &O, const OStats &ostats);
    ~QGen();
    vector<tuple<string,vector<RangeIRQuery>>> construct_Q();
    vector<RangeIRQuery> construct_Q(const string &pattern, int num_q_override);
    void set_patterns(const vector<string> &patterns_override);
    void set_workload_cfg(unordered_map<string, unordered_map<string,string>> cfg);
    optional<RangeIRQuery> construct_q(
        const pair<Timestamp,Timestamp> &temp_buckets_tids_selected,
        const vector<vector<ElementId>> &elem_buckets_eids_selected,
        const vector<pair<double, double>> &ext_range_buckets,
        int elem_cnt,
        int domain);
    
    void expand_queries(vector<RangeIRQuery>& Q, int intended_count) const;

private:
    int num_threads;

    void load_pattern(const string &pattern);
    void compute_frequencies();
    void compute_element_buckets();
    void compute_temporal_skew_buckets();
    OStats os;
    const IRelation& O;
    int num_q;
    int max_gen_tries;
    int domain;
    
    unordered_map<ElementId, int> elem_freq_map;

    vector<string> patterns;
    unordered_map<string, unordered_map<string,string>> m_workload_cfg;

    vector<int> elem_cnt;

    vector<pair<double, double>> ext_range_buckets;
    vector<pair<double, double>> ext_skew_buckets;
    vector<pair<double, double>> elem_buckets;
    
    vector<vector<ElementId>> elem_buckets_eids;
    vector<pair<Timestamp,Timestamp>> temp_buckets_tids;

    /// Lazily created when a pattern with "select" is encountered.
    unique_ptr<QGenSelect> selectivity_filter;
};

#endif // _QGEN_H_
