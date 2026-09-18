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

#include "qgen.h"
#include "qgenselect.h"
#include <unordered_map>
#include <numeric>
#include <sstream>


// ---------------------------------------------------------------------------
// FNV-1a 32-bit hash
// ---------------------------------------------------------------------------
uint32_t fnv1a_32(const string &s)
{
    uint32_t h = 2166136261u;
    for (unsigned char c : s) { h ^= c; h *= 16777619u; }
    return h;
}

// ---------------------------------------------------------------------------
// Override-block parsing helpers
// ---------------------------------------------------------------------------

// Parse a single workload chunk that may carry an optional {key=[val,...];...} suffix.
// Returns the cleaned chunk string (without braces) and a map of key -> comma-separated value string.
// Example: "10RND{ext.skew=[0-50];ext.ranges=[0-0.01,0-0.001]}"
//   -> clean = "10RND", overrides = {{"ext.skew","0-50"},{"ext.ranges","0-0.01,0-0.001"}}
static pair<string, unordered_map<string,string>>
parse_chunk_overrides(const string &chunk)
{
    auto brace_open = chunk.find('{');
    if (brace_open == string::npos)
        return {chunk, {}};

    if (chunk.back() != '}')
        throw runtime_error("Unmatched '{' in workload chunk: \"" + chunk + "\"");

    string clean = chunk.substr(0, brace_open);
    string body  = chunk.substr(brace_open + 1, chunk.size() - brace_open - 2);

    unordered_map<string,string> overrides;
    // Split body on ';' to get individual key=[...] entries
    size_t s = 0;
    while (s <= body.size())
    {
        size_t semi = body.find(';', s);
        string entry = (semi == string::npos) ? body.substr(s) : body.substr(s, semi - s);
        s = (semi == string::npos) ? body.size() + 1 : semi + 1;
        if (entry.empty()) continue;

        auto eq = entry.find('=');
        if (eq == string::npos)
            throw runtime_error("Invalid override entry (missing '='): \"" + entry + "\"");

        string key = entry.substr(0, eq);
        string val = entry.substr(eq + 1);
        // Strip surrounding [ ] if present
        if (!val.empty() && val.front() == '[' && val.back() == ']')
            val = val.substr(1, val.size() - 2);
        overrides[key] = val;
    }
    return {clean, move(overrides)};
}

// Parse a full pattern spec (possibly compound with '+') that may contain per-chunk
// {key=[...]} override blocks.  Returns the cleaned pattern (override blocks stripped)
// and a map from cleaned chunk (including any ratio prefix) -> override map.
// Bare name = chunk with both numeric ratio prefix AND override block stripped.
// Example: "20LOW+10RND{ext.skew=[0-50]}"
//   -> clean_pattern = "20LOW+10RND",
//      overrides = {{"10RND", {{"ext.skew","0-50"}}}}
pair<string, unordered_map<string, unordered_map<string,string>>>
parse_pattern_overrides(const string &full_pattern)
{
    // Split on '+', safe because '+' never appears inside [...] values.
    vector<string> chunks;
    size_t s = 0, p;
    while ((p = full_pattern.find('+', s)) != string::npos)
    {
        chunks.push_back(full_pattern.substr(s, p - s));
        s = p + 1;
    }
    chunks.push_back(full_pattern.substr(s));

    string clean_pattern;
    unordered_map<string, unordered_map<string,string>> result;

    for (size_t i = 0; i < chunks.size(); ++i)
    {
        auto [clean_chunk, ov] = parse_chunk_overrides(chunks[i]);
        if (i > 0) clean_pattern += '+';
        clean_pattern += clean_chunk;

        if (!ov.empty())
        {
            // Keep the ratio prefix so identical workloads can have distinct overrides.
            result[clean_chunk] = move(ov);
        }
    }
    return {clean_pattern, move(result)};
}

// Build a canonical, deterministic string from the override map for hashing.
// Format: "NAME1:{k1=v1;k2=v2}|NAME2:{...}" (names and keys sorted).
string overrides_canonical(
    const unordered_map<string, unordered_map<string,string>> &ov)
{
    // Sort workload names
    vector<string> names;
    names.reserve(ov.size());
    for (const auto &[n, _] : ov) names.push_back(n);
    sort(names.begin(), names.end());

    string out;
    for (const auto &name : names)
    {
        if (!out.empty()) out += '|';
        out += name + ":{";
        const auto &kv = ov.at(name);
        vector<string> keys;
        keys.reserve(kv.size());
        for (const auto &[k, _] : kv) keys.push_back(k);
        sort(keys.begin(), keys.end());
        bool first = true;
        for (const auto &k : keys)
        {
            if (!first) out += ';';
            out += k + '=' + kv.at(k);
            first = false;
        }
        out += '}';
    }
    return out;
}

// ---------------------------------------------------------------------------
// Parse an optional inline ratio prefix from a sub-pattern component.
// "4RND" -> {4, "RND"},  "RND" -> {-1, "RND"},  "42" -> {-1, "42"} (caught later)
static pair<int,string> parse_subpattern(const string &sp)
{
    size_t i = 0;
    while (i < sp.size() && isdigit((unsigned char)sp[i])) ++i;
    if (i == 0 || i == sp.size()) return {-1, sp}; // no prefix, or all-digits (invalid)
    return {stoi(sp.substr(0, i)), sp.substr(i)};
}


// Validate that every component of every pattern has a non-empty bare name.
// Allows optional numeric ratio prefixes (e.g. "4RND") but rejects all-digit
// components ("42") or components that are entirely empty.
static void validate_patterns(const vector<string> &patterns)
{
    for (const auto &pat : patterns)
    {
        size_t s = 0, p;
        while ((p = pat.find('+', s)) != string::npos)
        {
            string sp = pat.substr(s, p - s);
            // Strip any {override} block before checking the bare name.
            auto [clean_sp, _ov] = parse_chunk_overrides(sp);
            auto [pfx, bare] = parse_subpattern(clean_sp);
            if (bare.empty())
                throw runtime_error(
                    "Workload component has no name after numeric prefix: \""
                    + sp + "\" (in pattern \"" + pat + "\")");
            s = p + 1;
        }
        string sp = pat.substr(s);
        auto [clean_sp2, _ov2] = parse_chunk_overrides(sp);
        auto [pfx2, bare2] = parse_subpattern(clean_sp2);
        if (bare2.empty())
            throw runtime_error(
                "Workload component has no name after numeric prefix: \""
                + sp + "\" (in pattern \"" + pat + "\")");
    }
}


QGen::QGen(const IRelation &O, const OStats &ostats)
    : os(ostats),
      O(O),
      num_threads(Cfg::get_threads()),
      max_gen_tries(Cfg::get<int>("q.gen.max-gen-tries")),
      patterns(Cfg::get<vector<string>>("q.gen.patterns-active"))
{
    // Validate that no workload name starts with a digit.
    // Compound Q names embed per-component ratios as numeric prefixes
    // (e.g. 100HOT+3COLD), so a name beginning with a digit would be
    // indistinguishable from its own ratio prefix.
    validate_patterns(this->patterns);

    this->compute_frequencies();
}


QGen::~QGen() = default;


void QGen::set_patterns(const vector<string> &patterns_override)
{
    validate_patterns(patterns_override);
    this->patterns = patterns_override;
}


void QGen::set_workload_cfg(unordered_map<string, unordered_map<string,string>> cfg)
{
    this->m_workload_cfg = move(cfg);
}


void QGen::load_pattern(const string &pattern)
{
    string prefix = "q.gen.workload." + pattern + ".";

    Log::w(1, "Q pattern", prefix.substr(0, prefix.length() - 1));
    
    this->domain = (int)pow(10, this->os.domain_log);

    // Look up any CLI overrides for this pattern name.
    const unordered_map<string,string>* ov = nullptr;
    {
        auto it = this->m_workload_cfg.find(pattern);
        if (it != this->m_workload_cfg.end()) ov = &it->second;
    }

    // elem.cnt: override is a comma-separated list of ints, e.g. "1,2,3"
    if (ov && ov->count("elem.cnt"))
    {
        this->elem_cnt.clear();
        istringstream ss(ov->at("elem.cnt"));
        string tok;
        while (getline(ss, tok, ','))
            if (!tok.empty()) this->elem_cnt.push_back(stoi(tok));
    }
    else
    {
        this->elem_cnt = Cfg::get<vector<int>>(prefix + "elem.cnt");
    }

    // Helper: parse a comma-separated string of range specs into pairs.
    // Range spec: "lo-hi" (divided by 100) or a single value.
    auto parse_ranges_str =
        [](const string &csv, vector<pair<double, double>> &res)
    {
        res.clear();
        istringstream ss(csv);
        string e;
        while (getline(ss, e, ','))
        {
            if (e.empty()) continue;
            size_t dash_pos = e.find('-');
            if (dash_pos != string::npos)
            {
                double start = stod(e.substr(0, dash_pos)) / 100;
                double end   = stod(e.substr(dash_pos + 1)) / 100;
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

    // Original helper that reads from Cfg.
    auto parse_ranges =
        [&parse_ranges_str](const string &key, vector<pair<double, double>> &res)
    {
        auto strs = Cfg::get<vector<string>>(key);
        string joined;
        for (size_t i = 0; i < strs.size(); ++i)
        {
            if (i) joined += ',';
            joined += strs[i];
        }
        parse_ranges_str(joined, res);
    };

    if (ov && ov->count("ext.ranges"))
        parse_ranges_str(ov->at("ext.ranges"), this->ext_range_buckets);
    else
        parse_ranges(prefix + "ext.ranges", this->ext_range_buckets);

    if (ov && ov->count("ext.skew"))
        parse_ranges_str(ov->at("ext.skew"), this->ext_skew_buckets);
    else
        parse_ranges(prefix + "ext.skew", this->ext_skew_buckets);

    if (ov && ov->count("elem.freqs"))
        parse_ranges_str(ov->at("elem.freqs"), this->elem_buckets);
    else
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
    // If the pattern is a compound (e.g. "HOT+COLD"), split on '+', compute
    // proportional query counts and generate each sub-pattern separately.
    if (pattern.find('+') != string::npos)
    {
        vector<string> sub_patterns;
        size_t start = 0, pos = 0;
        while ((pos = pattern.find('+', start)) != string::npos)
        {
            if (pos > start) sub_patterns.push_back(pattern.substr(start, pos - start));
            start = pos + 1;
        }
        if (start < pattern.length()) sub_patterns.push_back(pattern.substr(start));

        // Collect per-pattern ratios for proportional allocation.
        // Each component may carry an inline ratio prefix (e.g. "4RND");
        // if absent, fall back to the config "ratio" key.
        vector<string> sub_names; // bare workload names (prefix stripped)
        vector<int> original_nums;
        int total_num = 0;
        for (const auto &sp : sub_patterns)
        {
            auto [pfx, name] = parse_subpattern(sp);
            int n = (pfx >= 0) ? pfx : Cfg::get<int>("q.gen.workload." + name + ".ratio");
            sub_names.push_back(move(name));
            original_nums.push_back(n);
            total_num += n;
        }

        int target = num_q_override;

        // Proportional allocation using largest-remainder method so allocations
        // sum exactly to target.
        vector<int> allocs(sub_patterns.size(), 0);
        vector<double> remainders(sub_patterns.size(), 0.0);
        int alloc_sum = 0;
        for (size_t i = 0; i < sub_patterns.size(); ++i)
        {
            double exact = total_num > 0
                ? (static_cast<double>(target) * original_nums[i]) / total_num
                : 0.0;
            allocs[i] = static_cast<int>(exact);
            remainders[i] = exact - allocs[i];
            alloc_sum += allocs[i];
        }
        int leftover = target - alloc_sum;
        vector<size_t> order(sub_patterns.size());
        iota(order.begin(), order.end(), 0);
        sort(order.begin(), order.end(),
            [&](size_t a, size_t b) { return remainders[a] > remainders[b]; });
        for (int k = 0; k < leftover; ++k) allocs[order[k]]++;

        vector<RangeIRQuery> Q;
        for (size_t i = 0; i < sub_patterns.size(); ++i)
        {
            Log::w(1, "Q sub-pattern", sub_names[i]
                + " (n=" + to_string(allocs[i]) + ")");

            auto previous_override = m_workload_cfg.find(sub_names[i]);
            bool had_previous_override = previous_override != m_workload_cfg.end();
            unordered_map<string, string> saved_override;
            if (had_previous_override)
                saved_override = previous_override->second;
            auto chunk_override = m_workload_cfg.find(sub_patterns[i]);
            if (chunk_override != m_workload_cfg.end())
                m_workload_cfg[sub_names[i]] = chunk_override->second;

            vector<RangeIRQuery> sub_Q;

            auto select_opt = QGenSelect::parse_select(sub_names[i]);
            if (select_opt)
            {
                if (!this->selectivity_filter)
                    this->selectivity_filter = make_unique<QGenSelect>(this->O);
                sub_Q = this->selectivity_filter->construct_Q_filtered(
                    [&](int batch_size) { return this->construct_Q(sub_names[i], batch_size); },
                    allocs[i], select_opt->first, select_opt->second, sub_names[i]);
            }
            else
            {
                sub_Q = this->construct_Q(sub_names[i], allocs[i]);
            }

            Q.insert(Q.end(), sub_Q.begin(), sub_Q.end());
            if (had_previous_override)
                m_workload_cfg[sub_names[i]] = move(saved_override);
            else
                m_workload_cfg.erase(sub_names[i]);
        }

        if (Cfg::get<bool>("q.gen.shuffle-workloads"))
        {
            mt19937 shuf_rng(42u);
            shuffle(Q.begin(), Q.end(), shuf_rng);
        }

        return Q;
    }

    // -- single pattern path --------------------------------------------------
    vector<RangeIRQuery> Q;

    this->load_pattern(pattern);

    this->num_q = num_q_override;

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
        // Create thread-local random generator seeded deterministically:
        // base seed from config XOR-mixed with the thread id so each thread
        // produces a distinct but reproducible stream.
        thread_local static RandomGen local_randomgen(
            42u ^ static_cast<uint32_t>(omp_get_thread_num() * 2654435761u));

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


void QGen::expand_queries(vector<RangeIRQuery>& Q, int intended_count) const
{
    int seed = Cfg::get<int>("q.gen.expand.seed");
    if (seed <= 0 || (int)Q.size() < seed) return;

    int mutation_rate_pct = Cfg::get<int>("q.gen.expand.mutation-rate-pct");
    int target = max((int)Q.size(), intended_count);

    Log::w(1, "Expanding from query subset",
        "seed=" + to_string(seed)
        + " -> total " + to_string(target)
        + " (mutation " + to_string(mutation_rate_pct) + "%)");

    Q.resize(seed);
    Q.reserve(target);

    RandomGen rng(42u);
    for (int xi = seed; xi < target; ++xi)
    {
        RangeIRQuery q = Q[xi % seed];
        if (mutation_rate_pct > 0 && rng.rndi(0, 99) < mutation_rate_pct)
        {
            int donor = rng.rndi(0, seed - 1);
            if (donor == xi % seed && seed > 1)
                donor = (donor + 1) % seed;
            q.elems = Q[donor].elems;
        }
        Q.push_back(move(q));
    }
    {
        mt19937 shuf_rng(42u);
        shuffle(Q.begin(), Q.end(), shuf_rng);
    }
}


vector<tuple<string,vector<RangeIRQuery>>> QGen::construct_Q()
{
    vector<tuple<string,vector<RangeIRQuery>>> Qx;

    const int num_q_override = Cfg::get<int>("q.gen.num");
    const bool do_combine = Cfg::get<bool>("q.gen.combine-all-workloads");

    // When combining multiple patterns, pre-compute per-pattern query allocs
    // proportionally by ratio so they sum exactly to num_q_override.
    vector<int> combine_allocs;
    if (do_combine && this->patterns.size() > 1)
    {
        int total_ratio = 0;
        vector<int> ratios;
        for (const auto &pat : this->patterns)
        {
            int r = 0;
            size_t s = 0, p;
            while ((p = pat.find('+', s)) != string::npos)
            {
                auto [pfx, name] = parse_subpattern(pat.substr(s, p - s));
                r += (pfx >= 0) ? pfx : Cfg::get<int>("q.gen.workload." + name + ".ratio");
                s = p + 1;
            }
            auto [pfx, name] = parse_subpattern(pat.substr(s));
            r += (pfx >= 0) ? pfx : Cfg::get<int>("q.gen.workload." + name + ".ratio");
            ratios.push_back(r);
            total_ratio += r;
        }
        int alloc_sum = 0;
        vector<double> remainders;
        for (int r : ratios)
        {
            double exact = total_ratio > 0 ? (double)num_q_override * r / total_ratio : 0.0;
            combine_allocs.push_back((int)exact);
            remainders.push_back(exact - (int)exact);
            alloc_sum += (int)exact;
        }
        int leftover = num_q_override - alloc_sum;
        vector<size_t> order(ratios.size());
        iota(order.begin(), order.end(), 0);
        sort(order.begin(), order.end(),
            [&](size_t a, size_t b) { return remainders[a] > remainders[b]; });
        for (int k = 0; k < leftover; ++k)
            combine_allocs[order[k]]++;
    }

    for (size_t pi = 0; pi < this->patterns.size(); ++pi)
    {
        const auto &pattern = this->patterns[pi];
        const int pattern_target = combine_allocs.empty() ? num_q_override : combine_allocs[pi];
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
        string effective_pattern = pattern;

        // Calculate ratios if there are sub-patterns
        if (sub_patterns.size() > 1)
        {
            // First pass: collect ratios and bare names for each sub-pattern
            vector<string> sub_names;
            vector<int> original_nums;
            int total_num = 0;
            
            for (const auto &sub_pattern : sub_patterns)
            {
                string prefix = "q.gen.workload." + sub_pattern + ".";
                auto [pfx, bare] = parse_subpattern(sub_pattern);
                int sub_num = (pfx >= 0) ? pfx : Cfg::get<int>("q.gen.workload." + bare + ".ratio");
                sub_names.push_back(move(bare));
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

            // Build composite name using bare names and actual allocs: e.g. "800RND+200BASE"
            effective_pattern.clear();
            for (size_t i = 0; i < sub_names.size(); ++i)
            {
                if (i > 0) effective_pattern += '+';
                effective_pattern += to_string(allocs[i]) + sub_names[i];
            }

            for (size_t i = 0; i < sub_patterns.size(); ++i)
            {
                Log::w(2, "Generating queries for sub-pattern", 
                    sub_patterns[i] + " (override: " + to_string(allocs[i]) + ")");

                auto [pfx_o, bare_o] = parse_subpattern(sub_patterns[i]);
                auto previous_override = m_workload_cfg.find(bare_o);
                bool had_previous_override = previous_override != m_workload_cfg.end();
                unordered_map<string, string> saved_override;
                if (had_previous_override)
                    saved_override = previous_override->second;
                auto chunk_override = m_workload_cfg.find(sub_patterns[i]);
                if (chunk_override != m_workload_cfg.end())
                    m_workload_cfg[bare_o] = chunk_override->second;

                vector<RangeIRQuery> sub_Q;
                auto select_opt = QGenSelect::parse_select(bare_o);
                if (select_opt)
                {
                    if (!this->selectivity_filter)
                        this->selectivity_filter = make_unique<QGenSelect>(this->O);

                    int needed = allocs[i];

                    sub_Q = this->selectivity_filter->construct_Q_filtered(
                        [&](int batch_size) { return this->construct_Q(bare_o, batch_size); },
                        needed,
                        select_opt->first,
                        select_opt->second,
                        bare_o);
                }
                else
                {
                    sub_Q = this->construct_Q(bare_o, allocs[i]);
                }
                Q.insert(Q.end(), sub_Q.begin(), sub_Q.end());
                if (had_previous_override)
                    m_workload_cfg[bare_o] = move(saved_override);
                else
                    m_workload_cfg.erase(bare_o);
            }
        }
        else
        {
            // Single pattern: generate exactly q.gen.num queries
            for (const auto &sub_pattern : sub_patterns)
            {
                auto [pfx_s, bare_s] = parse_subpattern(sub_pattern);
                Log::w(2, "Generating queries for sub-pattern", sub_pattern);

                auto select_opt = QGenSelect::parse_select(bare_s);
                if (select_opt)
                {
                    if (!this->selectivity_filter)
                        this->selectivity_filter = make_unique<QGenSelect>(this->O);

                    int needed = pattern_target;

                    auto sub_Q = this->selectivity_filter->construct_Q_filtered(
                        [&](int batch_size) { return this->construct_Q(bare_s, batch_size); },
                        needed,
                        select_opt->first,
                        select_opt->second,
                        bare_s);
                    Q.insert(Q.end(), sub_Q.begin(), sub_Q.end());
                }
                else
                {
                    auto sub_Q = this->construct_Q(bare_s, pattern_target);
                    Q.insert(Q.end(), sub_Q.begin(), sub_Q.end());
                }
            }
        }
        
        // Shuffle if configured
        if (Cfg::get<bool>("q.gen.shuffle-workloads"))
        {
            Log::w(2, "Shuffling sub-pattern queries", pattern);
            mt19937 shuf_rng(42u);
            shuffle(Q.begin(), Q.end(), shuf_rng);
        }

        // Expand seed pool if configured (expand.seed > 0)
        int expand_seed_used = 0;
        {
            int intended_count = pattern_target;
            int seed = Cfg::get<int>("q.gen.expand.seed");
            if (seed > 0 && (int)Q.size() > seed) expand_seed_used = seed;
            this->expand_queries(Q, intended_count);
        }

        if(!Q.empty())
        {
            string qname = "qcnt" + to_string(Q.size());
            if (expand_seed_used > 0)
            {
                int mrate = Cfg::get<int>("q.gen.expand.mutation-rate-pct");
                qname += "s" + to_string(expand_seed_used) + "m" + to_string(mrate);
            }
            qname += "-" + effective_pattern;
            Qx.emplace_back(move(qname), move(Q));
        }
    }

    if (do_combine)
    {
        vector<RangeIRQuery> combined_queries;
        for (const auto &[qname, queries] : Qx)
            combined_queries.insert(combined_queries.end(), queries.begin(), queries.end());

        // Build name: qcnt<total><exp_tag>-NNNpat1+MMMpat2
        string combined_name;
        if (!combine_allocs.empty())
        {
            // Multiple patterns combined: build ratio-prefixed composite name
            string pattern_part;
            for (size_t i = 0; i < this->patterns.size(); ++i)
            {
                if (i > 0) pattern_part += '+';
                // Strip any inline ratio prefix to show just the bare name
                auto [pfx_c, bare_c] = parse_subpattern(this->patterns[i]);
                pattern_part += to_string(combine_allocs[i]) + bare_c;
            }
            int exp_seed_r = Cfg::get<int>("q.gen.expand.seed");
            string exp_tag_r = exp_seed_r > 0
                ? "s" + to_string(exp_seed_r) + "m"
                  + to_string(Cfg::get<int>("q.gen.expand.mutation-rate-pct"))
                : "";
            combined_name = "qcnt" + to_string(combined_queries.size())
                + exp_tag_r + "-" + pattern_part;
        }
        else
        {
            // Single pattern entry: the per-pattern qname is already correct.
            combined_name = get<0>(Qx[0]);
        }

        if (Cfg::get<bool>("q.gen.shuffle-workloads"))
        {
            mt19937 shuf_rng(42u);
            shuffle(combined_queries.begin(), combined_queries.end(), shuf_rng);
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