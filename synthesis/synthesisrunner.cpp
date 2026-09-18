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

#include "synthesisrunner.h"
#include "../utils/global.h"
#include "../utils/logging.h"
#include "../utils/persistence.h"
#include "../utils/processexec.h"
#include "../learning/statsserializer.h"
#include "../learning/statscomp.h"
#include "../generation/igen.h"
#include "../structure/idxschemaencoder.h"
#include "../structure/bandedsynthdex.h"
#include <regex>

using namespace std;

SynthesisRunner::SynthesisRunner(
    const vector<tuple<string, vector<RangeIRQuery>>>& queries,
    StatsComp& stats_comp,
    const vector<OStats>& per_q_ostats,
    const vector<string>& groups,
    const vector<IdxSchema>& custom_templates
) : Q(queries), statscomp(stats_comp), per_q_ostats(per_q_ostats),
    groups(groups), custom_templates(custom_templates)
{
}


const IdxSchemaSuggestion* SynthesisRunner::select_size_aware(
    const vector<IdxSchemaSuggestion>& suggestions)
{
    if (suggestions.empty()) return nullptr;

    auto fastest = min_element(suggestions.begin(), suggestions.end(),
        [](const IdxSchemaSuggestion& a, const IdxSchemaSuggestion& b)
        { return a.predicted_performance < b.predicted_performance; });

    double size_delta = Cfg::get<double>("synthesis.size-delta");
    if (size_delta < 0.0 || size_delta >= 1.0)
        throw runtime_error("synthesis.size-delta must be in [0, 1)");
    if (size_delta <= 0.0) return &*fastest;

    double threshold = fastest->predicted_performance
        - log10(1.0 - size_delta);
    auto selected = fastest;

    for (auto candidate = suggestions.begin(); candidate != suggestions.end();
         ++candidate)
    {
        if (candidate->predicted_performance > threshold)
            continue;

        if (candidate->predicted_size < selected->predicted_size)
            selected = candidate;
    }

    return &*selected;
}


map<string, vector<IdxSchemaSuggestion>> SynthesisRunner::run()
{
    vector<string> path_OQ_stats = this->generate_oq_statistics();
    
    string path_I_stats = this->generate_i_statistics();
    
    ProcessExec::python_setup(false);

    this->execute_synthesis_variants(path_I_stats, path_OQ_stats);

    return this->process_suggestions(path_OQ_stats);
}


map<string, vector<IdxSchemaSuggestion>> SynthesisRunner::process_suggestions(
    const vector<string>& path_OQ_stats)
{
    map<string, vector<IdxSchemaSuggestion>> suggestions_map;

    // Rebuild the design space to recover the original template schemas.
    // The i_template field in each CSV row is the row-index (0, 2, 4, ...) in the
    // I-statistics file, which is written in the same order as compute_design_space().
    // Dividing by 2 gives the 0-based index into the design_space vector.
    // This mapping lets us copy the terminal variant letters (a/b/c) that
    // IdxSchemaEncoder::encode drops, they are structural choices fixed by the
    // template and must be restored after decode().
    auto design_space = this->custom_templates.empty()
        ? IGen(this->groups).compute_design_space()
        : IGen(this->custom_templates).compute_design_space();

    for (const auto& oq_file : path_OQ_stats)
    {
        vector<IdxSchemaSuggestion> suggestions;

        string isynth_file = oq_file;
        size_t pos = isynth_file.find(".OQ.csv");
        if (pos != string::npos)
            isynth_file.replace(pos, 7, ".I-synthesis.csv");

        pos = isynth_file.find("/OQ/");
        if (pos != string::npos)
            isynth_file.replace(pos, 4, "/I-synthesis/");

        auto suggestion_raw = Persistence::read_I_synthesis_csv(isynth_file);
        
        for (const auto& [metadata, encoding] : suggestion_raw)
        {
            IdxSchemaSuggestion suggestion;
            suggestion.variant = metadata[0];          // Column 0: variant name
            suggestion.oq_file = metadata[1];          // Column 1: OQ file path
            suggestion.temporal_band = metadata[2];    // Column 2: temporal band
            suggestion.i_template = metadata[3];       // Column 3: template_id
            suggestion.synth_id = stoi(metadata[4]);   // Column 4: synth_id
            suggestion.predicted_performance = metadata[5].empty() ? 0.0 : stod(metadata[5]);
            suggestion.predicted_size = metadata[6].empty() ? 0.0 : stod(metadata[6]);
            suggestion.idxschema = IdxSchemaEncoder::decode(encoding);

            // Restore the terminal variant (a/b/c) that was lost during encoding.
            // i_template is the I-statistics row index (0, 2, 4, ...); dividing by 2
            // gives the matching entry in design_space.
            if (!suggestion.i_template.empty())
            {
                int tmpl_idx = stoi(suggestion.i_template) / 2;
                if (tmpl_idx >= 0 && tmpl_idx < (int)design_space.size())
                {
                    const auto& [min_tmpl, max_tmpl] = design_space[tmpl_idx];
                    IdxSchemaEncoder::apply_template_variants(
                        suggestion.idxschema, max_tmpl);
                }
            }

            // Extract setting from oq_file
            auto oq_file_name = suggestion.oq_file;
            size_t last_slash = oq_file_name.find_last_of("/\\");
            if (last_slash != string::npos)
                oq_file_name = oq_file_name.substr(last_slash + 1);

            vector<string> parts;
            size_t start = 0, end = 0;
            while ((end = oq_file_name.find('.', start)) != string::npos)
            {
                parts.push_back(oq_file_name.substr(start, end - start));
                start = end + 1;
            }
            parts.push_back(oq_file_name.substr(start));

            if (parts.size() > 2) parts.resize(parts.size() - 2);

            if (parts.size() > 2 && parts[2].substr(0, 6) == "Q-rnd_")
                parts[2] = parts[2].substr(6);

            suggestion.workload = parts;
            
            suggestions.push_back(suggestion);
        }

        sort(suggestions.begin(), suggestions.end(),
            [](const IdxSchemaSuggestion& a, const IdxSchemaSuggestion& b)
            { return a.predicted_performance < b.predicted_performance; });

        suggestions_map.emplace(oq_file, suggestions);

        for (auto& suggestion : suggestions)
            Log::w(2, "Suggestion", suggestion.str());
    }

    return suggestions_map;
}


vector<string> SynthesisRunner::generate_oq_statistics()
{
    vector<string> path_OQ_statss;
    
    auto original_Ostats = this->statscomp.Ostats;

    for (size_t qi = 0; qi < this->Q.size(); qi++)
    {
        auto& Q = this->Q[qi];

        // Use per-Q OStats if available (batched temporal slicing)
        if (!this->per_q_ostats.empty())
            this->statscomp.Ostats = this->per_q_ostats[qi];

        // Sample queries before analysis
        auto queries = get<1>(Q);
        int sample_size = Cfg::get<int>("synthesis.sample-size");
        
        vector<RangeIRQuery> sampled_queries;
        if (sample_size >= (int)queries.size())
        {
            sampled_queries = queries;
        }
        else
        {
            vector<size_t> indices(queries.size());
            iota(indices.begin(), indices.end(), 0);
            random_shuffle(indices.begin(), indices.end());
            
            for (int i = 0; i < sample_size; ++i)
                sampled_queries.push_back(queries[indices[i]]);
        }

        // Clear Qstats before each Q so that per-band analyses from a preceding
        // banded-group phase (Phase 1) do not contaminate the stats written for
        // a subsequent non-banded-group phase (Phase 2).  This mirrors the
        // clear/analyze/clear pattern used in predict().
        this->statscomp.Qstats.clear();
        this->statscomp.analyze_Q(sampled_queries);

        auto name = Persistence::compose_name(this->statscomp.Ostats, Q);

        auto path_OQ_stats = Persistence::write_OQ_stats_csv(
            this->statscomp.Ostats, this->statscomp.Qstats, name);

        this->statscomp.Qstats.clear();

        path_OQ_statss.push_back(path_OQ_stats);
    }

    // Restore original OStats
    if (!this->per_q_ostats.empty())
        this->statscomp.Ostats = original_Ostats;

    Log::w(2, "Generated OQ statistics files", strs(path_OQ_statss));
    return path_OQ_statss;
}


string SynthesisRunner::generate_i_statistics()
{
    Log::w(0, "Design Space");

    auto design_space = this->custom_templates.empty()
        ? IGen(this->groups).compute_design_space()
        : IGen(this->custom_templates).compute_design_space();

    vector<iStats> istatss;
    for (const auto& [min_i, max_i] : design_space) {
        auto istats_min = this->statscomp.analyze_i(min_i, "design-space-min");
        auto istats_max = this->statscomp.analyze_i(max_i, "design-space-max");

        istatss.push_back(istats_min);
        istatss.push_back(istats_max);

        Log::w(2, "i pattern (encoded) ",
            StatsSerializer::to_csv_header(istats_max, false) 
            + "\n" + StatsSerializer::to_csv(istats_max, false)
            + "\n" + StatsSerializer::to_csv(istats_min, false));
    }

    auto path_I_stats = Persistence::write_I_stats_csv(istatss);

    Log::w(2, "Generated I statistics file", path_I_stats);
    return path_I_stats;
}


void SynthesisRunner::execute_synthesis_variants(
    const string& path_I_stats,
    const vector<string>& path_OQ_stats)
{
    vector<string> quoted_paths;
    for (const auto& path : path_OQ_stats)
        quoted_paths.push_back("\"" + path + "\"");
    
    auto path_OQ_stats_str = strs(quoted_paths, " ");

    auto variants = Cfg::get<vector<string>>("synthesis.variant");

    for (const auto& v : variants)
    {
        Log::w(0, "Synthesis");
        Log::w(1, "Optimization runner", "sy_" + v + ".py");

        auto start = chrono::high_resolution_clock::now();

        ProcessExec::python_run("Inference",
            "./synthesis/sy_" + v + ".py \"" + Cfg::config_path
            + "\" \"" + Cfg::get_out_dir() + "\" \"" + path_I_stats + "\" " + path_OQ_stats_str, 1);

        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::seconds>(end - start).count();

        Log::w(1, "Synthesis duration (s)", to_string(duration));
    }
}


vector<BandedSuggestion> SynthesisRunner::merge_best_per_band(
    const map<string, vector<IdxSchemaSuggestion>>& result,
    const map<int, size_t>& band_query_counts)
{
    // Group suggestions by temporal band before applying the final selection policy.
    map<string, vector<const IdxSchemaSuggestion*>> candidates_per_band;
    for (const auto& [oq_file, suggestions] : result)
    {
        for (const auto& s : suggestions)
        {
            if (!s.temporal_band.empty())
                candidates_per_band[s.temporal_band].push_back(&s);
        }
    }

    // Select the smallest candidate within the throughput budget for each band.
    map<string, const IdxSchemaSuggestion*> best_per_band;
    double size_delta = Cfg::get<double>("synthesis.size-delta");
    if (size_delta < 0.0 || size_delta >= 1.0)
        throw runtime_error("synthesis.size-delta must be in [0, 1)");

    for (const auto& [band, candidates] : candidates_per_band)
    {
        auto fastest = *min_element(candidates.begin(), candidates.end(),
            [](const auto* a, const auto* b)
            { return a->predicted_performance < b->predicted_performance; });
        const auto* selected = fastest;

        if (size_delta > 0.0)
        {
            double threshold = fastest->predicted_performance
                - log10(1.0 - size_delta);

            for (const auto* candidate : candidates)
            {
                if (candidate->predicted_performance <= threshold
                    && candidate->predicted_size < selected->predicted_size)
                    selected = candidate;
            }
        }

        best_per_band[band] = selected;
    }

    if (best_per_band.empty()) return {};

    // Parse absolute timestamps from band strings and normalize to [0,1]
    regex band_re(R"(slice\d+\[(\d+)-(\d+)\))");

    struct BandEntry {
        Timestamp abs_start; Timestamp abs_end;
        const IdxSchemaSuggestion* suggestion;
        string temporal_band;
    };
    vector<BandEntry> entries;

    for (const auto& [band_str, suggestion] : best_per_band)
    {
        smatch match;
        if (!regex_search(band_str, match, band_re))
        {
            Log::w(1, "Warning: could not parse temporal band", band_str);
            continue;
        }

        entries.push_back({
            stoull(match[1].str()),
            stoull(match[2].str()),
            suggestion,
            band_str});
    }

    Timestamp domain = 0;
    for (const auto& e : entries)
        domain = max(domain, e.abs_end);

    if (domain == 0) return {};

    // Normalize to [0,1] and sort by start
    struct NormEntry {
        double start_rel; double end_rel;
        BandedSuggestion bs;
    };
    vector<NormEntry> normed;
    normed.reserve(entries.size());

    for (auto& e : entries)
    {
        double start_rel = static_cast<double>(e.abs_start) / domain;
        double end_rel = static_cast<double>(e.abs_end) / domain;

        BandedSuggestion bs;
        bs.schema = e.suggestion->idxschema;
        bs.predicted_performance = e.suggestion->predicted_performance;
        bs.predicted_size = e.suggestion->predicted_size;

            // Look up query count by stable slice id. Timestamp formatting can
            // differ between the partition name and the synthesized suggestion.
            regex slice_id_re(R"(slice(\d+))");
            smatch slice_match;
            if (regex_search(e.temporal_band, slice_match, slice_id_re))
            {
                int slice_id = stoi(slice_match[1].str());
                auto qit = band_query_counts.find(slice_id);
                bs.query_count = (qit != band_query_counts.end()) ? qit->second : 0;
            }
            else
            {
                bs.query_count = 0;
            }

        ostringstream tb;
        tb << fixed << start_rel << "-" << end_rel;
        bs.schema.band_range = tb.str();

        normed.push_back({start_rel, end_rel, move(bs)});
    }

    sort(normed.begin(), normed.end(),
        [](const NormEntry& a, const NormEntry& b)
        { return a.start_rel < b.start_rel; });

    vector<BandedSuggestion> merged;
    merged.reserve(normed.size());
    for (auto& e : normed)
        merged.push_back(move(e.bs));

    return merged;
}


BandedResult SynthesisRunner::process_banded(
    const map<string, vector<IdxSchemaSuggestion>>& suggestions,
    const vector<tuple<string, vector<RangeIRQuery>>>& batched_Q,
    const vector<pair<Timestamp, Timestamp>>& all_slice_bounds,
    Timestamp domain)
{
    // Build slice id -> query_count map from batched Q names.
    map<int, size_t> band_query_counts;
    {
        regex slice_re(R"(slice(\d+))");
        for (const auto& [name, qs] : batched_Q)
        {
            smatch m;
            if (regex_search(name, m, slice_re))
                band_query_counts[stoi(m[1].str())] += qs.size();
        }
    }

    auto banded = merge_best_per_band(suggestions, band_query_counts);
    if (banded.empty()) return {};

    // Extract schemas for gap-filling
    vector<IdxSchema> banded_schemas;
    banded_schemas.reserve(banded.size());
    for (const auto& bs : banded)
        banded_schemas.push_back(bs.schema);

    // Fill gaps: slices with no queries were skipped during synthesis.
    // Clone the nearest band's schema for each uncovered slice.
    if (!banded_schemas.empty() && !all_slice_bounds.empty())
    {
        if (domain == 0)
        {
            for (const auto& [a, b] : all_slice_bounds)
                domain = max(domain, b);
        }
        if (domain == 0) domain = 1;

        for (const auto& [abs_start, abs_end] : all_slice_bounds)
        {
            double cl_start = static_cast<double>(abs_start) / domain;
            double cl_end = static_cast<double>(abs_end) / domain;

            bool covered = false;
            for (const auto& bs : banded_schemas)
            {
                double bs_start, bs_end;
                BandedSynthDex::parse_band_range(
                    bs.band_range, bs_start, bs_end);
                if (abs(bs_start - cl_start) < 1e-6
                    && abs(bs_end - cl_end) < 1e-6)
                { covered = true; break; }
            }

            if (!covered)
            {
                // Find adjacent neighbor: prefer left (preceding)
                // band whose end touches this gap's start, else
                // right (following) band whose start touches this
                // gap's end, else fallback to nearest midpoint.
                int left_idx = -1, right_idx = -1;
                size_t fallback = 0;
                double min_dist = numeric_limits<double>::max();
                for (size_t i = 0; i < banded_schemas.size(); ++i)
                {
                    double bs_start, bs_end;
                    BandedSynthDex::parse_band_range(
                        banded_schemas[i].band_range,
                        bs_start, bs_end);
                    if (abs(bs_end - cl_start) < 1e-6)
                        left_idx = static_cast<int>(i);
                    if (abs(bs_start - cl_end) < 1e-6)
                        right_idx = static_cast<int>(i);
                    double dist = abs(
                        (bs_start + bs_end) / 2.0
                        - (cl_start + cl_end) / 2.0);
                    if (dist < min_dist)
                    { min_dist = dist; fallback = i; }
                }
                size_t donor = (left_idx >= 0) ? left_idx
                    : (right_idx >= 0) ? right_idx : fallback;

                IdxSchema fill = banded_schemas[donor];
                ostringstream tb;
                tb << fixed << cl_start << "-" << cl_end;
                fill.band_range = tb.str();
                Log::w(1, "Gap-fill band", fill.band_range
                    + " (cloned from " + banded_schemas[donor].band_range + ")");
                banded_schemas.push_back(move(fill));
            }
        }

        // Re-sort by start after gap-filling
        sort(banded_schemas.begin(), banded_schemas.end(),
            [](const IdxSchema& a, const IdxSchema& b)
            {
                double as, ae, bs, be;
                BandedSynthDex::parse_band_range(
                    a.band_range, as, ae);
                BandedSynthDex::parse_band_range(
                    b.band_range, bs, be);
                return as < bs;
            });
    }

    // Log per-band predicted metrics and compute combined metrics
    {
        double total_time = 0.0;
        size_t total_queries = 0;
        double total_size_bytes = 0.0;

        for (const auto& bs : banded)
        {
            double avg_spq = pow(10.0, bs.predicted_performance);
            total_time += bs.query_count * avg_spq;
            total_queries += bs.query_count;
            total_size_bytes += pow(10.0, bs.predicted_size);
        }

        if (total_queries > 0 && total_time > 0.0)
        {
            for (const auto& bs : banded)
            {
                double avg_spq = pow(10.0, bs.predicted_performance);
                double throughput_qps = (avg_spq > 0) ? 1.0 / avg_spq : 0.0;
                double size_mb = pow(10.0, bs.predicted_size)
                    / (1024.0 * 1024.0);
                double query_share_pct =
                    100.0 * static_cast<double>(bs.query_count)
                    / static_cast<double>(total_queries);

                Log::w(1, "  Band " + bs.schema.band_range,
                    "queries=" + to_string(bs.query_count)
                    + " query_share=" + to_string(query_share_pct) + "%"
                    + " throughput=" + to_string(throughput_qps) + " q/s"
                    + " size=" + to_string(size_mb) + " MB (predicted)");
            }

            double combined_qps = total_queries / total_time;
            double combined_size_mb = total_size_bytes / (1024.0 * 1024.0);
            Log::w(1, "Combined predicted throughput [q/s]", combined_qps);
            Log::w(1, "Combined predicted size [MB]", combined_size_mb);
        }
    }

    return { move(banded), move(banded_schemas) };
}


BandingPrep SynthesisRunner::prepare_banding(
    const IRelation& O, const string& file_O,
    const vector<tuple<string, vector<RangeIRQuery>>>& Q,
    StatsComp& statscomp,
    const vector<string>& groups)
{
    BandingPrep prep;

    // Collect distinct temporal partition configurations from active templates.
    set<string> bands_values;
    set<vector<string>> ranges_values;
    set<string> non_banded_group_names;
    set<string> banded_group_names;
    {
        auto resolved = IGen::resolve_active_templates(groups);
        for (const auto& [group, name] : resolved)
        {
            auto json = Cfg::get_json(
                "i.gen.design-space." + group + "." + name);
            auto schema = IdxSchemaSerializer::from_json(json);
            if (!schema.bands.empty() && !schema.ranges.empty())
                throw runtime_error(
                    "A temporal template cannot define both 'bands' and 'ranges'.");
            if (!schema.bands.empty())
            {
                bands_values.insert(schema.bands);
                banded_group_names.insert(group);
                auto inner = schema;
                inner.bands = "";
                inner.ranges.clear();
                prep.band_inner_templates.push_back(inner);
            }
            else if (!schema.ranges.empty())
            {
                ranges_values.insert(schema.ranges);
                banded_group_names.insert(group);
                auto inner = schema;
                inner.ranges.clear();
                prep.band_inner_templates.push_back(inner);
            }
            else
            {
                prep.has_non_banded_templates = true;
                non_banded_group_names.insert(group);
            }
        }

        // A group whose templates mix banded and non-banded schemas would be
        // evaluated twice (once in each phase) with incorrect template sets.
        // Enforce the invariant: every group is purely banded or purely non-banded.
        for (const auto& g : banded_group_names)
        {
            if (non_banded_group_names.count(g))
                throw runtime_error(
                    "Group '" + g + "' has both banded and non-banded templates "
                    "in the active design space. Groups must be uniformly banded or "
                    "non-banded for mixed-group synthesis to work correctly.");
        }
    }
    prep.non_banded_groups.assign(
        non_banded_group_names.begin(), non_banded_group_names.end());

    if (!bands_values.empty() && !ranges_values.empty())
        throw runtime_error(
            "Active temporal templates mix dynamic 'bands' and static 'ranges'. "
            "Use one temporal partitioning mode per synthesis run.");

    prep.has_banded_templates = !bands_values.empty() || !ranges_values.empty();
    if (!prep.has_banded_templates) return prep;

    // Flatten all queries for temporal slice computation.
    vector<RangeIRQuery> all_Q;
    for (const auto& [qname, queries] : Q)
        all_Q.insert(all_Q.end(), queries.begin(), queries.end());

    int sample_size = Cfg::get<int>("synthesis.sample-size");
    if (sample_size < (int)all_Q.size())
        all_Q.resize(sample_size);

    OStats full_stats;
    vector<OSliceStats> slices;
    if (!bands_values.empty())
    {
        // Dynamic temporal slices are shared by all dynamic band-count configs.
        tie(full_stats, slices) = statscomp.compute_temporal_slices(
            O, file_O, all_Q);
    }
    else
    {
        // Static ranges do not need slice analysis or clustering.
        full_stats = statscomp.analyze_O(O, file_O);
    }

    auto append_partitions = [&](auto &partitions)
    {
        for (auto& [cluster, Ox, Qx, part_stats] : partitions)
        {
            prep.all_slice_bounds.emplace_back(
                static_cast<Timestamp>(cluster.temporal_start),
                static_cast<Timestamp>(cluster.temporal_end));
            if (Qx.empty()) continue;
            prep.batched_Q.push_back({ part_stats.name, move(Qx) });
            prep.per_q_ostats.push_back(part_stats);
        }
    };

    // For each distinct dynamic bands config, cluster and produce per-band OQ data.
    for (const string& bands_str : bands_values)
    {
        int band_count = 0;
        bool auto_k = false;
        {
            string s = bands_str;
            if (!s.empty() && s.front() == '[') s = s.substr(1);
            if (!s.empty() && s.back() == ']') s.pop_back();
            auto dash = s.find('-');
            if (dash != string::npos)
            {
                band_count = stoi(s.substr(dash + 1));
                auto_k = true;
            }
            else
            {
                band_count = stoi(s);
            }
        }

        Log::w(0, "Temporal banding (bands=" + bands_str + ")");

        auto sliced = statscomp.cluster_and_partition(
            O, file_O, all_Q, slices, full_stats, band_count, auto_k);

        // cluster_and_partition calls analyze_O() for each band partition which
        // overwrites statscomp.Ostats with the last band's stats.  Restore the
        // full-collection value so that any synthesis using statscomp after this
        // call (generate_i_statistics, Phase 2 non-banded synthesis, etc.) still
        // sees the correct full-collection statistics.
        statscomp.Ostats = full_stats;
        append_partitions(sliced);
    }

    for (const auto &ranges : ranges_values)
    {
        Log::w(0, "Temporal banding (static ranges)",
            IdxSchemaSerializer::to_json_line(IdxSchema{.ranges = ranges}));

        auto partitioned = statscomp.partition_by_ranges(
            O, file_O, all_Q, ranges);
        statscomp.Ostats = full_stats;
        append_partitions(partitioned);
    }

    return prep;
}


vector<OStats> SynthesisRunner::compute_per_band_ostats(
    const vector<IdxSchema>& banded_schemas,
    const IRelation& O, StatsComp& statscomp)
{
    vector<OStats> per_band_ostats;
    for (const auto& bs : banded_schemas)
    {
        double start_rel, end_rel;
        BandedSynthDex::parse_band_range(bs.band_range, start_rel, end_rel);
        Timestamp abs_start = static_cast<Timestamp>(start_rel * O.gend);
        Timestamp abs_end = static_cast<Timestamp>(end_rel * O.gend);

        IRelation Ox;
        for (const auto& rec : O)
        {
            if (rec.start < abs_end && rec.end > abs_start)
            {
                IRecord lr(rec.id,
                    max(rec.start, abs_start) - abs_start,
                    min(rec.end, abs_end) - abs_start);
                lr.elements = rec.elements;
                Ox.push_back(lr);
            }
        }
        Ox.determineDomain();
        per_band_ostats.push_back(
            statscomp.analyze_O(Ox, "band-" + bs.band_range));
    }
    return per_band_ostats;
}