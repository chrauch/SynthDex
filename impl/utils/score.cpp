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

#include "score.h"
#include "persistence.h"
#include "logging.h"
#include "cfg.h"
#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <utility>
#include <dirent.h>
#include <cstring>

using namespace std;


Score::Score()
{
}


vector<ScoreEntry> Score::load_scores(const string &file_path)
{
    // Read CSV file using Persistence class
    auto data = Persistence::read_score_csv(file_path);
    
    vector<ScoreEntry> scores;
    
    for (const auto &row : data)
    {
        const auto &strings = get<0>(row);
        const auto &doubles = get<1>(row);
        
        if (strings.size() >= 2 && doubles.size() >= 4)
        {
            ScoreEntry entry;
            entry.objects = strings[0];
            entry.queries = strings[1];
            entry.category = strings.size() > 2 ? strings[2] : "";
            entry.schema = strings.size() > 3 ? strings[3] : "";
            
            entry.xor_result = doubles[0];
            entry.size_mb = doubles[1];
            entry.construction_s = doubles[2];
            entry.throughput_qps = doubles[3];
            
            // Fill template from schema
            fill_template(entry);
            
            scores.push_back(entry);
        }
    }
    
    return scores;
}


vector<ScoreEntry> Score::filter_scores(const vector<ScoreEntry> &scores, const string &filter)
{
    if (filter.empty())
    {
        return scores;
    }
    
    // Helper to split by comma
    auto split_by_comma = [](const string &str) -> vector<string> {
        vector<string> result;
        if (str.empty()) return result;
        
        stringstream ss(str);
        string item;
        while (getline(ss, item, ','))
        {
            // Trim whitespace
            size_t start = item.find_first_not_of(" \t");
            size_t end = item.find_last_not_of(" \t");
            if (start != string::npos && end != string::npos)
            {
                result.push_back(item.substr(start, end - start + 1));
            }
            else if (start != string::npos)
            {
                result.push_back(item.substr(start));
            }
        }
        return result;
    };
    
    // Parse filter: format example: "Object1,Object2|Query1,Query2"
    vector<string> objects_filters;
    vector<string> queries_filters;
    bool use_or_logic = false;
    
    size_t pipe_pos = filter.find('|');
    if (pipe_pos != string::npos)
    {
        // Split by pipe - use AND logic (both must match)
        string objects_part = filter.substr(0, pipe_pos);
        string queries_part = filter.substr(pipe_pos + 1);
        
        objects_filters = split_by_comma(objects_part);
        queries_filters = split_by_comma(queries_part);
        use_or_logic = false;
    }
    else
    {
        // No pipe - use OR logic (applies to both objects and queries)
        auto terms = split_by_comma(filter);
        objects_filters = terms;
        queries_filters = terms;
        use_or_logic = true;
    }
    
    // Filter scores
    vector<ScoreEntry> filtered_scores;
    for (const auto &score : scores)
    {
        // Check if any objects filter matches
        bool match_objects = objects_filters.empty();
        for (const auto &obj_filter : objects_filters)
        {
            if (score.objects.find(obj_filter) != string::npos)
            {
                match_objects = true;
                break;
            }
        }
        
        // Check if any queries filter matches
        bool match_queries = queries_filters.empty();
        for (const auto &qry_filter : queries_filters)
        {
            if (score.queries.find(qry_filter) != string::npos)
            {
                match_queries = true;
                break;
            }
        }
        
        bool include = use_or_logic ? (match_objects || match_queries) : (match_objects && match_queries);
        
        if (include)
        {
            filtered_scores.push_back(score);
        }
    }
    
    return filtered_scores;
}


string Score::extract_method_template(const string &method)
{
    // Split method by semicolon and remove numeric values from the end
    vector<string> parts;
    stringstream ss(method);
    string part;
    
    while (getline(ss, part, ';'))
    {
        parts.push_back(part);
    }
    
    // Remove numeric values from the end (doubles and integers)
    while (!parts.empty())
    {
        const string &last = parts.back();
        
        // Check if it's a number (integer or double)
        bool is_number = true;
        bool has_dot = false;
        bool has_digit = false;
        
        for (size_t i = 0; i < last.length(); i++)
        {
            char c = last[i];
            if (c == '.' || c == '-' || c == '+')
            {
                if (c == '.') has_dot = true;
            }
            else if (isdigit(c))
            {
                has_digit = true;
            }
            else
            {
                is_number = false;
                break;
            }
        }
        
        // Only remove if it's actually a number with digits
        if (is_number && has_digit)
        {
            parts.pop_back();
        }
        else
        {
            break;
        }
    }
    
    // Rebuild the method string without numeric values
    string result;
    for (size_t i = 0; i < parts.size(); i++)
    {
        if (i > 0) result += ";";
        result += parts[i];
    }
    
    return result;
}


void Score::fill_template(ScoreEntry &entry)
{
    const string &schema = entry.schema;
    
    if (schema.empty())
    {
        entry.templatex = "";
        return;
    }
    
    // Check if schema contains fanout structure
    size_t fanout_pos = schema.find("\"fanout\":");
    
    if (fanout_pos != string::npos)
    {
        // Determine prefix based on schema type
        string prefix;
        if (schema.find("\"refine\":\"elemfreq\"") != string::npos)
        {
            prefix = "dref-p-";
        }
        else if (schema.find("\"split\":\"temporal\"") != string::npos)
        {
            prefix = "tspl-p-";
        }
        else if (schema.find("\"slice\":\"temporal\"") != string::npos)
        {
            prefix = "tsli-p-";
        }
        else
        {
            throw runtime_error("Unknown fanout schema: " + schema);
        }
        
        // Extract all methods from fanout array
        vector<string> methods;
        size_t search_pos = fanout_pos;
        
        while (true)
        {
            size_t method_pos = schema.find("\"method\":\"", search_pos);
            if (method_pos == string::npos) break;
            
            // Find the closing quote
            size_t start = method_pos + 10; // Length of "method":"
            size_t end = schema.find("\"", start);
            if (end == string::npos) break;
            
            string method = schema.substr(start, end - start);
            methods.push_back(extract_method_template(method));
            
            search_pos = end + 1;
        }
        
        // Build template string with fanout number
        if (!methods.empty())
        {
            entry.templatex = prefix + to_string(methods.size()) + ": { ";
            for (size_t i = 0; i < methods.size(); i++)
            {
                if (i > 0) entry.templatex += " | ";
                entry.templatex += methods[i];
            }
            entry.templatex += " }";
        }
        else
        {
            throw runtime_error("No methods extracted: " + schema);
        }
    }
    else
    {
        // Simple method case (not a fanout): {"method":"..."}
        size_t method_pos = schema.find("\"method\":\"");
        if (method_pos != string::npos)
        {
            size_t start = method_pos + 10; // Length of "method":"
            size_t end = schema.find("\"", start);
            if (end != string::npos)
            {
                string method = schema.substr(start, end - start);
                entry.templatex = "pure-unp: " + extract_method_template(method);
            }
            else
            {
                throw runtime_error("No method extracted: " + schema);
            }
        }
        else
        {
            throw runtime_error("No method found: " + schema);
        }
    }
}


map<pair<string, string>, vector<ScoreEntry>> Score::group_by_objects_queries(const vector<ScoreEntry> &scores)
{
    map<pair<string, string>, vector<ScoreEntry>> groups;
    
    for (const auto &entry : scores)
    {
        groups[make_pair(entry.objects, entry.queries)].push_back(entry);
    }
    
    return groups;
}


vector<ScoreEntry> Score::process_group(const vector<ScoreEntry> &group, bool apply_skyline_filter)
{
    if (group.empty())
    {
        return group;
    }
    
    vector<ScoreEntry> processed_group = group;
    
    // Apply skyline filtering if requested
    if (apply_skyline_filter)
    {
        // Group entries by templatex within this Objects+Queries group
        map<string, vector<ScoreEntry>> template_groups;
        for (const auto &entry : group)
        {
            template_groups[entry.templatex].push_back(entry);
        }
        
        processed_group.clear();
        
        // Apply skyline algorithm to each template group
        for (auto &[templatex, tpl_group] : template_groups)
        {
            vector<ScoreEntry> skyline_group;
            
            for (const auto &candidate : tpl_group)
            {
                bool is_dominated = false;
                
                // Check if candidate is dominated by any other entry in the template group
                for (const auto &other : tpl_group)
                {
                    if (&candidate == &other) continue;
                    
                    // Entry 'other' dominates 'candidate' if:
                    // - other.throughput_qps >= candidate.throughput_qps AND
                    // - other.size_mb <= candidate.size_mb AND
                    // - at least one is strict inequality
                    
                    bool throughput_better_or_equal = other.throughput_qps >= candidate.throughput_qps;
                    bool size_better_or_equal = other.size_mb <= candidate.size_mb;
                    bool strictly_better = (other.throughput_qps > candidate.throughput_qps) || 
                                          (other.size_mb < candidate.size_mb);
                    
                    if (throughput_better_or_equal && size_better_or_equal && strictly_better)
                    {
                        is_dominated = true;
                        break;
                    }
                }
                
                // If not dominated, add to skyline
                if (!is_dominated)
                {
                    skyline_group.push_back(candidate);
                }
            }
            
            // Add all skyline entries from this template group to results
            processed_group.insert(processed_group.end(), skyline_group.begin(), skyline_group.end());
        }
    }
    
    // Compute relative scores within this Objects+Queries group
    if (!processed_group.empty())
    {
        // Find the entry with the highest throughput in this group
        double max_throughput = 0.0;
        double reference_size = 0.0;
        double reference_construction = 0.0;
        
        for (const auto &entry : processed_group)
        {
            if (entry.throughput_qps > max_throughput)
            {
                max_throughput = entry.throughput_qps;
                reference_size = entry.size_mb;
                reference_construction = entry.construction_s;
            }
        }
        
        // Compute relative scores for all entries in this group
        for (auto &entry : processed_group)
        {
            if (max_throughput > 0.0)
            {
                entry.throughput_qps_relative = (entry.throughput_qps / max_throughput) * 100.0;
            }
            else
            {
                entry.throughput_qps_relative = 0.0;
            }
            
            if (reference_size > 0.0)
            {
                entry.size_mb_relative = (entry.size_mb / reference_size) * 100.0;
            }
            else
            {
                entry.size_mb_relative = 0.0;
            }
            
            if (reference_construction > 0.0)
            {
                entry.construction_s_relative = (entry.construction_s / reference_construction) * 100.0;
            }
            else
            {
                entry.construction_s_relative = 0.0;
            }
        }
    }
    
    // Sort by throughput (descending) within the group
    sort(processed_group.begin(), processed_group.end(),
        [](const ScoreEntry &a, const ScoreEntry &b) {
            return a.throughput_qps > b.throughput_qps;
        });
    
    return processed_group;
}


vector<ScoreEntry> Score::apply_skyline(const vector<ScoreEntry> &scores)
{
    // This method is now deprecated in favor of group-based processing
    // but kept for backward compatibility if needed
    if (scores.empty())
    {
        return scores;
    }
    
    // Group entries by templatex
    map<string, vector<ScoreEntry>> groups;
    for (const auto &entry : scores)
    {
        groups[entry.templatex].push_back(entry);
    }
    
    vector<ScoreEntry> skyline_results;
    
    // Apply skyline algorithm to each group
    for (auto &[templatex, group] : groups)
    {
        vector<ScoreEntry> skyline_group;
        
        for (const auto &candidate : group)
        {
            bool is_dominated = false;
            
            // Check if candidate is dominated by any other entry in the group
            for (const auto &other : group)
            {
                if (&candidate == &other) continue;
                
                // Entry 'other' dominates 'candidate' if:
                // - other.throughput_qps >= candidate.throughput_qps AND
                // - other.size_mb <= candidate.size_mb AND
                // - at least one is strict inequality
                
                bool throughput_better_or_equal = other.throughput_qps >= candidate.throughput_qps;
                bool size_better_or_equal = other.size_mb <= candidate.size_mb;
                bool strictly_better = (other.throughput_qps > candidate.throughput_qps) || 
                                      (other.size_mb < candidate.size_mb);
                
                if (throughput_better_or_equal && size_better_or_equal && strictly_better)
                {
                    is_dominated = true;
                    break;
                }
            }
            
            // If not dominated, add to skyline
            if (!is_dominated)
            {
                skyline_group.push_back(candidate);
            }
        }
        
        // Add all skyline entries from this group to results
        skyline_results.insert(skyline_results.end(), skyline_group.begin(), skyline_group.end());
    }
    
    // Sort combined results by throughput (descending)
    sort(skyline_results.begin(), skyline_results.end(),
        [](const ScoreEntry &a, const ScoreEntry &b) {
            return a.throughput_qps > b.throughput_qps;
        });
    
    return skyline_results;
}


void Score::compute_relative_scores(vector<ScoreEntry> &scores)
{
    if (scores.empty())
    {
        return;
    }
    
    // Find the entry with the highest throughput (top score)
    double max_throughput = 0.0;
    double reference_size = 0.0;
    double reference_construction = 0.0;
    
    for (const auto &entry : scores)
    {
        if (entry.throughput_qps > max_throughput)
        {
            max_throughput = entry.throughput_qps;
            reference_size = entry.size_mb;
            reference_construction = entry.construction_s;
        }
    }
    
    // Compute relative scores for all entries
    for (auto &entry : scores)
    {
        if (max_throughput > 0.0)
        {
            entry.throughput_qps_relative = (entry.throughput_qps / max_throughput) * 100.0;
        }
        else
        {
            entry.throughput_qps_relative = 0.0;
        }
        
        if (reference_size > 0.0)
        {
            entry.size_mb_relative = (entry.size_mb / reference_size) * 100.0;
        }
        else
        {
            entry.size_mb_relative = 0.0;
        }
        
        if (reference_construction > 0.0)
        {
            entry.construction_s_relative = (entry.construction_s / reference_construction) * 100.0;
        }
        else
        {
            entry.construction_s_relative = 0.0;
        }
    }
}


void Score::print_scores(const vector<ScoreEntry> &scores)
{
    if (scores.empty())
    {
        Log::w(0, "No scores found");
        return;
    }
    
    // Sort by Objects, Queries, ThroughputQpS (descending)
    auto sorted_scores = scores;
    sort(sorted_scores.begin(), sorted_scores.end(),
        [](const ScoreEntry &a, const ScoreEntry &b) {
            if (a.objects != b.objects)
                return a.objects < b.objects;
            if (a.queries != b.queries)
                return a.queries < b.queries;
            return a.throughput_qps > b.throughput_qps; // Descending
        });
    
    // Track current groups for spacing
    string current_objects = "";
    string current_queries = "";
    bool is_first = true;
    
    // Print scores grouped by Objects+Queries
    for (size_t i = 0; i < sorted_scores.size(); ++i)
    {
        const auto &score = sorted_scores[i];
        
        // Check if Objects+Queries group changed
        if (score.objects != current_objects || score.queries != current_queries)
        {
            // Add spacing before new group (except for first)
            if (!is_first)
            {
                cout << "\n\n";
            }
            else
            {
                cout << "\n";
                is_first = false;
            }
            
            // Print header for this Objects+Queries group
            cout << "ThroughputQpS\tThroughput%\tSizeMB\tSize%\tConstructionS\tConstruction%\tObjects\tQueries\tFamily\tSchema\n";
            cout << "\n";
            
            current_objects = score.objects;
            current_queries = score.queries;
        }
        
        cout << fixed << setprecision(2) << score.throughput_qps << "\t"
             << fixed << setprecision(1) << score.throughput_qps_relative << "%\t"
             << fixed << setprecision(2) << score.size_mb << "\t"
             << fixed << setprecision(1) << score.size_mb_relative << "%\t"
             << fixed << setprecision(3) << score.construction_s << "\t"
             << fixed << setprecision(1) << score.construction_s_relative << "%\t"
             << score.objects << "\t"
             << score.queries << "\t"
             << setw(80) << left << score.templatex << "\t"
             << score.schema << "\n";
    }
    cout << "\n";
}


void Score::process_scores(const string &filter)
{
    string prefix = Cfg::get<string>("out.machine-prefix");

    Log::w(0, "Displaying scores");
    
    if (!filter.empty())
    {
        Log::w(1, "Filter", filter);
    }
    
    // Find all score files in the directory
    string score_dir = Cfg::get<string>("out.dir") + "/score";
    vector<string> score_files;
    
    DIR *dir = opendir(score_dir.c_str());
    if (dir != nullptr)
    {
        struct dirent *entry;
        while ((entry = readdir(dir)) != nullptr)
        {
            string filename = entry->d_name;
            // Check if file ends with .score.csv
            if (filename.length() > 10 && 
                filename.substr(filename.length() - 10) == ".score.csv")
            {
                score_files.push_back(score_dir + "/" + filename);
            }
        }
        closedir(dir);
    }
    
    // Sort files alphabetically for consistent processing
    sort(score_files.begin(), score_files.end());
    
    if (score_files.empty())
    {
        Log::w(0, "No score files found in " + score_dir);
        return;
    }
    
    // Log files to be processed at the beginning
    Log::w(1, "Found " + to_string(score_files.size()) + " score file(s) to process:");
    for (const string &file : score_files)
    {
        Log::w(1, "  - " + file);
    }
    
    // Parse mode and actual filter
    string mode = "skyline"; // default mode
    string actual_filter = filter;
    
    if (!filter.empty())
    {
        size_t colon_pos = filter.find(':');
        if (colon_pos != string::npos)
        {
            string mode_prefix = filter.substr(0, colon_pos);
            if (mode_prefix == "skyline" || mode_prefix == "complete")
            {
                mode = mode_prefix;
                actual_filter = filter.substr(colon_pos + 1);
            }
        }
    }
    
    Log::w(1, "Mode", mode);
    
    // Merge (concatenate) all score files into one vector
    vector<ScoreEntry> all_scores;
    vector<string> processed_files;
    
    for (const string &file_path : score_files)
    {
        Log::w(1, "Loading file", file_path);
        
        auto scores = load_scores(file_path);
        
        if (scores.empty())
        {
            Log::w(1, "No scores in file", file_path);
            continue;
        }
        
        Log::w(1, "Loaded scores from this file", to_string(scores.size()));
        
        // Concatenate to all_scores
        all_scores.insert(all_scores.end(), scores.begin(), scores.end());
        processed_files.push_back(file_path);
    }
    
    if (all_scores.empty())
    {
        Log::w(0, "No scores found in any file");
        return;
    }

    // Remove duplicate scores (keep the row with the highest throughput_qps)
    map<tuple<string, string, string, double, double, double>, ScoreEntry> best;

    for (const auto &score : all_scores)
    {
        auto key = make_tuple(score.objects, score.queries, score.schema,
                              score.xor_result, score.size_mb,
                              score.construction_s);

        auto it = best.find(key);
        if (it == best.end() || score.throughput_qps > it->second.throughput_qps)
        {
            best[key] = score;
        }
    }

    vector<ScoreEntry> deduplicated_scores;
    deduplicated_scores.reserve(best.size());
    for (auto &[key, score] : best)
    {
        deduplicated_scores.push_back(move(score));
    }

    all_scores = deduplicated_scores;
    
    Log::w(1, "Total merged scores", to_string(all_scores.size()));
    
    // Filter the merged scores
    auto filtered_scores = filter_scores(all_scores, actual_filter);
    
    if (filtered_scores.empty())
    {
        Log::w(0, "No scores found matching filter: " + filter);
        return;
    }
    
    Log::w(1, "Filtered scores", to_string(filtered_scores.size()));
    
    // Extract and log distinct Objects and Queries
    set<string> distinct_objects;
    set<string> distinct_queries;
    for (const auto &score : filtered_scores)
    {
        distinct_objects.insert(score.objects);
        distinct_queries.insert(score.queries);
    }
    
    Log::w(1, "Distinct Objects (" + to_string(distinct_objects.size()) + ")", 
           str(vector<string>(distinct_objects.begin(), distinct_objects.end())));
    Log::w(1, "Distinct Queries (" + to_string(distinct_queries.size()) + ")", 
           str(vector<string>(distinct_queries.begin(), distinct_queries.end())));
    
    // Group by Objects+Queries
    auto groups = group_by_objects_queries(filtered_scores);
    Log::w(1, "Number of Objects+Queries groups", to_string(groups.size()));
    
    // Process each group independently (skyline + relative scores)
    vector<ScoreEntry> final_scores;
    bool apply_skyline_filter = (mode == "skyline");
    
    for (const auto &[key, group] : groups)
    {
        auto processed_group = process_group(group, apply_skyline_filter);
        final_scores.insert(final_scores.end(), processed_group.begin(), processed_group.end());
    }
    
    if (apply_skyline_filter)
    {
        Log::w(1, "Skyline scores (across all groups)", to_string(final_scores.size()));
    }
    
    this->create_diagram_script(final_scores);
    this->create_text_table(final_scores);
    this->print_scores(final_scores);
    
    // Log all processed files to Log 1 at the end
    if (!processed_files.empty())
    {
        Log::w(1, "Successfully processed " + to_string(processed_files.size()) + " score file(s):");
        for (const string &file : processed_files)
        {
            Log::w(1, "  - " + file);
        }
    }
}


void Score::create_text_table(const vector<ScoreEntry> &scores)
{
    if (scores.empty()) return;

    string prefix = Cfg::get<string>("out.machine-prefix");
    string output_dir = Cfg::get<string>("out.dir") + "/plots";
    string table_file = output_dir + "/" + prefix + ".table.csv";

    ofstream out(table_file);
    if (!out.is_open())
    {
        Log::w(0, "Failed to create table file: " + table_file);
        return;
    }

    Log::w(1, "Creating text table", table_file);

    // Group by objects
    map<string, map<string, vector<ScoreEntry>>> by_obj_qry;
    for (const auto &e : scores)
        by_obj_qry[e.objects][e.queries].push_back(e);

    bool first_table = true;
    for (const auto &[objects, qry_map] : by_obj_qry)
    {
        if (!first_table) out << "\n\n";
        first_table = false;

        // Collect all distinct families (templatex) for this objects group,
        // in the order they first appear sorted by name
        set<string> family_set;
        for (const auto &[qry, entries] : qry_map)
            for (const auto &e : entries)
                family_set.insert(e.templatex);
        vector<string> families(family_set.begin(), family_set.end());

        // Header
        out << "=== " << objects << " ===\n";

        // Helper: find the winner entry (highest throughput) per (family, queries)
        auto find_winner = [&](const string &family,
                               const vector<ScoreEntry> &entries) -> const ScoreEntry*
        {
            const ScoreEntry* winner = nullptr;
            for (const auto &e : entries)
                if (e.templatex == family)
                    if (!winner || e.throughput_qps > winner->throughput_qps)
                        winner = &e;
            return winner;
        };

        // Table 1: throughput of the winner
        out << "--- Throughput [q/s] ---\n";
        out << "Queries";
        for (const auto &f : families) out << "\t" << f;
        out << "\n";
        for (const auto &[queries, entries] : qry_map)
        {
            out << queries;
            for (const auto &family : families)
            {
                out << "\t";
                const ScoreEntry* w = find_winner(family, entries);
                if (w) out << fixed << setprecision(2) << w->throughput_qps;
                else   out << "-";
            }
            out << "\n";
        }

        // Table 2: memory (size_mb) of the throughput winner
        out << "\n";
        out << "--- Memory [MB] ---\n";
        out << "Queries";
        for (const auto &f : families) out << "\t" << f;
        out << "\n";
        for (const auto &[queries, entries] : qry_map)
        {
            out << queries;
            for (const auto &family : families)
            {
                out << "\t";
                const ScoreEntry* w = find_winner(family, entries);
                if (w) out << fixed << setprecision(2) << w->size_mb;
                else   out << "-";
            }
            out << "\n";
        }

        // Table 3: construction time of the throughput winner
        out << "\n";
        out << "--- Construction [s] ---\n";
        out << "Queries";
        for (const auto &f : families) out << "\t" << f;
        out << "\n";
        for (const auto &[queries, entries] : qry_map)
        {
            out << queries;
            for (const auto &family : families)
            {
                out << "\t";
                const ScoreEntry* w = find_winner(family, entries);
                if (w) out << fixed << setprecision(3) << w->construction_s;
                else   out << "-";
            }
            out << "\n";
        }

        // Table 4: schema of the throughput winner
        out << "\n";
        out << "--- Schema ---\n";
        out << "Queries";
        for (const auto &f : families) out << "\t" << f;
        out << "\n";
        for (const auto &[queries, entries] : qry_map)
        {
            out << queries;
            for (const auto &family : families)
            {
                out << "\t";
                const ScoreEntry* w = find_winner(family, entries);
                if (w) out << w->schema;
                else   out << "-";
            }
            out << "\n";
        }
    }

    out.close();
}


void Score::create_diagram_script(const vector<ScoreEntry> &scores)
{
    if (scores.empty())
    {
        return;
    }
    
    // Helper function to escape quotes for gnuplot
    auto escape_quotes = [](const string &str) -> string {
        string result;
        for (char c : str)
        {
            if (c == '"')
                result += "\\\"";
            else
                result += c;
        }
        return result;
    };
    
    // Helper function to sanitize names for gnuplot data blocks
    auto sanitize_name = [](const string &str) -> string {
        string result;
        for (char c : str)
        {
            if (isalnum(c) || c == '_')
                result += c;
            else
                result += '_';
        }
        return result;
    };
    
    string prefix = Cfg::get<string>("out.machine-prefix");
    string output_dir = Cfg::get<string>("out.dir") + "/plots";
    string script_file = output_dir + "/" + prefix + ".diagram.gp";
    
    // Group scores by Objects
    map<string, vector<ScoreEntry>> objects_groups;
    for (const auto &score : scores)
    {
        objects_groups[score.objects].push_back(score);
    }
    
    // Sort each group by queries
    for (auto &[obj, entries] : objects_groups)
    {
        sort(entries.begin(), entries.end(),
            [](const ScoreEntry &a, const ScoreEntry &b) {
                if (a.queries != b.queries)
                    return a.queries < b.queries;
                return a.throughput_qps > b.throughput_qps;
            });
    }
    
    // Open script file
    ofstream script(script_file);
    if (!script.is_open())
    {
        Log::w(0, "Failed to create gnuplot script: " + script_file);
        return;
    }
    
    Log::w(1, "Creating gnuplot script", script_file);
    
    // Write gnuplot header with explicit size
    script << "set term postscript eps enhanced \"Times-New-Roman\" 36 color size 10in,6in\n\n\n\n\n";
    
    // For each Objects group, create a figure
    for (const auto &[objects, entries] : objects_groups)
    {
        script << "reset\n";
        script << "set pointsize 2.5\n";
        script << "set size 2,1.3\n";
        script << "set output '" << objects << ".eps'\n";
        script << "set multiplot\n\n";
        
        // Create legend at top
        script << "# Dummy plot for legend only (centered at top)\n";
        script << "set size 2,0.3\n";
        script << "set origin 0,1.0\n";
        script << "unset border\n";
        script << "unset xtics\n";
        script << "unset ytics\n";
        script << "unset xlabel\n";
        script << "set xrange [0:1]\n";
        script << "set yrange [0:1]\n";
        script << "set key center center box horizontal samplen 2\n";
        
        // Collect all unique schemas for legend
        set<string> unique_schemas;
        for (const auto &entry : entries)
        {
            unique_schemas.insert(entry.schema);
        }
        
        // Define colors for different schemas
        map<string, string> schema_colors;
        vector<string> colors = {"brown", "skyblue", "gold", "black", "red", "green", "blue", "orange", "purple"};
        int color_idx = 0;
        for (const auto &schema : unique_schemas)
        {
            schema_colors[schema] = colors[color_idx % colors.size()];
            color_idx++;
        }
        
        // Write legend plot
        script << "plot ";
        bool first_legend = true;
        for (const auto &schema : unique_schemas)
        {
            if (!first_legend) script << ", \\\n     ";
            script << "-1 t \"" << escape_quotes(schema) << "\" w boxes lt 1 fs solid 1 fc '" << schema_colors[schema] << "'";
            first_legend = false;
        }
        script << "\n\n";
        
        // Reset settings for data plots
        script << "# Reset settings for data plots\n";
        script << "set border\n";
        script << "set style fill pattern border\n";
        script << "set boxwidth 9\n";
        script << "set style fill pattern border -1\n";
        script << "set grid ytics\n";
        script << "set nokey\n\n";
        
        // Group by queries
        map<string, vector<ScoreEntry>> queries_groups;
        for (const auto &entry : entries)
        {
            queries_groups[entry.queries].push_back(entry);
        }
        
        // Single plot with all query groups as bar clusters
        script << "# Single plot for all queries\n";
        script << "set size 2,1\n";
        script << "set origin 0,0\n";
        
        // Calculate x-axis range and positions
        int num_query_groups = queries_groups.size();
        int bars_per_group = unique_schemas.size();
        int bar_spacing = 9;
        int group_spacing = 55;  // Space between query groups
        int total_width = num_query_groups * group_spacing + 40;
        
        script << "set xrange [5:" << total_width + 5 << "]\n";
        script << "set yrange [0:*]\n";
        
        // Build xtics for each query group
        script << "set xtics (";
        bool first_tic = true;
        int group_position = 36;
        for (const auto &[queries, query_entries] : queries_groups)
        {
            if (!first_tic) script << ", ";
            int group_center = group_position + (bars_per_group * bar_spacing) / 2;
            script << "\"" << queries << "\" " << group_center;
            first_tic = false;
            group_position += group_spacing;
        }
        script << ")\n";
        
        script << "set ytics\n";
        script << "set xlabel \"Queries\"\n";
        script << "set ylabel \"Throughput [queries/sec]\" offset 2\n";
        
        // Create data blocks for each query group
        group_position = 36;
        for (const auto &[queries, query_entries] : queries_groups)
        {
            string data_block_name = "$data_" + sanitize_name(objects) + "_" + sanitize_name(queries);
            script << data_block_name << " << EOD\n";
            
            // Create one row with all schema values as columns
            bool first_value = true;
            for (const auto &schema : unique_schemas)
            {
                if (!first_value) script << " ";
                // Find the entry with this schema
                double value = 0.0;
                for (const auto &entry : query_entries)
                {
                    if (entry.schema == schema)
                    {
                        value = entry.throughput_qps;
                        break;
                    }
                }
                script << value;
                first_value = false;
            }
            script << "\n";
            script << "EOD\n\n";
            
            group_position += group_spacing;
        }
        
        // Plot command - one series per schema, plotting across all query groups
        script << "plot ";
        bool first_plot = true;
        int schema_idx = 0;
        for (const auto &schema : unique_schemas)
        {
            group_position = 36;
            for (const auto &[queries, query_entries] : queries_groups)
            {
                if (!first_plot) script << ", \\\n     ";
                
                string data_block_name = "$data_" + sanitize_name(objects) + "_" + sanitize_name(queries);
                int bar_position = group_position + schema_idx * bar_spacing;
                
                script << data_block_name << " ";
                script << "u (" << bar_position << "):" << (schema_idx + 1) << " ";
                script << "t \"\" ";
                script << "w boxes lt 1 fs solid 1 fc '" << schema_colors[schema] << "'";
                
                first_plot = false;
                group_position += group_spacing;
            }
            schema_idx++;
        }
        script << "\n\n";
        
        script << "unset multiplot\n\n";
    }
    
    script.close();
    
    Log::w(1, "Run command", "(cd " + output_dir + " && gnuplot " + prefix + ".diagram.gp && for f in *.eps; do ps2pdf -dEPSCrop $f; done)");
}
