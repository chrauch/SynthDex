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
#include <optional>
#include <filesystem>
#include <unordered_map>
#include <ctime>
#include "persistence.h"
#include "../learning/statsserializer.h"
#include "../utils/global.h"
#include "../utils/parsing.h"
#include "../utils/logging.h"

using namespace std;


map<string, int> Persistence::write_counters;


tuple<string, ofstream, bool> Persistence::open_csv_file(
    const optional<string> &name,
    const string &attr,
    const bool &single,
    const bool &limit_size,
    const bool &timestamp)
{
    // Any attr that IS "score" or ENDS with ".score" (e.g. "query.score") routes to
    // the score/ subdir; everything else goes under stats/<attr>/.
    bool is_score_family = (attr == "score")
        || (attr.size() > 6 && attr.substr(attr.size() - 6) == ".score");
    string dir = is_score_family
        ? Cfg::get_out_dir() + "/score/"
        : Cfg::get_out_dir() + "/stats/" + attr + "/";
    filesystem::create_directories(dir);

    auto generate_path = [&]()
    {
        auto time = timestamp ? ("." + Persistence::timestamp(single, attr)) : "";
        auto file = Cfg::get<string>("out.machine-prefix")
            + (name.has_value() ? "." + name.value() : "")
            + time
            + "." + attr + ".csv";
        return dir + file;
    };

    string path = generate_path();
    
    if (!filesystem::exists(path)) Persistence::write_counters[attr] = 1;
    // Quarantee unqiue file name
    else if (single)
    {
        while (filesystem::exists(path))
        {
            Log::w(2, "Creating new file. " + path + " already exists.");
            path = generate_path();
        }
    }
    // Check if file exists and exceeds threshold
    else if (limit_size)
    {
        while (filesystem::exists(path) && filesystem::file_size(path) > 
                 Cfg::get<int>("out.max-file-size-megabytes") * 1024 * 1024)
        {
            Log::w(2, "Creating new file. " + path + " exceeded size limit.");
            path = generate_path();
        }
    }
    
    bool needs_header = !ifstream(path).good()
        || ifstream(path).peek() == ifstream::traits_type::eof();

    ofstream fs(path, ios::app);

    if (!fs.is_open()) throw runtime_error("Error opening: " + path);

    return make_tuple(path, move(fs), needs_header);
}


inline string Persistence::write_csv(
    const optional<string> &name,
    const bool &all,
    const bool &single,
    const optional<OStats> &O,
    const optional<vector<qStats>> &Q,
    const optional<iStats> &i,
    const optional<vector<pStats>> &P)
{
    string attr = "";
    if (O.has_value()) attr += "O";
    if (Q.has_value()) attr += "Q";
    if (i.has_value()) attr += "I";
    if (P.has_value()) attr += "P";

    auto timestamp = attr != "OQIP" || !Cfg::get<bool>("out.single-OQIP-file-per-O");

    if (all) attr += "-complete";

    auto [path, fs, needs_header] = Persistence::open_csv_file(
        name, attr, single, Q.has_value() || P.has_value(), timestamp);

    if (needs_header)
    {
        string header = "";
        if (O.has_value()) header += StatsSerializer::to_csv_header(O.value(), all);
        if (Q.has_value()) header += StatsSerializer::to_csv_header(Q->at(0), all);
        if (i.has_value()) header += StatsSerializer::to_csv_header(i.value(), all);
        if (P.has_value()) header += StatsSerializer::to_csv_header(P->at(0), all);
        fs << Persistence::sanitize(header) << '\n';
    }

    string O_csv = O.has_value() ? StatsSerializer::to_csv(O.value(), all) : "";
    string i_csv = i.has_value() ? StatsSerializer::to_csv(i.value(), all) : "";

    if (Q.has_value() && P.has_value() && i.has_value())
    {
        if (Q->size() != P->size())
            throw runtime_error("Cannot match Q and P statistics");

        Log::w(2, "Num of OQIP statistics", Q->size());

        for (size_t j = 0; j < Q->size(); ++j)
        {
            string row = O_csv 
                + StatsSerializer::to_csv(Q->at(j), all)
                + i_csv
                + StatsSerializer::to_csv(P->at(j), all);
            fs << Persistence::sanitize(row) << '\n';
        }
    }
    else if (Q.has_value())
    {
        for (const auto &q : *Q)
        {
            string row = O_csv + StatsSerializer::to_csv(q, all);
            if (i.has_value()) row += i_csv;
            fs << Persistence::sanitize(row) << '\n';
        }
    }
    else
    {
        fs << Persistence::sanitize(O_csv) << '\n';
    }

    fs.close();
    return path;
}


void Persistence::write_O_stats(const OStats &O, const int &id)
{
    Persistence::write_O_stats_csv(O, false);
    if (Cfg::get<bool>("out.formatted") || Cfg::get<bool>("o.reuse-ostats"))
        Persistence::write_O_stats_json(O, id, false);

    if (Cfg::get<bool>("out.detailed"))
    {
        Persistence::write_O_stats_csv(O, true);
        if (Cfg::get<bool>("out.formatted"))
            Persistence::write_O_stats_json(O, id, true);
    }
}


void Persistence::write_OQIP_stats_csv(
    const OStats &O,
    const vector<qStats> &Q,
    const iStats &i,
    const vector<pStats> &P,
    const bool all)
{
    optional<string> name 
        = Cfg::get<bool>("out.single-OQIP-file-per-O")
        ? optional<string>(O.name) : nullopt;

    auto path = Persistence::write_csv(
        name, false, false, O, Q, i, P);
    Log::w(2, "OQIP statistics file", path);

    if (all)
    {
        auto path = Persistence::write_csv(
            name, true, false, O, Q, i, P);
        Log::w(2, "OQIP statistics file", path);
    }
}


string Persistence::write_OQI_stats_csv(
    const OStats &O,
    const vector<qStats> &Q,
    const iStats &i,
    const string &name)
{
    auto path = Persistence::write_csv(
        name, false, true, O, Q, i, nullopt);
    Log::w(2, "OQI statistics file", path);

    return path;
}


string Persistence::write_OQ_stats_csv(
    const OStats &O,
    const vector<qStats> &Q,
    const string &name)
{
    auto path = Persistence::write_csv(
        name, false, true, O, Q, nullopt, nullopt);
    Log::w(2, "OQ statistics file", path);

    return path;
}


string Persistence::write_I_stats_csv(
    const vector<iStats> &I)
{
    auto [path, fs, needs_header] = Persistence::open_csv_file(
        nullopt, "I", true, false, true);

    if (needs_header)
    {
        string header = StatsSerializer::to_csv_header(I[0], false);
        fs << Persistence::sanitize(header) << '\n';
    }

    for (const auto &i : I)
    {
        string row = StatsSerializer::to_csv(i, false);
        fs << Persistence::sanitize(row) << '\n';
    }
    
    Log::w(2, "I statistics file", path);

    return path;
}


void Persistence::write_Q_stats_csv(
    const vector<qStats> &Q)
{
    auto path = Persistence::write_csv(
        nullopt, true, false, nullopt, Q, nullopt, nullopt);
    Log::w(2, "Q statistics file", path);
}


void Persistence::write_O_stats_sliced(
    const vector<tuple<SliceCluster, IRelation, vector<RangeIRQuery>, OStats>> &sliced)
{
    for (const auto &[cluster, Ox, Qx, stats] : sliced)
    {
        Persistence::write_O_stats_csv(stats, false);
        if (Cfg::get<bool>("out.formatted"))
            Persistence::write_O_stats_json(stats, cluster.cluster_id, false);

        if (Cfg::get<bool>("out.detailed"))
        {
            Persistence::write_O_stats_csv(stats, true);
            if (Cfg::get<bool>("out.formatted"))
                Persistence::write_O_stats_json(stats, cluster.cluster_id, true);
        }
    }
}


void Persistence::write_O_stats_csv(
    const OStats &O,
    const bool &all)
{
    auto path = Persistence::write_csv(
        nullopt, all, false, O, nullopt, nullopt, nullopt);
    Log::w(2, "O statistics file", path);
}


void Persistence::write_score_csv(
    const OStats &O,
    const string &Q_name,
    const string &workload_pattern,
    const size_t &xor_result,
    const size_t &size,
    const double &construction,
    const double &throughput,
    const iStats &i)
{
    auto [path, fs, needs_header] = Persistence::open_csv_file(
        optional<string>(O.name), "query.score", false, false, false);

    if (needs_header)
    {
        string header = "Objects\tQueries\tWorkloadPattern\tXorResult\tSizeMB\tConstructionS\tThroughputQpS\tCategory\tSchema";
        fs << header << '\n';
    }

    string row 
        = O.name + "\t"
        + Q_name + "\t"
        + workload_pattern + "\t"
        + to_string(xor_result) + "\t"
        + to_string(size / (1024.0 * 1024.0)) + "\t"
        + to_string(construction) + "\t"
        + to_string(throughput) + "\t"
        + i.category + "\t"
        + i.params;
    fs << row << '\n';

    Log::w(2, "Score statistics file", path);
}


string Persistence::write_comparison_table_csv(
    const string& caption,
    const string& file_tag,
    const vector<string>& workloads,
    const vector<vector<double>>& throughput,
    bool percentage,
    const vector<vector<string>>& display,
    const optional<string>& collection_name)
{
    if (throughput.size() != workloads.size())
        throw runtime_error("Invalid comparison protocol row count");
    for (const auto& row : throughput)
        if (row.size() != workloads.size())
            throw runtime_error("Invalid comparison protocol column count");
    if (!display.empty())
    {
        if (display.size() != workloads.size())
            throw runtime_error("Invalid comparison protocol display row count");
        for (const auto& row : display)
            if (row.size() != workloads.size())
                throw runtime_error("Invalid comparison protocol display column count");
    }

    const bool intermediate = collection_name.has_value();
    const string dir = Cfg::get_out_dir()
        + (intermediate ? "/comparison/intermediate/" : "/comparison/");
    filesystem::create_directories(dir);
    const string path = dir + Cfg::get<string>("out.machine-prefix")
        + (intermediate ? "." + collection_name.value() : "")
        + (intermediate ? ".comparison-intermediate." : ".comparison-protocol.")
        + file_tag
        + (intermediate ? ".csv"
                        : "." + Persistence::timestamp(true, file_tag) + ".csv");

    const string temporary_path = intermediate ? path + ".tmp" : path;
    ofstream fs(temporary_path);
    if (!fs.is_open()) throw runtime_error("Error opening: " + temporary_path);

    fs << "# " << caption << "\n";
    if (collection_name.has_value())
        fs << "# Collection: " << collection_name.value() << "\n";
    fs << "\t";
    for (size_t column = 0; column < workloads.size(); ++column)
    {
        if (column > 0) fs << "\t";
        fs << workloads[column];
    }
    fs << "\n";

    for (size_t row = 0; row < workloads.size(); ++row)
    {
        fs << workloads[row];
        for (size_t column = 0; column < workloads.size(); ++column)
        {
            fs << "\t";
            if (!display.empty())
                fs << display[row][column];
            else if (isfinite(throughput[row][column]))
            {
                fs << llround(throughput[row][column]);
                if (percentage) fs << "%";
            }
            else if (intermediate)
                fs << "nan";
        }
        fs << "\n";
    }

    fs.close();
    if (intermediate)
        filesystem::rename(temporary_path, path);
    Log::w(1, "Comparison protocol file", path);
    return path;
}


void Persistence::write_mutation_score_csv(
    const OStats &O,
    const string &operation,
    const string &work_file,
    const double &objs_affected_pct,
    const size_t &objs_before_cnt,
    const size_t &objs_after_cnt,
    const size_t &size_before,
    const size_t &size_after,
    const double &construction_s,
    const double &mutation_s,
    const string &category,
    const string &schema)
{
    auto [path, fs, needs_header] = Persistence::open_csv_file(
        optional<string>(O.name), operation + ".score", false, false, false);

    if (needs_header)
    {
        string header = "Objects\tWorkFile\tObjsAffectedPct\tObjsBeforeCnt\tObjsAfterCnt\tSizeBeforeMB\tSizeAfterMB\tConstructionS\tMutationS\tCategory\tSchema";
        fs << header << '\n';
    }

    string row
        = O.name + "\t"
        + work_file + "\t"
        + to_string(objs_affected_pct) + "\t"
        + to_string(objs_before_cnt) + "\t"
        + to_string(objs_after_cnt) + "\t"
        + to_string(size_before / (1024.0 * 1024.0)) + "\t"
        + to_string(size_after  / (1024.0 * 1024.0)) + "\t"
        + to_string(construction_s) + "\t"
        + to_string(mutation_s) + "\t"
        + category + "\t"
        + schema;
    fs << row << '\n';

    Log::w(2, "Mutation score statistics file", path);
}


template<typename Container>
static string write_dat(
    const string &file_name,
    const Container &data,
    const string &extension)
{
    Timer timer;
    timer.start();

    string dir = Cfg::get_out_dir() + "/data";
    filesystem::create_directories(dir);

    auto path = dir + "/" + file_name + "." + extension;

    ofstream file(path, ios::out | ios::trunc | ios::binary);
    if (!file.is_open())
        throw runtime_error("Error opening file: " + path);

    // Use a large buffer for efficiency
    constexpr size_t BUF_SIZE = 1 << 20; // 1MB
    vector<char> buffer;
    buffer.reserve(BUF_SIZE);

    for (const auto &item : data)
    {
        const string &line = item.str(false);
        buffer.insert(buffer.end(), line.begin(), line.end());
        buffer.push_back('\n');
        if (buffer.size() >= BUF_SIZE)
        {
            file.write(buffer.data(), buffer.size());
            buffer.clear();
        }
    }

    if (!buffer.empty())
        file.write(buffer.data(), buffer.size());

    file.close();
    
    double time = timer.stop();
    filesystem::path file_path(path);
    auto file_size = filesystem::file_size(file_path);
    size_t num_entries = data.size();
    
    string size_str = to_string(file_size / (1024.0 * 1024.0)) + " MB";

    Log::w(2, "Entries", num_entries);
    Log::w(2, "Persistence [s]", time);
    Log::w(2, "Persistence [x/s]", num_entries / time);
    Log::w(2, "Size", size_str);

    return path;
}


void Persistence::write_O_dat(
    const IRelation &O,
    const OStats &Os)
{
    auto path = write_dat(Os.name, O, "dat");
    Log::w(1, "Identifier", Os.name);
    Log::w(1, "O data file", path);
}


string Persistence::get_O_dat_path(const OStats &Os)
{
    return Cfg::get_out_dir() + "/data/" + Os.name + ".dat";
}


void Persistence::write_Q_dat(
    const tuple<string,vector<RangeIRQuery>> &Q,
    const OStats &Os,
    const unordered_map<string, unordered_map<string,string>> &workload_cfg,
    const string &raw_pattern)
{
    auto name = Persistence::compose_name(Os, Q);

    // ---- Build comment header -----------------------------------------------
    // Parse bare workload names from the tuple name.
    // Tuple name format: "qcnt<N>[-<pattern>][-ov-<hash>]"
    // Pattern examples: "RND", "20LOW+10RND", "20LOW+10RND-ov-a3f1bc22"
    string header;

    // Generation timestamp (UTC)
    {
        auto now = chrono::system_clock::now();
        time_t tt = chrono::system_clock::to_time_t(now);
        char tbuf[32];
        strftime(tbuf, sizeof(tbuf), "%Y-%m-%dT%H:%M:%SZ", gmtime(&tt));
        header += "# generated: ";
        header += tbuf;
        header += '\n';
    }

    // Object name
    header += "# object: " + Os.name + '\n';

    // Extract the pattern portion: strip "qcnt<N>" prefix and any "-ov_XXXXXXXX" suffix.
    string qname = get<0>(Q);
    // Find the pattern after the leading "qcnt<digits>" or "qcnt<digits>s<N>m<N>" segment.
    {
        // Strip everything up to and including the first '-' following qcnt prefix.
        auto dash = qname.find('-');
        string pattern_raw = (dash != string::npos) ? qname.substr(dash + 1) : "";

        // Strip trailing "-ov_XXXXXXXX" if present.
        {
            auto ov_pos = pattern_raw.rfind("-ov-");
            if (ov_pos == string::npos) ov_pos = pattern_raw.rfind("-ov_");
            if (ov_pos != string::npos)
                pattern_raw = pattern_raw.substr(0, ov_pos);
        }

        header += "# pattern: " + pattern_raw + '\n';

        // Full raw CLI pattern (e.g. "1HOTA{ext.ranges=[0.1-1,1-10]}+1COLDA{...}").
        // Written when the Q was generated from an on-demand %pattern argument.
        if (!raw_pattern.empty())
            header += "# raw_pattern: " + raw_pattern + '\n';

        // Split pattern on '+' to enumerate individual workload names.
        // Each chunk may have a leading numeric ratio prefix, e.g. "20LOW".
        vector<string> chunks;
        {
            size_t s = 0, p;
            while ((p = pattern_raw.find('+', s)) != string::npos)
            {
                chunks.push_back(pattern_raw.substr(s, p - s));
                s = p + 1;
            }
            chunks.push_back(pattern_raw.substr(s));
        }

        // Compute per-workload query counts via the same largest-remainder
        // proportional allocation that QGen::construct_Q uses.
        size_t total_queries = get<1>(Q).size();
        vector<size_t> workload_counts(chunks.size(), 0);
        {
            vector<int> ratios;
            int total_ratio = 0;
            for (const auto &chunk : chunks)
            {
                size_t k = 0;
                while (k < chunk.size() && isdigit((unsigned char)chunk[k])) ++k;
                int ratio = (k > 0 && k < chunk.size()) ? stoi(chunk.substr(0, k)) : 1;
                ratios.push_back(ratio);
                total_ratio += ratio;
            }
            vector<double> remainders(chunks.size(), 0.0);
            size_t alloc_sum = 0;
            for (size_t k = 0; k < chunks.size(); ++k)
            {
                double exact = total_ratio > 0
                    ? (static_cast<double>(total_queries) * ratios[k]) / total_ratio
                    : 0.0;
                workload_counts[k] = static_cast<size_t>(exact);
                remainders[k] = exact - static_cast<double>(workload_counts[k]);
                alloc_sum += workload_counts[k];
            }
            size_t leftover = total_queries - alloc_sum;
            vector<size_t> order(chunks.size());
            for (size_t k = 0; k < order.size(); ++k) order[k] = k;
            sort(order.begin(), order.end(),
                [&](size_t a, size_t b) { return remainders[a] > remainders[b]; });
            for (size_t k = 0; k < leftover; ++k) workload_counts[order[k]]++;
        }

        for (size_t ci = 0; ci < chunks.size(); ++ci)
        {
            const auto &chunk = chunks[ci];
            // Strip leading digits to get bare workload name.
            size_t j = 0;
            while (j < chunk.size() && isdigit((unsigned char)chunk[j])) ++j;
            string bare = chunk.substr(j);
            if (bare.empty()) continue;

            string pfx = "q.gen.workload." + bare + ".";
            header += "#   workload: " + bare
                + " (n=" + to_string(workload_counts[ci]) + ")\n";

            // Collect all overridable attributes and note source (cfg or cli).
            auto emit = [&](const string &key, const string &attr_label)
            {
                auto it = workload_cfg.find(bare);
                if (it != workload_cfg.end())
                {
                    auto it2 = it->second.find(key);
                    if (it2 != it->second.end())
                    {
                        header += "#     " + attr_label + " (cli): " + it2->second + '\n';
                        return;
                    }
                }
                try
                {
                    auto vals = Cfg::get<vector<string>>(pfx + key);
                    string joined;
                    for (size_t i = 0; i < vals.size(); ++i)
                    {
                        if (i) joined += ',';
                        joined += vals[i];
                    }
                    header += "#     " + attr_label + " (cfg): " + joined + '\n';
                }
                catch (...) {}
            };

            auto emit_int = [&](const string &key, const string &attr_label)
            {
                auto it = workload_cfg.find(bare);
                if (it != workload_cfg.end())
                {
                    auto it2 = it->second.find(key);
                    if (it2 != it->second.end())
                    {
                        header += "#     " + attr_label + " (cli): " + it2->second + '\n';
                        return;
                    }
                }
                try
                {
                    auto vals = Cfg::get<vector<int>>(pfx + key);
                    string joined;
                    for (size_t i = 0; i < vals.size(); ++i)
                    {
                        if (i) joined += ',';
                        joined += to_string(vals[i]);
                    }
                    header += "#     " + attr_label + " (cfg): " + joined + '\n';
                }
                catch (...) {}
            };

            auto emit_str = [&](const string &key, const string &attr_label)
            {
                auto it = workload_cfg.find(bare);
                if (it != workload_cfg.end())
                {
                    auto it2 = it->second.find(key);
                    if (it2 != it->second.end())
                    {
                        header += "#     " + attr_label + " (cli): " + it2->second + '\n';
                        return;
                    }
                }
                try
                {
                    auto val = Cfg::get<string>(pfx + key);
                    header += "#     " + attr_label + " (cfg): " + val + '\n';
                }
                catch (...) {}
            };

            emit_int("elem.cnt",   "elem.cnt");
            emit("elem.freqs",     "elem.freqs");
            emit("ext.ranges",     "ext.ranges");
            emit("ext.skew",       "ext.skew");
            emit_str("select",     "select");
        }
    }

    header += "# count: " + to_string(get<1>(Q).size()) + '\n';
    // ---- End header ---------------------------------------------------------

    // Write file directly (cannot use the generic write_dat template since we
    // need to prepend the comment header).
    Timer timer;
    timer.start();

    string dir = Cfg::get_out_dir() + "/data";
    filesystem::create_directories(dir);
    string path = dir + "/" + name + ".qry";

    ofstream file(path, ios::out | ios::trunc | ios::binary);
    if (!file.is_open())
        throw runtime_error("Error opening file: " + path);

    file.write(header.data(), header.size());

    constexpr size_t BUF_SIZE = 1 << 20;
    vector<char> buffer;
    buffer.reserve(BUF_SIZE);
    for (const auto &q : get<1>(Q))
    {
        const string &line = q.str(false);
        buffer.insert(buffer.end(), line.begin(), line.end());
        buffer.push_back('\n');
        if (buffer.size() >= BUF_SIZE)
        {
            file.write(buffer.data(), buffer.size());
            buffer.clear();
        }
    }
    if (!buffer.empty())
        file.write(buffer.data(), buffer.size());

    file.close();

    Log::w(1, "Q data file", path);
}


IRelation Persistence::read_O_dat(const string &file)
{
    Log::w(1, "O data file", file);

    Timer timer;
    timer.start();

    if (file.empty())
        throw runtime_error("File not specified");

    if (!filesystem::exists(file))
        throw runtime_error("File not found: " + file);

    IRelation O;
    Timestamp rstart, rend;
    string relems;
    ifstream in(file);
    if (!in.is_open())
        throw runtime_error("Error opening file: " + file);

    RecordId num = 0;
    string line;
    
    O.init();
    while (getline(in, line))
    {
        // Skip blank lines and comment lines.
        size_t first = line.find_first_not_of(" \t\r");
        if (first == string::npos || line[first] == '#') continue;

        istringstream ls(line);
        if (!(ls >> rstart >> rend >> relems)) continue;

        if (rstart > rend)
            throw invalid_argument("Invalid interval: "
                + to_string(rstart) + " > " + to_string(rend));

        IRecord r(num, rstart, rend);
        stringstream ss(relems);
        string rt;
        while (getline(ss, rt, ','))
        {
            ElementId tid = stoi(rt);
            r.elements.push_back(tid);
        }

        O.push_back(r);
        num++;

        O.gstart = min(O.gstart, rstart);
        O.gend = max(O.gend, rend);
    }
    
    in.close();
    double time = timer.stop();

    Log::w(2, "Persistence [s]", time);
    Log::w(2, "Persistence [o/s]", O.size() / time);

    Log::w(2, "Num of o", O.size());
    Log::w(2, "Interval domain", 
        "[" + to_string(O.gstart) + ".." + to_string(O.gend) + "]");

    return O;
}


RelationId Persistence::read_Oids_dat(const string &file)
{
    Log::w(1, "O IDs data file", file);

    Timer timer;
    timer.start();

    if (file.empty())
        throw runtime_error("File not specified");

    if (!filesystem::exists(file))
        throw runtime_error("File not found: " + file);

    RelationId ids;
    ifstream in(file);
    if (!in.is_open())
        throw runtime_error("Error opening file: " + file);

    RecordId id;
    while (in >> id)
    {
        ids.push_back(id);
    }
    
    in.close();
    double time = timer.stop();

    Log::w(2, "Persistence [s]", time);
    Log::w(2, "Persistence [ids/s]", ids.size() / time);
    Log::w(2, "Num of IDs", ids.size());

    return ids;
}


vector<tuple<string,vector<RangeIRQuery>>> Persistence::read_q_dat(
    const string &file)
{
    Log::w(1, "Q data file", file);

    string filename = filesystem::path(file).filename().string();
    if (filename.size() >= 4 && filename.substr(filename.size() - 4) == ".qry")
        filename = filename.substr(0, filename.size() - 4);

    vector<tuple<string,vector<RangeIRQuery>>> queries;
    queries.push_back({filename, {}});
    Timestamp qstart, qend;
    string qelems;
    ifstream in(file);
    if (!in.is_open())
        throw runtime_error("Error opening file: " + file);
    
    RecordId num = 0;
    string line;

    while (getline(in, line))
    {
        // Skip blank lines and comment lines.
        size_t first = line.find_first_not_of(" \t\r");
        if (first == string::npos || line[first] == '#') continue;

        istringstream ls(line);
        if (!(ls >> qstart >> qend >> qelems)) continue;

        if (qstart > qend)
            throw invalid_argument("Invalid interval: "
                + to_string(qstart) + " > " + to_string(qend));

        RangeIRQuery q(num, qstart, qend);
        stringstream ss(qelems);
        string qt;
        while (getline(ss, qt, ','))
        {
            ElementId tid = stoi(qt);
            q.elems.push_back(tid);
        }

        get<1>(queries.back()).push_back(q);
        num++;
    }
    
    in.close();

    for (const auto& [pattern, qs] : queries)
    {
        if (pattern != filename) Log::w(1, "Q pattern", pattern);
        Log::w(1, "Num of q", qs.size());
    }

    return queries;
}


string Persistence::read_q_raw_pattern(const string &file)
{
    // Scan the comment header of a .qry file for the "# raw_pattern:" annotation
    // written by write_Q_dat when the file was generated from a %pattern CLI argument.
    // Returns an empty string when the annotation is absent (e.g. legacy files).
    ifstream in(file);
    if (!in.is_open()) return "";

    string line;
    while (getline(in, line))
    {
        // Comment lines start with '#'; data lines start with digits or whitespace.
        if (line.empty() || line[0] != '#') break;

        const string prefix = "# raw_pattern: ";
        if (line.size() > prefix.size() && line.substr(0, prefix.size()) == prefix)
            return line.substr(prefix.size());
    }

    return "";
}


string Persistence::find_O_stats_json_path(const string &file_O)
{
    // Extract the O name: strip directory and .dat extension
    size_t sep = file_O.find_last_of("/\\");
    string name = (sep == string::npos) ? file_O : file_O.substr(sep + 1);
    if (name.size() > 4 && name.substr(name.size() - 4) == ".dat")
        name = name.substr(0, name.size() - 4);

    string dir = Cfg::get_out_dir() + "/stats/formatted";
    if (!filesystem::exists(dir)) return "";

    string needle = "\"O_name\": \"" + name + "\"";

    for (const auto &entry : filesystem::directory_iterator(dir))
    {
        if (!entry.is_regular_file()) continue;
        string fname = entry.path().filename().string();
        if (fname.size() < 7 || fname.substr(fname.size() - 7) != ".O.json")
            continue;

        ifstream in(entry.path());
        if (!in.is_open()) continue;

        // The O_name field is near the start of the file; read a small prefix.
        char buf[512];
        in.read(buf, sizeof(buf));
        string head(buf, in.gcount());
        if (head.find(needle) != string::npos)
            return entry.path().string();
    }

    return "";
}


bool Persistence::exists_O_stats_json(const string &file_O)
{
    return !find_O_stats_json_path(file_O).empty();
}


OStats Persistence::read_O_stats_json(const string &file)
{
    string path = find_O_stats_json_path(file);
    if (path.empty()) path = file;  // fallback: treat as direct path

    Log::w(1, "O statistics file", path);

    ifstream in(path);
    if (!in.is_open())
    {
        throw runtime_error("Error opening file: " + path);
    }

    stringstream ss;
    ss << in.rdbuf();
    auto Os = StatsSerializer::from_json(ss.str());

    return Os;
}


vector<OStats> Persistence::read_O_stats_jsons(
    const vector<string> &files)
{
    Log::w(2, "O statistics templates", str(files));

    vector<OStats> Oss;
    for (const auto &name : files)
    {
        const string path = "o.templates." + name;
        try
        {
            auto json = Cfg::get_json(path);
            Log::w(2, "O template (config)", name);
            Oss.push_back(StatsSerializer::from_json(json));
        }
        catch (const exception &e)
        {
            throw runtime_error(
                "Template '" + name + "' listed in o.gen.patterns-active "
                "not found in o.templates: " + e.what());
        }
    }

    return Oss;
}


vector<tuple<vector<string>,vector<double>>> Persistence::read_I_synthesis_csv(const string &file)
{
    Log::w(2, "Optimal I statistics file", file);

    if (!filesystem::exists(file))
        throw runtime_error("File not found: " + file);

    vector<tuple<vector<string>,vector<double>>> results;
    ifstream in(file);
    
    if (!in.is_open())
        throw runtime_error("Cannot open file: " + file);

    string line;
    bool is_header = true;
    
    while (getline(in, line))
    {
        if (line.empty()) continue;
        
        // Skip header row
        if (is_header)
        {
            is_header = false;
            continue;
        }
        
        vector<string> row;
        stringstream ss(line);
        string cell;
        
        // Parse line by tab separator
        while (getline(ss, cell, '\t'))
        {
            row.push_back(cell);
        }
        
        if (row.size() < 7)
            throw runtime_error("Invalid row: " + line);
        
        // Extract metadata: variant, OQ file, temporal_band, template_id, synth_id, throughput prediction, size prediction
        vector<string> metadata;
        for (size_t i = 0; i < 7 && i < row.size(); ++i)
        {
            metadata.push_back(row[i]);
        }
        
        // Extract encoding (remaining columns as doubles)
        vector<double> encoding;
        for (size_t i = 7; i < row.size(); ++i)
        {
            double value = stod(row[i]);
            encoding.push_back(value);
        }
        
        results.push_back(make_tuple(metadata, encoding));
    }
    
    in.close();
    
    return results;
}


vector<tuple<vector<string>,vector<double>>> Persistence::read_score_csv(const string &file)
{
    Log::w(2, "Score statistics file", file);

    if (!filesystem::exists(file))
        throw runtime_error("File not found: " + file);

    vector<tuple<vector<string>,vector<double>>> results;
    ifstream in(file);
    
    if (!in.is_open())
        throw runtime_error("Cannot open file: " + file);

    string line;
    
    while (getline(in, line))
    {
        // Skip empty lines
        if (line.empty()) continue;
        
        vector<string> row;
        stringstream ss(line);
        string cell;
        
        // Parse line by tab separator
        while (getline(ss, cell, '\t'))
        {
            row.push_back(cell);
        }
        
        // Expected schema (9 columns): Objects, Queries, WorkloadPattern, XorResult, SizeMB, ConstructionS, ThroughputQpS, Category, Schema
        // Legacy schema  (8 columns): Objects, Queries, XorResult, SizeMB, ConstructionS, ThroughputQpS, Category, Schema
        // Both are accepted; presence of 9+ columns identifies the new format.
        if (row.size() < 6)
        {
            Log::w(3, "Skipping invalid row (insufficient columns)", line);
            continue;
        }
        
        try
        {
            // Detect format by column count: 9+ columns = new format with WorkloadPattern.
            bool new_format = row.size() >= 9;

            // Extract string fields
            vector<string> strings;
            strings.push_back(row[0]); // Objects
            strings.push_back(row[1]); // Queries
            if (new_format)
            {
                strings.push_back(row[2]); // WorkloadPattern
                if (row.size() > 7) strings.push_back(row[7]); // Category
                if (row.size() > 8) strings.push_back(row[8]); // Schema
            }
            else
            {
                strings.push_back(""); // WorkloadPattern (empty for legacy rows)
                if (row.size() > 6) strings.push_back(row[6]); // Category
                if (row.size() > 7) strings.push_back(row[7]); // Schema
            }
            
            // Extract numeric fields
            vector<double> doubles;
            int base = new_format ? 3 : 2; // XorResult starts at col 3 (new) or 2 (old)
            doubles.push_back(stod(row[base + 0])); // XorResult
            doubles.push_back(stod(row[base + 1])); // SizeMB
            doubles.push_back(stod(row[base + 2])); // ConstructionS
            doubles.push_back(stod(row[base + 3])); // ThroughputQpS
            
            results.push_back(make_tuple(strings, doubles));
        }
        catch (const exception &e)
        {
            Log::w(3, "Skipping malformed row", line + " (error: " + e.what() + ")");
            continue;
        }
    }
    
    in.close();
    
    return results;
}


vector<tuple<vector<string>,vector<double>>> Persistence::read_mutation_score_csv(const string &file)
{
    Log::w(2, "Mutation score statistics file", file);

    if (!filesystem::exists(file))
        throw runtime_error("File not found: " + file);

    vector<tuple<vector<string>,vector<double>>> results;
    ifstream in(file);

    if (!in.is_open())
        throw runtime_error("Cannot open file: " + file);

    string line;

    while (getline(in, line))
    {
        if (line.empty()) continue;

        vector<string> row;
        stringstream ss(line);
        string cell;

        while (getline(ss, cell, '\t'))
            row.push_back(cell);

        // Expected columns: Objects, WorkFile, ObjsAffectedPct, ObjsBeforeCnt, ObjsAfterCnt, SizeBeforeMB, SizeAfterMB, ConstructionS, MutationS, Category, Schema
        if (row.size() < 9)
        {
            Log::w(3, "Skipping invalid mutation score row (insufficient columns)", line);
            continue;
        }

        try
        {
            vector<string> strings;
            strings.push_back(row[0]); // Objects
            strings.push_back(row[1]); // WorkFile
            if (row.size() > 9)  strings.push_back(row[9]);  // Category
            if (row.size() > 10) strings.push_back(row[10]); // Schema

            vector<double> doubles;
            doubles.push_back(stod(row[2])); // ObjsAffectedPct
            doubles.push_back(stod(row[3])); // ObjsBeforeCnt
            doubles.push_back(stod(row[4])); // ObjsAfterCnt
            doubles.push_back(stod(row[5])); // SizeBeforeMB
            doubles.push_back(stod(row[6])); // SizeAfterMB
            doubles.push_back(stod(row[7])); // ConstructionS
            doubles.push_back(stod(row[8])); // MutationS

            results.push_back(make_tuple(strings, doubles));
        }
        catch (const exception &e)
        {
            Log::w(3, "Skipping malformed mutation score row", line + " (error: " + e.what() + ")");
            continue;
        }
    }

    in.close();

    return results;
}


void Persistence::write_O_stats_json(
    const OStats &Os, const int &id, const bool &all)
{
    string dir = Cfg::get_out_dir() + "/stats/formatted";
    filesystem::create_directories(dir);

    string path = dir + "/" + Persistence::timestamp(true, "O") + "-" 
        + to_string(id) + (all ? ".O-complete" : ".O") + ".json";

    ofstream fs(path);
    if (!fs.is_open()) throw runtime_error("Error opening file: " + path);

    Log::w(2, "Identifier", Os.name);
    Log::w(2, "O statistics file", path);

    fs << StatsSerializer::to_json(Os, all);
    fs.close();
}


string Persistence::timestamp(const bool single, const string& attr)
{
    auto now = chrono::system_clock::now();
    time_t t = chrono::system_clock::to_time_t(now);
    tm tm;
    localtime_r(&t, &tm);

    auto format = single ? "%Y%m%d-%H%M%S" : "%Y%m%d-%H";
    ostringstream oss;
    oss << put_time(&tm, format);

    if (write_counters.find(attr) == write_counters.end())
        write_counters[attr] = 1;
    
    oss << "-" << setfill('0') << setw(3) << write_counters[attr];
    if (single) write_counters[attr]++;

    return oss.str();
}


string Persistence::get_Q_dat_path(
    const OStats &Os,
    const tuple<string,vector<RangeIRQuery>> &Q)
{
    auto name = Persistence::compose_name(Os, Q);
    return Cfg::get_out_dir() + "/data/" + name + ".qry";
}


string Persistence::compose_name(
    const OStats &Os,
    const tuple<string,vector<RangeIRQuery>> &Q)
{
    auto Qname = get<0>(Q);
    if (Qname.find(".Q-rnd_") == string::npos)
        return Os.name + ".Q-rnd_" + Qname;
    if (Qname.size() >= 4 && Qname.substr(Qname.size() - 4) == ".qry")
        Qname = Qname.substr(0, Qname.size() - 4);
    return Qname;
}


inline string Persistence::sanitize(const string& csv_line)
{
    if (!csv_line.empty() && csv_line.back() == StatsSerializer::sep[0])
        return csv_line.substr(0, csv_line.length() - 1);

    return csv_line;
}


void Persistence::clean(const vector<string>& artifacts)
{
    if (artifacts.empty()) return;

    string dir = Cfg::get_out_dir();

    int removed_files_cnt = 0;
    vector<string> removed_files;
    for (const auto &entry : filesystem::recursive_directory_iterator(dir))
    {
        // Skip directories, only process files
        if (!entry.is_regular_file()) continue;
        
        string filename = entry.path().filename().string();
        string extension = entry.path().extension().string();
        
        if (any_of(artifacts.begin(), artifacts.end(),
            [&](const string &a)
            {
                if (filename == ".keep") return false;
                // Match by artifact name or by file extension for plot files
                if (a == "plots" && (extension == ".gp" || extension == ".eps" || extension == ".pdf"))
                    return true;
                // Match parquet files only in the direct train/ directory (not subdirs)
                if (a == "train-parquet" && extension == ".parquet"
                    && entry.path().parent_path().filename() == "train")
                    return true;
                return filename.find(a) != string::npos;
            }))
        {
            filesystem::remove(entry.path());
            removed_files_cnt++;
            removed_files.push_back(entry.path().string());
        }
    }

    Log::w(1, "Deleted files count", to_string(removed_files_cnt));
    Log::w(2, "Artifacts", str(artifacts));
    Log::w(2, "Paths", str(removed_files));
}
