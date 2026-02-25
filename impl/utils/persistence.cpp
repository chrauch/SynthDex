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
#include <optional>
#include <filesystem>
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
    string dir = (attr == "score") 
        ? Cfg::get<string>("out.dir") + "/" + attr + "/"
        : Cfg::get<string>("out.dir") + "/stats/" + attr + "/";
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
    if (Cfg::get<bool>("out.formatted"))
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
    const size_t &xor_result,
    const size_t &size,
    const double &construction,
    const double &throughput,
    const iStats &i)
{
    auto [path, fs, needs_header] = Persistence::open_csv_file(
        optional<string>(O.name), "score", false, false, false);

    if (needs_header)
    {
        string header = "Objects\tQueries\tXorResult\tSizeMB\tConstructionS\tThroughputQpS\tCategory\tSchema";
        fs << header << '\n';
    }

    string row 
        = O.name + "\t"
        + Q_name + "\t"
        + to_string(xor_result) + "\t"
        + to_string(size / (1024.0 * 1024.0)) + "\t"
        + to_string(construction) + "\t"
        + to_string(throughput) + "\t"
        + i.category + "\t"
        + i.params;
    fs << row << '\n';

    Log::w(2, "Score statistics file", path);
}


template<typename Container>
static string write_dat(
    const string &file_name,
    const Container &data,
    const string &extension)
{
    Timer timer;
    timer.start();

    string dir = Cfg::get<string>("out.dir") + "/data";
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


void Persistence::write_Q_dat(
    const tuple<string,vector<RangeIRQuery>> &Q,
    const OStats &Os)
{
    auto name = Persistence::compose_name(Os, Q);
        // + "_" + Persistence::timestamp(true, "qry");
    auto path = write_dat<vector<RangeIRQuery>>(name, get<1>(Q), "qry");
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
    
    O.init();
    while (in >> rstart >> rend >> relems)
    {
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
    
    while (in >> qstart >> qend >> qelems)
    {
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


OStats Persistence::read_O_stats_json(const string &file)
{
    Log::w(2, "O statistics file", file);

    ifstream in(file);
    if (!in.is_open())
    {
        throw runtime_error("Error opening file: " + file);
    }

    stringstream ss;
    ss << in.rdbuf();
    auto Os = StatsSerializer::from_json(ss.str());

    return Os;
}


vector<OStats> Persistence::read_O_stats_jsons(
    const string &dir, const vector<string> &files)
{
    auto dirx = dir.empty() ? Cfg::instance().config_dir 
        + "/O-templates" : dir;

    Log::w(2, "O statistics templates",
        "Directory: " + dirx + "\nFilter:    " + str(files));

    vector<OStats> Oss;
    for (const auto &entry : filesystem::directory_iterator(dirx))
    {
        string filename = entry.path().stem().string();

        auto match = [&](const string &f)
            { return filename == f || filename == f + ".o"; };

        if (none_of(files.begin(), files.end(), match)) continue;

        const string path = entry.path().string();
        Oss.push_back(Persistence::read_O_stats_json(path));
    }

    if (Oss.empty())
        throw runtime_error("No valid OStats found");

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
        
        if (row.size() < 6)
            throw runtime_error("Invalid row: " + line);
        
        // Extract metadata: variant, OQ file, template_id, synth_id, throughput prediction, size prediction
        vector<string> metadata;
        for (size_t i = 0; i < 6 && i < row.size(); ++i)
        {
            metadata.push_back(row[i]);
        }
        
        // Extract encoding (remaining columns as doubles)
        vector<double> encoding;
        for (size_t i = 6; i < row.size(); ++i)
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
        
        // Expected schema: Objects, Queries, XorResult, SizeMB, ConstructionS, ThroughputQpS, Category, Schema
        if (row.size() < 6)
        {
            Log::w(3, "Skipping invalid row (insufficient columns)", line);
            continue;
        }
        
        try
        {
            // Extract string fields: Objects, Queries, Category, Schema
            vector<string> strings;
            strings.push_back(row[0]); // Objects
            strings.push_back(row[1]); // Queries
            if (row.size() > 6) strings.push_back(row[6]); // Category
            if (row.size() > 7) strings.push_back(row[7]); // Schema
            
            // Extract numeric fields: XorResult, SizeMB, ConstructionS, ThroughputQpS
            vector<double> doubles;
            doubles.push_back(stod(row[2])); // XorResult
            doubles.push_back(stod(row[3])); // SizeMB
            doubles.push_back(stod(row[4])); // ConstructionS
            doubles.push_back(stod(row[5])); // ThroughputQpS
            
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


void Persistence::write_O_stats_json(
    const OStats &Os, const int &id, const bool &all)
{
    string dir = Cfg::get<string>("out.dir") + "/stats/formatted";
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

    string dir = Cfg::get<string>("out.dir");

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
