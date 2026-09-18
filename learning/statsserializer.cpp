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

#include "statsserializer.h"
#include "statscomp.h"
#include "stats.h"
#include "../utils/parsing.h"
#include <sstream>
#include <stdexcept>
#include <cctype>
#include <algorithm>
#include <iostream>
#include <functional>
#include <iomanip>


const string StatsSerializer::sep = "\t";


OStats StatsSerializer::from_json(const string &str)
{
    istringstream in(str);
    OStats stats;
    stats.elements.resize(Cfg::get<int>("o.elem.bins"));
    stats.extents.resize(Cfg::get<int>("o.ext.bins"));

    Parsing::skip_ws(in);
    Parsing::expect(in, '{');

    while (true)
    {
        Parsing::skip_ws(in);
        if (in.peek() == '}')
        {
            in.get();
            break;
        }

        string key = Parsing::parse_key(in);

        // Parse value
        string value;
        char c = in.peek();
        if (c == '"') 
        {
            value = Parsing::parse_string(in);
        }
        else
        {
            // Manually read numeric value up to ',' or '}'
            string token;
            while (true)
            {
                char ch = in.peek();
                if (ch == ',' || ch == '}' || isspace(ch)) break;
                token += in.get();
            }
            value = token;
        }

        if (key == "O_params")
        {
            stats.params = value;
        }
        else if (key == "O_name")
        {
            stats.name = value;
        }
        else if (key == "O_card_log")
        {
            stats.card_log = stod(value);
        }
        else if (key == "O_domain_log")
        {
            stats.domain_log = stod(value);
        }
        else if (key == "O_dict_log")
        {
            stats.dict_log = stod(value);
        }
        else if (key.rfind("O_ext-bin", 0) == 0)
        {
            const string prefix = "O_ext-bin-";
            size_t index_start = prefix.size();
            size_t underscore_pos = key.find('_', index_start);

            if (underscore_pos == string::npos)
                throw runtime_error("Invalid O_ext key format: " + key);

            string index_str = key.substr(index_start, underscore_pos - index_start);
            string suffix = key.substr(underscore_pos + 1);

            size_t index = stoi(index_str);

            if (suffix != "min_len_log" && suffix != "max_len_log" &&
                suffix != "avg_elem_cnt_log" && suffix != "dev_elem_cnt_log")
                throw runtime_error("Invalid O_ext suffix: " + suffix);

            auto &ext_stats = stats.extents[index];
            if (suffix == "min_len_log")
                ext_stats.min_len_log = stod(value);
            else if (suffix == "max_len_log")
                ext_stats.max_len_log = stod(value);
            else if (suffix == "avg_elem_cnt_log")
                ext_stats.avg_elem_cnt_log = stod(value);
            else if (suffix == "dev_elem_cnt_log")
                ext_stats.dev_elem_cnt_log = stod(value);
        }
        else if (key.rfind("O_elem-bin-", 0) == 0)
        {
            const string prefix = "O_elem-bin-";
            size_t index_start = prefix.size();
            size_t underscore_pos = key.find('_', index_start);

            if (underscore_pos == string::npos)
                throw runtime_error("Invalid O_elem key format: " + key);

            string index_str = key.substr(index_start, underscore_pos - index_start);
            string suffix = key.substr(underscore_pos + 1);

            size_t index = stoi(index_str);

            if (suffix != "hi_rank_log")
                throw runtime_error("Invalid O_elem suffix: " + suffix);

            stats.elements[index].hi_rank_log = stod(value);
        }

        Parsing::skip_ws(in);
        if (in.peek() == ',')
        {
            in.get();
            continue;
        }
        else if (in.peek() == '}')
        {
            in.get();
            break;
        }
        else
        {
            throw runtime_error("Expected ',' or '}'");
        }
    }

    StatsComp::fill(stats);

    return stats;
}


static string dtos(double v)
{
    ostringstream s;
    s << setprecision(17) << v;
    return s.str();
}

string StatsSerializer::to_json(const OStats &os, const bool &all)
{
    ostringstream out;
    string indent = "\t";
    out << "{\n";

    bool first = true;
    auto emit = [&](const string &k, const string &v)
    {
        if (!first)
            out << ",\n";
        out << indent << "\"" << k << "\": " << v;
        first = false;
    };

    emit("O_name", "\"" + os.name + "\"");
    if (all)
    {
        emit("O_params", "\"" + os.params + "\"");
        emit("O_est_bytes", dtos(os.est_bytes));
        emit("O_bytes", dtos(os.bytes));
    }

    if (all) emit("O_card", to_string(os.card));
    emit("O_card_log", dtos(os.card_log));

    if (all) emit("O_domain", to_string(os.domain));
    emit("O_domain_log", dtos(os.domain_log));

    if (all)
    {
        emit("O_dict", to_string(os.dict));
        emit("O_dict_log", dtos(os.dict_log));
    }

    auto ext_bin = 0;
    for (const auto &b : os.extents)
    {
        string prefix = "O_ext-bin-" + to_string(ext_bin++);

        if (all)
        {
            emit(prefix + "_min_len", dtos(b.min_len));
            emit(prefix + "_min_len_log", dtos(b.min_len_log));
            emit(prefix + "_max_len", dtos(b.max_len));
        }

        emit(prefix + "_max_len_log", dtos(b.max_len_log));

        if (all) emit(prefix + "_avg_elem_cnt", dtos(b.avg_elem_cnt));
        emit(prefix + "_avg_elem_cnt_log", dtos(b.avg_elem_cnt_log));

        if (all) emit(prefix + "_dev_elem_cnt", dtos(b.dev_elem_cnt));
        emit(prefix + "_dev_elem_cnt_log", dtos(b.dev_elem_cnt_log));
    }

    auto elem_bin = 0;
    for (const auto &b : os.elements)
    {
        string prefix = "O_elem-bin-" + to_string(elem_bin++);

        if (all)
        {
            emit(prefix + "_lo_rank", to_string(b.lo_rank));
            emit(prefix + "_lo_rank_log", dtos(b.lo_rank_log));
            emit(prefix + "_hi_rank", to_string(b.hi_rank));
        }
        
        emit(prefix + "_hi_rank_log", dtos(b.hi_rank_log));
        
        if (all)
        {
            emit(prefix + "_max_freq", dtos(b.max_freq));
            emit(prefix + "_max_freq_log", dtos(b.max_freq_log));
            emit(prefix + "_dict_ratio", dtos(b.dict_ratio));
            emit(prefix + "_dict_ratio_log", dtos(b.dict_ratio_log));
            emit(prefix + "_max_post_ratio", dtos(b.max_post_ratio));
            emit(prefix + "_max_post_ratio_log", dtos(b.max_post_ratio_log));
            emit(prefix + "_rel_drop", dtos(b.rel_drop));
            emit(prefix + "_rel_drop_log", dtos(b.rel_drop_log));
        }
    }

    out << "\n}";
    return out.str();
}


string StatsSerializer::to_csv(
    const OStats &os, const bool &all)
{
    stringstream ss;
    ss << setprecision(17);

    ss << os.name << sep;

    if (all)
    {
        ss << os.params << sep;
        ss << os.est_bytes << sep;
        ss << os.bytes << sep;
    }

    if (all) ss << os.card << sep;
    ss << os.card_log << sep;

    if (all) ss << os.domain << sep;
    ss << os.domain_log << sep;

    if (all)
    {
        ss << os.dict << sep;
        ss << os.dict_log << sep;
    }

    for (const auto &b : os.extents)
    {
        if (all)
        {
            ss << b.min_len << sep;
            ss << b.min_len_log << sep;
            ss << b.max_len << sep;
        }

        ss << b.max_len_log << sep;
        
        if (all) ss << b.avg_elem_cnt << sep;
        ss << b.avg_elem_cnt_log << sep;

        if (all) ss << b.dev_elem_cnt << sep;
        ss << b.dev_elem_cnt_log << sep;
    }

    for (const auto &b : os.elements)
    {
        if (all)
        {
            ss << b.lo_rank << sep;
            ss << b.hi_rank << sep;
            ss << b.lo_rank_log << sep;
        }
        
        ss << b.hi_rank_log << sep;
        
        if (all)
        {
            ss << b.max_freq << sep;
            ss << b.max_freq_log << sep;
            ss << b.dict_ratio << sep;
            ss << b.dict_ratio_log << sep;
            ss << b.max_post_ratio << sep;
            ss << b.max_post_ratio_log << sep;
            ss << b.rel_drop << sep;
            ss << b.rel_drop_log << sep;
        }
    }

    return ss.str();
}


string StatsSerializer::to_csv_header(
    const OStats &os, const bool &all)
{
    string prefix = "O";
    stringstream ss;

    ss << prefix << "_name" << sep;

    if (all)
    {
        ss << prefix << "_params" << sep;
        ss << prefix << "_est_bytes" << sep;
        ss << prefix << "_bytes" << sep;
    }

    if (all) ss << prefix << "_card" << sep;
    ss << prefix << "_card_log" << sep;

    if (all) ss << prefix << "_domain" << sep;
    ss << prefix << "_domain_log" << sep;

    if (all)
    {
        ss << prefix << "_dict" << sep;
        ss << prefix << "_dict_log" << sep;
    }

    for (size_t b = 0; b < os.extents.size(); ++b)
    {
        string caption = prefix + "_ext-bin-" + to_string(b);

        if (all)
        {
            ss << caption << "_min_len" << sep;
            ss << caption << "_min_len_log" << sep;
            ss << caption << "_max_len" << sep;
        }

        ss << caption << "_max_len_log" << sep;
        
        if (all) ss << caption << "_avg_elem_cnt" << sep;
        ss << caption << "_avg_elem_cnt_log" << sep;

        if (all) ss << caption << "_dev_elem_cnt" << sep;
        ss << caption << "_dev_elem_cnt_log" << sep;
    }

    for (size_t b = 0; b < os.elements.size(); ++b)
    {
        string caption = prefix + "_elem-bin-" + to_string(b);

        if (all)
        {
            ss << caption << "_lo_rank" << sep;
            ss << caption << "_hi_rank" << sep;
            ss << caption << "_lo_rank_log" << sep;
        }

        ss << caption << "_hi_rank_log" << sep;
        
        if (all)
        {
            ss << caption << "_max_freq" << sep;
            ss << caption << "_max_freq_log" << sep;
            ss << caption << "_dict_ratio" << sep;
            ss << caption << "_dict_ratio_log" << sep;
            ss << caption << "_max_post_ratio" << sep;
            ss << caption << "_max_post_ratio_log" << sep;
            ss << caption << "_rel_drop" << sep;
            ss << caption << "_rel_drop_log" << sep;
        }
    }

    return ss.str();
}


string StatsSerializer::map_enc(int index)
{
    // Encoding structure: 4 one-hot tIF + alpha + beta + dict_boundary + irhint_levels = 8
    switch (index)
    {
    case 0: return "tif_basic";
    case 1: return "tif_slicing";
    case 2: return "tif_sharding";
    case 3: return "tif_hint";
    case 4: return "alpha";
    case 5: return "beta";
    case 6: return "dict_boundary";
    case 7: return "irhint_levels";
    default:
        throw runtime_error("Invalid encoding index: " + to_string(index));
    }
}


string StatsSerializer::to_csv_header(
    const iStats &is, const bool &all)
{
    if (is.encoding.size() != Cfg::get<int>("i.encoding-length"))
        throw runtime_error("iStats encoding size mismatch");

    string prefix = "i";
    stringstream ss;

    if (all) ss << prefix << "_params" << sep;

    for (int i = 0; i < is.encoding.size(); i++)
        ss << prefix << "_enc-" << i 
        << "_" << StatsSerializer::map_enc(i) << sep;

    return ss.str();
}


string StatsSerializer::to_csv(
    const iStats &is, const bool &all)
{
    stringstream ss;

    if (all) ss << is.params << sep;

    for (int i = 0; i < is.encoding.size(); i++)
        ss << is.encoding[i] << sep;

    return ss.str();
}


string StatsSerializer::to_csv_header(
    const qStats &qs, const bool &all)
{
    stringstream ss;
    if (all) ss << "q_query" << sep;

    ss << "q_start_rel" << sep;
    ss << "q_end_rel" << sep;

    if (all) ss << "q_extent_rel_log" << sep;

    for (int i = 1; i <= Cfg::get<int>("q.elem.max-cnt"); ++i)
        ss << "q_elem_" << i << "_rank_rel_log" << sep;

    return ss.str();
}


string StatsSerializer::to_csv(
    const qStats &qs, const bool &all)
{
    stringstream ss;
    if (all) ss << qs.query << sep;

    ss << qs.start_rel << sep;
    ss << qs.end_rel << sep;

    if (all) ss << qs.extent_rel_log << sep;

    if (qs.element_rank_logs.size() != Cfg::get<int>("q.elem.max-cnt"))
        throw runtime_error("Elements mismatch");

    for (const auto& rank_log : qs.element_rank_logs)
        ss << rank_log << sep;

    return ss.str();
}


string StatsSerializer::to_csv_header(
    const pStats &ps, const bool &all)
{
    stringstream ss;
    ss << "p_throughput_log" << sep;
    ss << "p_size_log" << sep;
    if (all)
    {
        ss << "p_throughput" << sep;
        ss << "p_size" << sep;
        ss << "p_result_cnt" << sep;
        ss << "p_selectivity_ratio_log" << sep;
        ss << "p_result_xor" << sep;
    }

    return ss.str();
}


string StatsSerializer::to_csv(
    const pStats &ps, const bool &all)
{
    stringstream ss;
    ss << ps.throughput_log << sep;
    ss << ps.size_log << sep;
    if (all)
    {
        ss << ps.throughput << sep;
        ss << ps.size << sep;
        ss << ps.result_cnt << sep;
        ss << ps.selectivity_ratio_log << sep;
        ss << ps.result_xor << sep;
    }

    return ss.str();
}
