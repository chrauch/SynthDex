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

#include "igen.h"
#include <limits>
#include <regex>

IGen::IGen()
    : randomgen()
{
    auto patterns = Cfg::get<vector<string>>("i.gen.patterns-active");
    for (const string& a : patterns)
    {
        auto idxschema_str = Cfg::get_json("i.gen.design-space." + a);
        auto idxschema = IdxSchemaSerializer::from_json(idxschema_str);

        this->idxschemas.push_back(idxschema);
    }
}


IdxSchema IGen::construct_I(const optional<string> &pattern)
{
    IdxSchema idxschema = pattern.has_value() && !pattern.value().empty()
        ? IdxSchemaSerializer::from_json(pattern.value()) 
        : this->idxschemas[this->randomgen.rndi(0, this->idxschemas.size() - 1)];

    auto idxschema_org = IdxSchemaSerializer::to_json_line(idxschema);
    Log::w(2, pattern.has_value() ? "Provided i pattern"
        : "Random i pattern", idxschema_org);

    this->change(idxschema);

    auto idxschema_new = IdxSchemaSerializer::to_json_line(idxschema);
    if (idxschema_org != idxschema_new)
        Log::w(2, "Generated i pattern", idxschema_new);

    return idxschema;
}


set<string> IGen::augment_closure(string &in)
{
    regex pattern(R"(\[([a-zA-Z,]+)\])");
    smatch match;
    set<string> results;

    if (regex_search(in, match, pattern))
    {
        string contents = match[1].str();
        vector<string> elements;
        stringstream ss(contents);
        string item;

        while (getline(ss, item, ',')) elements.push_back(item);

        for (const auto& elem : elements)
        {
            string replaced = in.substr(0, match.position(0)) + elem + in.substr(match.position(0) + match.length(0));
            auto replaced_augmented = this->augment_closure(replaced);
            results.insert(replaced_augmented.begin(), replaced_augmented.end());
        }

    }
    else
    {
        results.insert(in);
    }

    return results;
}


vector<IdxSchema> IGen::compute_closure(IdxSchema &in)
{
    auto in_str = IdxSchemaSerializer::to_json_line(in);
    auto closure_str = this->augment_closure(in_str);

    vector<IdxSchema> results;
    string msg = "Input schema:\n" + in_str
        + "\nOutput " + to_string(closure_str.size()) + " schemas:";
    for (const auto& c : closure_str)
    {
        results.push_back(IdxSchemaSerializer::from_json(c));
        msg +=  "\n" + c;
    }
    Log::w(2, "Closure", msg);

    return results;
}


vector<tuple<IdxSchema,IdxSchema>> IGen::compute_design_space()
{
    vector<tuple<IdxSchema,IdxSchema>> design_space;

    auto isminmax = [](const string &s)
    {
        return all_of(s.begin(), s.end(),
            [](char c) { return isdigit(c) || c == '.' || c == ',' 
                || c == '-' || c == '[' || c == ']'; });
    };

    set<string> unique_patterns;
    for (auto &i : this->idxschemas)
    {
        auto pattern = IdxSchemaSerializer::to_json_line(i);
        if (unique_patterns.find(pattern) != unique_patterns.end()) continue;
        unique_patterns.insert(pattern);

        Log::w(1, "Template", pattern);

        auto i_closure = this->compute_closure(i);

        for (size_t s = 0; s < i_closure.size(); s++)
        {
            auto &i = i_closure[s];
            
            IdxSchema min_i = i;
            IdxSchema max_i = i;

            // Process range
            auto [min_range, max_range] = this->min_max(i.range);
            min_i.range = min_range;
            max_i.range = max_range;

            // Process methods
            for (size_t m = 0; m < i.method.size(); m++)
            {
                auto method = i.method[m];
                if (isminmax(method))
                {
                    auto [min_method, max_method] = this->min_max(method);
                    min_i.method[m] = min_method;
                    max_i.method[m] = max_method;
                }
                else
                {
                    min_i.method[m] = method;
                    max_i.method[m] = method;
                }
            }

            // Process fanouts
            for (size_t f = 0; f < i.fanout.size(); f++)
            {
                auto [min_range, max_range] = this->min_max(i.fanout[f].range);
                min_i.fanout[f].range = min_range;
                max_i.fanout[f].range = max_range;

                for (size_t m = 0; m < i.fanout[f].method.size(); m++)
                {
                    if (isminmax(i.fanout[f].method[m]))
                    {
                        auto [min_method, max_method] = this->min_max(i.fanout[f].method[m]);
                        min_i.fanout[f].method[m] = min_method;
                        max_i.fanout[f].method[m] = max_method;
                    }
                    else
                    {
                        min_i.fanout[f].method[m] = i.fanout[f].method[m];
                        max_i.fanout[f].method[m] = i.fanout[f].method[m];
                    }
                }
            }

            this->sanitize(min_i, true);
            this->sanitize(max_i, false);

            design_space.push_back(make_tuple(min_i, max_i));

            Log::w(2, "Sub design space " + to_string(s+1),
                "Upper:\n" + IdxSchemaSerializer::to_json_line(max_i) + "\n" +
                "Lower:\n" + IdxSchemaSerializer::to_json_line(min_i));
        }
    }

    return design_space;
}


void IGen::change(IdxSchema &idxschema)
{
    idxschema.range = this->select(idxschema.range);

    for (auto &m : idxschema.method)
        m = this->select(m);

    for (auto &f : idxschema.fanout)
        this->change(f);

    this->sanitize(idxschema, nullopt);
}


void IGen::sanitize(IdxSchema &idxschema, const optional<bool> &min)
{
    if (!idxschema.split.empty())
    {
        this->sanitize_split(idxschema, min);
        return;
    }

    if (!idxschema.slice.empty())
    {
        this->sanitize_slice(idxschema, min);
        return;
    }

    if (!idxschema.refine.empty())
    {
        this->sanitize_refine(idxschema, min);
        return;
    }
}


void IGen::process_previous_references(IdxSchema &idxschema)
{
    // Process "-p" (previous) references in ranges
    for (size_t i = 0; i < idxschema.fanout.size(); i++)
    {
        auto& current = idxschema.fanout[i];

        if (current.range.length() < 3)
            throw runtime_error("Invalid range format: " + current.range);
        
        // Check if range ends with "-p"
        if (current.range.substr(current.range.length() - 2) == "-p")
        {
            if (i == 0)
                throw runtime_error(
                    "First fanout cannot reference previous range: "
                    + current.range);
            
            // Get the previous fanout's range
            auto& prev = idxschema.fanout[i - 1];
            size_t dash_pos = prev.range.find('-');
            if (dash_pos == string::npos)
                throw runtime_error(
                    "Invalid previous range format: " + prev.range);
            
            // Extract the lower bound of the previous range
            string prev_lower = prev.range.substr(0, dash_pos);
            
            // Replace "-p" with the previous lower bound
            string current_lower = current.range.substr(
                0, current.range.length() - 2);
            current.range = current_lower + "-" + prev_lower;
            
            Log::w(3, "Replaced '-p' reference", 
                "Set range to " + current.range 
                + " using previous lower bound " + prev_lower);
        }
    }
}


void IGen::sanitize_split(IdxSchema &idxschema, const optional<bool> &min)
{
    if (idxschema.fanout.empty())
        throw runtime_error("Empty fanout: " + idxschema.split);
    if (idxschema.split != "temporal")
        throw runtime_error("Unknown split: " + idxschema.split);

    this->process_previous_references(idxschema);
}


void IGen::sanitize_slice(IdxSchema &idxschema, const optional<bool> &min)
{
    if (idxschema.fanout.empty())
        throw runtime_error("Empty fanout: " + idxschema.slice);
    if (idxschema.slice != "temporal")
        throw runtime_error("Unknown slice: " + idxschema.slice);

    this->process_previous_references(idxschema);
}


void IGen::sanitize_refine(IdxSchema &idxschema, const optional<bool> &min)
{
    if (idxschema.fanout.size() > 3)
        throw runtime_error("Invalid fanout size: " + idxschema.refine);
    if (idxschema.refine != "elemfreq")
        throw runtime_error("Unknown refine: " + idxschema.refine);

    if (idxschema.fanout[0].range.empty() || 
        idxschema.fanout[0].range.substr(
            idxschema.fanout[0].range.length() - 2) != "-1")
        throw runtime_error("First fanout range must end with '-1': " 
            + idxschema.fanout[0].range);

    if (idxschema.fanout.back().range.empty() || 
        idxschema.fanout.back().range.substr(0, 2) != "0-")
        throw runtime_error("Last fanout range must start with '0-': " 
            + idxschema.fanout.back().range);

    this->process_previous_references(idxschema);
}


string IGen::select(string &values_str)
{
    if (values_str.find('[') == string::npos) return values_str;

    string result = values_str;
    
    // Process all [pattern] occurrences
    while (true)
    {
        size_t start_pos = result.find('[');
        if (start_pos == string::npos) break;
        
        size_t end_pos = result.find(']', start_pos);
        if (end_pos == string::npos) 
            throw runtime_error("Unmatched '[' in " + values_str);
        
        // Extract the pattern content (without brackets)
        string pattern_content = result.substr(
            start_pos + 1, end_pos - start_pos - 1);
        
        // Parse and select from this pattern
        string selected_value = this->select_from_pattern(pattern_content);
        
        // Replace the entire [pattern] with the selected value
        result.replace(start_pos, end_pos - start_pos + 1, selected_value);
    }

    Log::w(3, "Selected value", values_str + " -> " + result);
    
    return result;
}


string IGen::select_from_pattern(const string &pattern_content)
{
    vector<string> values;
    stringstream ss(pattern_content);
    string item;
    while (getline(ss, item, ','))
    {
        if (item.empty())
            throw runtime_error("Empty value in pattern: " + pattern_content);
        values.push_back(item);
    }

    if (values.empty())
        throw runtime_error("No values in pattern: " + pattern_content);

    int idx = this->randomgen.rndi(0, values.size() - 1);
    string value = values[idx];

    if (value.find('-') != string::npos)
    {
        size_t dash_pos = value.find('-');
        double min_val = stod(value.substr(0, dash_pos));
        double max_val = stod(value.substr(dash_pos + 1));
        double gen_val = this->randomgen.rndd(min_val, max_val);
        value = to_string(gen_val);
    }

    return value;
}


tuple<string,string> IGen::min_max_from_pattern(const string &pattern_content)
{
    vector<string> values;
    stringstream ss(pattern_content);
    string item;
    while (getline(ss, item, ','))
    {
        if (item.empty())
            throw runtime_error("Empty value in pattern: " + pattern_content);
        values.push_back(item);
    }

    if (values.empty())
        throw runtime_error("No values in pattern: " + pattern_content);

    string min_value, max_value;
    bool first = true;
    double overall_min = numeric_limits<double>::max();
    double overall_max = numeric_limits<double>::lowest();

    for (const string &value : values)
    {
        double val_min, val_max;
        
        if (value.find('-') != string::npos)
        {
            // Range value: extract min and max
            size_t dash_pos = value.find('-');
            val_min = stod(value.substr(0, dash_pos));
            val_max = stod(value.substr(dash_pos + 1));
        }
        else
        {
            // Single value: min and max are the same
            val_min = val_max = stod(value);
        }
        
        if (first || val_min < overall_min)
        {
            overall_min = val_min;
            min_value = to_string(val_min);
        }
        
        if (first || val_max > overall_max)
        {
            overall_max = val_max;
            max_value = to_string(val_max);
        }
        
        first = false;
    }

    return tuple(min_value, max_value);
}


tuple<string,string> IGen::min_max(string &values_str)
{
    if (values_str.find('[') == string::npos) return tuple(values_str, values_str);

    string min_result = values_str;
    string max_result = values_str;
    
    // Process all [pattern] occurrences
    while (true)
    {
        size_t start_pos = min_result.find('[');
        if (start_pos == string::npos) break;
        
        size_t end_pos = min_result.find(']', start_pos);
        if (end_pos == string::npos) 
            throw runtime_error("Unmatched '[' in " + values_str);
        
        // Extract the pattern content (without brackets)
        string pattern_content = min_result.substr(
            start_pos + 1, end_pos - start_pos - 1);
        
        // Parse and get min/max from this pattern
        auto [min_value, max_value] = this->min_max_from_pattern(pattern_content);
        
        // Replace the entire [pattern] with min/max values
        min_result.replace(start_pos, end_pos - start_pos + 1, min_value);
        max_result.replace(start_pos, end_pos - start_pos + 1, max_value);
    }

    Log::w(3, "Min and max values", values_str + " -> [" + min_result + ", " + max_result + "]");
    
    return tuple(min_result, max_result);
}