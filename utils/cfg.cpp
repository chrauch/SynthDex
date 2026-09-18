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

#include "cfg.h"
#include "parsing.h"
#include <iomanip>
#include <sstream>
#include <filesystem>
#include <functional>


Cfg *Cfg::singleton = nullptr;
string Cfg::config_path;


Cfg::Cfg(const string &config_dir)
{
    Cfg::compose_config(config_dir);

    ifstream file(config_path);
    if (!file.is_open())
        throw runtime_error("Could not open configuration file");

    ostringstream filtered;
    string line;
    while (getline(file, line))
    {
        size_t first = line.find_first_not_of(" \t");
        if (first == string::npos)
            continue;
        if (first + 1 < line.size() && line[first] == '/' && line[first + 1] == '/')
            continue;
        // strip inline // comments (outside of strings)
        bool in_str = false;
        for (size_t i = first; i < line.size(); ++i)
        {
            if (line[i] == '"') in_str = !in_str;
            if (!in_str && i + 1 < line.size() && line[i] == '/' && line[i + 1] == '/')
            {
                line = line.substr(0, i);
                break;
            }
        }
        filtered << line << '\n';
    }
    istringstream content(filtered.str());
    root = parse(content);
}


Cfg &Cfg::instance()
{
    if (!singleton) throw runtime_error("Configuration not initialized.");
    return *singleton;
}


void Cfg::compose_config(const string &config_dir)
{
    string src_path = config_dir + "/config.core.json";

    string out_dir = Cfg::get_out_dir() + "/config";
    filesystem::create_directories(out_dir);

    ifstream src(src_path);
    if (!src.is_open())
        throw runtime_error("Could not open configuration file: " + src_path);

    Cfg::config_path = out_dir + "/config.json";

    ofstream dst(Cfg::config_path, ios::trunc);
    if (!dst.is_open())
        throw runtime_error("Could not write config to output directory: " + out_dir);

    function<void(istream&, const string&, bool)> write_lines = [&](istream &in, const string &src_name, bool resolve_includes)
    {
        string line;
        while (getline(in, line))
        {
            size_t first = line.find_first_not_of(" \t");
            if (first == string::npos) { dst << '\n'; continue; }

            // strip # comment lines
            if (line[first] == '#')
            {
                if (resolve_includes && line.compare(first, 9, "#include ") == 0)
                {
                    size_t q1 = line.find('"', first + 9);
                    size_t q2 = (q1 != string::npos) ? line.find('"', q1 + 1) : string::npos;
                    if (q1 == string::npos || q2 == string::npos)
                        throw runtime_error("Malformed #include directive: " + line);

                    string inc_path = config_dir + "/" + line.substr(q1 + 1, q2 - q1 - 1);
                    ifstream inc(inc_path);
                    if (!inc.is_open())
                        throw runtime_error("Could not open included config file: " + inc_path);

                    write_lines(inc, inc_path, false);
                }
                // else: plain # comment, skip
                continue;
            }

            dst << line << '\n';
        }
    };

    write_lines(src, src_path, true);
}


SetValue Cfg::parse(istream &in)
{
    Parsing::skip_ws(in);
    char c = in.peek();

    if (c == '"')
    {
        return SetValue(Parsing::parse_string(in));
    }
    else if (c == '{')
    {
        in.get(); // consume '{'
        SetValue obj;
        obj.type = SetValue::Object;

        Parsing::skip_ws(in);
        if (in.peek() != '}')
        {
            while (true)
            {
                string key = Parsing::parse_key(in);
                obj.object_values[key] = parse(in);

                Parsing::skip_ws(in);
                if (in.peek() == '}')
                    break;
                Parsing::expect(in, ',');
                Parsing::skip_ws(in);
                if (in.peek() == '}')
                    break; // trailing comma
            }
        }
        in.get(); // consume '}'
        return obj;
    }
    else if (isdigit(c) || c == '-')
    {
        double num;
        in >> num;
        return SetValue(num);
    }
    else if (c == 't' || c == 'f')
    {
        string val;
        while (isalpha(in.peek()))
            val += in.get();
        return SetValue(val == "true");
    }
    else if (c == '[')
    {
        in.get(); // consume '['
        SetValue arr;
        arr.type = SetValue::Array;

        Parsing::skip_ws(in);
        if (in.peek() != ']')
        {
            while (true)
            {
                arr.array_values.push_back(parse(in));

                Parsing::skip_ws(in);
                if (in.peek() == ']')
                    break;
                Parsing::expect(in, ',');
                Parsing::skip_ws(in);
                if (in.peek() == ']')
                    break; // trailing comma
            }
        }
        in.get(); // consume ']'
        return arr;
    }
    else
    {
        throw runtime_error("Unexpected character in JSON");
    }
}


const SetValue &Cfg::get(const string &path) const
{
    vector<string> parts;
    stringstream ss(path);
    string part;

    while (getline(ss, part, '.'))
    {
        parts.push_back(part);
    }

    const SetValue *current = &root;
    for (const auto &p : parts)
    {
        if (current->type != SetValue::Object || current->object_values.find(p) == current->object_values.end())
        {
            throw runtime_error("Path not found: " + path);
        }
        current = &current->object_values.at(p);
    }
    return *current;
}


string Cfg::str() const
{
    return Cfg::config_path + "\n" + str(this->root, 0);
}


string Cfg::str(const SetValue &value, int indent) const
{
    ostringstream oss;
    string indentation(indent, '\t');  // 1 tab per indentation level

    switch (value.type)
    {
    case SetValue::String:
        oss << "\"" << value.string_value << "\"";
        break;
    case SetValue::Number:
        oss << value.number_value;
        break;
    case SetValue::Boolean:
        oss << (value.bool_value ? "true" : "false");
        break;
    case SetValue::Object:
        oss << "{\n";
        {
            auto it = value.object_values.begin();
            for (size_t i = 0; i < value.object_values.size(); ++i, ++it)
            {
                oss << indentation << "\t\"" << it->first << "\": ";
                
                // Add newline and extra indent for nested objects/arrays
                if (it->second.type == SetValue::Object || it->second.type == SetValue::Array)
                    oss << "\n" << indentation << "\t" << str(it->second, indent + 1);
                else
                    oss << str(it->second, indent + 1);
                
                if (i < value.object_values.size() - 1)
                    oss << ",";
                oss << "\n";
            }
        }
        oss << indentation << "}";
        break;
    case SetValue::Array:
        oss << "[\n";
        for (size_t i = 0; i < value.array_values.size(); ++i)
        {
            oss << indentation << "\t"
                << str(value.array_values[i], indent + 1);
            if (i < value.array_values.size() - 1)
                oss << ",";
            oss << "\n";
        }
        oss << indentation << "]";
        break;
    default:
        oss << "null";
    }

    return oss.str();
}


string Cfg::get_json_at(const string &path) const
{
    const SetValue &val = get(path);
    return str(val, 0);
}


static void merge_values(SetValue &dst, const SetValue &src)
{
    if (src.type == SetValue::Object && dst.type == SetValue::Object)
    {
        for (const auto &kv : src.object_values)
        {
            auto it = dst.object_values.find(kv.first);
            if (it != dst.object_values.end())
                merge_values(it->second, kv.second);
            else
                dst.object_values[kv.first] = kv.second;
        }
    }
    else
    {
        dst = src;
    }
}


void Cfg::apply_stage(const string &stage)
{
    const string path = "sequences.stages." + stage + ".config";
    const SetValue &stage_cfg = get(path);

    merge_values(root, stage_cfg);

    this->persist();
}


void Cfg::persist() const
{
    ofstream out(config_path, ios::trunc);
    if (!out.is_open())
        throw runtime_error("Could not write config to: " + config_path);
    out << str(root, 0) << '\n';
}


bool Cfg::has(const string &path)
{
    try
    {
        instance().get(path);
        return true;
    }
    catch (const exception &)
    {
        return false;
    }
}


SetValue *Cfg::mutable_node(const string &path)
{
    vector<string> parts;
    stringstream ss(path);
    string part;
    while (getline(ss, part, '.'))
        parts.push_back(part);

    SetValue *current = &root;
    for (const auto &p : parts)
    {
        if (current->type != SetValue::Object)
            throw runtime_error("Path is not an object: " + path);
        // Missing intermediates are created so that new settings can be
        // injected into methods that do not declare them in the config.
        current = &current->object_values[p];
        if (current->type == SetValue::Null)
            current->type = SetValue::Object;
    }
    return current;
}


void Cfg::merge_at(const string &path, const SetValue &src)
{
    merge_values(*mutable_node(path), src);
    this->persist();
}


void Cfg::set_at(const string &path, const SetValue &value)
{
    *mutable_node(path) = value;
    this->persist();
}


void Cfg::restore(const SetValue &snapshot)
{
    root = snapshot;
    this->persist();
}


SetValue::SetValue() : type(Null), number_value(0), bool_value(false) {}
SetValue::SetValue(double val) : type(Number), number_value(val), bool_value(false) {}
SetValue::SetValue(const string &val) : type(String), number_value(0), string_value(val), bool_value(false) {}
SetValue::SetValue(bool val) : type(Boolean), number_value(0), bool_value(val) {}