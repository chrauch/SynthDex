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

#include "cfg.h"
#include "parsing.h"
#include <iomanip>
#include <sstream>


Cfg *Cfg::singleton = nullptr;


Cfg::Cfg(const string &config_dir)
    : config_dir(config_dir)
{
    ifstream file(config_dir + "/config.json");
    if (!file.is_open())
        throw runtime_error("Could not open configuration file");

    ostringstream filtered;
    string line;
    while (getline(file, line))
    {
        size_t first = line.find_first_not_of(" \t");
        if (first == string::npos)
            continue;
        if (line[first] == '#')
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
    return this->config_dir + "/config.json\n" + str(this->root, 0);
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


SetValue::SetValue() : type(Null), number_value(0), bool_value(false) {}
SetValue::SetValue(double val) : type(Number), number_value(val), bool_value(false) {}
SetValue::SetValue(const string &val) : type(String), number_value(0), string_value(val), bool_value(false) {}
SetValue::SetValue(bool val) : type(Boolean), number_value(0), bool_value(val) {}