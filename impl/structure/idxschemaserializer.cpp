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

#include "idxschemaserializer.h"
#include "../utils/parsing.h"
#include <sstream>
#include <stdexcept>
#include <cctype>
#include <algorithm>
#include <iostream>
#include <utility>

string IdxSchemaSerializer::to_json(
    const IdxSchema &idxsch, int indent, const string &tab, const string &nl)
{
    string ind(indent, tab[0]);
    string out = ind + "{" + nl;
    bool first = true;

    auto add_field = [&](const string &key, const string &value)
    {
        if (!first)
            out += "," + nl;
        out += ind + tab + "\"" + key + "\": \"" + value + "\"";
        first = false;
    };

    // Check if we should collapse fanout (only 1 non-empty child)
    bool should_collapse_fanout = false;
    IdxSchema active_child;
    
    if (!idxsch.fanout.empty())
    {
        int non_empty_count = 0;
        size_t active_idx = 0;
        
        for (size_t i = 0; i < idxsch.fanout.size(); ++i)
        {
            const auto& child = idxsch.fanout[i];
            // A child is non-empty if it has a method or its own fanout
            bool is_empty = child.method.empty() && child.fanout.empty();
            if (!is_empty)
            {
                non_empty_count++;
                active_idx = i;
            }
        }
        
        // Collapse if exactly 1 non-empty child
        if (non_empty_count == 1)
        {
            should_collapse_fanout = true;
            active_child = idxsch.fanout[active_idx];
        }
    }

    // Only show refine/split/slice/hybrid if we're NOT collapsing fanout
    if (!should_collapse_fanout)
    {
        if (!idxsch.refine.empty())
        {
            add_field("refine", idxsch.refine);
        }
        if (!idxsch.split.empty())
        {
            add_field("split", idxsch.split);
        }
        if (!idxsch.slice.empty())
        {
            add_field("slice", idxsch.slice);
        }
        if (!idxsch.hybrid.empty())
        {
            add_field("hybrid", idxsch.hybrid);
        }
    }
    
    // If collapsing fanout, show the active child's method directly
    if (should_collapse_fanout)
    {
        if (!active_child.method.empty() && !active_child.method[0].empty())
        {
            string method_str;
            for (size_t i = 0; i < active_child.method.size(); ++i)
            {
                if (i > 0)
                    method_str += ";";
                method_str += active_child.method[i];
            }
            add_field("method", method_str);
        }
        // If the child has its own fanout, display it
        if (!active_child.fanout.empty())
        {
            // Filter out empty nodes
            vector<IdxSchema> non_empty_children;
            for (const auto& child : active_child.fanout)
            {
                bool is_empty = child.method.empty() && child.fanout.empty();
                if (!is_empty)
                    non_empty_children.push_back(child);
            }
            
            if (!non_empty_children.empty())
            {
                if (!first)
                    out += "," + nl;
                out += ind + tab + "\"fanout\":" + nl + ind + tab + "[" + nl;
                for (size_t i = 0; i < non_empty_children.size(); ++i)
                {
                    out += IdxSchemaSerializer::to_json(
                        non_empty_children[i], indent + 2, tab, nl);
                    if (i + 1 < non_empty_children.size())
                        out += "," + nl;
                }
                out += nl + ind + tab + "]";
                first = false;
            }
        }
    }
    else
    {
        // Normal case: show range and method if present
        if (!idxsch.range.empty())
        {
            add_field("range", idxsch.range);
        }
        
        if (!idxsch.method.empty() && !idxsch.method[0].empty())
        {
            string method_str;
            for (size_t i = 0; i < idxsch.method.size(); ++i)
            {
                if (i > 0)
                    method_str += ";";
                method_str += idxsch.method[i];
            }
            add_field("method", method_str);
        }

        if (!idxsch.fanout.empty())
        {
            // Filter out empty nodes
            vector<IdxSchema> non_empty_children;
            for (const auto& child : idxsch.fanout)
            {
                bool is_empty = child.method.empty() && child.fanout.empty();
                if (!is_empty)
                    non_empty_children.push_back(child);
            }
            
            if (!non_empty_children.empty())
            {
                if (!first)
                    out += "," + nl;
                out += ind + tab + "\"fanout\":" + nl + ind + tab + "[" + nl;
                for (size_t i = 0; i < non_empty_children.size(); ++i)
                {
                    out += IdxSchemaSerializer::to_json(
                        non_empty_children[i], indent + 2, tab, nl);
                    if (i + 1 < non_empty_children.size())
                        out += "," + nl;
                }
                out += nl + ind + tab + "]";
                first = false;
            }
        }
    }

    out += nl + ind + "}";
    return out;
}

string IdxSchemaSerializer::to_json_line(const IdxSchema &idxsch)
{
    string out = "{";
    bool first = true;

    auto add_field = [&](const string &key, const string &value)
    {
        if (!first)
            out += ",";
        out += "\"" + key + "\":\"" + value + "\"";
        first = false;
    };

    // Check if we should collapse fanout (only 1 non-empty child)
    bool should_collapse_fanout = false;
    IdxSchema active_child;
    
    if (!idxsch.fanout.empty())
    {
        int non_empty_count = 0;
        size_t active_idx = 0;
        
        for (size_t i = 0; i < idxsch.fanout.size(); ++i)
        {
            const auto& child = idxsch.fanout[i];
            // A child is non-empty if it has a method or its own fanout
            bool is_empty = child.method.empty() && child.fanout.empty();
            if (!is_empty)
            {
                non_empty_count++;
                active_idx = i;
            }
        }
        
        // Collapse if exactly 1 non-empty child
        if (non_empty_count == 1)
        {
            should_collapse_fanout = true;
            active_child = idxsch.fanout[active_idx];
        }
    }

    // Only show refine/split/slice/hybrid if we're NOT collapsing fanout
    if (!should_collapse_fanout)
    {
        if (!idxsch.refine.empty())
        {
            add_field("refine", idxsch.refine);
        }
        if (!idxsch.range.empty())
        {
            add_field("range", idxsch.range);
        }
        if (!idxsch.split.empty())
        {
            add_field("split", idxsch.split);
        }
        if (!idxsch.slice.empty())
        {
            add_field("slice", idxsch.slice);
        }
        if (!idxsch.hybrid.empty())
        {
            add_field("hybrid", idxsch.hybrid);
        }
    }
    
    // If collapsing fanout, show the active child's method directly
    if (should_collapse_fanout)
    {
        if (!active_child.method.empty() && !active_child.method[0].empty())
        {
            string method_str;
            for (size_t i = 0; i < active_child.method.size(); ++i)
            {
                if (i > 0)
                    method_str += ";";
                method_str += active_child.method[i];
            }
            add_field("method", method_str);
        }
        // If the child has its own fanout, display it
        if (!active_child.fanout.empty())
        {
            // Filter out empty nodes
            vector<IdxSchema> non_empty_children;
            for (const auto& child : active_child.fanout)
            {
                bool is_empty = child.method.empty() && child.fanout.empty();
                if (!is_empty)
                    non_empty_children.push_back(child);
            }
            
            if (!non_empty_children.empty())
            {
                if (!first)
                    out += ",";
                out += "\"fanout\":[";
                for (size_t i = 0; i < non_empty_children.size(); ++i)
                {
                    out += IdxSchemaSerializer::to_json_line(non_empty_children[i]);
                    if (i + 1 < non_empty_children.size())
                        out += ",";
                }
                out += "]";
                first = false;
            }
        }
    }
    else
    {
        // Normal case: show range and method if present
        if (!idxsch.method.empty() && !idxsch.method[0].empty())
        {
            string method_str;
            for (size_t i = 0; i < idxsch.method.size(); ++i)
            {
                if (i > 0)
                    method_str += ";";
                method_str += idxsch.method[i];
            }
            add_field("method", method_str);
        }

        if (!idxsch.fanout.empty())
        {
            // Filter out empty nodes
            vector<IdxSchema> non_empty_children;
            for (const auto& child : idxsch.fanout)
            {
                bool is_empty = child.method.empty() && child.fanout.empty();
                if (!is_empty)
                    non_empty_children.push_back(child);
            }
            
            if (!non_empty_children.empty())
            {
                if (!first)
                    out += ",";
                out += "\"fanout\":[";
                for (size_t i = 0; i < non_empty_children.size(); ++i)
                {
                    out += IdxSchemaSerializer::to_json_line(non_empty_children[i]);
                    if (i + 1 < non_empty_children.size())
                        out += ",";
                }
                out += "]";
                first = false;
            }
        }
    }

    out += "}";
    return out;
}

vector<IdxSchema> IdxSchemaSerializer::from_json_array(istream &in)
{
    Parsing::expect(in, '[');
    vector<IdxSchema> elements;
    Parsing::skip_ws(in);
    while (in.peek() != ']')
    {
        elements.push_back(from_json_element(in));
        Parsing::skip_ws(in);
        if (in.peek() == ',')
        {
            in.get();
            Parsing::skip_ws(in);
        }
    }
    Parsing::expect(in, ']');
    return elements;
}

IdxSchema IdxSchemaSerializer::from_json_element(istream &in)
{
    Parsing::expect(in, '{');
    IdxSchema elem;

    while (true)
    {
        Parsing::skip_ws(in);
        if (in.peek() == '}')
        {
            in.get();
            break;
        }

        string key = Parsing::parse_key(in);
        if (key == "fanout")
        {
            elem.fanout = IdxSchemaSerializer::from_json_array(in);
        }
        else if (key == "range")
        {
            elem.range = Parsing::parse_string(in);
        }
        else if (key == "split")
        {
            elem.split = Parsing::parse_string(in);
        }
        else if (key == "slice")
        {
            elem.slice = Parsing::parse_string(in);
        }
        else if (key == "hybrid")
        {
            elem.hybrid = Parsing::parse_string(in);
        }
        else if (key == "refine")
        {
            elem.refine = Parsing::parse_string(in);
        }
        else if (key == "method")
        {
            auto method_str = Parsing::parse_string(in);
            istringstream ss(method_str);
            string token;
            while (getline(ss, token, ';'))
                elem.method.push_back(Parsing::trim(token));
        }
        else
        {
            throw runtime_error("Unknown key: " + key);
        }

        Parsing::skip_ws(in);
        if (in.peek() == ',')
        {
            in.get();
        }
    }

    return elem;
}

IdxSchema IdxSchemaSerializer::from_json(const string &json)
{
    auto is = istringstream(json);
    return IdxSchemaSerializer::from_json_element(is);
}


