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

#include "idxschemaencoder.h"
#include "../utils/cfg.h"
#include "idxschemaserializer.h"
#include <stdexcept>
#include <cmath>


vector<double> IdxSchemaEncoder::encode(const IdxSchema &idxsch)
{
    auto idxsch_sanitized = IdxSchemaEncoder::sanitize(idxsch);

    auto enc = IdxSchemaEncoder::encode_x(idxsch_sanitized);

    auto enc_length = Cfg::get<int>("i.encoding-length");

    if (enc.size() != enc_length)
        throw runtime_error("Encoding mismatch: " + to_string(enc.size()) 
            + " != " + to_string(enc_length) + " : " 
            + IdxSchemaSerializer::to_json_line(idxsch_sanitized));

    return enc;
}


IdxSchema IdxSchemaEncoder::decode(const vector<double> &encoding)
{
    auto enc_length = Cfg::get<int>("i.encoding-length");

    if (encoding.size() != enc_length)
        throw runtime_error("Encoding mismatch: " + to_string(encoding.size()) 
            + " != " + to_string(enc_length));

    auto result = IdxSchemaEncoder::decode_x(encoding, 0);
    return result.first;
}


vector<double> IdxSchemaEncoder::encode_x(const IdxSchema &idxsch)
{
    vector<double> enc;

    if (idxsch.fanout.empty())
        enc = encode_leaf(idxsch);
    else
        enc = encode_fanout(idxsch);

    //enc.insert(enc.begin(), enc.size() + 1);

    return enc;
}


vector<double> IdxSchemaEncoder::encode_range(const string &range)
{
    vector<double> enc;

    if (range.empty())
    {
        enc.push_back(-1.0);
        enc.push_back(-1.0);
    }
    else if (range == "*")
    {
        enc.push_back(0.0); // start = 0
        enc.push_back(1.0); // end = 1
    }
    else
    {
        auto dash_pos = range.find('-');
        if (dash_pos == string::npos)
            throw runtime_error("Invalid range format: " + range);

        string from = range.substr(0, dash_pos);
        string to = range.substr(dash_pos + 1);

        enc.push_back(from == "*" ? 0.0 : stod(from));
        enc.push_back(to == "*" ? 1.0 : stod(to));
    }

    return enc;
}


vector<double> IdxSchemaEncoder::encode_method(const vector<string> &method)
{
    vector<double> enc;

    if (method.empty())
    {
        for (int i = 0; i < METHOD_ONEHOT_SIZE + 2; ++i)
            enc.push_back(-1.0);

        return enc;
    }
    
    auto onehot = [&](int index, double p1, double p2)
    {
        for (int i = 0; i < METHOD_ONEHOT_SIZE; ++i)
            enc.push_back(i == index ? 1.0 : 0.0);
        enc.push_back(p1);
        enc.push_back(p2);
    };

    // One-hot encoding of methods
    if (method[0] == "tif")
    {
        if (method.size() == 2 && method[1] == "basic")
        {
            onehot(TIF_BASIC, -1.0, -1.0);
        }
        else if (method.size() == 4 && method[1] == "slicing" && method[2] == "static")
        {
            onehot(TIF_SLICING_STATIC, stoi(method[3]), -1.0);
        }
        else if (method.size() == 5 && method[1] == "slicing" && method[2] == "dyn")
        {
            onehot(TIF_SLICING_DYN, stod(method[3]), stod(method[4]));
        }
        else if (method.size() == 4 && method[1] == "sharding" && method[2] == "static")
        {
            onehot(TIF_SHARDING_STATIC, stod(method[3]), -1.0);
        }
        else if (method.size() == 5 && method[1] == "sharding" && method[2] == "dyn")
        {
            onehot(TIF_SHARDING_DYN, stod(method[3]), stod(method[4]));
        }
        else if (method.size() == 5 && method[1] == "hint" && method[3] == "static")
        {
            if (method[2] == "mrg")
                onehot(TIF_HINT_MRG_STATIC, stoi(method[4]), -1.0);
            else
                throw runtime_error("Unsupported hint mode: " + method[2]);
        }
        else if (method.size() == 6 && method[1] == "hint" && method[3] == "dyn")
        {
            if (method[2] == "mrg")
                onehot(TIF_HINT_MRG_DYN, stod(method[4]), stod(method[5]));
            else
                throw runtime_error("Unsupported hint mode: " + method[2]);
        }
        else
        {
            throw runtime_error("Unsupported tif method: " + strs(method));
        }
    }
    else if (method[0] == "irhint")
    {
        if (method.size() == 3 && method[1] == "perf")
        {
            onehot(IRHINT_PERF, stod(method[2]), -1.0);
        }
        else
            throw runtime_error("Unsupported irhint method");
    }
    else
    {
        throw runtime_error("Unsupported method family: " + method[0]);
    }

    return enc;
}


vector<double> IdxSchemaEncoder::encode_leaf(const IdxSchema &idxsch)
{
    vector<double> enc;
    
    //enc.push_back(0.0); // mark as leaf

    auto range_enc = IdxSchemaEncoder::encode_range(idxsch.range);
    enc.insert(enc.end(), range_enc.begin(), range_enc.end());

    auto method_enc = IdxSchemaEncoder::encode_method(idxsch.method);
    enc.insert(enc.end(), method_enc.begin(), method_enc.end());

    return enc;
}


vector<double> IdxSchemaEncoder::encode_fanout_type(const IdxSchema &idxsch)
{
    vector<double> enc;
    
    auto onehot = [&](int index)
    {
        for (int i = 0; i < FANOUT_TYPE_ONEHOT_SIZE; ++i)
            enc.push_back(i == index ? 1.0 : 0.0);
    };

    if (idxsch.refine == "elemfreq")
        onehot(FANOUT_REFINE_ELEMFREQ);
    else if (idxsch.split == "temporal")
        onehot(FANOUT_SPLIT_TEMPORAL);
    else if (idxsch.slice == "temporal")
        onehot(FANOUT_SLICE_TEMPORAL);
    else if (idxsch.hybrid == "moveout-refine")
        onehot(FANOUT_HYBRID_MOVEOUT_REFINE);
    else
        throw runtime_error("Fanout not supported: " 
            + IdxSchemaSerializer::to_json_line(idxsch));

    return enc;
}


vector<double> IdxSchemaEncoder::encode_fanout(const IdxSchema &idxsch)
{
    vector<double> enc;
    
    //enc.push_back(static_cast<double>(idxsch.fanout.size()));

    auto fanout_type_enc = encode_fanout_type(idxsch);
    enc.insert(enc.end(), fanout_type_enc.begin(), fanout_type_enc.end());

    for (const auto &c : idxsch.fanout)
    {
        if (c.fanout.size() > 0)
            throw runtime_error("Nested fanout not supported");

        auto child_enc = encode_leaf(c);
        enc.insert(enc.end(), child_enc.begin(), child_enc.end());
    }

    return enc;
}


pair<IdxSchema, size_t> IdxSchemaEncoder::decode_x(
    const vector<double> &encoding, size_t offset)
{
    if (offset >= encoding.size())
    {
        throw runtime_error("Encoding offset out of bounds");
    }

    IdxSchema schema;
    size_t current_offset = offset;

    // After sanitization, all encodings are fanout structures with 3 children
    // Structure: fanout_type (5) + child1 (21) + child2 (21) + child3 (21) = 68 values
    
    if (current_offset + FANOUT_TYPE_ONEHOT_SIZE >= encoding.size())
    {
        throw runtime_error("Encoding too short for fanout type");
    }

    // Read fanout type
    decode_fanout_type(encoding, current_offset, schema);
    current_offset += FANOUT_TYPE_ONEHOT_SIZE;

    // Read exactly 3 children (always present after sanitization)
    for (int i = 0; i < 3; ++i)
    {
        if (current_offset + 2 >= encoding.size())
        {
            throw runtime_error("Encoding too short for child " + to_string(i));
        }

        IdxSchema child;
        
        // Read range (2 values)
        double start = encoding[current_offset++];
        double end = encoding[current_offset++];

        // Check if this is a padding child (range is -1, -1)
        if (start == -1.0 && end == -1.0)
        {
            // This is a padding child - skip method encoding
            current_offset += METHOD_ONEHOT_SIZE + 2; // Skip method one-hot + 2 params
            schema.fanout.push_back(child); // Add empty child
            continue;
        }

        // Non-padding child - decode range
        auto clean_double_str = [](double val) -> string
        {
            string s = to_string(val);
            size_t dot_pos = s.find('.');
            if (dot_pos != string::npos)
            {
                s.erase(s.find_last_not_of('0') + 1, string::npos);
                if (!s.empty() && s.back() == '.') s.pop_back();
            }
            return s;
        };

        string start_str = (start == 0.0) ? "0" : clean_double_str(start);
        string end_str = (end == 1.0) ? "1" : clean_double_str(end);

        child.range = start_str + "-" + end_str;

        if (current_offset + METHOD_ONEHOT_SIZE + 2 > encoding.size())
        {
            throw runtime_error("Encoding too short for method");
        }

        // Read one-hot method encoding
        int method_index = -1;
        for (int i = 0; i < METHOD_ONEHOT_SIZE; ++i)
        {
            if (encoding[current_offset + i] == 1.0)
            {
                method_index = i;
                break;
            }
        }
        current_offset += METHOD_ONEHOT_SIZE;

        double p1 = encoding[current_offset++];
        double p2 = encoding[current_offset++];

        child.method = decode_method(method_index, p1, p2);
        
        schema.fanout.push_back(child);
    }

    return make_pair(schema, current_offset);
}


vector<string> IdxSchemaEncoder::decode_method(int method_index, double p1, double p2)
{
    switch (method_index)
    {
    case TIF_BASIC:
        return {"tif", "basic"};
        
    case TIF_SLICING_STATIC:
        return {"tif", "slicing", "static", to_string(p1)};
        
    case TIF_SLICING_DYN:
        return {"tif", "slicing", "dyn", to_string(p1), to_string(p2)};
        
    case TIF_SHARDING_STATIC:
        return {"tif", "sharding", "static", to_string(p1)};

    case TIF_SHARDING_DYN:
        return {"tif", "sharding", "dyn", to_string(p1), to_string(p2)};
        
    case TIF_HINT_MRG_STATIC:
        return {"tif", "hint", "mrg", "static", to_string(p1)};
        
    case TIF_HINT_MRG_DYN:
        return {"tif", "hint", "mrg", "dyn", to_string(p1), to_string(p2)};
        
    case IRHINT_PERF:
        return {"irhint", "perf", to_string(p1)};
        
    default:
        throw runtime_error("Unknown method index: " + to_string(method_index));
    }
}

int IdxSchemaEncoder::decode_fanout_type(const vector<double> &encoding, size_t offset, IdxSchema &schema)
{
    int fanout_type_index = -1;
    
    for (int i = 0; i < FANOUT_TYPE_ONEHOT_SIZE; ++i)
    {
        if (encoding[offset + i] == 1.0)
        {
            fanout_type_index = i;
            break;
        }
    }

    switch (fanout_type_index)
    {
    case FANOUT_REFINE_ELEMFREQ:
        schema.refine = "elemfreq";
        break;
        
    case FANOUT_SPLIT_TEMPORAL:
        schema.split = "temporal";
        break;
        
    case FANOUT_SLICE_TEMPORAL:
        schema.slice = "temporal";
        break;
        
    case FANOUT_HYBRID_MOVEOUT_REFINE:
        schema.hybrid = "moveout-refine";
        break;
        
    default:
        throw runtime_error("Unknown fanout type index: " + to_string(fanout_type_index));
    }

    return fanout_type_index;
}

IdxSchema IdxSchemaEncoder::sanitize(const IdxSchema &org)
{
    // wrap the idxschema in another (as fanout) of type split=temporal
    if (org.split.empty() && 
        org.refine.empty() && 
        org.fanout.empty() && 
        !org.method.empty())
    {
        IdxSchema wrapped = org;
        wrapped.range = "0-1";

        IdxSchema wrapper;
        wrapper.split = "temporal";
        wrapper.refine = "";
        wrapper.hybrid = "";
        wrapper.method = {};

        IdxSchema padding1;
        IdxSchema padding2;
        wrapper.fanout.push_back(padding1);
        wrapper.fanout.push_back(padding2);
        wrapper.fanout.push_back(wrapped);
        
        return wrapper;
    }

    if (org.fanout.size() > 3 || org.fanout.empty())
    {
        throw runtime_error("Fanout size not allowed: " 
            + to_string(org.fanout.size()));
    }

    if (org.fanout.size() < 3)
    {
        IdxSchema extended = org;

        while (extended.fanout.size() < 3)
        {
            IdxSchema padding;
            extended.fanout.insert(extended.fanout.begin(), padding);
        }

        return extended;
    }
    
    return org;
}
