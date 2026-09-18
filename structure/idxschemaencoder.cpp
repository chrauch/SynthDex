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

#include "idxschemaencoder.h"
#include "../utils/cfg.h"
#include "idxschemaserializer.h"
#include <stdexcept>
#include <cmath>


int IdxSchemaEncoder::identify_tif_method(const vector<string> &method)
{
    if (method.empty() || method[0] != "tif")
        throw runtime_error("Expected tif method");

    if (method.size() >= 2 && method[1] == "basic")
        return TIF_BASIC;
    else if (method.size() >= 2 && method[1] == "slicing")
        return TIF_SLICING_DYN;
    else if (method.size() >= 2 && method[1] == "sharding")
        return TIF_SHARDING_DYN;
    else if (method.size() >= 2 && method[1] == "hint")
        return TIF_HINT_MRG_DYN;
    else
        throw runtime_error("Unsupported tif method: " + strs(method));
}


pair<double, double> IdxSchemaEncoder::extract_params(const vector<string> &method)
{
    if (method.empty())
        return {-1.0, -1.0};

    if (method[0] == "tif" && method.size() >= 2)
    {
        if (method[1] == "basic")
            return {-1.0, -1.0};
        else if (method[1] == "slicing" && method.size() >= 4)
        {
            if (method[3] == "dyn" && method.size() >= 6)
                return {stod(method[4]), stod(method[5])};
            else if (method[3] == "static" && method.size() >= 5)
                return {stod(method[4]), -1.0};
        }
        else if (method[1] == "sharding" && method.size() >= 3)
        {
            if (method[2] == "dyn" && method.size() >= 5)
                return {stod(method[3]), stod(method[4])};
            else if (method[2] == "static" && method.size() >= 4)
                return {stod(method[3]), -1.0};
        }
        else if (method[1] == "hint" && method.size() >= 4)
        {
            if (method[3] == "dyn" && method.size() >= 6)
                return {stod(method[4]), stod(method[5])};
            else if (method[3] == "static" && method.size() >= 5)
                return {stod(method[4]), -1.0};
        }

        throw runtime_error("Unsupported tif params: " + strs(method));
    }

    throw runtime_error("Expected tif method for params: " + strs(method));
}


double IdxSchemaEncoder::parse_range_boundary(const string &range)
{
    if (range.empty())
        return -1.0;

    auto dash_pos = range.find('-');
    if (dash_pos == string::npos)
        throw runtime_error("Invalid range format: " + range);

    string from = range.substr(0, dash_pos);
    string to = range.substr(dash_pos + 1);

    double from_val = (from == "*" || from == "0") ? 0.0 : stod(from);
    double to_val = (to == "*" || to == "1") ? 1.0 : stod(to);

    // Return the lower bound (which is the dict boundary)
    return from_val;
}


vector<double> IdxSchemaEncoder::encode(const IdxSchema &idxsch)
{
    auto enc_length = Cfg::get<int>("i.encoding-length");
    vector<double> enc(enc_length, 0.0);

    // Case 1: Pure terminal (no fanout)
    if (idxsch.fanout.empty() && !idxsch.method.empty())
    {
        if (idxsch.method[0] == "irhint")
        {
            // Pure irHINT: boundary=1, levels from method
            enc[POS_BOUNDARY] = 1.0;
            enc[POS_IRHINT_LEVELS] = stod(idxsch.method[2]);
            // tIF one-hot and alpha/beta stay 0/-1
            enc[POS_ALPHA] = -1.0;
            enc[POS_BETA] = -1.0;
        }
        else if (idxsch.method[0] == "tif")
        {
            // Pure tIF: boundary=0
            int mi = identify_tif_method(idxsch.method);
            enc[mi] = 1.0;
            auto [alpha, beta] = extract_params(idxsch.method);
            enc[POS_ALPHA] = alpha;
            enc[POS_BETA] = beta;
            enc[POS_BOUNDARY] = 0.0;
            enc[POS_IRHINT_LEVELS] = -1.0;
        }
        else
        {
            throw runtime_error("Unsupported method: " + strs(idxsch.method));
        }
    }
    // Case 2: Fanout with refine=elemfreq (2 children: tIF + irHINT)
    else if (!idxsch.fanout.empty() && idxsch.refine == "elemfreq")
    {
        // Find the tIF child and the irHINT child
        const IdxSchema *tif_child = nullptr;
        const IdxSchema *irhint_child = nullptr;

        for (const auto &child : idxsch.fanout)
        {
            if (!child.method.empty() && child.method[0] == "tif")
                tif_child = &child;
            else if (!child.method.empty() && child.method[0] == "irhint")
                irhint_child = &child;
        }

        if (!tif_child)
            throw runtime_error("No tIF child in refine-elemfreq fanout");
        if (!irhint_child)
            throw runtime_error("No irHINT child in refine-elemfreq fanout");

        // Encode tIF method
        int mi = identify_tif_method(tif_child->method);
        enc[mi] = 1.0;
        auto [alpha, beta] = extract_params(tif_child->method);
        enc[POS_ALPHA] = alpha;
        enc[POS_BETA] = beta;

        // Boundary: lower bound of the tIF child's range
        // tIF covers [boundary, 1], irHINT covers [0, boundary]
        enc[POS_BOUNDARY] = parse_range_boundary(tif_child->range);

        // irHINT levels
        enc[POS_IRHINT_LEVELS] = stod(irhint_child->method[2]);
    }
    else
    {
        throw runtime_error("Unsupported schema structure: "
            + IdxSchemaSerializer::to_json_line(idxsch));
    }

    if (enc.size() != enc_length)
        throw runtime_error("Encoding mismatch: " + to_string(enc.size())
            + " != " + to_string(enc_length) + " : "
            + IdxSchemaSerializer::to_json_line(idxsch));

    return enc;
}


IdxSchema IdxSchemaEncoder::decode(const vector<double> &encoding)
{
    auto enc_length = Cfg::get<int>("i.encoding-length");

    if (encoding.size() != enc_length)
        throw runtime_error("Encoding mismatch: " + to_string(encoding.size())
            + " != " + to_string(enc_length));

    double boundary = encoding[POS_BOUNDARY];
    double tolerance = Cfg::get<double>("synthesis.boundary-tolerance");

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

    // Identify the tIF method from one-hot
    int tif_method = -1;
    for (int i = 0; i < METHOD_ONEHOT_SIZE; ++i)
    {
        if (encoding[i] >= 0.5)
        {
            tif_method = i;
            break;
        }
    }

    double alpha = encoding[POS_ALPHA];
    double beta = encoding[POS_BETA];
    double levels = encoding[POS_IRHINT_LEVELS];

    // Case 1: Pure tIF (boundary near 0)
    if (boundary <= tolerance)
    {
        if (tif_method < 0)
            throw runtime_error("No tIF method selected but boundary=0");
        IdxSchema schema;
        schema.method = decode_tif_method(tif_method, alpha, beta);
        return schema;
    }

    // Case 2: Pure irHINT (boundary near 1)
    if (boundary >= 1.0 - tolerance)
    {
        IdxSchema schema;
        schema.method = {"irhint", "a", clean_double_str(levels)};
        return schema;
    }

    // Case 3: Two-partition refine-elemfreq
    IdxSchema schema;
    schema.refine = "elemfreq";

    // Child 0: tIF at [boundary, 1]
    IdxSchema tif_child;
    tif_child.range = clean_double_str(boundary) + "-1";
    if (tif_method < 0)
        throw runtime_error("No tIF method selected for two-partition index");
    tif_child.method = decode_tif_method(tif_method, alpha, beta);

    // Child 1: irHINT at [0, boundary]
    IdxSchema irhint_child;
    irhint_child.range = "0-" + clean_double_str(boundary);
    irhint_child.method = {"irhint", "a", clean_double_str(levels)};

    schema.fanout.push_back(tif_child);
    schema.fanout.push_back(irhint_child);

    return schema;
}


vector<string> IdxSchemaEncoder::decode_tif_method(
    int method_index, double alpha, double beta)
{
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

    switch (method_index)
    {
    case TIF_BASIC:
        return {"tif", "basic"};

    case TIF_SLICING_DYN:
        if (beta < 0.0)
            return {"tif", "slicing", "a", "static",
                clean_double_str(alpha)};
        return {"tif", "slicing", "a", "dyn",
            clean_double_str(alpha), clean_double_str(beta)};

    case TIF_SHARDING_DYN:
        if (beta < 0.0)
            return {"tif", "sharding", "static",
                clean_double_str(alpha)};
        return {"tif", "sharding", "dyn",
            clean_double_str(alpha), clean_double_str(beta)};

    case TIF_HINT_MRG_DYN:
        if (beta < 0.0)
            return {"tif", "hint", "mrg", "static",
                clean_double_str(alpha)};
        return {"tif", "hint", "mrg", "dyn",
            clean_double_str(alpha), clean_double_str(beta)};

    default:
        throw runtime_error("Unknown method index: " + to_string(method_index));
    }
}


void IdxSchemaEncoder::apply_template_variants(IdxSchema &decoded, const IdxSchema &tmpl)
{
    auto copy_terminal_variant = [](IdxSchema &target, const IdxSchema &source)
    {
        if (source.method.empty() || target.method.empty()
            || source.method[0] != target.method[0])
            return false;

        if (source.method[0] == "tif"
            && source.method.size() > 2 && target.method.size() > 2
            && source.method[1] == "slicing"
            && target.method[1] == "slicing")
        {
            target.method[2] = source.method[2];
            return true;
        }

        if (source.method[0] == "irhint"
            && source.method.size() > 1 && target.method.size() > 1)
        {
            target.method[1] = source.method[1];
            return true;
        }

        return false;
    };

    // Copy the variant letter (position 2 for tif;slicing, position 1 for irhint)
    // from the template method into the decoded method, leaving all other tokens
    // (type, mode, params) untouched.
    bool variant_applied = copy_terminal_variant(decoded, tmpl);

    if (!variant_applied && tmpl.method.empty() && !tmpl.fanout.empty()
        && !decoded.method.empty() && decoded.fanout.empty())
    {
        for (const auto& child : tmpl.fanout)
        {
            if (copy_terminal_variant(decoded, child))
                break;
        }
    }

    // Recurse into fanout children (same positional order is guaranteed since
    // the decoded schema is structurally derived from the template).
    for (size_t i = 0; i < decoded.fanout.size() && i < tmpl.fanout.size(); ++i)
        apply_template_variants(decoded.fanout[i], tmpl.fanout[i]);
}
