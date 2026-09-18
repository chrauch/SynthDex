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

#pragma once

#ifndef _PARSING_H_
#define _PARSING_H_

#include <sstream>
#include <map>
#include <string>
#include <cmath>
#include <stdexcept>
#include <algorithm>
using namespace std;


class Parsing {
public:
    // Trim whitespace
    static string trim(const string& s);

    // Expect and remove a literal
    static void expect(istream& in, char expected);

    // Parse a quoted string
    static string parse_string(istream& in);

    // Skip to next non-space
    static void skip_ws(istream& in);

    // Parse a key-value pair (expecting `"key": value`)
    static string parse_key(istream& in);
};


// Parse a "min-max" range string (e.g. "0.6-0.9") into integer boundaries.
// min_v/max_v are clamped against the caller-supplied initial values.
// log_scale=true:  values are fractions of max_log (log10 scale).
// log_scale=false: values are fractions of 10^max_log (linear scale).
inline void parse_min_max(
    const string &range, int &min_v, int &max_v,
    const double &max_log, const bool &log_scale)
{
    size_t pos = range.find('-');
    if (pos != string::npos)
    {
        double min_rel = stod(range.substr(0, pos));
        double max_rel = stod(range.substr(pos + 1));

        if (log_scale)
        {
            int min_calc = min_rel == 0 ? 0 : (int)ceil(pow(10, min_rel * max_log));
            int max_calc = (int)ceil(pow(10, max_rel * max_log));

            min_v = max(min_v, min_calc);
            max_v = min(max_v, max_calc);
        }
        else
        {
            int maxval = (int)pow(10, max_log);
            min_v = (int)ceil(min_rel * maxval);
            max_v = (int)ceil(max_rel * maxval);
        }
    }
    else
        throw runtime_error("Wrong min-max format");
}


#endif // _PARSING_H_
