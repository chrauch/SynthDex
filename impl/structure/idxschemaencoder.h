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

#ifndef _IDXSCHEMAENCODER_H_
#define _IDXSCHEMAENCODER_H_

#include "idxschema.h"
#include <vector>
#include <utility>
#include <string>
#include <map>

using namespace std;

class IdxSchemaEncoder
{
public:
    static vector<double> encode(const IdxSchema &idxsch);
    static IdxSchema decode(const vector<double> &encoding);
    static IdxSchema sanitize(const IdxSchema &idxsch);

    // Method encoding constants
    static constexpr int METHOD_ONEHOT_SIZE = 17;
    static constexpr int METHOD_PARAM_COUNT = 2;
    
    // Method indices in one-hot encoding
    enum MethodIndex
    {
        TIF_BASIC = 0,

        TIF_SLICING_STATIC = 2,
        TIF_SLICING_DYN = 3,

        TIF_SHARDING_STATIC = 5,
        TIF_SHARDING_DYN = 6,

        TIF_HINT_DESC_STATIC = 8,
        TIF_HINT_DESC_DYN = 9,

        TIF_HINT_MRG_STATIC = 11,
        TIF_HINT_MRG_DYN = 12,

        IRHINT_PERF = 16
    };

    // Fanout type encoding constants
    static constexpr int FANOUT_TYPE_ONEHOT_SIZE = 5;
    
    // Fanout type indices in one-hot encoding
    enum FanoutTypeIndex
    {
        FANOUT_REFINE_ELEMFREQ = 0,
        FANOUT_SPLIT_TEMPORAL = 1,
        FANOUT_SLICE_TEMPORAL = 2,
        FANOUT_HYBRID_MOVEOUT_REFINE = 3
    };

private:
    static vector<double> encode_x(const IdxSchema &idxsch);
    static vector<double> encode_leaf(const IdxSchema &idxsch);
    static vector<double> encode_fanout(const IdxSchema &idxsch);
    static vector<double> encode_range(const string &range);
    static vector<double> encode_method(const vector<string> &method);
    static vector<double> encode_fanout_type(const IdxSchema &idxsch);
    static pair<IdxSchema, size_t> decode_x(const vector<double> &encoding, size_t offset);
    static vector<string> decode_method(int method_index, double p1, double p2);
    static int decode_fanout_type(const vector<double> &encoding, size_t offset, IdxSchema &schema);
};

#endif // _IDXSCHEMAENCODER_H_
