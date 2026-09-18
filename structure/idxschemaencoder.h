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

    // Copy terminal variant letters ("a"/"b"/"c") from a template schema into a
    // decoded schema.  IdxSchemaEncoder::encode/decode only captures continuous
    // parameters; the variant is a discrete structural choice fixed by the design
    // space template and is therefore never encoded.  Call this after decode() to
    // restore the correct variant.
    static void apply_template_variants(IdxSchema &decoded, const IdxSchema &tmpl);

    // Encoding layout: 4 one-hot tIF method + alpha + beta + boundary + irhint-levels = 8
    static constexpr int ENCODING_SIZE = 8;
    static constexpr int METHOD_ONEHOT_SIZE = 4;

    // Positions in the encoding vector
    static constexpr int POS_TIF_BASIC = 0;
    static constexpr int POS_TIF_SLICING = 1;
    static constexpr int POS_TIF_SHARDING = 2;
    static constexpr int POS_TIF_HINT = 3;
    static constexpr int POS_ALPHA = 4;
    static constexpr int POS_BETA = 5;
    static constexpr int POS_BOUNDARY = 6;
    static constexpr int POS_IRHINT_LEVELS = 7;

    // Method indices in one-hot encoding
    enum MethodIndex
    {
        TIF_BASIC = 0,
        TIF_SLICING_DYN = 1,
        TIF_SHARDING_DYN = 2,
        TIF_HINT_MRG_DYN = 3
    };

private:
    static int identify_tif_method(const vector<string> &method);
    static pair<double, double> extract_params(const vector<string> &method);
    static vector<string> decode_tif_method(int method_index, double alpha, double beta);
    static double parse_range_boundary(const string &range);
};

#endif // _IDXSCHEMAENCODER_H_
