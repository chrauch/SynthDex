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

#ifndef _IGEN_H_
#define _IGEN_H_

#include "../utils/global.h"
#include "../containers/relations.h"
#include "../structure/synthdex.h"
#include "../learning/stats.h"
#include "../learning/statsserializer.h"
#include "../structure/idxschema.h"
#include "randomgen.h"
#include <optional>

using namespace std;

class IGen
{
public:
    IGen();
    IdxSchema construct_I(const optional<string> &pattern);
    vector<tuple<IdxSchema,IdxSchema>> compute_design_space();
    set<string> augment_closure(string &in);
    vector<IdxSchema> compute_closure(IdxSchema &in);


private:
    void change(IdxSchema &idxschema);
    void sanitize(IdxSchema &idxschema, const optional<bool> &min);
    void sanitize_split(IdxSchema &idxschema, const optional<bool> &min);
    void sanitize_slice(IdxSchema &idxschema, const optional<bool> &min);
    void sanitize_refine(IdxSchema &idxschema, const optional<bool> &min);
    void process_previous_references(IdxSchema &idxschema);

    string select(string &values_str);
    string select_from_pattern(const string &pattern_content);
    tuple<string,string> min_max(string &val_str);
    tuple<string,string> min_max_from_pattern(const string &pattern_content);

    RandomGen randomgen;
    vector<IdxSchema> idxschemas;
};

#endif // _IGEN_H_
