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

#ifndef _RESULTVALIDATOR_H_
#define _RESULTVALIDATOR_H_

#include "../containers/relations.h"
#include "../containers/temporal_inverted_file.h"
#include "../structure/framework.h"
#include "../learning/stats.h"
#include "../utils/persistence.h"
#include "../utils/logging.h"
#include "../utils/indexevaluator.h"
#include <vector>
#include <memory>

using namespace std;

class ResultValidator : public IndexEvaluator
{
public:
    ResultValidator(
        const IRelation& O,
        const vector<tuple<string,vector<RangeIRQuery>>>& Q,
        const OStats& Ostats,
        const bool write_O,
        const bool write_Q);

    void run(IRIndex* idx, const iStats& Istats, const double& construction) override;

private:
    const bool write_O;
    const bool write_Q;
    
    vector<vector<RelationId>> get_reference_results();
    vector<vector<RelationId>> get_test_results(
        IRIndex* test_index);

    void compare_and_report_results(
        const vector<vector<RelationId>>& reference_results,
        const vector<vector<RelationId>>& test_results);
};

#endif // _RESULTVALIDATOR_H_
