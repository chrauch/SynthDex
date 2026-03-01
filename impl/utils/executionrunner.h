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

#ifndef _EXECUTIONRUNNER_H_
#define _EXECUTIONRUNNER_H_

#include "../containers/relations.h"
#include "../generation/igen.h"
#include "../learning/stats.h"
#include "../learning/statscomp.h"
#include "../learning/statsserializer.h"
#include "../structure/synthdex.h"
#include "../structure/synthdexopt.h"
#include "../structure/idxschema.h"
#include "../structure/idxschemaserializer.h"
#include "../utils/persistence.h"
#include "../utils/indexevaluator.h"
#include "../utils/resultvalidator.h"
#include "../utils/performanceanalyzer.h"
#include "../utils/processexec.h"
#include "../utils/synthesisrunner.h"
#include <string>
#include <vector>
#include <set>
#include <optional>
#include <numeric>
#include <iomanip>
#include <sstream>

using namespace std;

struct Cmd;


class ExecutionRunner
{
public:
    ExecutionRunner(
        const Cmd& cmd,
        const IRelation& O,
        const vector<tuple<string,vector<RangeIRQuery>>>& Q,
        const OStats& Ostats,
        StatsComp& statscomp);

    int execute();

    static void update(const Cmd& cmd, StatsComp& statscomp);
    static void remove(const Cmd& cmd, StatsComp& statscomp);
    static void softdelete(const Cmd& cmd, StatsComp& statscomp);
    static void synthesize(const Cmd& cmd,
        const vector<tuple<string,vector<RangeIRQuery>>>& Q,
        const OStats& Ostats, StatsComp& statscomp);

private:
    void execute_synthetic();
    int execute_userdefined();

    double construct(const IdxSchema& schema);
    void predict();

    const Cmd& cmd;
    const IRelation& O;
    const vector<tuple<string,vector<RangeIRQuery>>>& Q;
    const OStats& Ostats;
    StatsComp& statscomp;

    IdxSchema idxschema;
    iStats Istats;
    IRIndex* idx;
};

#endif // _EXECUTIONRUNNER_H_
