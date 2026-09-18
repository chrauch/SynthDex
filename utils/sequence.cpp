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

#include "sequence.h"
#include "cfg.h"
#include "logging.h"
#include "processexec.h"

#include <stdexcept>

using namespace std;


Sequence::Sequence(const string &flow) : flow(flow) {}


void Sequence::run()
{
    const string base = "sequences.flows." + this->flow;

    const vector<string> cmds =
        Cfg::get<vector<string>>(base + ".steps");

    const int repeat =
        Cfg::get<int>(base + ".repeat");

    const string repeat_str = (repeat < 0) ? "endless" : to_string(repeat);

    Log::w(0, "Sequence", this->flow
        + " (" + to_string(cmds.size()) + " steps, repeat=" + repeat_str + ")");

    for (int iteration = 0; repeat < 0 || iteration < repeat; ++iteration)
    {
        if (repeat != 1)
            Log::w(0, "Sequence", this->flow
                + " iteration " + (repeat < 0 ? to_string(iteration + 1) : to_string(iteration + 1) + "/" + to_string(repeat)));

        for (size_t i = 0; i < cmds.size(); ++i)
        {
            const string &cmd = cmds[i];

            Log::w(0, "Step " + to_string(i + 1) + "/" + to_string(cmds.size()), cmd);

            ProcessExec::run(cmd, 0, false, true);
        }
    }
}
