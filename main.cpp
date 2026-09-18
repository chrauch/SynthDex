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

#include "utils/global.h"
#include "utils/controller.h"
#include "utils/logging.h"

#include <iostream>
#include <string>
#include <vector>
#include <stdexcept>

using namespace std;


const string usage = R"(
----------------- Neural Index Synthesis -----------------
Toward Automated Adaptive Indexing via Learned Cost Models


USAGE
    ./nis <command> [--config:<dir>] [--stage:<stage>]

OPTIONS
    --config:<dir>              Configuration directory (default: ./config)
    --stage:<stage>             Stage to run (overwrites configuration)

COMMANDS
    help                        Open manual
    
    query [<idx> [<obj> [<qry>]]]
        Run queries with specified index, objects, and queries
        Omit <idx>, <obj>, and <qry> to generate random indexes and data
        Use ? for <obj> to generate random objects but not queries
        Use ?[:groups] for <idx> to construct a random index
        Use ![:groups] for <idx> to construct a workload-optimized index
        Groups are comma-separated design-space groups (e.g. ?:m,d or !:dt)
        Use | as a separator to specify multiple indexes or query files

    compare <idx> <obj> <qry>
        Compare indexes on random objects
        Use ? for <obj> to generate random objects

    update <idx> <obj> <obj2>
        Update index <idx> built on <obj> with new objects in <obj2>

    delete <idx> <obj> <obj2>
        Delete objects in <obj2> from index <idx> built on <obj>

    softdelete <idx> <obj> <obj2>
        Soft-delete objects in <obj2> from index <idx> built on <obj>
        (replaces IDs with tombstone -1)

    analyze <obj> [<qry>]       Analyze objects [and queries]

    generate-Q <obj> [<pat>]    Generate random queries for <obj>
                                <pat> overrides patterns-active
                                (e.g. '4RND+1BASE')
    generate-O                  Generate random objects
    generate-OQ                 Generate random objects and queries
    generate-O-stats            Generate random object statistics

    convert                     Convert statistics to training format

    learn                       Train the cost model

    predict <idx> <obj> [<qry>] Predict the performance

    synthesize ![:groups] <obj> [<qry>]
                                Synthesize a workload-optimized index
                                Groups are comma-separated design-space groups

    score [opr:][<mod>:][<fil>] Emit scores
                                <opr> operation: query (default), update, 
                                                 delete, softdelete
                                <mod> mode: skyline (default) or complete
                                <fil> filter: substring matched against O, Q

    lcm-accuracy [?[:groups]]   Measure LCM prediction accuracy
                                Generates random O, Q, I combinations,
                                runs actual queries and predictions

    query-noise [?[:groups]]    Measure query execution noise
                                Generates one random O, Q, I and repeats
                                the same query set to quantify throughput
                                fluctuation across runs

    idx-ranking [?[:groups]]    Check LCM ranking correctness
                                Generates random O, Q, runs grid search,
                                selects spread-out configs, executes each
                                to verify predicted vs. actual ranking

    synth-tuning [?[:groups]]   Calibrate synthesis hyper-parameters
                                Runs every setting under tuning.exps against
                                the configured synthesis as reference and
                                reports probes, time and degradation

    logs [<num>]                Display last <num> lines from log files

    config <part>               Edit the configuration (e.g., core)

    setup                       Set up neural network environment

    clean <artifacts>           Clean specified artifacts (comma-separated)

    sequence <flow>             Run a predefined sequence of configured stages

)";

int main(int argc, char **argv)
{
    Cmd cmd;
    bool execute = false;

    try
    {
        if (argc < 2) { cout << usage; return 1; }

        vector<string> args;
        for (int i = 1; i < argc; ++i)
        {
            string arg(argv[i]);
            if (arg.rfind("--config:", 0) == 0)
                cmd.cfg_dir = arg.substr(9);
            else if (arg.rfind("--stage:", 0) == 0)
                cmd.stage = arg.substr(8);
            else if (arg == "--stage" && i + 1 < argc)
                cmd.stage = argv[++i];
            else
                args.push_back(arg);
        }

        if (args.empty())
        {
            cout << usage;
            return 1;
        }

        // Parse "?:m,d" or "!:dt,m" into (base_char, groups_vector)
        auto parse_mode = [](const string& s) -> pair<string, vector<string>>
        {
            if (s.empty()) return {s, {}};
            char c = s[0];
            if (c != '?' && c != '!') return {s, {}};
            vector<string> groups;
            if (s.size() > 2 && s[1] == ':')
            {
                stringstream ss(s.substr(2));
                string g;
                while (getline(ss, g, ','))
                    if (!g.empty()) groups.push_back(g);
            }
            return {string(1, c), groups};
        };

        if (args[0] == "help")
        {
            cmd.help = true;
        }
        else if (args[0] == "clean" && (args.size() == 1 || args.size() == 2))
        {
            cmd.clean = args.size() > 1 ? args[1] : ".csv,.log,.dat,.qry";
        }
        else if (args[0] == "analyze" 
            && (args.size() == 2 || args.size() == 3))
        {
            cmd.analyze = true;
            cmd.file_O = args[1];
            if (args.size() == 3)
                cmd.file_Q = args[2];
        }
        else if (args[0] == "compare" && args.size() == 4)
        {
            cmd.compare = true;
            cmd.query = true;
            cmd.idxschema = args[1];
            cmd.file_O = args[2];
            if (cmd.file_O == "?")
            {
                cmd.file_O.clear();
                cmd.gen_O = true;
            }
            cmd.file_Q = args[3];
        }
        else if (args[0] == "query" && args.size() <= 4)
        {
            cmd.query = true;

            if (args.size() >= 2)
            {
                cmd.idxschema = args[1];
                
                if (args.size() >= 3)
                {
                    cmd.file_O = args[2];

                    if (cmd.file_O == "?")
                    {
                        cmd.file_O.clear();
                        cmd.gen_O = true;
                    }
        
                    if (args.size() == 4)
                        cmd.file_Q = args[3];
                    else
                        cmd.gen_Q = true;
                }
                else
                {
                    cmd.gen_O = true;
                    cmd.gen_Q = true;
                }
            }
            else
            {
                cmd.gen_O = true;
                cmd.gen_Q = true;
            }

            if (cmd.idxschema == "?" || cmd.idxschema.rfind("?:", 0) == 0)
            {
                auto [base, groups] = parse_mode(cmd.idxschema);
                cmd.idxschema = "";
                cmd.groups = groups;
            }
            // !:groups tokens (synthesis requests) are passed through as-is and
            // recognised by ExecutionRunner::execute() when splitting on '|'.
        }
        else if (args[0] == "update" && (args.size() == 4 || args.size() == 5))
        {
            cmd.update = true;
            cmd.idxschema = args[1];
            cmd.file_O = args[2];
            cmd.file_O2 = args[3];
            if (args.size() == 5) cmd.file_Q = args[4];
            if (cmd.idxschema == "?" || cmd.idxschema.rfind("?:", 0) == 0)
            {
                auto [base, groups] = parse_mode(cmd.idxschema);
                cmd.idxschema = "?";
                cmd.groups = groups;
            }
        }
        else if (args[0] == "delete" && (args.size() == 4 || args.size() == 5))
        {
            cmd.remove = true;
            cmd.idxschema = args[1];
            cmd.file_O = args[2];
            cmd.file_O2 = args[3];
            if (args.size() == 5) cmd.file_Q = args[4];
            if (cmd.idxschema == "?" || cmd.idxschema.rfind("?:", 0) == 0)
            {
                auto [base, groups] = parse_mode(cmd.idxschema);
                cmd.idxschema = "?";
                cmd.groups = groups;
            }
        }
        else if (args[0] == "softdelete" && (args.size() == 4 || args.size() == 5))
        {
            cmd.softdelete = true;
            cmd.idxschema = args[1];
            cmd.file_O = args[2];
            cmd.file_O2 = args[3];
            if (args.size() == 5) cmd.file_Q = args[4];
            if (cmd.idxschema == "?" || cmd.idxschema.rfind("?:", 0) == 0)
            {
                auto [base, groups] = parse_mode(cmd.idxschema);
                cmd.idxschema = "?";
                cmd.groups = groups;
            }
        }
        else if (args[0] == "generate-O-stats" && args.size() == 1)
        {
            cmd.gen_Os = true;
        }
        else if (args[0] == "generate-O" && args.size() == 1)
        {
            cmd.gen_O = true;
        }
        else if (args[0] == "generate-Q" && args.size() >= 2 && args.size() <= 3)
        {
            cmd.gen_Q = true;
            cmd.file_O = args[1];
            if (args.size() == 3)
            {
                string pat = args[2];
                if (!pat.empty() && pat[0] == '%') pat = pat.substr(1);
                cmd.q_pattern = pat;
            }
        }
        else if (args[0] == "generate-OQ" && args.size() == 1)
        {
            cmd.gen_O = true;
            cmd.gen_Q = true;
        }
        else if (args[0] == "learn" && args.size() == 1)
        {
            cmd.learn = true;
        }
        else if (args[0] == "convert" && args.size() == 1)
        {
            cmd.convert = true;
        }
        else if (args[0] == "setup" && args.size() == 1)
        {
            cmd.setup = true;
        }
        else if (args[0] == "predict" 
            && (args.size() == 4 || args.size() == 3))
        {
            cmd.predict   = true;
            cmd.idxschema = args[1];
            cmd.file_O    = args[2];
            if (args.size() == 4)
                cmd.file_Q    = args[3];
            else
                cmd.gen_Q = true;
        }
        else if (args[0] == "synthesize" 
            && (args.size() == 3 || args.size() == 4)
            && (args[1] == "!" || args[1].rfind("!:", 0) == 0))
        {
            cmd.synth = true;
            auto [base, groups] = parse_mode(args[1]);
            cmd.groups = groups;
            cmd.file_O = args[2];
            if (args.size() == 4)
                cmd.file_Q = args[3];
            else
                cmd.gen_Q = true;
        }
        else if (args[0] == "lcm-accuracy" && args.size() <= 2)
        {
            cmd.accuracy = true;
            if (args.size() == 2)
            {
                auto [base, groups] = parse_mode(args[1]);
                cmd.groups = groups;
            }
        }
        else if (args[0] == "query-noise" && args.size() <= 2)
        {
            cmd.noise = true;
            if (args.size() == 2)
            {
                auto [base, groups] = parse_mode(args[1]);
                cmd.groups = groups;
            }
        }
        else if (args[0] == "idx-ranking" && args.size() <= 2)
        {
            cmd.ranking = true;
            if (args.size() == 2)
            {
                auto [base, groups] = parse_mode(args[1]);
                cmd.groups = groups;
            }
        }
        else if (args[0] == "synth-tuning" && args.size() <= 2)
        {
            cmd.tuning = true;
            if (args.size() == 2)
            {
                auto [base, groups] = parse_mode(args[1]);
                cmd.groups = groups;
            }
        }
        else if (args[0] == "score" && (args.size() == 1 || args.size() == 2))
        {
            cmd.score = true;
            if (args.size() == 2)
                cmd.filter = args[1];
        }
        else if (args[0] == "logs" && (args.size() == 1 || args.size() == 2))
        {
            cmd.logs = true;
            if (args.size() == 2)
                cmd.logs_num = stoi(args[1]);
        }
        else if (args[0] == "config" && args.size() == 2)
        {
            cmd.config = true;
            cmd.config_part = args[1];
        }
        else if (args[0] == "sequence" && args.size() == 2)
        {
            cmd.seq = true;
            cmd.seq_flow = args[1];
        }
        else
        {
            throw invalid_argument("Unknown command: " + args[0]);
        }

        execute = true;
    }
    catch (const exception &e)
    {
        cerr << "ERROR: " << e.what() << endl
             << endl << usage << endl;
        return 1;
    }

    if (!execute)
        return 1;

    try
    {
        Cfg::singleton = new Cfg(cmd.cfg_dir);
        if (!cmd.stage.empty())
        {
            stringstream ss(cmd.stage);
            string s;
            while (getline(ss, s, ','))
                if (!s.empty())
                    Cfg::singleton->apply_stage(s);
        }
        Controller(cmd).start();
    }
    catch (const exception &e)
    {
        Log::w(0, e.what());

        delete Cfg::singleton;
        Cfg::singleton = nullptr;
        return 1;
    }
    
    delete Cfg::singleton;
    Cfg::singleton = nullptr;

    return 0;
}
