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
    ./nis <command> [--config:<dir>]

OPTIONS
    --config:<dir>              Configuration directory (default: ./config)

COMMANDS
    help                        Open manual
    
    query [<idx> [<obj> [<qry>]]]
        Run queries with specified index, objects, and queries
        Omit <idx>, <obj>, and <qry> to generate random indexes and data
        Use ? for <idx> to construct a random index
        Use ! for <idx> to construct a workload-optimized index
        Use | as a separator to specify multiple indexes or query files

    update <idx> <obj> <obj2>
        Update index <idx> built on <obj> with new objects in <obj2>

    delete <idx> <obj> <obj2>
        Delete objects in <obj2> from index <idx> built on <obj>

    analyze <obj> [<qry>]       Analyze objects [and queries]

    generate-Q <obj>            Generate random queries for <obj>
    generate-O                  Generate random objects
    generate-OQ                 Generate random objects and queries
    generate-O-stats            Generate random object statistics

    learn                       Train the cost model

    predict <idx> <obj> [<qry>] Predict the performance

    synthesize <obj> [<qry>]    Synthesize a workload-optimized index

    score <top>:[<obj>]|[<qry>] Emit scores (top: skyline or complete)

    logs [<num>]                Display last <num> lines from log files

    config                      Edit the configuration

    setup                       Set up neural network environment

    clean <artifacts>           Clean specified artifacts (comma-separated)

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
                cmd.cfg_dir = arg.substr(11);
            else
                args.push_back(arg);
        }

        if (args.empty())
        {
            cout << usage;
            return 1;
        }

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
        else if (args[0] == "query" && args.size() <= 4)
        {
            cmd.query = true;

            if (args.size() >= 2)
            {
                cmd.idxschema = args[1];
                
                if (args.size() >= 3)
                {
                    cmd.file_O = args[2];
        
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

            if (cmd.idxschema == "?")
                cmd.idxschema = "";
        }
        else if (args[0] == "update" && args.size() == 4)
        {
            cmd.update = true;
            cmd.idxschema = args[1];
            cmd.file_O = args[2];
            cmd.file_O2 = args[3];
        }
        else if (args[0] == "delete" && args.size() == 4)
        {
            cmd.remove = true;
            cmd.idxschema = args[1];
            cmd.file_O = args[2];
            cmd.file_O2 = args[3];
        }
        else if (args[0] == "generate-O-stats" && args.size() == 1)
        {
            cmd.gen_Os = true;
        }
        else if (args[0] == "generate-O" && args.size() == 1)
        {
            cmd.gen_O = true;
        }
        else if (args[0] == "generate-Q" && args.size() == 2)
        {
            cmd.gen_Q = true;
            cmd.file_O = args[1];
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
            && (args.size() == 2 || args.size() == 3))
        {
            cmd.synth = true;
            cmd.file_O = args[1];
            if (args.size() == 3)
                cmd.file_Q = args[2];
            else
                cmd.gen_Q = true;
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
        else if (args[0] == "config" && args.size() == 1)
        {
            cmd.config = true;
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
