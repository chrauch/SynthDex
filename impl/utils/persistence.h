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

#ifndef _PERSISTENCE_H_
#define _PERSISTENCE_H_


#include "../utils/global.h"
#include "../learning/stats.h"

using namespace std;


class Persistence
{
public:

    static void write_OQIP_stats_csv(
        const OStats &Os,
        const vector<qStats> &Qs,
        const iStats &is,
        const vector<pStats> &Ps,
        const bool all);

    static string write_OQI_stats_csv(
        const OStats &Os,
        const vector<qStats> &Qs,
        const iStats &is,
        const string &name);

    static string write_OQ_stats_csv(
        const OStats &Os,
        const vector<qStats> &Qs,
        const string &name);

    static string write_I_stats_csv(
        const vector<iStats> &is);

    static void write_Q_stats_csv(
        const vector<qStats> &Qs);

    static void write_O_stats(
        const OStats &Os,
        const int &id);

    static void write_O_stats_csv(
        const OStats &Os,
        const bool &all);

    static void write_O_stats_json(
        const OStats &Os,
        const int &id,
        const bool &all);

    static void write_score_csv(
        const OStats &Os,
        const string &Q_workload,
        const size_t &xor_result,
        const size_t &size,
        const double &construction,
        const double &throughput,
        const iStats &is);

    static void write_O_dat(
        const IRelation &O,
        const OStats &Os);

    static void write_Q_dat(
        const tuple<string,vector<RangeIRQuery>> &Q,
        const OStats &Os);

    static IRelation read_O_dat(
        const string &file);

    static RelationId read_Oids_dat(
        const string &file);

    static vector<tuple<string,vector<RangeIRQuery>>> read_q_dat(
        const string &file);

    static OStats read_O_stats_json(
        const string &file);

    static vector<OStats> read_O_stats_jsons(
        const string &dir,
        const vector<string> &files);

    static vector<tuple<vector<string>,vector<double>>> read_I_synthesis_csv(
        const string &file);

    static vector<tuple<vector<string>,vector<double>>> read_score_csv(
        const string &file);

    static string timestamp(const bool time, const string& attr);

    static string compose_name(
        const OStats &Os,
        const tuple<string,vector<RangeIRQuery>> &Q);

    static void clean(const vector<string>& artifacts);

private:
    static tuple<string, ofstream, bool> open_csv_file(
        const optional<string> &name,
        const string &attr,
        const bool &single,
        const bool &limit_size,
        const bool &timestamp);

    static string write_csv(
        const optional<string> &name,
        const bool &all,
        const bool &single,
        const optional<OStats> &O = nullopt,
        const optional<vector<qStats>> &Q = nullopt,
        const optional<iStats> &i = nullopt,
        const optional<vector<pStats>> &P = nullopt);

    inline static string sanitize(const string& csv_line);

    static map<string, int> write_counters;
};


#endif // _PERSISTENCE_H_