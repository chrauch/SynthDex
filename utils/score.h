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

#ifndef _SCORE_H_
#define _SCORE_H_

#include <string>
#include <vector>
#include <tuple>
#include <map>

using namespace std;


struct ScoreEntry
{
    string objects;
    string queries;
    string workload_pattern;
    double xor_result;
    double size_mb;
    double construction_s;
    double throughput_qps;
    double throughput_qps_relative;
    double size_mb_relative;
    double construction_s_relative;
    string category;
    string schema;
    string templatex;
};


// Score entry for update / delete / softdelete operations.
struct MutationScoreEntry
{
    string objects;
    string work_file;
    double objs_affected_pct          = 0.0;
    double objs_before_cnt            = 0.0;
    double objs_after_cnt             = 0.0;
    double size_before_mb             = 0.0;
    double size_after_mb              = 0.0;
    double construction_s             = 0.0;
    double mutation_s                 = 0.0;
    double mutation_s_relative        = 0.0;
    double size_before_mb_relative    = 0.0;
    double size_after_mb_relative     = 0.0;
    double construction_s_relative    = 0.0;
    string category;
    string schema;
    string templatex;
};


class Score
{
public:
    Score();

    void process_scores(const string &filter = "");

private:
    // --- query score path ---
    vector<ScoreEntry> load_scores(const string &file_path);
    vector<ScoreEntry> filter_scores(const vector<ScoreEntry> &scores, const string &filter);
    void print_scores(const vector<ScoreEntry> &scores);
    void create_diagram_script(const vector<ScoreEntry> &scores);
    void create_text_table(const vector<ScoreEntry> &scores);
    void fill_template(ScoreEntry &entry);
    void fill_template(MutationScoreEntry &entry);
    string extract_method_template(const string &method);
    vector<ScoreEntry> apply_skyline(const vector<ScoreEntry> &scores);
    void compute_relative_scores(vector<ScoreEntry> &scores);
    map<pair<string, string>, vector<ScoreEntry>> group_by_objects_queries(const vector<ScoreEntry> &scores);
    vector<ScoreEntry> process_group(const vector<ScoreEntry> &group, bool apply_skyline_filter);

    void process_query_scores(const vector<string> &files, const string &mode, const string &filter);

    // --- mutation score path ---
    vector<MutationScoreEntry> load_mutation_scores(const string &file_path);
    vector<MutationScoreEntry> filter_mutation_scores(const vector<MutationScoreEntry> &scores, const string &filter);
    void print_mutation_scores(const vector<MutationScoreEntry> &scores, const string &op);
    map<pair<string, string>, vector<MutationScoreEntry>> group_mutation_scores(const vector<MutationScoreEntry> &scores);
    vector<MutationScoreEntry> process_mutation_group(const vector<MutationScoreEntry> &group, bool apply_skyline_filter);

    void process_mutation_scores(const vector<string> &files, const string &op, const string &mode, const string &filter);
};

#endif // _SCORE_H_
