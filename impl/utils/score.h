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


class Score
{
public:
    Score();

    void process_scores(const string &filter = "");

private:
    vector<ScoreEntry> load_scores(const string &file_path);
    vector<ScoreEntry> filter_scores(const vector<ScoreEntry> &scores, const string &filter);
    void print_scores(const vector<ScoreEntry> &scores);
    void create_diagram_script(const vector<ScoreEntry> &scores);
    void create_text_table(const vector<ScoreEntry> &scores);
    void fill_template(ScoreEntry &entry);
    string extract_method_template(const string &method);
    vector<ScoreEntry> apply_skyline(const vector<ScoreEntry> &scores);
    void compute_relative_scores(vector<ScoreEntry> &scores);
    
    // Grouping and per-group processing
    map<pair<string, string>, vector<ScoreEntry>> group_by_objects_queries(const vector<ScoreEntry> &scores);
    vector<ScoreEntry> process_group(const vector<ScoreEntry> &group, bool apply_skyline_filter);
};

#endif // _SCORE_H_
