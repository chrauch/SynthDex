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

#ifndef _STATISTICS_H_
#define _STATISTICS_H_


#include "../utils/global.h"
#include "../containers/relations.h"
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <iostream>
#include <limits>
#include <vector>
#include <string>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <chrono>
#include <unistd.h>
#include <tuple>
#include <map>
using namespace std;


struct pStats
{
	double throughput = 0.0;
	double throughput_log = 0.0;
	double size = 0.0;
	double size_log = 0.0;
	RecordId result_cnt = 0;
	double selectivity_ratio_log = 0.0;
	int result_xor = 0;
};


struct qStats
{
	string query = "";
	double start_rel = 0.0;
	double end_rel = 0.0;
	double extent_rel_log = 0.0;
	int    elem_count = 0;
	vector<double> element_rank_logs;
};


struct iStats
{
	string category = "";
	string params;
	vector<double> encoding;
};


struct OElemStats
{
	int lo_rank = 0;
	int hi_rank = 0;

	unsigned int max_freq = 0;
	double dict_ratio = 0.0;
	double max_post_ratio = 0.0;
	double rel_drop = 0.0;
	
	double lo_rank_log = 0.0;
	double hi_rank_log = 0.0;
	double max_freq_log = 0.0;
	double dict_ratio_log = 0.0;
	double max_post_ratio_log = 0.0;
	double rel_drop_log = 0.0;
};


struct OExtStats
{
	double min_len = 0.0;
	double max_len = 0.0;
	double min_len_log = 0.0;
	double max_len_log = 0.0;

	double avg_elem_cnt = 0.0;
	double dev_elem_cnt = 0.0;
	double avg_elem_cnt_log = 0.0;
	double dev_elem_cnt_log = 0.0;
};


struct OStats
{
	string name = "";
	string params = "";

	RecordId card = 0;
	double card_log = 0.0;

	Timestamp domain;
	double domain_log = 0.0;

	ElementId dict = 0;
	double dict_log = 0.0;

	vector<OElemStats> elements;
	vector<OExtStats> extents;

	double est_bytes = 0.0;
	double bytes = 0.0;
};


#endif // _STATISTICS_H_